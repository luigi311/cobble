//! Weather provider: location → Open-Meteo forecast → Pebble watch.
//!
//! * **Open-Meteo** (free HTTPS API, no key): current, seven days, and hourly forecasts.
//! * **Connection-gated**: only fetches when the watch is connected.
//! * **Refresh timer**: fetches immediately on connect, then every 3 hours.

use std::time::Duration;

use libpebble_ble::{WeatherDailyForecast, WeatherForecast, WeatherHourlyForecast};
use serde_json::Value;
use tracing::{debug, warn};

use crate::{http, location, service::CobbleDaemon};

/// Deterministic 16-byte location key so the watch weather app can identify
/// and update the same location entry across restarts.
fn location_key() -> [u8; 16] {
    let host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_default();
    let mut key = [0u8; 16];
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in host.bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    key[..8].copy_from_slice(&hash.to_le_bytes());
    key[8..].copy_from_slice(b"weather\0");
    key
}

/// Refresh after connection, effective temperature-unit changes, and every three hours.
pub async fn run_weather(daemon: CobbleDaemon) {
    let key = location_key();
    let mut rx = daemon.watch_connection();
    loop {
        // Wait until connected.
        while !*rx.borrow() {
            let _ = rx.changed().await;
        }
        let Ok(pebble) = daemon.require_pebble() else {
            let _ = rx.changed().await;
            continue;
        };
        // Subscribe after startup sync: initial preferences are already represented
        // by the first fetch, while later effective unit changes need an immediate update.
        let mut units = pebble.watch_weather_units();
        if let Err(e) = refresh(&daemon, key).await {
            warn!("weather: refresh failed: {e}");
        }
        // Unit changes wake the same loop without waiting for the next periodic refresh.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(3 * 3600)) => {}
            _ = rx.changed() => {}
            _ = units.changed() => {}
        }
        // Loop back: the `while` above re-verifies connection state
        // before calling refresh again.
    }
}

/// Fetch a full forecast and deliver it through the connection that requested it.
async fn refresh(daemon: &CobbleDaemon, key: [u8; 16]) -> anyhow::Result<()> {
    let pebble = daemon.require_pebble()?;
    let (lat, lon, location_name) = location::get_location(daemon.db()).await?;
    debug!("weather: {lat:.4},{lon:.4} ({location_name})");
    let forecast = fetch_forecast(lat, lon, &location_name).await?;
    pebble.push_weather_forecast(&key, &forecast).await?;
    Ok(())
}

// ── Open-Meteo ────────────────────────────────────────────────────────────

/// Request local calendar days, UTC timestamps, Celsius temperatures, and mph wind.
fn forecast_url(lat: f64, lon: f64) -> String {
    format!(
        "https://api.open-meteo.com/v1/forecast\
         ?latitude={lat:.4}&longitude={lon:.4}\
         &current=temperature_2m,weather_code,apparent_temperature,wind_speed_10m,wind_direction_10m\
         &daily=temperature_2m_max,temperature_2m_min,weather_code,apparent_temperature_max,uv_index_max,precipitation_probability_max,wind_speed_10m_max,wind_direction_10m_dominant,relative_humidity_2m_mean,visibility_min,precipitation_sum,sunrise,sunset\
         &hourly=temperature_2m,weather_code,uv_index\
         &forecast_days=7&timezone=auto&timeformat=unixtime\
         &temperature_unit=celsius&wind_speed_unit=mph&precipitation_unit=mm"
    )
}

/// Fetch Open-Meteo weather without inventing clear skies or zero temperatures on missing data.
async fn fetch_forecast(lat: f64, lon: f64, name: &str) -> anyhow::Result<WeatherForecast> {
    let body = http::http_get(&forecast_url(lat, lon)).await?;
    let json: Value =
        serde_json::from_str(&body).map_err(|e| anyhow::anyhow!("Open-Meteo parse: {e}"))?;
    parse_forecast(&json, lat, lon, name)
}

/// Map provider arrays by timestamp so missing hours cannot shift the watch's local clock dial.
fn parse_forecast(json: &Value, lat: f64, lon: f64, name: &str) -> anyhow::Result<WeatherForecast> {
    let current = &json["current"];
    let daily = &json["daily"];
    let times = daily["time"]
        .as_array()
        .filter(|a| a.len() >= 2)
        .ok_or_else(|| anyhow::anyhow!("Open-Meteo: expected at least two local forecast days"))?;
    let current_temp = temp(&current["temperature_2m"])
        .ok_or_else(|| anyhow::anyhow!("Open-Meteo: missing current temperature"))?;
    let midnight = times[0]
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("Open-Meteo: invalid daily timestamp"))?;
    let tomorrow_midnight = times[1]
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("Open-Meteo: invalid tomorrow timestamp"))?;
    if tomorrow_midnight <= midnight {
        return Err(anyhow::anyhow!("Open-Meteo: expected 2 forecast days"));
    }
    let feels_like = temp(&current["apparent_temperature"]);
    let wind_speed = unsigned(&current["wind_speed_10m"]).and_then(|v| u16::try_from(v).ok());
    let wind_dir = unsigned(&current["wind_direction_10m"])
        .filter(|v| *v <= 360)
        .map(|v| v as u16);
    let mut days: Vec<_> = (0..times.len().min(7))
        .map(|i| WeatherDailyForecast {
            high_temp: temp(&daily["temperature_2m_max"][i]),
            low_temp: temp(&daily["temperature_2m_min"][i]),
            weather_type: weather_code(&daily["weather_code"][i]),
            precip_probability: percent(&daily["precipitation_probability_max"][i]),
            wind_speed_mph: unsigned(&daily["wind_speed_10m_max"][i])
                .and_then(|v| u16::try_from(v).ok()),
            uv_index_x10: uv(&daily["uv_index_max"][i]),
            feels_like: temp(&daily["apparent_temperature_max"][i]),
            wind_direction_deg: unsigned(&daily["wind_direction_10m_dominant"][i])
                .filter(|v| *v <= 360)
                .map(|v| v as u16),
            wmo_code: unsigned(&daily["weather_code"][i]).and_then(|v| u8::try_from(v).ok()),
            humidity_pct: percent(&daily["relative_humidity_2m_mean"][i]),
            visibility_m: unsigned(&daily["visibility_min"][i]),
            precip_sum_mm: unsigned(&daily["precipitation_sum"][i]),
            sunrise_utc: daily["sunrise"][i]
                .as_u64()
                .and_then(|v| u32::try_from(v).ok()),
            sunset_utc: daily["sunset"][i]
                .as_u64()
                .and_then(|v| u32::try_from(v).ok()),
        })
        .collect();
    // Day zero's current readings agree with the fixed today_* fields.
    days[0].feels_like = feels_like.or(days[0].feels_like);
    days[0].wind_speed_mph = wind_speed.or(days[0].wind_speed_mph);
    days[0].wind_direction_deg = wind_dir.or(days[0].wind_direction_deg);
    Ok(WeatherForecast {
        location_name: name.into(),
        forecast_short: wmo_description(
            unsigned(&current["weather_code"])
                .and_then(|v| u16::try_from(v).ok())
                .unwrap_or(u16::MAX),
        )
        .into(),
        current_temp,
        current_weather: weather_code(&current["weather_code"]),
        today_high: days[0].high_temp.unwrap_or(i16::MAX),
        today_low: days[0].low_temp.unwrap_or(i16::MAX),
        tomorrow_weather: days[1].weather_type,
        tomorrow_high: days[1].high_temp.unwrap_or(i16::MAX),
        tomorrow_low: days[1].low_temp.unwrap_or(i16::MAX),
        is_current_location: true,
        feels_like,
        latitude: Some(lat),
        longitude: Some(lon),
        uv_index_x10: days[0].uv_index_x10,
        precip_probability: days[0].precip_probability,
        wind_speed_mph: wind_speed,
        wind_direction_deg: wind_dir,
        utc_offset_minutes: json["utc_offset_seconds"]
            .as_i64()
            .filter(|v| v % 60 == 0)
            .and_then(|v| i16::try_from(v / 60).ok()),
        daily: days,
        today_hourly: hourly_forecast(&json["hourly"], midnight),
        tomorrow_hourly: hourly_forecast(&json["hourly"], tomorrow_midnight),
        sunrise_sunset_pins: true,
    })
}

/// Keep hour indices anchored to local midnight, filling gaps with unknown readings.
fn hourly_forecast(hourly: &Value, midnight: i64) -> Vec<WeatherHourlyForecast> {
    let mut hours = Vec::new();
    if let Some(times) = hourly["time"].as_array() {
        for (i, timestamp) in times.iter().enumerate() {
            let Some(delta) = timestamp.as_i64().and_then(|t| t.checked_sub(midnight)) else {
                continue;
            };
            if !(0..86400).contains(&delta) || delta % 3600 != 0 {
                continue;
            }
            let hour = (delta / 3600) as usize;
            hours.resize_with(hours.len().max(hour + 1), WeatherHourlyForecast::default);
            hours[hour] = WeatherHourlyForecast {
                temp: temp(&hourly["temperature_2m"][i]),
                weather_type: weather_code(&hourly["weather_code"][i]),
                uv_index_x10: uv(&hourly["uv_index"][i]),
            };
        }
    }
    hours
}

/// Round valid temperatures, reserving i16::MAX for the firmware's unknown sentinel.
fn temp(value: &Value) -> Option<i16> {
    value
        .as_f64()
        .map(f64::round)
        .filter(|v| v.is_finite() && *v >= i16::MIN as f64 && *v < i16::MAX as f64)
        .map(|v| v as i16)
}
/// Round a finite nonnegative provider metric without wrapping out-of-range values.
fn unsigned(value: &Value) -> Option<u32> {
    value
        .as_f64()
        .map(f64::round)
        .filter(|v| v.is_finite() && *v >= 0.0 && *v <= u32::MAX as f64)
        .map(|v| v as u32)
}
/// Accept only probability or humidity percentages within 0–100.
fn percent(value: &Value) -> Option<u8> {
    unsigned(value).filter(|v| *v <= 100).map(|v| v as u8)
}
/// Scale a finite, nonnegative UV index by ten while preserving missing readings.
fn uv(value: &Value) -> Option<u16> {
    value
        .as_f64()
        .map(|v| (v * 10.0).round())
        .filter(|v| v.is_finite() && *v >= 0.0 && *v < u16::MAX as f64)
        .map(|v| v as u16)
}
/// Preserve absent and unrecognized provider weather codes as the unknown icon.
fn weather_code(value: &Value) -> u8 {
    unsigned(value)
        .and_then(|v| u16::try_from(v).ok())
        .map_or(255, wmo_to_pebble)
}

/// Describe WMO weather without presenting an unknown reading as a forecast.
fn wmo_description(code: u16) -> &'static str {
    match code {
        0 => "Clear",
        1 => "Mostly Clear",
        2 => "Partly Cloudy",
        3 => "Overcast",
        45 | 48 => "Fog",
        51 | 53 | 55 => "Drizzle",
        56 | 57 => "Freezing Drizzle",
        61 | 63 | 65 => "Rain",
        66 | 67 => "Freezing Rain",
        71 | 73 | 75 => "Snow",
        77 => "Snow Grains",
        80..=82 => "Rain Showers",
        85 | 86 => "Snow Showers",
        95 => "Thunderstorm",
        96 | 99 => "Hail",
        _ => "Unknown",
    }
}

/// Map WMO conditions to the watch's built-in icons, preserving unknown codes.
fn wmo_to_pebble(code: u16) -> u8 {
    match code {
        0 => 7,                                // Sun
        1 => 0,                                // PartlyCloudy
        2 => 1,                                // CloudyDay
        3 => 1,                                // CloudyDay
        45 | 48 => 6,                          // Generic (fog)
        51 | 53 | 55 | 56 | 57 => 3,           // LightRain
        61 => 3,                               // LightRain
        63 | 65 | 66 | 67 | 80 | 81 | 82 => 4, // HeavyRain
        71 | 73 | 77 | 85 => 2,                // LightSnow
        75 | 86 => 5,                          // HeavySnow
        95 | 96 | 99 => 6,                     // Generic (thunderstorm)
        _ => 255,                              // Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn response() -> Value {
        let start = 1700000000i64;
        json!({
            "utc_offset_seconds":19800,
            "current":{"time":start,"temperature_2m":12.4,"weather_code":61,
                "apparent_temperature":10.6,"relative_humidity_2m":72,
                "wind_speed_10m":8.4,"wind_direction_10m":270},
            "daily":{"time":[start,start+86400,start+172800],
                "temperature_2m_max":[21.4,18.4,null],"temperature_2m_min":[7.2,6.8,null],
                "weather_code":[61,1,null],"apparent_temperature_max":[13.0,12.8,null],
                "relative_humidity_2m_mean":[86,80,null],"visibility_min":[500,1000,null],
                "uv_index_max":[6.44,null,null],"precipitation_probability_max":[45,null,null],
                "wind_speed_10m_max":[15.0,17.4,null],"wind_direction_10m_dominant":[180,190,null],
                "precipitation_sum":[3.2,null,null],"sunrise":[start+20000,start+106400,null],
                "sunset":[start+60000,start+146400,null]},
            "hourly":{"time":[start,start+7200,start+86400,start+90000],
                "temperature_2m":[10.4,null,8.6,9.1],"weather_code":[0,null,1,61],
                "uv_index":[0.4,null,1.2,2.0],"visibility":[12200,null,10000,9000]}
        })
    }

    /// Join local hourly timestamps without shifting a missing hour or tomorrow's first hour.
    #[test]
    fn forecast_preserves_local_day_slots_units_and_extended_metrics() {
        let data = parse_forecast(&response(), 52.52, 13.41, "Berlin").unwrap();
        assert_eq!(data.current_temp, 12);
        assert_eq!(data.current_weather, 3);
        assert_eq!(data.utc_offset_minutes, Some(330));
        assert_eq!(data.uv_index_x10, Some(64));
        assert_eq!(data.today_hourly.len(), 3);
        assert_eq!(data.today_hourly[0].temp, Some(10));
        assert_eq!(data.today_hourly[1], WeatherHourlyForecast::default());
        assert_eq!(data.today_hourly[2], WeatherHourlyForecast::default());
        assert_eq!(data.tomorrow_hourly.len(), 2);
        assert_eq!(data.tomorrow_hourly[0].temp, Some(9));
        assert_eq!(data.tomorrow_hourly[1].weather_type, 3);
        assert_eq!(data.daily[0].feels_like, data.feels_like);
        assert_eq!(data.daily[0].wind_speed_mph, Some(8));
        assert_eq!(data.daily[0].wind_direction_deg, Some(270));
        assert_eq!(data.daily[0].humidity_pct, Some(86));
        assert_eq!(data.daily[0].visibility_m, Some(500));
        assert_eq!(data.daily[0].precip_sum_mm, Some(3));
        assert_eq!(data.daily[0].sunrise_utc, Some(1700020000));
        assert_eq!(data.daily[1].feels_like, Some(13));
        assert_eq!(data.daily[1].wind_speed_mph, Some(17));
        assert_eq!(data.daily[2].high_temp, None);
        assert_eq!(data.daily[2].weather_type, 255);
        assert!(data.sunrise_sunset_pins);
    }

    /// Required missing readings fail; absent optional forecasts remain unknown instead of clear/zero.
    #[test]
    fn missing_optional_data_is_unknown_and_malformed_core_data_fails() {
        let mut json = response();
        json.as_object_mut().unwrap().remove("hourly");
        json["current"]["weather_code"] = Value::Null;
        json["daily"]["temperature_2m_max"][1] = Value::Null;
        let data = parse_forecast(&json, 0.0, 0.0, "Test").unwrap();
        assert!(data.today_hourly.is_empty());
        assert!(data.tomorrow_hourly.is_empty());
        assert_eq!(data.current_weather, 255);
        assert_eq!(data.forecast_short, "Unknown");
        assert_eq!(data.tomorrow_high, i16::MAX);
        json["current"]["temperature_2m"] = Value::Null;
        assert!(parse_forecast(&json, 0.0, 0.0, "Test").is_err());
        let mut json = response();
        json["daily"]["time"] = json!([]);
        assert!(parse_forecast(&json, 0.0, 0.0, "Test").is_err());
        assert_eq!(weather_code(&json!(-1)), 255);
        assert_eq!(weather_code(&json!(8)), 255);
        assert_eq!(percent(&json!(101)), None);
        assert_eq!(unsigned(&json!(-10)), None);
        let mut json = response();
        json["current"]["time"] = json!(i64::MAX);
        json["hourly"]["time"][0] = json!(i64::MIN);
        let data = parse_forecast(&json, 0.0, 0.0, "Test").unwrap();
        assert_eq!(data.daily[0].visibility_m, Some(500));
    }

    /// The provider request explicitly fixes the wire units and starts series at local midnight.
    #[test]
    fn request_covers_rich_forecasts_in_canonical_units() {
        let url = forecast_url(52.52, 13.41);
        for required in [
            "temperature_unit=celsius",
            "wind_speed_unit=mph",
            "precipitation_unit=mm",
            "timezone=auto",
            "timeformat=unixtime",
            "forecast_days=7",
            "uv_index",
            "relative_humidity_2m_mean",
            "visibility_min",
            "apparent_temperature_max",
            "sunrise",
            "sunset",
        ] {
            assert!(url.contains(required), "missing {required}");
        }
    }
}
