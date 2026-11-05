//! Capability-selected weather records. The v4.5 fixed block matches upstream
//! WeatherAppBlobRecordV4: 250 bytes followed by the same pstring16s as v3.

use cobble_contracts::{WeatherDailyForecast, WeatherForecast, WeatherHourlyForecast};

use super::blob_db::{WeatherType, build_weather_blob};

pub const WEATHER_APP_CAPABILITY: u64 = 1 << 11;
pub const WEATHER_DB_V4_CAPABILITY: u64 = 1 << 24;
pub const UNKNOWN_TIMELINE_ATTRIBUTES_CAPABILITY: u64 = 1 << 25;
pub const MAX_DAILY: usize = 7;
pub const MAX_HOURLY: usize = 24;
const TEMP_UNKNOWN: i16 = i16::MAX;

/// Resolve the separate temperature preference, falling back to distance units.
pub fn weather_in_fahrenheit(temperature: Option<u8>, imperial: bool) -> bool {
    match temperature {
        Some(1) => false,
        Some(2) => true,
        _ => imperial,
    }
}

/// Convert Celsius to the watch's units without converting the unknown sentinel.
pub fn temperature(value: i16, fahrenheit: bool) -> i16 {
    if !fahrenheit || value == TEMP_UNKNOWN {
        return value;
    }
    ((f64::from(value) * 1.8 + 32.0).round() as i32).clamp(i16::MIN as i32, i16::MAX as i32 - 1)
        as i16
}

/// Encode v4.5 only for capability bit 24; otherwise retain the exact v3 layout.
pub fn build_weather_record(
    forecast: &WeatherForecast,
    timestamp: u32,
    capabilities: u64,
    fahrenheit: bool,
) -> Result<Vec<u8>, &'static str> {
    let strings_len = forecast.location_name.len() + forecast.forecast_short.len() + 4;
    if strings_len > u16::MAX as usize {
        return Err("weather strings exceed 65535 bytes");
    }
    for (coordinate, bound) in [(forecast.latitude, 90.0), (forecast.longitude, 180.0)] {
        if coordinate.is_some_and(|v| !v.is_finite() || v.abs() > bound) {
            return Err("invalid weather coordinates");
        }
    }
    if forecast
        .utc_offset_minutes
        .is_some_and(|v| !(-1440..=1440).contains(&v))
    {
        return Err("invalid weather UTC offset");
    }
    let t = |value| temperature(value, fahrenheit);
    let legacy = build_weather_blob(
        &forecast.location_name,
        &forecast.forecast_short,
        t(forecast.current_temp),
        WeatherType::from_u8(forecast.current_weather),
        t(forecast.today_high),
        t(forecast.today_low),
        WeatherType::from_u8(forecast.tomorrow_weather),
        t(forecast.tomorrow_high),
        t(forecast.tomorrow_low),
        timestamp,
        forecast.is_current_location,
    );
    if capabilities & WEATHER_DB_V4_CAPABILITY == 0 {
        return Ok(legacy);
    }
    let mut out = legacy[..18].to_vec();
    out[0] = 4;
    out.push(5); // minor version
    put_i16(&mut out, forecast.feels_like.map(t).unwrap_or(TEMP_UNKNOWN));
    put_i16(
        &mut out,
        forecast
            .uv_index_x10
            .and_then(|v| i16::try_from(v).ok())
            .unwrap_or(-1),
    );
    put_i16(
        &mut out,
        forecast
            .precip_probability
            .filter(|v| *v <= 100)
            .map(i16::from)
            .unwrap_or(-1),
    );
    put_u16(&mut out, forecast.wind_speed_mph.unwrap_or(0));
    put_u16(
        &mut out,
        direction(forecast.wind_direction_deg).unwrap_or(u16::MAX),
    );
    for coord in [forecast.latitude, forecast.longitude] {
        put_i16(
            &mut out,
            coord
                .map(|v| (v * 100.0).round() as i16)
                .unwrap_or(i16::MIN),
        );
    }
    out.push(forecast.daily.len().min(MAX_DAILY) as u8);
    for i in 0..MAX_DAILY {
        let day = forecast.daily.get(i);
        put_i16(
            &mut out,
            day.and_then(|d| d.high_temp).map(t).unwrap_or(TEMP_UNKNOWN),
        );
        put_i16(
            &mut out,
            day.and_then(|d| d.low_temp).map(t).unwrap_or(TEMP_UNKNOWN),
        );
        out.push(day.map_or(255, |d| WeatherType::from_u8(d.weather_type) as u8));
    }
    hourly(&mut out, &forecast.today_hourly, fahrenheit);
    put_i16(&mut out, forecast.utc_offset_minutes.unwrap_or(i16::MIN));
    for i in 0..MAX_DAILY {
        let day = forecast.daily.get(i);
        out.push(metric(
            day.and_then(|d| d.precip_probability)
                .filter(|v| *v <= 100)
                .map(u16::from),
        ));
        out.push(metric(day.and_then(|d| d.wind_speed_mph)));
        out.push(metric(day.and_then(|d| d.uv_index_x10)));
    }
    let today = forecast.daily.first();
    out.push(today.and_then(|d| d.wmo_code).unwrap_or(255));
    out.push(
        today
            .and_then(|d| d.humidity_pct)
            .filter(|v| *v <= 100)
            .unwrap_or(255),
    );
    put_u16(&mut out, metric_u16(today.and_then(|d| d.visibility_m)));
    put_u16(&mut out, metric_u16(today.and_then(|d| d.precip_sum_mm)));
    for i in 0..MAX_DAILY {
        put_i16(
            &mut out,
            forecast
                .daily
                .get(i)
                .and_then(|d| d.feels_like)
                .map(t)
                .unwrap_or(TEMP_UNKNOWN),
        );
    }
    put_i16(
        &mut out,
        direction(forecast.wind_direction_deg)
            .map(|v| v as i16)
            .unwrap_or(-1),
    );
    for i in 0..MAX_DAILY {
        put_i16(
            &mut out,
            forecast
                .daily
                .get(i)
                .and_then(|d| direction(d.wind_direction_deg))
                .map(|v| v as i16)
                .unwrap_or(-1),
        );
    }
    for i in 0..MAX_HOURLY {
        out.push(metric(
            forecast.today_hourly.get(i).and_then(|h| h.uv_index_x10),
        ));
    }
    hourly(&mut out, &forecast.tomorrow_hourly, fahrenheit);
    debug_assert_eq!(out.len(), 250);
    out.extend_from_slice(&legacy[18..]);
    Ok(out)
}

/// Append fixed arrays, enabling temperature display only for a complete local day.
fn hourly(out: &mut Vec<u8>, hours: &[WeatherHourlyForecast], fahrenheit: bool) {
    out.push(hourly_count(hours));
    for i in 0..MAX_HOURLY {
        out.push(
            hours
                .get(i)
                .map_or(255, |h| WeatherType::from_u8(h.weather_type) as u8),
        );
    }
    for i in 0..MAX_HOURLY {
        out.push(
            hours
                .get(i)
                .and_then(|h| h.temp)
                .filter(|v| *v != TEMP_UNKNOWN)
                .map(|v| temperature(v, fahrenheit).clamp(i8::MIN as i16, i8::MAX as i16) as i8)
                .unwrap_or(0) as u8,
        );
    }
}

/// Firmware trusts every temperature when count is 24 and has no unknown int8 value.
/// Disable incomplete days so padded slots cannot appear as real 0° readings.
/// UV has its own fixed array and remains available independently.
pub(crate) fn hourly_count(hours: &[WeatherHourlyForecast]) -> u8 {
    if hours.len() >= MAX_HOURLY
        && hours
            .iter()
            .take(MAX_HOURLY)
            .all(|hour| hour.temp.is_some_and(|temp| temp != TEMP_UNKNOWN))
    {
        MAX_HOURLY as u8
    } else {
        0
    }
}

fn put_i16(out: &mut Vec<u8>, value: i16) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}
/// Reserve 255 for missing or out-of-range byte metrics, as the firmware requires.
fn metric(value: Option<u16>) -> u8 {
    value.filter(|v| *v < 255).map_or(255, |v| v as u8)
}
/// Clamp known extended metrics below the reserved 65535 unknown value.
fn metric_u16(value: Option<u32>) -> u16 {
    value.map_or(u16::MAX, |v| v.min(65534) as u16)
}
/// Keep only physical wind directions so invalid angles serialize as unknown.
fn direction(value: Option<u16>) -> Option<u16> {
    value.filter(|v| *v <= 360)
}

pub(crate) struct WeatherPin {
    pub key: [u8; 16],
    /// None deletes a previously sent event whose time is now unavailable.
    pub blob: Option<Vec<u8>>,
}

/// Build stable sunrise/sunset slots for three days, gating the new attribute on bit 25.
/// Keep these separate from the weather record; the firmware reads solar times from pins.
pub(crate) fn build_weather_pins(
    forecast: &WeatherForecast,
    location_key: &[u8; 16],
    capabilities: u64,
    fahrenheit: bool,
    now: u32,
) -> Vec<WeatherPin> {
    if !forecast.sunrise_sunset_pins
        || !forecast.is_current_location
        || capabilities & WEATHER_APP_CAPABILITY == 0
    {
        return Vec::new();
    }
    let mut pins = Vec::new();
    for day in 0..3 {
        for sunset in [false, true] {
            // Separate slots per location, relative day, and event, reused on every refresh.
            let mut key = *location_key;
            for (byte, salt) in key.iter_mut().zip(*b"cobble-solar-pin") {
                *byte ^= salt;
            }
            key[15] ^= (day * 2 + usize::from(sunset)) as u8;
            let daily = forecast.daily.get(day);
            let time = daily.and_then(|d| if sunset { d.sunset_utc } else { d.sunrise_utc });
            let blob = time.filter(|time| *time != 0).map(|time| {
                solar_pin(
                    &key,
                    forecast,
                    daily.unwrap(),
                    time,
                    sunset,
                    capabilities,
                    fahrenheit,
                    now,
                )
            });
            pins.push(WeatherPin { key, blob });
        }
    }
    pins
}

/// Encode a weather-layout timeline item with an action opening the weather app.
#[allow(clippy::too_many_arguments)]
fn solar_pin(
    key: &[u8; 16],
    forecast: &WeatherForecast,
    day: &WeatherDailyForecast,
    timestamp: u32,
    sunset: bool,
    capabilities: u64,
    fahrenheit: bool,
    now: u32,
) -> Vec<u8> {
    let title = if sunset { "Sunset" } else { "Sunrise" };
    let temp = |value: Option<i16>| {
        value
            .filter(|v| *v != TEMP_UNKNOWN)
            .map(|v| temperature(v, fahrenheit).to_string())
            .unwrap_or_else(|| "–".into())
    };
    let subtitle = format!("{}° / {}°", temp(day.high_temp), temp(day.low_temp));
    let icon = (0x80000000u32 | if sunset { 85 } else { 84 }).to_le_bytes();
    let mut attrs = Vec::new();
    attribute(&mut attrs, 1, title.as_bytes());
    attribute(&mut attrs, 2, subtitle.as_bytes());
    attribute(&mut attrs, 4, &icon);
    attribute(&mut attrs, 6, &icon);
    attribute(&mut attrs, 14, &now.to_le_bytes());
    attribute(
        &mut attrs,
        11,
        utf8_prefix(&forecast.location_name, 64).as_bytes(),
    );
    let mut count = 6;
    if capabilities & UNKNOWN_TIMELINE_ATTRIBUTES_CAPABILITY != 0 {
        attribute(&mut attrs, 0x35, &[if sunset { 2 } else { 1 }]);
        count += 1;
    }
    let mut action = vec![0, 7, 1]; // action ID, OpenWatchapp, attribute count
    attribute(&mut action, 1, b"More");
    let mut out = key.to_vec();
    out.extend_from_slice(
        uuid::Uuid::parse_str(crate::uuids::WEATHER_APP_UUID)
            .unwrap()
            .as_bytes(),
    );
    out.extend_from_slice(&timestamp.to_le_bytes());
    put_u16(&mut out, 0); // duration
    out.push(2); // Pin
    put_u16(&mut out, 0); // flags
    out.push(6); // WeatherPin layout
    put_u16(&mut out, (attrs.len() + action.len()) as u16);
    out.extend_from_slice(&[count, 1]);
    out.extend_from_slice(&attrs);
    out.extend_from_slice(&action);
    out
}

fn attribute(out: &mut Vec<u8>, id: u8, value: &[u8]) {
    out.push(id);
    put_u16(out, value.len() as u16);
    out.extend_from_slice(value);
}

/// Limit timeline text by UTF-8 bytes without splitting a character.
fn utf8_prefix(text: &str, limit: usize) -> &str {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> WeatherForecast {
        WeatherForecast {
            location_name: "San Francisco".into(),
            forecast_short: "Clear".into(),
            current_temp: 18,
            current_weather: 7,
            today_high: 21,
            today_low: 13,
            tomorrow_weather: 0,
            tomorrow_high: 18,
            tomorrow_low: 11,
            is_current_location: true,
            feels_like: Some(17),
            latitude: Some(37.77),
            longitude: Some(-122.42),
            uv_index_x10: Some(50),
            precip_probability: Some(20),
            wind_speed_mph: Some(12),
            wind_direction_deg: Some(270),
            utc_offset_minutes: Some(-420),
            daily: [
                (21, 13, 7),
                (18, 11, 0),
                (23, 11, 1),
                (13, 5, 3),
                (21, 10, 4),
                (16, 7, 0),
                (20, 12, 7),
            ]
            .into_iter()
            .map(|(high, low, kind)| WeatherDailyForecast {
                high_temp: Some(high),
                low_temp: Some(low),
                weather_type: kind,
                ..Default::default()
            })
            .collect(),
            ..Default::default()
        }
    }

    fn hex(text: &str) -> Vec<u8> {
        text.split_whitespace()
            .map(|v| u8::from_str_radix(v, 16).unwrap())
            .collect()
    }

    /// Consume the same complete JSON payload as the Python public client.
    #[test]
    fn public_forecast_json_roundtrips_without_losing_optional_fields() {
        let json = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../cobble-contracts/tests/fixtures/weather_forecast.json"
        ));
        let data: WeatherForecast = serde_json::from_str(json).unwrap();
        assert_eq!(data.daily[0].sunrise_utc, Some(1700020000));
        assert_eq!(data.today_hourly[1], WeatherHourlyForecast::default());
        let actual = serde_json::to_value(data).unwrap();
        assert_eq!(
            actual,
            serde_json::from_str::<serde_json::Value>(json).unwrap()
        );
        let mut invalid = actual;
        invalid["unknown_field"] = serde_json::json!(true);
        assert!(serde_json::from_value::<WeatherForecast>(invalid).is_err());
    }

    /// Compare the entire record to upstream's independent v4.5 golden fixture.
    #[test]
    fn v4_5_matches_upstream_golden_bytes() {
        let mut data = sample();
        data.daily[0] = WeatherDailyForecast {
            high_temp: Some(21),
            low_temp: Some(13),
            weather_type: 7,
            precip_probability: Some(20),
            wind_speed_mph: Some(12),
            uv_index_x10: Some(50),
            feels_like: Some(17),
            wind_direction_deg: Some(270),
            wmo_code: Some(45),
            humidity_pct: Some(72),
            visibility_m: Some(12200),
            precip_sum_mm: Some(3),
            ..Default::default()
        };
        let today = [
            13, 13, 13, 13, 13, 13, 13, 14, 15, 16, 17, 18, 19, 20, 20, 21, 20, 20, 19, 17, 16, 15,
            15, 14,
        ];
        let tomorrow = [
            11, 11, 11, 11, 11, 11, 11, 12, 13, 14, 15, 16, 17, 18, 18, 18, 17, 17, 16, 14, 13, 12,
            12, 11,
        ];
        let uv = [
            0, 0, 0, 0, 0, 0, 0, 5, 10, 20, 35, 55, 70, 80, 75, 60, 40, 25, 10, 5, 0, 0, 0, 0,
        ];
        data.today_hourly = today
            .into_iter()
            .zip(uv)
            .map(|(temp, uv)| WeatherHourlyForecast {
                weather_type: 7,
                temp: Some(temp),
                uv_index_x10: Some(uv),
            })
            .collect();
        data.tomorrow_hourly = tomorrow
            .into_iter()
            .map(|temp| WeatherHourlyForecast {
                weather_type: 0,
                temp: Some(temp),
                uv_index_x10: None,
            })
            .collect();
        let expected = hex("
            04 12 00 07 15 00 0D 00 00 12 00 0B 00 80 E1 4E
            68 01 05 11 00 32 00 14 00 0C 00 0E 01 C1 0E 2E
            D0 07 15 00 0D 00 07 12 00 0B 00 00 17 00 0B 00
            01 0D 00 05 00 03 15 00 0A 00 04 10 00 07 00 00
            14 00 0C 00 07 18 07 07 07 07 07 07 07 07 07 07
            07 07 07 07 07 07 07 07 07 07 07 07 07 07 0D 0D
            0D 0D 0D 0D 0D 0E 0F 10 11 12 13 14 14 15 14 14
            13 11 10 0F 0F 0E 5C FE 14 0C 32 FF FF FF FF FF
            FF FF FF FF FF FF FF FF FF FF FF FF FF 2D 48 A8
            2F 03 00 11 00 FF 7F FF 7F FF 7F FF 7F FF 7F FF
            7F 0E 01 0E 01 FF FF FF FF FF FF FF FF FF FF FF
            FF 00 00 00 00 00 00 00 05 0A 14 23 37 46 50 4B
            3C 28 19 0A 05 00 00 00 00
            18
            00 00 00 00 00 00 00 00 00 00 00 00
            00 00 00 00 00 00 00 00 00 00 00 00
            0B 0B 0B 0B 0B 0B 0B 0C 0D 0E 0F 10
            11 12 12 12 11 11 10 0E 0D 0C 0C 0B
            16 00 0D 00 53 61 6E 20 46 72 61 6E 63 69 73 63 6F
            05 00 43 6C 65 61 72");
        assert_eq!(expected.len(), 274);
        assert_eq!(
            build_weather_record(&data, 1750000000, WEATHER_DB_V4_CAPABILITY, false).unwrap(),
            expected
        );
    }

    /// Capability bits other than bit 24 must preserve the established v3 wire layout.
    #[test]
    fn legacy_capabilities_keep_the_v3_record() {
        let expected = hex("03 12 00 07 15 00 0D 00 00 12 00 0B 00 80 E1 4E 68 01
            16 00 0D 00 53 61 6E 20 46 72 61 6E 63 69 73 63 6F 05 00 43 6C 65 61 72");
        for caps in [0, WEATHER_APP_CAPABILITY, 1 << 22, 1 << 25] {
            assert_eq!(
                build_weather_record(&sample(), 1750000000, caps, false).unwrap(),
                expected
            );
        }
    }

    /// Incomplete, icon-only and unknown series fall back to daily data; complete days stay live.
    #[test]
    fn incomplete_hourly_series_do_not_invent_zero_degree_readings() {
        let mut data = sample();
        data.today_hourly = vec![
            WeatherHourlyForecast {
                temp: Some(18),
                ..Default::default()
            },
            WeatherHourlyForecast::default(),
        ];
        data.tomorrow_hourly = vec![WeatherHourlyForecast {
            weather_type: 7,
            ..Default::default()
        }];
        let record = build_weather_record(&data, 0, WEATHER_DB_V4_CAPABILITY, false).unwrap();
        assert_eq!(record[69], 0);
        assert_eq!(record[201], 0);
        assert_eq!(record[94], 18);
        assert_eq!(&record[95..118], &[0; 23]);
        assert_eq!(record[202], 7);
        assert_eq!(&record[203..226], &[255; 23]);
        data.today_hourly = vec![WeatherHourlyForecast::default(); 24];
        data.tomorrow_hourly = data.today_hourly.clone();
        let record = build_weather_record(&data, 0, WEATHER_DB_V4_CAPABILITY, false).unwrap();
        assert_eq!(record[69], 0);
        assert_eq!(record[201], 0);
        data.today_hourly[0].uv_index_x10 = Some(5);
        data.tomorrow_hourly[0].uv_index_x10 = Some(5);
        let record = build_weather_record(&data, 0, WEATHER_DB_V4_CAPABILITY, false).unwrap();
        assert_eq!(record[69], 0);
        assert_eq!(record[201], 0);
        assert_eq!(record[177], 5);
        data.today_hourly = vec![
            WeatherHourlyForecast {
                temp: Some(0),
                ..Default::default()
            };
            24
        ];
        data.tomorrow_hourly = data.today_hourly.clone();
        let record = build_weather_record(&data, 0, WEATHER_DB_V4_CAPABILITY, false).unwrap();
        assert_eq!(record[69], 24);
        assert_eq!(record[201], 24); // real 0°C is valid
        data.today_hourly[12].temp = None;
        data.tomorrow_hourly[5].temp = Some(i16::MAX);
        let record = build_weather_record(&data, 0, WEATHER_DB_V4_CAPABILITY, false).unwrap();
        assert_eq!(record[69], 0);
        assert_eq!(record[201], 0);
    }

    /// Missing extras retain all fixed offsets and use the firmware's unknown values.
    #[test]
    fn sparse_unicode_forecast_uses_unknown_sentinels() {
        let mut data = sample();
        data.location_name = "České Budějovice".into();
        data.forecast_short = "Déšť 🌧️".into();
        data.feels_like = None;
        data.latitude = None;
        data.longitude = None;
        data.uv_index_x10 = None;
        data.precip_probability = None;
        data.wind_speed_mph = None;
        data.wind_direction_deg = None;
        data.utc_offset_minutes = None;
        data.daily.truncate(1);
        let record = build_weather_record(&data, 0, WEATHER_DB_V4_CAPABILITY, false).unwrap();
        assert_eq!(
            &record[18..34],
            &hex("05 FF 7F FF FF FF FF 00 00 FF FF 00 80 00 80 01")
        );
        assert_eq!(&record[39..44], &hex("FF 7F FF 7F FF"));
        assert_eq!(record[69], 0);
        assert_eq!(&record[70..94], &[255; 24]);
        assert_eq!(&record[94..118], &[0; 24]);
        assert_eq!(&record[118..120], &[0, 128]);
        assert_eq!(&record[120..147], &[255; 27]);
        assert_eq!(
            &record[147..161],
            hex("FF 7F FF 7F FF 7F FF 7F FF 7F FF 7F FF 7F")
        );
        assert_eq!(&record[161..201], &[255; 40]);
        assert_eq!(record[201], 0);
        assert_eq!(&record[202..226], &[255; 24]);
        assert_eq!(&record[226..250], &[0; 24]);
        let string_len = data.location_name.len() + data.forecast_short.len() + 4;
        assert_eq!(record.len(), 252 + string_len);
        assert_eq!(
            u16::from_le_bytes(record[250..252].try_into().unwrap()) as usize,
            string_len
        );
        assert_eq!(
            u16::from_le_bytes(record[252..254].try_into().unwrap()) as usize,
            data.location_name.len()
        );
    }

    /// Apply separate temperature units, keep sentinels, and clamp int8 hourly temperatures.
    #[test]
    fn temperature_units_and_fixed_array_limits_are_respected() {
        assert!(!weather_in_fahrenheit(Some(1), true));
        assert!(weather_in_fahrenheit(Some(2), false));
        assert!(weather_in_fahrenheit(None, true));
        assert!(!weather_in_fahrenheit(Some(0), false));
        assert_eq!(temperature(-40, true), -40);
        assert_eq!(temperature(i16::MAX, true), i16::MAX);
        let mut data = sample();
        data.daily.resize(9, WeatherDailyForecast::default());
        data.today_hourly = vec![
            WeatherHourlyForecast {
                temp: Some(100),
                ..Default::default()
            };
            30
        ];
        data.tomorrow_hourly = vec![
            WeatherHourlyForecast {
                temp: Some(-100),
                ..Default::default()
            };
            30
        ];
        let record = build_weather_record(&data, 0, WEATHER_DB_V4_CAPABILITY, true).unwrap();
        assert_eq!(i16::from_le_bytes(record[1..3].try_into().unwrap()), 64);
        assert_eq!(record[33], 7);
        assert_eq!(record[69], 24);
        assert_eq!(record[201], 24);
        assert_eq!(&record[94..118], &[127; 24]);
        assert_eq!(&record[226..250], &[128; 24]);
        data.latitude = Some(f64::NAN);
        assert!(build_weather_record(&data, 0, 0, false).is_err());
        data.latitude = Some(91.0);
        assert!(build_weather_record(&data, 0, 0, false).is_err());
    }

    /// Older firmware never receives attribute 0x35, and repeated refreshes reuse pin keys.
    #[test]
    fn solar_pins_gate_new_attributes_and_delete_missing_events() {
        let mut data = sample();
        data.sunrise_sunset_pins = true;
        data.daily[0].sunrise_utc = Some(1750001000);
        data.daily[0].sunset_utc = Some(1750040000);
        let key = [42; 16];
        let legacy = build_weather_pins(&data, &key, WEATHER_APP_CAPABILITY, false, 1750000000);
        let modern = build_weather_pins(
            &data,
            &key,
            WEATHER_APP_CAPABILITY | UNKNOWN_TIMELINE_ATTRIBUTES_CAPABILITY,
            false,
            1750000000,
        );
        assert_eq!(legacy.len(), 6);
        assert!(legacy[2..].iter().all(|p| p.blob.is_none()));
        for (old, new) in legacy.iter().zip(&modern).take(2) {
            assert_eq!(old.key, new.key);
            let old = old.blob.as_ref().unwrap();
            let new = new.blob.as_ref().unwrap();
            assert_eq!(old[38], 2);
            assert_eq!(old[41], 6);
            assert_eq!(old[44], 6);
            assert_eq!(new[44], 7);
            let mut offset = 46;
            for _ in 0..old[44] {
                assert_ne!(old[offset], 0x35);
                offset += 3 + u16::from_le_bytes(old[offset + 1..offset + 3].try_into().unwrap())
                    as usize;
            }
            assert_eq!(old[offset..offset + 3], [0, 7, 1]);
            assert_eq!(
                old.len() - 46,
                u16::from_le_bytes(old[42..44].try_into().unwrap()) as usize
            );
        }
        data.daily[0].sunrise_utc = Some(1750001010);
        let updated = build_weather_pins(&data, &key, WEATHER_APP_CAPABILITY, false, 1750000001);
        assert_eq!(legacy[0].key, updated[0].key);
        assert_ne!(legacy[0].key, legacy[1].key);
        assert!(build_weather_pins(&data, &key, 0, false, 0).is_empty());
        data.is_current_location = false;
        assert!(build_weather_pins(&data, &key, WEATHER_APP_CAPABILITY, false, 0).is_empty());
    }
}
