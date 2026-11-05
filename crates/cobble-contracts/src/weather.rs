//! Weather input shared by the daemon, BLE encoder, and clients.
//! All input temperatures are Celsius; the encoder applies the watch's unit preference.

use serde::{Deserialize, Serialize};

/// A location's current conditions and optional extended forecast.
///
/// Weather icons use Pebble codes 0–8 or 255 (unknown). Wind is always mph,
/// visibility metres, precipitation millimetres, UV index times ten, and times
/// UTC Unix seconds. Missing optional values become firmware unknown sentinels.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeatherForecast {
    pub location_name: String,
    #[serde(default)]
    pub forecast_short: String,
    pub current_temp: i16,
    pub current_weather: u8,
    pub today_high: i16,
    pub today_low: i16,
    pub tomorrow_weather: u8,
    pub tomorrow_high: i16,
    pub tomorrow_low: i16,
    #[serde(default)]
    pub is_current_location: bool,
    #[serde(default)]
    pub feels_like: Option<i16>,
    #[serde(default)]
    pub latitude: Option<f64>,
    #[serde(default)]
    pub longitude: Option<f64>,
    #[serde(default)]
    pub uv_index_x10: Option<u16>,
    #[serde(default)]
    pub precip_probability: Option<u8>,
    #[serde(default)]
    pub wind_speed_mph: Option<u16>,
    #[serde(default)]
    pub wind_direction_deg: Option<u16>,
    #[serde(default)]
    pub utc_offset_minutes: Option<i16>,
    /// Today first; at most seven days are encoded.
    #[serde(default)]
    pub daily: Vec<WeatherDailyForecast>,
    /// Local hours 0–23, including earlier hours today. The curve requires 24 known temperatures.
    #[serde(default)]
    pub today_hourly: Vec<WeatherHourlyForecast>,
    /// Tomorrow's local hours 0–23; incomplete temperature series fall back to daily data.
    #[serde(default)]
    pub tomorrow_hourly: Vec<WeatherHourlyForecast>,
    /// Also update sunrise/sunset timeline pins for the first three forecast days.
    #[serde(default)]
    pub sunrise_sunset_pins: bool,
}

/// One local day's forecast. Missing temperatures are encoded as unknown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeatherDailyForecast {
    #[serde(default)]
    pub high_temp: Option<i16>,
    #[serde(default)]
    pub low_temp: Option<i16>,
    #[serde(default = "unknown_weather")]
    pub weather_type: u8,
    #[serde(default)]
    pub precip_probability: Option<u8>,
    #[serde(default)]
    pub wind_speed_mph: Option<u16>,
    #[serde(default)]
    pub uv_index_x10: Option<u16>,
    #[serde(default)]
    pub feels_like: Option<i16>,
    #[serde(default)]
    pub wind_direction_deg: Option<u16>,
    #[serde(default)]
    pub wmo_code: Option<u8>,
    #[serde(default)]
    /// Daily mean relative humidity, not the current observation.
    pub humidity_pct: Option<u8>,
    #[serde(default)]
    /// Daily minimum visibility; the watch uses it for weather warnings.
    pub visibility_m: Option<u32>,
    #[serde(default)]
    pub precip_sum_mm: Option<u32>,
    #[serde(default)]
    pub sunrise_utc: Option<u32>,
    #[serde(default)]
    pub sunset_utc: Option<u32>,
}

impl Default for WeatherDailyForecast {
    fn default() -> Self {
        Self {
            high_temp: None,
            low_temp: None,
            weather_type: 255,
            precip_probability: None,
            wind_speed_mph: None,
            uv_index_x10: None,
            feels_like: None,
            wind_direction_deg: None,
            wmo_code: None,
            humidity_pct: None,
            visibility_m: None,
            precip_sum_mm: None,
            sunrise_utc: None,
            sunset_utc: None,
        }
    }
}

/// One local hour. A missing temperature disables that day's curve, preserving daily fallback.
/// Today's hourly UV is independent and can still be supplied for incomplete days.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeatherHourlyForecast {
    #[serde(default = "unknown_weather")]
    pub weather_type: u8,
    #[serde(default)]
    pub temp: Option<i16>,
    #[serde(default)]
    pub uv_index_x10: Option<u16>,
}

impl Default for WeatherHourlyForecast {
    fn default() -> Self {
        Self {
            weather_type: 255,
            temp: None,
            uv_index_x10: None,
        }
    }
}

fn unknown_weather() -> u8 {
    255
}
