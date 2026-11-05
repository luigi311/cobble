"""Weather forecasts shared with the daemon's PushWeatherForecast JSON schema.

Temperatures are Celsius; wind is mph, visibility metres, precipitation
millimetres, UV indices times ten, and solar times UTC Unix seconds.
"""

from dataclasses import dataclass, field


@dataclass(slots=True, kw_only=True)
class WeatherDailyForecast:
    """One local day, with None representing an unavailable reading.

    Humidity is the daily mean; visibility is the daily minimum.
    """

    high_temp: int | None = None
    low_temp: int | None = None
    weather_type: int = 255
    precip_probability: int | None = None
    wind_speed_mph: int | None = None
    uv_index_x10: int | None = None
    feels_like: int | None = None
    wind_direction_deg: int | None = None
    wmo_code: int | None = None
    humidity_pct: int | None = None
    visibility_m: int | None = None
    precip_sum_mm: int | None = None
    sunrise_utc: int | None = None
    sunset_utc: int | None = None


@dataclass(slots=True, kw_only=True)
class WeatherHourlyForecast:
    """One local hour; keep missing hours in place rather than removing them.

    The watch's curve requires 24 known temperatures; otherwise it uses daily
    data. Today's hourly UV remains available independently.
    """

    weather_type: int = 255
    temp: int | None = None
    uv_index_x10: int | None = None


@dataclass(slots=True, kw_only=True)
class WeatherForecast:
    """Current conditions plus up to seven days and two 24-hour forecast series.

    Weather icons use Pebble codes 0–8 or 255 (unknown). The watch's capabilities
    select v4.5 or v3. Input temperatures are converted to its preferred units.
    Solar timeline pins are optional and apply to the current location only.
    """

    location_name: str
    current_temp: int
    current_weather: int
    today_high: int
    today_low: int
    tomorrow_weather: int
    tomorrow_high: int
    tomorrow_low: int
    forecast_short: str = ""
    is_current_location: bool = False
    feels_like: int | None = None
    latitude: float | None = None
    longitude: float | None = None
    uv_index_x10: int | None = None
    precip_probability: int | None = None
    wind_speed_mph: int | None = None
    wind_direction_deg: int | None = None
    utc_offset_minutes: int | None = None
    daily: list[WeatherDailyForecast] = field(default_factory=list)
    today_hourly: list[WeatherHourlyForecast] = field(default_factory=list)
    tomorrow_hourly: list[WeatherHourlyForecast] = field(default_factory=list)
    sunrise_sunset_pins: bool = False
