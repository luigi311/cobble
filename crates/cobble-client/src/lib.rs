//! Rust D-Bus client for the cobbled Pebble BLE daemon.
//!
//! Mirrors the `org.cobble.Daemon` interface 1:1 (see `cobbled/src/service.rs`).
//!
//! # Quick start
//!
//! ```no_run
//! # async fn example() -> cobble_client::Result<()> {
//! use cobble_client::CobbleClient;
//!
//! let client = CobbleClient::new().await?;
//! if client.is_running().await {
//!     client.reload_config().await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! For signal subscriptions, obtain a [`CobbleDaemonProxy`] via
//! [`CobbleClient::proxy`] and call the generated `receive_*()` methods.

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
};

pub use cobble_contracts::*;
use zbus::{Connection, proxy};
pub use zbus::{Error, Result};
pub use zvariant::OwnedValue;

/// AppMessage wire type matching the D-Bus signature `a{i(sv)}`.
///
/// Each entry maps an integer key to a `(tag, value)` pair where `tag` is one
/// of `"u8"`, `"u16"`, `"u32"`, `"i8"`, `"i16"`, `"i32"`, `"str"`, `"bytes"`.
pub type WireDict = HashMap<i32, (String, OwnedValue)>;

/// Self-describing `a{sv}` map (watch version/color/health-profile/settings).
pub type VarDict = HashMap<String, OwnedValue>;

fn xdg_path(variable: &str, home_suffix: &str) -> Option<PathBuf> {
    std::env::var_os(variable)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(|value| PathBuf::from(value).join(home_suffix))
        })
}

fn database_path_from_config(
    config_path: &Path,
    default_database_path: PathBuf,
) -> std::result::Result<PathBuf, String> {
    let text = match std::fs::read_to_string(config_path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(default_database_path);
        }
        Err(error) => {
            return Err(format!("read config file {}: {error}", config_path.display()));
        }
    };
    let config: cobble_config::Config = toml::from_str(&text)
        .map_err(|error| format!("parse config file {}: {error}", config_path.display()))?;
    Ok(config.db.map(PathBuf::from).unwrap_or(default_database_path))
}

/// Resolve the database used for offline read-only access from the standard
/// daemon config. A running daemon's reported active path must take precedence.
pub fn offline_database_path() -> std::result::Result<PathBuf, String> {
    let config_base = xdg_path("XDG_CONFIG_HOME", ".config")
        .ok_or_else(|| "neither XDG_CONFIG_HOME nor HOME is set".to_string())?;
    let data_base = xdg_path("XDG_DATA_HOME", ".local/share")
        .ok_or_else(|| "neither XDG_DATA_HOME nor HOME is set".to_string())?;
    database_path_from_config(
        &config_base.join("cobbled/config.toml"),
        data_base.join("cobbled/cobbled.db"),
    )
}

fn wire_value(value: impl Into<zvariant::Value<'static>>) -> Result<OwnedValue> {
    OwnedValue::try_from(value.into()).map_err(Error::Variant)
}

fn required_string(map: &VarDict, key: &str) -> Result<String> {
    map.get(key)
        .and_then(|value| <&str>::try_from(value).ok())
        .map(str::to_owned)
        .ok_or_else(|| Error::Failure(format!("missing or invalid daemon-config field {key}")))
}

fn required_bool(map: &VarDict, key: &str) -> Result<bool> {
    map.get(key)
        .and_then(|value| bool::try_from(value).ok())
        .ok_or_else(|| Error::Failure(format!("missing or invalid daemon-config field {key}")))
}

fn required_u16(map: &VarDict, key: &str) -> Result<u16> {
    map.get(key)
        .and_then(|value| u16::try_from(value).ok())
        .ok_or_else(|| Error::Failure(format!("missing or invalid daemon-config field {key}")))
}

fn required_u8(map: &VarDict, key: &str) -> Result<u8> {
    map.get(key)
        .and_then(|value| u8::try_from(value).ok())
        .ok_or_else(|| Error::Failure(format!("missing or invalid device-config field {key}")))
}

fn required_u64(map: &VarDict, key: &str) -> Result<u64> {
    map.get(key)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| Error::Failure(format!("missing or invalid daemon-config field {key}")))
}

fn decode_daemon_config(map: &VarDict) -> Result<DaemonConfigSnapshot> {
    let api_version = required_u16(map, "api_version")?;
    if api_version != CONFIG_API_VERSION {
        return Err(Error::Failure(format!(
            "unsupported daemon-config API version {api_version}"
        )));
    }
    let database_path = required_string(map, "database_path")?;
    Ok(DaemonConfigSnapshot {
        api_version,
        revision: required_u64(map, "revision")?,
        config_path: required_string(map, "config_path")?,
        active_database_path: required_string(map, "active_database_path")?,
        resolved_database_path: required_string(map, "resolved_database_path")?,
        active_verbose: required_bool(map, "active_verbose")?,
        error: map.get("error_message").and_then(|value| <&str>::try_from(value).ok()).map(|message| ConfigError {
            kind: ConfigErrorKind::InvalidData,
            field: None,
            message: message.to_owned(),
        }),
        config: DaemonConfig {
            address: required_string(map, "address")?,
            adapter: required_string(map, "adapter")?,
            verbose: required_bool(map, "verbose")?,
            database_path: (!database_path.is_empty()).then_some(database_path),
            intervals_icu: IntervalsIcuConfig {
                enabled: required_bool(map, "intervals_enabled")?,
                athlete_id: required_string(map, "intervals_athlete_id")?,
                api_key_configured: required_bool(map, "intervals_api_key_configured")?,
            },
        },
    })
}

fn field_availability(map: &VarDict, key: &str) -> FieldAvailability {
    match map.get(key).and_then(|value| <&str>::try_from(value).ok()) {
        Some("available") => FieldAvailability::Available,
        Some("unsupported") => FieldAvailability::Unsupported,
        Some("invalid") => FieldAvailability::Invalid,
        _ => FieldAvailability::NotReceived,
    }
}

fn device_config_error(map: &VarDict) -> Option<ConfigError> {
    let message = map.get("error_message").and_then(|value| <&str>::try_from(value).ok())?;
    let kind = match map.get("error_kind").and_then(|value| <&str>::try_from(value).ok()) {
        Some("not_supported") => ConfigErrorKind::NotSupported,
        Some("disconnected") => ConfigErrorKind::Disconnected,
        Some("timeout") => ConfigErrorKind::Timeout,
        Some("rejected") => ConfigErrorKind::Rejected,
        Some("readback_mismatch") => ConfigErrorKind::ReadbackMismatch,
        _ => ConfigErrorKind::Internal,
    };
    Some(ConfigError { kind, field: None, message: message.to_owned() })
}

fn decode_device_config(map: &VarDict) -> Result<DeviceConfigSnapshot> {
    let api_version = required_u16(map, "api_version")?;
    if api_version != DEVICE_CONFIG_API_VERSION {
        return Err(Error::Failure(format!(
            "unsupported device-config API version {api_version}"
        )));
    }
    let state = match required_string(map, "state")?.as_str() {
        "disconnected" => DeviceConfigState::Disconnected,
        "loading" => DeviceConfigState::Loading,
        "ready" => DeviceConfigState::Ready,
        "partial" => DeviceConfigState::Partial,
        "error" => DeviceConfigState::Error,
        "unsupported" => DeviceConfigState::Unsupported,
        value => return Err(Error::Failure(format!("unknown device-config state {value}"))),
    };
    let activity_availability = field_availability(map, "health.activity.availability");
    let health = if activity_availability == FieldAvailability::Available {
        let units_availability = field_availability(map, "health.units.availability");
        let distance_units = map
            .get("health.distance_units")
            .and_then(|value| <&str>::try_from(value).ok())
            .map(|value| match value {
                "metric" => DistanceUnits::Metric,
                "imperial" => DistanceUnits::Imperial,
                _ => DistanceUnits::Unknown(255),
            });
        let hrm_availability = field_availability(map, "health.hrm.availability");
        let hrm = if hrm_availability == FieldAvailability::Available {
            Some(HrmConfig {
            enabled: required_bool(map, "health.hrm.enabled")?,
            measurement_interval: map
                .get("health.hrm.measurement_interval")
                .and_then(|value| u8::try_from(value).ok())
                .map(|value| match value {
                    0 => HrmMeasurementInterval::TenMinutes,
                    1 => HrmMeasurementInterval::ThirtyMinutes,
                    2 => HrmMeasurementInterval::OneHour,
                    3 => HrmMeasurementInterval::Off,
                    other => HrmMeasurementInterval::Unknown(other),
                }),
            during_activity: map
                .get("health.hrm.during_activity")
                .and_then(|value| bool::try_from(value).ok()),
            })
        } else {
            None
        };
        let thresholds_availability = field_availability(map, "health.thresholds.availability");
        let thresholds = if thresholds_availability == FieldAvailability::Available {
            Some(HeartRateThresholds {
                resting: required_u8(map, "health.thresholds.resting")?,
                elevated: required_u8(map, "health.thresholds.elevated")?,
                maximum: required_u8(map, "health.thresholds.maximum")?,
                zone_1: required_u8(map, "health.thresholds.zone1")?,
                zone_2: required_u8(map, "health.thresholds.zone2")?,
                zone_3: required_u8(map, "health.thresholds.zone3")?,
            })
        } else {
            None
        };
        Some(HealthConfig {
            height_mm: required_u16(map, "health.height_mm")?,
            weight_dag: required_u16(map, "health.weight_dag")?,
            tracking_enabled: required_bool(map, "health.tracking_enabled")?,
            activity_insights_enabled: required_bool(map, "health.activity_insights_enabled")?,
            sleep_insights_enabled: required_bool(map, "health.sleep_insights_enabled")?,
            age: required_u8(map, "health.age")?,
            gender: required_u8(map, "health.gender")?,
            distance_units: FieldValue { availability: units_availability, value: distance_units, error: None },
            hrm: FieldValue { availability: hrm_availability, value: hrm, error: None },
            heart_rate_thresholds: FieldValue { availability: thresholds_availability, value: thresholds, error: None },
        })
    } else {
        None
    };

    let mut preferences = BTreeMap::new();
    for key in map.keys().filter_map(|key| {
        key.strip_prefix("preference.")?.strip_suffix(".availability")
    }) {
        let availability = field_availability(map, &format!("preference.{key}.availability"));
        let value = map.get(&format!("preference.{key}.value")).and_then(|value| {
            if let Ok(value) = bool::try_from(value) {
                Some(PreferenceValue::Bool(value))
            } else if let Ok(value) = u32::try_from(value) {
                Some(PreferenceValue::Unsigned(value))
            } else {
                <&str>::try_from(value).ok().map(|value| PreferenceValue::Text(value.to_owned()))
            }
        });
        let raw = map
            .get(&format!("preference.{key}.raw"))
            .and_then(|value| value.try_clone().ok())
            .and_then(|value| Vec::<u8>::try_from(value).ok())
            .unwrap_or_default();
        preferences.insert(key.to_owned(), PreferenceField { availability, value, raw, error: None });
    }

    Ok(DeviceConfigSnapshot {
        api_version,
        revision: required_u64(map, "revision")?,
        state,
        watch: map.get("watch_id").and_then(|value| <&str>::try_from(value).ok()).map(|watch_id| WatchIdentity {
            watch_id: watch_id.to_owned(),
            display_name: None,
            platform: map.get("watch_platform").and_then(|value| <&str>::try_from(value).ok()).map(str::to_owned),
            firmware: map.get("watch_firmware").and_then(|value| <&str>::try_from(value).ok()).map(str::to_owned),
        }),
        capabilities: DeviceCapabilities {
            blob_db_version: map.get("blob_db_version").and_then(|value| u8::try_from(value).ok()).unwrap_or(0),
            supported: map.iter().filter_map(|(key, value)| {
                (key.starts_with("capability.") && bool::try_from(value).ok() == Some(true))
                    .then(|| key.trim_start_matches("capability.").to_owned())
            }).collect(),
        },
        last_read_at_ms: map.get("last_read_at_ms").and_then(|value| i64::try_from(value).ok()),
        health: FieldValue { availability: activity_availability, value: health, error: None },
        preferences,
        error: device_config_error(map),
    })
}

fn encode_daemon_config_patch(patch: DaemonConfigPatch) -> Result<VarDict> {
    let mut wire = VarDict::new();
    if let Some(value) = patch.address { wire.insert("address".into(), wire_value(value)?); }
    if let Some(value) = patch.adapter { wire.insert("adapter".into(), wire_value(value)?); }
    if let Some(value) = patch.verbose { wire.insert("verbose".into(), wire_value(value)?); }
    if let Some(value) = patch.database_path {
        wire.insert("database_path".into(), wire_value(value.unwrap_or_default())?);
    }
    if let Some(intervals) = patch.intervals_icu {
        if let Some(value) = intervals.enabled { wire.insert("intervals_enabled".into(), wire_value(value)?); }
        if let Some(value) = intervals.athlete_id { wire.insert("intervals_athlete_id".into(), wire_value(value)?); }
        match intervals.api_key {
            Some(SecretPatch::Replace(value)) => {
                wire.insert("intervals_api_key_replace".into(), wire_value(value)?);
            }
            Some(SecretPatch::Clear) => {
                wire.insert("intervals_api_key_clear".into(), wire_value(true)?);
            }
            None => {}
        }
    }
    Ok(wire)
}

fn encode_device_config_patch(patch: DeviceConfigPatch) -> Result<VarDict> {
    let mut wire = VarDict::new();
    for (key, value) in patch.preferences {
        let value = match value {
            PreferenceValue::Bool(value) => wire_value(value)?,
            PreferenceValue::Unsigned(value) | PreferenceValue::Color(value) => wire_value(value)?,
            PreferenceValue::Enum { code, .. } => wire_value(code)?,
            PreferenceValue::Text(value) => wire_value(value)?,
            PreferenceValue::Unknown => return Err(Error::Failure(format!("cannot write unknown preference {key}"))),
        };
        wire.insert(format!("preference.{key}"), value);
    }
    let Some(health) = patch.health else { return Ok(wire) };
    macro_rules! insert {
        ($key:literal, $value:expr) => {
            if let Some(value) = $value { wire.insert($key.into(), wire_value(value)?); }
        };
    }
    insert!("health.height_mm", health.height_mm);
    insert!("health.weight_dag", health.weight_dag);
    insert!("health.tracking_enabled", health.tracking_enabled);
    insert!("health.activity_insights_enabled", health.activity_insights_enabled);
    insert!("health.sleep_insights_enabled", health.sleep_insights_enabled);
    insert!("health.age", health.age);
    insert!("health.gender", health.gender);
    if let Some(units) = health.distance_units {
        let code = match units { DistanceUnits::Metric => 0, DistanceUnits::Imperial => 1, DistanceUnits::Unknown(code) => code };
        wire.insert("health.distance_units".into(), wire_value(code)?);
    }
    insert!("health.hrm.enabled", health.hrm_enabled);
    if let Some(interval) = health.hrm_measurement_interval {
        let code = match interval {
            HrmMeasurementInterval::TenMinutes => 0,
            HrmMeasurementInterval::ThirtyMinutes => 1,
            HrmMeasurementInterval::OneHour => 2,
            HrmMeasurementInterval::Off => 3,
            HrmMeasurementInterval::Unknown(code) => code,
        };
        wire.insert("health.hrm.measurement_interval".into(), wire_value(code)?);
    }
    insert!("health.hrm.during_activity", health.hrm_during_activity);
    if let Some(value) = health.heart_rate_thresholds {
        wire.insert("health.thresholds.resting".into(), wire_value(value.resting)?);
        wire.insert("health.thresholds.elevated".into(), wire_value(value.elevated)?);
        wire.insert("health.thresholds.maximum".into(), wire_value(value.maximum)?);
        wire.insert("health.thresholds.zone1".into(), wire_value(value.zone_1)?);
        wire.insert("health.thresholds.zone2".into(), wire_value(value.zone_2)?);
        wire.insert("health.thresholds.zone3".into(), wire_value(value.zone_3)?);
    }
    Ok(wire)
}

/// Extract a string field from an `a{sv}` map, or `""` if absent/not a string.
fn var_str(map: &VarDict, key: &str) -> String {
    map.get(key)
        .and_then(|v| <&str>::try_from(v).ok())
        .unwrap_or_default()
        .to_string()
}

/// Watch identity snapshot for display (subset of `GetWatchVersion`/`GetWatchColor`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchInfo {
    pub firmware_version: String,
    pub recovery_version: String,
    /// Watch model codename (from `watch_type`).
    pub model: String,
    pub board: String,
    pub serial: String,
    pub bt_address: String,
    pub language: String,
    /// Human-readable color/variant description.
    pub color: String,
}

/// A daemon/watch status change delivered to [`CobbleClient::watch_status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusEvent {
    /// The daemon (bus-name owner) appeared (`true`) or vanished (`false`).
    DaemonRunning(bool),
    /// The watch BLE link came up (`true`) or went down (`false`).
    Connected(bool),
    /// Battery percentage 0–100, or `-1` if unknown.
    Battery(i16),
    /// Fresh watch identity info (emitted after the link comes up).
    WatchInfo(WatchInfo),
    /// The effective daemon configuration or its on-disk error changed.
    DaemonConfigChanged(u64),
    DeviceConfigChanged { revision: u64, state: DeviceConfigState },
}

/// Typed zbus proxy for `org.cobble.Daemon`.
///
/// All methods mirror the daemon's D-Bus interface exactly.  For one-shot
/// calls prefer the higher-level [`CobbleClient`] methods.  Use this proxy
/// directly when you need to subscribe to signals via the generated
/// `receive_<signal_name>()` methods.
#[proxy(
    interface = "org.cobble.Daemon",
    default_service = "org.cobble.Daemon",
    default_path = "/org/cobble/Daemon"
)]
pub trait CobbleDaemon {
    // ---- Properties ----

    /// `true` when the BLE link to the watch is up.
    #[zbus(property)]
    fn connected(&self) -> Result<bool>;

    /// Configured watch Bluetooth address.
    #[zbus(property)]
    fn watch_address(&self) -> Result<String>;

    /// Watch battery percentage (0–100), or `-1` if unknown/disconnected.
    #[zbus(property)]
    fn battery_level(&self) -> Result<i16>;

    // ---- Methods ----

    async fn send_app_message(
        &self,
        app_uuid: &str,
        data: WireDict,
        wait_ack: bool,
    ) -> Result<u32>;

    async fn launch_app(&self, app_uuid: &str) -> Result<()>;
    async fn stop_app(&self, app_uuid: &str) -> Result<()>;
    async fn update_time(&self) -> Result<()>;
    async fn notify(&self, title: &str, body: &str, subtitle: &str) -> Result<u32>;
    async fn ping(&self) -> Result<bool>;
    async fn scan(&self, timeout_secs: f64) -> Result<Vec<(String, String)>>;
    async fn get_daemon_config(&self) -> Result<VarDict>;
    async fn update_daemon_config(&self, expected_revision: u64, patch: VarDict) -> Result<VarDict>;
    async fn get_device_config(&self) -> Result<VarDict>;
    async fn refresh_device_config(&self) -> Result<VarDict>;
    async fn update_device_config(&self, expected_revision: u64, patch: VarDict) -> Result<VarDict>;

    #[zbus(signal)]
    fn daemon_config_changed(&self, revision: u64) -> Result<()>;

    #[zbus(signal)]
    fn device_config_changed(&self, revision: u64, state: &str) -> Result<()>;

    // ---- Health ----

    async fn activate_health(
        &self,
        height_cm: u16,
        weight_kg: u16,
        age: u8,
        gender: u8,
        hrm_enabled: bool,
    ) -> Result<()>;

    async fn fetch_health_data(&self) -> Result<()>;
    async fn fetch_health_params(&self) -> Result<()>;
    async fn get_health_profile(&self) -> Result<VarDict>;
    async fn reprocess_health_data(&self) -> Result<()>;

    // ---- Watch info / settings ----

    async fn get_watch_settings(&self) -> Result<VarDict>;
    async fn get_watch_version(&self) -> Result<VarDict>;
    async fn get_watch_color(&self) -> Result<VarDict>;

    /// Capture the watch screen, returned as PNG bytes.
    async fn screenshot(&self) -> Result<Vec<u8>>;

    // ---- Music (push now-playing to the watch) ----

    async fn set_music_player_info(&self, pkg: &str, name: &str) -> Result<()>;

    async fn set_music_track(
        &self,
        artist: &str,
        album: &str,
        title: &str,
        track_length_ms: u32,
        track_count: u32,
        track_number: u32,
    ) -> Result<()>;

    /// `state`: 0=paused 1=playing 2=rewinding 3=fast-forwarding 4=unknown.
    /// `shuffle`: 0=unknown 1=off 2=on. `repeat`: 0=unknown 1=off 2=one 3=all.
    async fn set_music_playback_state(
        &self,
        state: u8,
        track_position_ms: u32,
        play_rate_pct: u32,
        shuffle: u8,
        repeat: u8,
    ) -> Result<()>;

    /// `volume_percent` is 0–100.
    async fn set_music_volume(&self, volume_percent: u8) -> Result<()>;

    // ---- Device management ----

    async fn reboot_watch(&self) -> Result<()>;
    async fn reset_into_recovery(&self) -> Result<()>;
    async fn create_core_dump(&self) -> Result<()>;
    /// DESTRUCTIVE — wipes the watch; requires `confirm = true`.
    async fn factory_reset(&self, confirm: bool) -> Result<()>;
    /// Remove the Bluetooth bond (unpair); re-pairs on next reconnect.
    async fn forget(&self) -> Result<()>;

    // ---- Weather ----

    #[allow(clippy::too_many_arguments)]
    async fn push_weather(
        &self,
        location_key: Vec<u8>,
        location_name: &str,
        forecast_short: &str,
        current_temp: i16,
        current_weather: u8,
        today_high: i16,
        today_low: i16,
        tomorrow_weather: u8,
        tomorrow_high: i16,
        tomorrow_low: i16,
        is_current_location: bool,
    ) -> Result<()>;

    // ---- Daemon control ----

    async fn sync_wellness(&self) -> Result<()>;
    async fn get_wellness_sync_status(&self) -> Result<VarDict>;
    async fn reload_config(&self) -> Result<()>;

    // ---- Signals ----

    #[zbus(signal)]
    fn app_message_received(&self, app_uuid: &str, data: WireDict) -> Result<()>;

    #[zbus(signal)]
    fn ack_received(&self, txn: u32) -> Result<()>;

    #[zbus(signal)]
    fn nack_received(&self, txn: u32) -> Result<()>;

    #[zbus(signal)]
    fn connection_changed(&self, connected: bool) -> Result<()>;

    #[zbus(signal)]
    #[allow(clippy::too_many_arguments)]
    fn health_data_received(
        &self,
        tag: u32,
        app_uuid: Vec<u8>,
        session_timestamp: u32,
        items_left: u32,
        crc: u32,
        item_type: u8,
        item_size: u16,
        data: Vec<u8>,
    ) -> Result<()>;

    #[zbus(signal)]
    fn health_profile_received(&self, profile: VarDict) -> Result<()>;

    #[zbus(signal)]
    fn watch_setting_received(&self, key: &str, value: OwnedValue) -> Result<()>;

    #[zbus(signal)]
    fn battery_changed(&self, level: i16) -> Result<()>;

    #[zbus(signal)]
    fn app_run_state_changed(&self, uuid: &str, running: bool) -> Result<()>;

    #[zbus(signal)]
    fn music_action_received(&self, action: &str) -> Result<()>;
}

const BUS_NAME: &str = "org.cobble.Daemon";

/// High-level client for the cobbled daemon.
///
/// Wraps a session D-Bus connection and exposes all daemon methods directly.
/// The underlying [`Connection`] is cheap to clone — pass clones into async
/// tasks rather than creating a new [`CobbleClient`] per call.
#[derive(Clone)]
pub struct CobbleClient {
    conn: Connection,
}

impl CobbleClient {
    /// Connect to the session bus.  Does **not** check whether the daemon is
    /// running; use [`is_running`](Self::is_running) for that.
    pub async fn new() -> Result<Self> {
        let conn = Connection::session().await?;
        Ok(Self { conn })
    }

    /// Returns `true` if the cobbled daemon currently owns its bus name.
    pub async fn is_running(&self) -> bool {
        self.conn
            .call_method(
                Some("org.freedesktop.DBus"),
                "/org/freedesktop/DBus",
                Some("org.freedesktop.DBus"),
                "NameHasOwner",
                &BUS_NAME,
            )
            .await
            .ok()
            .and_then(|reply| reply.body().deserialize::<bool>().ok())
            .unwrap_or(false)
    }

    /// Returns `true` if the daemon is running and the watch BLE link is up.
    pub async fn connected(&self) -> bool {
        let Ok(proxy) = self.proxy().await else { return false };
        proxy.connected().await.unwrap_or(false)
    }

    /// Build a typed proxy for signal subscriptions or less-common calls.
    /// The proxy borrows `self`'s connection; for owned/`'static` use cases
    /// clone the client and call `proxy()` on the clone.
    pub async fn proxy(&self) -> Result<CobbleDaemonProxy<'_>> {
        CobbleDaemonProxy::new(&self.conn).await
    }

    // ---- Properties ----

    pub async fn watch_address(&self) -> Result<String> {
        self.proxy().await?.watch_address().await
    }

    /// Watch battery percentage (0–100), or `-1` if unknown/disconnected.
    pub async fn battery_level(&self) -> Result<i16> {
        self.proxy().await?.battery_level().await
    }

    // ---- Apps / messaging ----

    pub async fn send_app_message(
        &self,
        app_uuid: &str,
        data: WireDict,
        wait_ack: bool,
    ) -> Result<u32> {
        self.proxy().await?.send_app_message(app_uuid, data, wait_ack).await
    }

    pub async fn launch_app(&self, app_uuid: &str) -> Result<()> {
        self.proxy().await?.launch_app(app_uuid).await
    }

    pub async fn stop_app(&self, app_uuid: &str) -> Result<()> {
        self.proxy().await?.stop_app(app_uuid).await
    }

    pub async fn update_time(&self) -> Result<()> {
        self.proxy().await?.update_time().await
    }

    pub async fn notify(&self, title: &str, body: &str, subtitle: &str) -> Result<u32> {
        self.proxy().await?.notify(title, body, subtitle).await
    }

    pub async fn ping(&self) -> Result<bool> {
        self.proxy().await?.ping().await
    }

    pub async fn scan(&self, timeout_secs: f64) -> Result<Vec<(String, String)>> {
        self.proxy().await?.scan(timeout_secs).await
    }

    pub async fn get_daemon_config(&self) -> Result<DaemonConfigSnapshot> {
        let map = self.proxy().await?.get_daemon_config().await?;
        decode_daemon_config(&map)
    }

    pub async fn update_daemon_config(
        &self,
        patch: DaemonConfigPatch,
    ) -> Result<DaemonConfigUpdate> {
        let expected_revision = patch.expected_revision;
        let wire = encode_daemon_config_patch(patch)?;
        let map = self
            .proxy()
            .await?
            .update_daemon_config(expected_revision, wire)
            .await?;
        let snapshot = decode_daemon_config(&map)?;
        let mut fields = BTreeMap::new();
        for (key, value) in &map {
            let Some(field) = key.strip_prefix("apply.") else { continue };
            let disposition = match <&str>::try_from(value).ok() {
                Some("applied_live") => ApplyDisposition::AppliedLive,
                Some("reconnecting") => ApplyDisposition::Reconnecting,
                Some("gui_data_source_reopen_required") => ApplyDisposition::GuiDataSourceReopenRequired,
                Some("daemon_restart_required") => ApplyDisposition::DaemonRestartRequired,
                Some("daemon_and_gui_restart_required") => ApplyDisposition::DaemonAndGuiRestartRequired,
                _ => continue,
            };
            fields.insert(field.to_owned(), disposition);
        }
        Ok(DaemonConfigUpdate { snapshot, fields })
    }

    // ---- Health ----

    pub async fn activate_health(
        &self,
        height_cm: u16,
        weight_kg: u16,
        age: u8,
        gender: u8,
        hrm_enabled: bool,
    ) -> Result<()> {
        self.proxy()
            .await?
            .activate_health(height_cm, weight_kg, age, gender, hrm_enabled)
            .await
    }

    pub async fn fetch_health_data(&self) -> Result<()> {
        self.proxy().await?.fetch_health_data().await
    }

    pub async fn fetch_health_params(&self) -> Result<()> {
        self.proxy().await?.fetch_health_params().await
    }

    /// Health profile keyed by field name (height_cm, weight_kg, age, gender, …).
    pub async fn get_health_profile(&self) -> Result<VarDict> {
        self.proxy().await?.get_health_profile().await
    }

    pub async fn reprocess_health_data(&self) -> Result<()> {
        self.proxy().await?.reprocess_health_data().await
    }

    // ---- Watch info / settings ----

    pub async fn get_watch_settings(&self) -> Result<VarDict> {
        self.proxy().await?.get_watch_settings().await
    }

    pub async fn get_device_config(&self) -> Result<DeviceConfigSnapshot> {
        let map = self.proxy().await?.get_device_config().await?;
        decode_device_config(&map)
    }

    pub async fn refresh_device_config(&self) -> Result<DeviceConfigSnapshot> {
        let map = self.proxy().await?.refresh_device_config().await?;
        decode_device_config(&map)
    }

    pub async fn update_device_config(&self, patch: DeviceConfigPatch) -> Result<DeviceConfigSnapshot> {
        let expected_revision = patch.expected_revision;
        let wire = encode_device_config_patch(patch)?;
        let map = self.proxy().await?.update_device_config(expected_revision, wire).await?;
        decode_device_config(&map)
    }

    pub async fn get_watch_version(&self) -> Result<VarDict> {
        self.proxy().await?.get_watch_version().await
    }

    pub async fn get_watch_color(&self) -> Result<VarDict> {
        self.proxy().await?.get_watch_color().await
    }

    /// Capture the watch screen, returned as PNG bytes.
    pub async fn screenshot(&self) -> Result<Vec<u8>> {
        self.proxy().await?.screenshot().await
    }

    // ---- Music ----

    pub async fn set_music_player_info(&self, pkg: &str, name: &str) -> Result<()> {
        self.proxy().await?.set_music_player_info(pkg, name).await
    }

    pub async fn set_music_track(
        &self,
        artist: &str,
        album: &str,
        title: &str,
        track_length_ms: u32,
        track_count: u32,
        track_number: u32,
    ) -> Result<()> {
        self.proxy()
            .await?
            .set_music_track(artist, album, title, track_length_ms, track_count, track_number)
            .await
    }

    pub async fn set_music_playback_state(
        &self,
        state: u8,
        track_position_ms: u32,
        play_rate_pct: u32,
        shuffle: u8,
        repeat: u8,
    ) -> Result<()> {
        self.proxy()
            .await?
            .set_music_playback_state(state, track_position_ms, play_rate_pct, shuffle, repeat)
            .await
    }

    pub async fn set_music_volume(&self, volume_percent: u8) -> Result<()> {
        self.proxy().await?.set_music_volume(volume_percent).await
    }

    // ---- Device management ----

    pub async fn reboot_watch(&self) -> Result<()> {
        self.proxy().await?.reboot_watch().await
    }

    pub async fn reset_into_recovery(&self) -> Result<()> {
        self.proxy().await?.reset_into_recovery().await
    }

    pub async fn create_core_dump(&self) -> Result<()> {
        self.proxy().await?.create_core_dump().await
    }

    /// DESTRUCTIVE — wipes the watch; requires `confirm = true`.
    pub async fn factory_reset(&self, confirm: bool) -> Result<()> {
        self.proxy().await?.factory_reset(confirm).await
    }

    /// Remove the Bluetooth bond (unpair); re-pairs on next reconnect.
    pub async fn forget(&self) -> Result<()> {
        self.proxy().await?.forget().await
    }

    // ---- Weather ----

    #[allow(clippy::too_many_arguments)]
    pub async fn push_weather(
        &self,
        location_key: Vec<u8>,
        location_name: &str,
        forecast_short: &str,
        current_temp: i16,
        current_weather: u8,
        today_high: i16,
        today_low: i16,
        tomorrow_weather: u8,
        tomorrow_high: i16,
        tomorrow_low: i16,
        is_current_location: bool,
    ) -> Result<()> {
        self.proxy()
            .await?
            .push_weather(
                location_key,
                location_name,
                forecast_short,
                current_temp,
                current_weather,
                today_high,
                today_low,
                tomorrow_weather,
                tomorrow_high,
                tomorrow_low,
                is_current_location,
            )
            .await
    }

    // ---- Daemon control ----

    pub async fn sync_wellness(&self) -> Result<()> {
        self.proxy().await?.sync_wellness().await
    }

    pub async fn get_wellness_sync_status(&self) -> Result<VarDict> {
        self.proxy().await?.get_wellness_sync_status().await
    }

    pub async fn reload_config(&self) -> Result<()> {
        self.proxy().await?.reload_config().await
    }

    // ---- High-level watch status ----

    /// Fetch a watch identity snapshot (firmware/model/board/serial/BT/color).
    /// Only meaningful while the watch is connected.
    pub async fn get_watch_info(&self) -> Result<WatchInfo> {
        let proxy = self.proxy().await?;
        let v = proxy.get_watch_version().await?;
        let mut info = WatchInfo {
            firmware_version: var_str(&v, "firmware_version"),
            recovery_version: var_str(&v, "recovery_version"),
            model: var_str(&v, "watch_type"),
            board: var_str(&v, "board"),
            serial: var_str(&v, "serial"),
            bt_address: var_str(&v, "bt_address"),
            language: var_str(&v, "language"),
            color: String::new(),
        };
        // Color is a separate call; tolerate failure (unknown color / older fw).
        if let Ok(c) = proxy.get_watch_color().await {
            info.color = var_str(&c, "description");
        }
        Ok(info)
    }

    /// Watch daemon/watch status via D-Bus signals (no polling), invoking
    /// `on_event` for every change. Emits the current state up front, then runs
    /// until the bus connection drops. Survives daemon restarts (tracked via the
    /// bus-name owner). Watch info is fetched and emitted whenever the link comes
    /// up.
    pub async fn watch_status<F>(&self, mut on_event: F) -> Result<()>
    where
        F: FnMut(StatusEvent) + Send,
    {
        use futures_util::stream::{select_all, StreamExt};

        let proxy = self.proxy().await?;

        // Initial snapshot — streams only deliver *changes*.
        let running = self.is_running().await;
        on_event(StatusEvent::DaemonRunning(running));
        if running {
            let connected = proxy.connected().await.unwrap_or(false);
            on_event(StatusEvent::Connected(connected));
            on_event(StatusEvent::Battery(proxy.battery_level().await.unwrap_or(-1)));
            if connected
                && let Ok(info) = self.get_watch_info().await
            {
                on_event(StatusEvent::WatchInfo(info));
            }
        }

        // Watch signals (events stream)
        // Merge the daemon-owner, connection, and battery signals into one stream.
        let owner = proxy
            .inner()
            .receive_owner_changed()
            .await?
            .map(|o| StatusEvent::DaemonRunning(o.is_some()))
            .boxed();
        let conn = proxy
            .receive_connection_changed()
            .await?
            .filter_map(|s| async move { s.args().ok().map(|a| StatusEvent::Connected(a.connected)) })
            .boxed();
        let batt = proxy
            .receive_battery_changed()
            .await?
            .filter_map(|s| async move { s.args().ok().map(|a| StatusEvent::Battery(a.level)) })
            .boxed();
        let config = proxy
            .receive_daemon_config_changed()
            .await?
            .filter_map(|s| async move {
                s.args().ok().map(|a| StatusEvent::DaemonConfigChanged(a.revision))
            })
            .boxed();
        let device_config = proxy
            .receive_device_config_changed()
            .await?
            .filter_map(|s| async move {
                let args = s.args().ok()?;
                let state = match args.state {
                    "disconnected" => DeviceConfigState::Disconnected,
                    "loading" => DeviceConfigState::Loading,
                    "ready" => DeviceConfigState::Ready,
                    "partial" => DeviceConfigState::Partial,
                    "error" => DeviceConfigState::Error,
                    "unsupported" => DeviceConfigState::Unsupported,
                    _ => return None,
                };
                Some(StatusEvent::DeviceConfigChanged { revision: args.revision, state })
            })
            .boxed();
        let mut events = select_all([owner, conn, batt, config, device_config]);

        while let Some(ev) = events.next().await {
            on_event(ev.clone());
            match ev {
                // Daemon (re)appeared: re-read the live state streams can't replay.
                StatusEvent::DaemonRunning(true) => {
                    let connected = proxy.connected().await.unwrap_or(false);
                    on_event(StatusEvent::Connected(connected));
                    on_event(StatusEvent::Battery(proxy.battery_level().await.unwrap_or(-1)));
                    if connected
                        && let Ok(info) = self.get_watch_info().await
                    {
                        on_event(StatusEvent::WatchInfo(info));
                    }
                }
                StatusEvent::DaemonRunning(false) => {
                    on_event(StatusEvent::Connected(false));
                    on_event(StatusEvent::Battery(-1));
                }
                // Link came up: pull fresh watch identity.
                StatusEvent::Connected(true) => {
                    if let Ok(info) = self.get_watch_info().await {
                        on_event(StatusEvent::WatchInfo(info));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_database_path_uses_configured_value() {
        let path = std::env::temp_dir().join(format!(
            "cobble-client-config-{}-{}.toml",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::write(&path, "db = '/tmp/custom-cobbled.db'\n").unwrap();
        let resolved = database_path_from_config(&path, PathBuf::from("/tmp/default.db")).unwrap();
        assert_eq!(resolved, PathBuf::from("/tmp/custom-cobbled.db"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn device_config_decoder_keeps_missing_health_absent() {
        let map = HashMap::from([
            ("api_version".into(), wire_value(DEVICE_CONFIG_API_VERSION).unwrap()),
            ("revision".into(), wire_value(3_u64).unwrap()),
            ("state".into(), wire_value("loading").unwrap()),
            ("blob_db_version".into(), wire_value(1_u8).unwrap()),
            ("capability.complete_refresh".into(), wire_value(true).unwrap()),
            ("health.activity.availability".into(), wire_value("not_received").unwrap()),
            ("health.hrm.availability".into(), wire_value("not_received").unwrap()),
            ("health.thresholds.availability".into(), wire_value("not_received").unwrap()),
            ("health.units.availability".into(), wire_value("not_received").unwrap()),
        ]);

        let snapshot = decode_device_config(&map).unwrap();
        assert_eq!(snapshot.state, DeviceConfigState::Loading);
        assert_eq!(snapshot.health.availability, FieldAvailability::NotReceived);
        assert!(snapshot.health.value.is_none());
        assert!(snapshot.capabilities.supported.contains("complete_refresh"));
    }
}
