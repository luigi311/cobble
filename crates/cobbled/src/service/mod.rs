//! D-Bus service interface (org.cobble.Daemon).
//!
//! Interface (org.cobble.Daemon on /org/cobble/Daemon):
//!
//!   Properties
//!     Connected     b    watch BLE link is up right now
//!     WatchAddress  s    configured watch address
//!     BatteryLevel  n    watch battery percentage (0–100), or -1 if unknown
//!
//!   Methods
//!     SendAppMessage(s uuid, a{i(sv)} data, b wait_ack) -> u txn
//!     LaunchApp(s uuid)
//!     StopApp(s uuid)
//!     UpdateTime()
//!     Notify(s title, s body, s subtitle) -> u token
//!     Ping() -> b
//!     Scan(d timeout_secs) -> a(ss)
//!     ActivateHealth(q height_cm, q weight_kg, y age, y gender, b hrm_enabled)
//!     FetchHealthData()
//!     FetchHealthParams()
//!     GetHealthProfile() -> a{sv}  health profile keyed by field name (height_cm, weight_kg, age, gender, …, imperial_units)
//!     GetWatchSettings() -> a{sv}  general watch settings (db 12), key -> bool/uint32/string
//!     GetDeviceConfig() -> a{sv}  coherent versioned device-settings snapshot
//!     RefreshDeviceConfig() -> a{sv}  refresh and return the completed snapshot
//!     GetWatchVersion() -> a{sv}  firmware/board/serial/BT/language/capabilities/platform
//!     GetWatchColor() -> a{sv}  watch color/variant (protocol_number, js_name, description, watch_type, supports_hrm)
//!     Screenshot() -> ay  capture the watch screen as PNG bytes
//!     SetMusicPlayerInfo(s pkg, s name)
//!     SetMusicTrack(s artist, s album, s title, u track_length_ms, u track_count, u track_number)
//!     SetMusicPlaybackState(y state, u track_position_ms, u play_rate_pct, y shuffle, y repeat)
//!     SetMusicVolume(y volume_percent)
//!     RebootWatch()
//!     ResetIntoRecovery()
//!     CreateCoreDump()
//!     FactoryReset(b confirm)  (DESTRUCTIVE; requires confirm=true)
//!     Forget()  remove the Bluetooth bond (unpair); re-pairs on next reconnect
//!     PushWeather(ay location_key, s location_name, s forecast_short, n current_temp, y current_weather, n today_high, n today_low, y tomorrow_weather, n tomorrow_high, n tomorrow_low, b is_current_location)
//!     ReprocessHealthData()
//!     SyncWellness()
//!     GetWellnessSyncStatus() -> a{sv}
//!
//!   Signals
//!     AppMessageReceived(s uuid, a{i(sv)} data)
//!     AckReceived(u txn)
//!     NackReceived(u txn)
//!     ConnectionChanged(b connected)
//!     HealthDataReceived(u tag, ay app_uuid, u session_timestamp, u items_left, u crc, y item_type, q item_size, ay data)
//!     HealthProfileReceived(a{sv} profile)
//!     WatchSettingReceived(s key, v value)
//!     DeviceConfigChanged(t revision, s state)
//!     BatteryChanged(n level)  watch battery percentage (-1 = unknown)
//!     AppRunStateChanged(s uuid, b running)  app opened/closed on the watch
//!     MusicActionReceived(s action)  media-control action from the watch
//!
//! AppMessage values cross the D-Bus hop as (tag, payload) pairs; see codec.rs.
#![allow(clippy::too_many_arguments)]

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use libpebble_ble::{
    ActivityPreferences, HeartRatePreferences, HrmPreferences,
    MusicPlaybackState, MusicRepeat, MusicShuffle, Pebble, WatchColorInfo, WatchPrefValue,
    WatchVersionInfo, WeatherType, HrMonitoringInterval, WatchPrefModel, WatchType,
    decode_watch_pref_for_model, encode_watch_pref,
};

use cobble_db::{AppDb, DateRange, WellnessExportStatus};
use cobble_config::{Config, IntervalsIcuConfig};
use cobble_contracts::{CONFIG_API_VERSION, DEVICE_CONFIG_API_VERSION, DeviceConfigState};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot, watch};
use tracing::{debug, warn};
use zbus::{
    interface,
    object_server::SignalEmitter,
    zvariant::OwnedValue,
    Connection,
};

use crate::codec::{decode_wire_dict, encode_wire_dict, WireDict};
use crate::notification::app_name_to_category;

fn daemon_config_map(
    config: &Config,
    config_path: &std::path::Path,
    revision: u64,
    error: Option<&str>,
    active_database_path: &std::path::Path,
    active_verbose: bool,
) -> Result<HashMap<String, OwnedValue>, DaemonError> {
    let resolved_db = crate::config::resolved_db_path(config)
        .map_err(|error| DaemonError::Failed(error.to_string()))?;
    let mut result = HashMap::from([
        ("api_version".into(), dbus_val(CONFIG_API_VERSION)),
        ("revision".into(), dbus_val(revision)),
        ("config_path".into(), dbus_val(config_path.display().to_string())),
        ("active_database_path".into(), dbus_val(active_database_path.display().to_string())),
        ("resolved_database_path".into(), dbus_val(resolved_db.display().to_string())),
        ("active_verbose".into(), dbus_val(active_verbose)),
        ("address".into(), dbus_val(config.address.clone())),
        ("adapter".into(), dbus_val(config.adapter.clone())),
        ("verbose".into(), dbus_val(config.verbose)),
        ("database_path".into(), dbus_val(config.db.clone().unwrap_or_default())),
        ("intervals_enabled".into(), dbus_val(config.integrations.intervals_icu.enabled)),
        ("intervals_athlete_id".into(), dbus_val(config.integrations.intervals_icu.athlete_id.clone())),
        ("intervals_api_key_configured".into(), dbus_val(!config.integrations.intervals_icu.api_key.is_empty())),
    ]);
    if let Some(message) = error {
        result.insert("error_kind".into(), dbus_val("invalid_data"));
        result.insert("error_message".into(), dbus_val(message.to_owned()));
    }
    Ok(result)
}

fn patch_string(patch: &HashMap<String, OwnedValue>, key: &str) -> Result<Option<String>, DaemonError> {
    patch.get(key).map(|value| {
        <&str>::try_from(value).map(str::to_owned)
            .map_err(|_| DaemonError::Failed(format!("patch field {key} must be a string")))
    }).transpose()
}

fn patch_bool(patch: &HashMap<String, OwnedValue>, key: &str) -> Result<Option<bool>, DaemonError> {
    patch.get(key).map(|value| {
        bool::try_from(value)
            .map_err(|_| DaemonError::Failed(format!("patch field {key} must be a boolean")))
    }).transpose()
}

fn device_config_state_name(state: DeviceConfigState) -> &'static str {
    match state {
        DeviceConfigState::Disconnected => "disconnected",
        DeviceConfigState::Loading => "loading",
        DeviceConfigState::Ready => "ready",
        DeviceConfigState::Partial => "partial",
        DeviceConfigState::Error => "error",
        DeviceConfigState::Unsupported => "unsupported",
    }
}

fn patch_u8(patch: &HashMap<String, OwnedValue>, key: &str) -> Result<Option<u8>, DaemonError> {
    patch.get(key).map(|value| u8::try_from(value)
        .map_err(|_| DaemonError::Failed(format!("invalid {key}")))).transpose()
}

fn patch_u16(patch: &HashMap<String, OwnedValue>, key: &str) -> Result<Option<u16>, DaemonError> {
    patch.get(key).map(|value| u16::try_from(value)
        .map_err(|_| DaemonError::Failed(format!("invalid {key}")))).transpose()
}

async fn write_records_in_order<F, Fut>(
    writes: Vec<(&'static str, Vec<u8>)>,
    mut write: F,
) -> Result<(), String>
where
    F: FnMut(&'static str, Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    for (key, value) in writes {
        write(key, value).await.map_err(|error| format!("{key}: {error}"))?;
    }
    Ok(())
}

fn completed_write_state(blob_db_version: u8, failed: bool) -> DeviceConfigState {
    if failed { DeviceConfigState::Error }
    else if blob_db_version >= 1 { DeviceConfigState::Ready }
    else { DeviceConfigState::Partial }
}

fn preference_model(watch: Option<&WatchVersionInfo>) -> WatchPrefModel {
    match watch.map(WatchVersionInfo::watch_type) {
        Some(WatchType::Emery) => WatchPrefModel::Emery,
        Some(WatchType::Gabbro) => WatchPrefModel::Gabbro,
        _ => WatchPrefModel::Standard,
    }
}

fn device_config_map(state: &DaemonState) -> HashMap<String, OwnedValue> {
    let mut result = HashMap::from([
        ("api_version".into(), dbus_val(DEVICE_CONFIG_API_VERSION)),
        ("revision".into(), dbus_val(state.device_config_revision)),
        ("state".into(), dbus_val(device_config_state_name(state.device_config_state))),
        ("blob_db_version".into(), dbus_val(state.device_config_blob_db_version)),
        (
            "capability.complete_refresh".into(),
            dbus_val(state.device_config_blob_db_version >= 1),
        ),
    ]);
    if let Some(timestamp) = state.device_config_last_read_at_ms {
        result.insert("last_read_at_ms".into(), dbus_val(timestamp));
    }
    if let Some(info) = &state.device_config_watch {
        result.insert("watch_id".into(), dbus_val(if info.serial.is_empty() {
            info.bt_address.clone()
        } else {
            info.serial.clone()
        }));
        result.insert("watch_platform".into(), dbus_val(info.watch_type().codename()));
        result.insert("watch_firmware".into(), dbus_val(info.running.string_version.clone()));
    }
    if let Some(error) = &state.device_config_error {
        let lower = error.to_ascii_lowercase();
        let kind = if lower.contains("readback mismatch") || lower.contains("was not returned") {
            "readback_mismatch"
        } else if lower.contains("rejected") {
            "rejected"
        } else if lower.contains("timed out") || lower.contains("timeout") {
            "timeout"
        } else if lower.contains("not connected") || lower.contains("disconnected") {
            "disconnected"
        } else if state.device_config_blob_db_version == 0 { "not_supported" } else { "internal" };
        result.insert(
            "error_kind".into(),
            dbus_val(kind),
        );
        result.insert("error_message".into(), dbus_val(error.clone()));
    }

    if let Some(profile) = state.device_health_activity {
        result.insert("health.activity.availability".into(), dbus_val("available"));
        result.insert("health.height_mm".into(), dbus_val(profile.height_mm));
        result.insert("health.weight_dag".into(), dbus_val(profile.weight_dag));
        result.insert("health.age".into(), dbus_val(profile.age));
        result.insert("health.gender".into(), dbus_val(profile.gender));
        result.insert("health.tracking_enabled".into(), dbus_val(profile.tracking_enabled));
        result.insert("health.activity_insights_enabled".into(), dbus_val(profile.activity_insights_enabled));
        result.insert("health.sleep_insights_enabled".into(), dbus_val(profile.sleep_insights_enabled));
    } else {
        result.insert("health.activity.availability".into(), dbus_val("not_received"));
    }
    if let Some(hrm) = state.hrm_prefs {
        result.insert("health.hrm.availability".into(), dbus_val("available"));
        result.insert("health.hrm.enabled".into(), dbus_val(hrm.enabled));
        if let Some(interval) = hrm.measurement_interval {
            result.insert("health.hrm.measurement_interval".into(), dbus_val(interval.code()));
        }
        if let Some(enabled) = hrm.activity_tracking_enabled {
            result.insert("health.hrm.during_activity".into(), dbus_val(enabled));
        }
    } else {
        result.insert("health.hrm.availability".into(), dbus_val("not_received"));
    }
    if let Some(hr) = state.heart_rate_prefs {
        result.insert("health.thresholds.availability".into(), dbus_val("available"));
        result.insert("health.thresholds.resting".into(), dbus_val(hr.resting_hr));
        result.insert("health.thresholds.elevated".into(), dbus_val(hr.elevated_hr));
        result.insert("health.thresholds.maximum".into(), dbus_val(hr.max_hr));
        result.insert("health.thresholds.zone1".into(), dbus_val(hr.zone1_threshold));
        result.insert("health.thresholds.zone2".into(), dbus_val(hr.zone2_threshold));
        result.insert("health.thresholds.zone3".into(), dbus_val(hr.zone3_threshold));
    } else {
        result.insert("health.thresholds.availability".into(), dbus_val("not_received"));
    }
    if let Some(imperial) = state.imperial_units {
        result.insert("health.units.availability".into(), dbus_val("available"));
        result.insert("health.distance_units".into(), dbus_val(if imperial { "imperial" } else { "metric" }));
    } else {
        result.insert("health.units.availability".into(), dbus_val("not_received"));
    }
    let model = preference_model(state.device_config_watch.as_ref());
    for (key, cached_value) in &state.watch_settings {
        let decoded = state.watch_setting_raw.get(key)
            .and_then(|raw| decode_watch_pref_for_model(key, raw, model));
        let value = decoded.as_ref().unwrap_or(cached_value);
        result.insert(format!("preference.{key}.availability"), dbus_val("available"));
        result.insert(format!("preference.{key}.value"), watch_pref_owned_value(value));
        if let Some(raw) = state.watch_setting_raw.get(key) {
            result.insert(format!("preference.{key}.raw"), dbus_val(raw.clone()));
        }
    }
    result
}

mod state;
pub(crate) use state::{
    DaemonError, DaemonEvent, DaemonState, HealthProfile, MusicState,
    BUS_NAME, MUSIC_APP_UUID, OBJECT_PATH,
    dbus_val, watch_pref_owned_value,
};

/// Render watch version info as a self-describing `a{sv}` map. Optional fields
/// (recovery firmware, health/JS versions) are omitted when absent.
fn watch_version_to_map(info: &WatchVersionInfo) -> HashMap<String, OwnedValue> {
    let r = &info.running;
    let mut m: HashMap<String, OwnedValue> = HashMap::from([
        ("firmware_version".into(), dbus_val(r.string_version.clone())),
        ("firmware_major".into(), dbus_val(r.major)),
        ("firmware_minor".into(), dbus_val(r.minor)),
        ("firmware_patch".into(), dbus_val(r.patch)),
        ("firmware_suffix".into(), dbus_val(r.suffix.clone())),
        ("firmware_git_hash".into(), dbus_val(r.git_hash.clone())),
        ("is_recovery".into(), dbus_val(r.is_recovery)),
        ("bootloader_timestamp".into(), dbus_val(info.bootloader_timestamp)),
        ("board".into(), dbus_val(info.board.clone())),
        ("serial".into(), dbus_val(info.serial.clone())),
        ("bt_address".into(), dbus_val(info.bt_address.clone())),
        ("resource_crc".into(), dbus_val(info.resource_crc)),
        ("resource_timestamp".into(), dbus_val(info.resource_timestamp)),
        ("language".into(), dbus_val(info.language.clone())),
        ("language_version".into(), dbus_val(info.language_version)),
        ("hardware_platform".into(), dbus_val(info.hardware_platform)),
        ("platform_revision".into(), dbus_val(info.platform_revision())),
        ("watch_type".into(), dbus_val(info.watch_type().codename())),
        ("capabilities".into(), dbus_val(info.capabilities)),
        ("is_unfaithful".into(), dbus_val(info.is_unfaithful)),
    ]);
    if let Some(recovery) = &info.recovery {
        m.insert("recovery_version".into(), dbus_val(recovery.string_version.clone()));
    }
    if let Some(v) = info.health_insights_version {
        m.insert("health_insights_version".into(), dbus_val(v));
    }
    if let Some(v) = info.javascript_version {
        m.insert("javascript_version".into(), dbus_val(v));
    }
    m
}

/// Encode RGBA8888 pixels as a PNG.
fn encode_png(width: u32, height: u32, rgba: &[u8]) -> Result<Vec<u8>, DaemonError> {
    let mut out = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut out, width, height);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder
            .write_header()
            .map_err(|e| DaemonError::Failed(format!("png header: {e}")))?;
        writer
            .write_image_data(rgba)
            .map_err(|e| DaemonError::Failed(format!("png data: {e}")))?;
    }
    Ok(out)
}

/// Render watch color info as a self-describing `a{sv}` map.
fn watch_color_to_map(c: &WatchColorInfo) -> HashMap<String, OwnedValue> {
    HashMap::from([
        ("protocol_number".into(), dbus_val(c.protocol_number)),
        ("js_name".into(), dbus_val(c.js_name)),
        ("description".into(), dbus_val(c.description)),
        ("watch_type".into(), dbus_val(c.watch_type.codename())),
        ("supports_hrm".into(), dbus_val(c.supports_hrm)),
    ])
}

fn wellness_status_to_dbus_map(
    config: &IntervalsIcuConfig,
    status: WellnessExportStatus,
    running: bool,
) -> HashMap<String, OwnedValue> {
    let format_timestamp = |timestamp: Option<i64>| {
        timestamp
            .and_then(|seconds| chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0))
            .map(|date| date.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_default()
    };
    HashMap::from([
        ("enabled".into(), dbus_val(config.enabled)),
        ("configured".into(), dbus_val(config.is_configured())),
        ("valid".into(), dbus_val(config.validate().is_ok())),
        ("running".into(), dbus_val(running)),
        ("athlete_id".into(), dbus_val(config.athlete_id.clone())),
        ("exported_dates".into(), dbus_val(status.exported_dates as u64)),
        ("pending_dates".into(), dbus_val(status.pending_dates as u64)),
        (
            "last_success".into(),
            dbus_val(format_timestamp(status.last_success_at)),
        ),
        (
            "last_error".into(),
            dbus_val(status.last_error.unwrap_or_default()),
        ),
        (
            "last_error_at".into(),
            dbus_val(format_timestamp(status.last_error_at)),
        ),
    ])
}

struct CachedWellnessStatus {
    revision: u64,
    account_id: String,
    status: WellnessExportStatus,
}

// ---------------------------------------------------------------------------
// CobbleDaemon
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct CobbleDaemon {
    state: Arc<Mutex<DaemonState>>,
    /// Bumped by reload_config so the supervisor can wait event-driven
    /// when no address is configured.
    config_revision: watch::Sender<u64>,
    /// Latest integration configuration; changes wake the exporter without
    /// being treated as BLE connection parameter changes.
    integration_config: watch::Sender<IntervalsIcuConfig>,
    /// Coalesced manual sync requests for the wellness exporter.
    wellness_sync_tx: watch::Sender<u64>,
    /// True from request/start until the serialized reconciliation completes.
    wellness_running: Arc<AtomicBool>,
    /// Bumped when wellness data, configuration, or export state changes.
    wellness_status_revision: Arc<AtomicU64>,
    /// Hash-aware status snapshot reused between unchanged D-Bus polls.
    wellness_status_cache: Arc<Mutex<Option<CachedWellnessStatus>>>,
    /// Notifies subscribers when the watch connects or disconnects.
    connection_tx: watch::Sender<bool>,
    /// Forwards watch music-control actions to the MPRIS monitor.
    music_action_tx: mpsc::UnboundedSender<String>,
    /// Forwards watch phone actions to the call monitor.
    phone_action_tx: mpsc::UnboundedSender<(String, u32)>,
    config_operation: Arc<AsyncMutex<()>>,
    device_config_operation: Arc<AsyncMutex<()>>,
}

impl CobbleDaemon {

    pub fn new(
        config: Config,
        wellness_sync_tx: watch::Sender<u64>,
        wellness_running: Arc<AtomicBool>,
        wellness_status_revision: Arc<AtomicU64>,
        config_path: PathBuf,
        active_database_path: PathBuf,
        active_verbose: bool,
        event_tx: mpsc::UnboundedSender<DaemonEvent>,
        db: Option<Arc<Mutex<AppDb>>>,
        music_action_tx: mpsc::UnboundedSender<String>,
        phone_action_tx: mpsc::UnboundedSender<(String, u32)>,
    ) -> Self {
        let (config_revision, _) = watch::channel(0);
        let (integration_config, _) = watch::channel(config.integrations.intervals_icu.clone());
        let (connection_tx, _) = watch::channel(false);
        Self {
            state: Arc::new(Mutex::new(DaemonState {
                address: config.address.clone(),
                adapter: config.adapter.clone(),
                config_path,
                config,
                config_error: None,
                active_database_path,
                active_verbose,
                pebble: None,
                connected: false,
                stopping: false,
                notify_blocklist: vec!["".to_string()],
                event_tx,
                db,
                health_profile: None,
                device_health_activity: None,
                device_health_activity_raw: None,
                hrm_prefs: None,
                hrm_prefs_raw: None,
                heart_rate_prefs: None,
                heart_rate_prefs_raw: None,
                imperial_units: None,
                imperial_units_raw: None,
                watch_settings: HashMap::new(),
                watch_setting_raw: HashMap::new(),
                device_config_revision: 0,
                device_config_state: DeviceConfigState::Disconnected,
                device_config_last_read_at_ms: None,
                device_config_watch: None,
                device_config_blob_db_version: 0,
                device_config_error: None,
                battery_level: None,
                music: MusicState::default(),
            })),
            config_revision,
            integration_config,
            wellness_sync_tx,
            wellness_running,
            wellness_status_revision,
            wellness_status_cache: Arc::new(Mutex::new(None)),
            music_action_tx,
            phone_action_tx,
            connection_tx,
            config_operation: Arc::new(AsyncMutex::new(())),
            device_config_operation: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Returns the current (address, adapter) used by the supervisor on each reconnect.
    pub fn current_connection_params(&self) -> (String, String) {
        let s = self.state.lock().unwrap();
        (s.address.clone(), s.adapter.clone())
    }

    pub(crate) fn event_tx(&self) -> mpsc::UnboundedSender<DaemonEvent> {
        self.state.lock().unwrap().event_tx.clone()
    }

    /// Returns a receiver that fires when [`reload_config`] is called.
    /// Used by the supervisor to wait event-driven when no address is set.
    pub fn config_changed(&self) -> watch::Receiver<u64> {
        self.config_revision.subscribe()
    }

    /// Returns a receiver that fires when integration settings change.
    pub fn integration_config_changed(&self) -> watch::Receiver<IntervalsIcuConfig> {
        self.integration_config.subscribe()
    }

    /// Returns a receiver that fires when the watch connects or disconnects.
    pub fn watch_connection(&self) -> watch::Receiver<bool> {
        self.connection_tx.subscribe()
    }

    /// Returns the shared app database handle, if available.
    pub fn db(&self) -> Option<Arc<Mutex<AppDb>>> {
        self.state.lock().unwrap().db.clone()
    }

    /// Returns a clone of the music-action sender; used by the signal
    /// emitter to forward watch control actions to the MPRIS monitor.
    pub(crate) fn music_action_tx(&self) -> mpsc::UnboundedSender<String> {
        self.music_action_tx.clone()
    }

    /// Returns a clone of the phone-action sender.
    pub(crate) fn phone_action_tx(&self) -> mpsc::UnboundedSender<(String, u32)> {
        self.phone_action_tx.clone()
    }

    pub(crate) fn require_pebble(&self) -> Result<Arc<Pebble>, DaemonError> {
        let state = self.state.lock().unwrap();
        if !state.connected {
            return Err(DaemonError::NotConnected("watch is not connected".into()));
        }
        state.pebble.clone().ok_or_else(|| DaemonError::NotConnected("watch is not connected".into()))
    }

    /// Called by the supervisor when the watch connects.
    pub fn set_connected(&self, pebble: Arc<Pebble>) {
        {
            let mut state = self.state.lock().unwrap();
            state.pebble = Some(pebble);
            state.connected = true;
            let _ = state.event_tx.send(DaemonEvent::ConnectionChanged(true));
        }
        self.connection_tx.send_replace(true);
        self.reset_device_config(DeviceConfigState::Loading);
    }

    fn reset_device_config(&self, new_state: DeviceConfigState) {
        let (event_tx, revision) = {
            let mut state = self.state.lock().unwrap();
            state.health_profile = None;
            state.device_health_activity = None;
            state.device_health_activity_raw = None;
            state.hrm_prefs = None;
            state.hrm_prefs_raw = None;
            state.heart_rate_prefs = None;
            state.heart_rate_prefs_raw = None;
            state.imperial_units = None;
            state.imperial_units_raw = None;
            state.watch_settings.clear();
            state.watch_setting_raw.clear();
            state.device_config_watch = None;
            state.device_config_blob_db_version = 0;
            state.device_config_last_read_at_ms = None;
            state.device_config_error = None;
            state.device_config_state = new_state;
            state.device_config_revision = state.device_config_revision.wrapping_add(1);
            (state.event_tx.clone(), state.device_config_revision)
        };
        let _ = event_tx.send(DaemonEvent::DeviceConfigChanged {
            revision,
            state: new_state,
        });
    }

    fn note_device_config_value(&self) {
        let event = {
            let mut state = self.state.lock().unwrap();
            if matches!(state.device_config_state, DeviceConfigState::Loading) {
                None
            } else {
                state.device_config_revision = state.device_config_revision.wrapping_add(1);
                Some((
                    state.event_tx.clone(),
                    state.device_config_revision,
                    state.device_config_state,
                ))
            }
        };
        if let Some((event_tx, revision, state)) = event {
            let _ = event_tx.send(DaemonEvent::DeviceConfigChanged { revision, state });
        }
    }

    fn complete_device_config_refresh(
        &self,
        info: Option<WatchVersionInfo>,
        blob_db_version: u8,
        error: Option<String>,
    ) -> (u64, DeviceConfigState) {
        let mut state = self.state.lock().unwrap();
        state.device_config_watch = info;
        state.device_config_blob_db_version = blob_db_version;
        state.device_config_error = error.clone();
        state.device_config_last_read_at_ms = error.is_none().then(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64
        });
        state.device_config_state = if !state.connected {
            DeviceConfigState::Disconnected
        } else if blob_db_version == 0 {
            DeviceConfigState::Unsupported
        } else if state.device_config_watch.is_none() {
            DeviceConfigState::Partial
        } else if error.is_some() {
            DeviceConfigState::Error
        } else {
            DeviceConfigState::Ready
        };
        state.device_config_revision = state.device_config_revision.wrapping_add(1);
        (state.device_config_revision, state.device_config_state)
    }

    /// Cache the demographic health profile synced from the watch and return the
    /// merged snapshot (profile + last-known HRM flag) for signal emission.
    pub(crate) fn cache_health_profile(&self, prefs: ActivityPreferences) -> HealthProfile {
        let mut s = self.state.lock().unwrap();
        s.health_profile = Some(prefs);
        Self::merged_profile(&s).expect("profile just set")
    }

    pub(crate) fn cache_health_activity_raw(
        &self,
        config: libpebble_ble::HealthActivityConfig,
        raw: Vec<u8>,
    ) {
        let mut state = self.state.lock().unwrap();
        state.device_health_activity = Some(config);
        state.device_health_activity_raw = Some(raw);
    }

    /// Cache the HRM record. Returns the merged snapshot only if the demographic
    /// profile is already known (otherwise there is nothing useful to signal yet).
    pub(crate) fn cache_hrm(&self, hrm: HrmPreferences, raw: Vec<u8>) -> Option<HealthProfile> {
        let mut s = self.state.lock().unwrap();
        s.hrm_prefs = Some(hrm);
        s.hrm_prefs_raw = Some(raw);
        Self::merged_profile(&s)
    }

    /// Cache the heart-rate record. Returns the merged snapshot only if the
    /// demographic profile is already known.
    pub(crate) fn cache_heart_rate(&self, hr: HeartRatePreferences, raw: Vec<u8>) -> Option<HealthProfile> {
        let mut s = self.state.lock().unwrap();
        s.heart_rate_prefs = Some(hr);
        s.heart_rate_prefs_raw = Some(raw);
        Self::merged_profile(&s)
    }

    /// Cache the distance-units flag (true = imperial). Returns the merged
    /// snapshot only if the demographic profile is already known.
    pub(crate) fn cache_units(&self, imperial: bool, raw: Vec<u8>) -> Option<HealthProfile> {
        let mut s = self.state.lock().unwrap();
        s.imperial_units = Some(imperial);
        s.imperial_units_raw = Some(raw);
        Self::merged_profile(&s)
    }

    /// Cache a decoded general watch setting (db 12).
    pub(crate) fn cache_watch_setting(&self, key: String, value: WatchPrefValue, raw: Vec<u8>) {
        let mut state = self.state.lock().unwrap();
        state.watch_setting_raw.insert(key.clone(), raw);
        state.watch_settings.insert(key, value);
    }

    /// Re-send the last pushed music state to the watch — used to answer the
    /// watch's GetCurrentTrack request (e.g. when its music app opens).
    pub(crate) async fn replay_music_state(&self) {
        let (pebble, music) = {
            let s = self.state.lock().unwrap();
            (s.pebble.clone(), s.music.clone())
        };
        let Some(pebble) = pebble else { return };
        debug!(
            "replaying music to watch: player={} track={} state={}",
            music.player.is_some(),
            music.track.is_some(),
            music.play_state.is_some(),
        );
        if let Some((pkg, name)) = music.player {
            let _ = pebble.update_music_player_info(&pkg, &name).await;
        }
        if let Some((artist, album, title, len, count, num)) = music.track {
            let _ = pebble
                .update_music_track(&artist, &album, &title, Some(len), Some(count), Some(num))
                .await;
        }
        if let Some((state, pos, rate, shuffle, repeat)) = music.play_state {
            let _ = pebble
                .update_music_play_state(
                    MusicPlaybackState::from_u8(state),
                    pos,
                    rate,
                    MusicShuffle::from_u8(shuffle),
                    MusicRepeat::from_u8(repeat),
                )
                .await;
        }
        if let Some(volume) = music.volume {
            let _ = pebble.update_music_volume(volume).await;
        }
    }

    /// Cache a battery level, but only while connected and only if it changed.
    /// Returns true when the cache was updated (caller should emit a signal).
    /// Dropping events while disconnected preserves the "-1 = unknown" contract
    /// against late notifications from a torn-down session.
    pub(crate) fn set_battery_level(&self, level: u8) -> bool {
        let mut state = self.state.lock().unwrap();
        if !state.connected || state.battery_level == Some(level) {
            return false;
        }
        state.battery_level = Some(level);
        true
    }

    /// The battery level held by the live watch session, if any.
    pub(crate) fn session_battery_level(&self) -> Option<u8> {
        let pebble = self.state.lock().unwrap().pebble.clone();
        pebble.and_then(|p| p.battery_level())
    }

    fn merged_profile(s: &DaemonState) -> Option<HealthProfile> {
        let p = s.health_profile?;
        let hrm = s.hrm_prefs;
        let hr = s.heart_rate_prefs;
        Some(HealthProfile {
            height_cm: p.height_cm,
            weight_kg: p.weight_kg,
            age: p.age as u16,
            gender: p.gender as u16,
            tracking_enabled: p.tracking_enabled,
            activity_insights_enabled: p.activity_insights_enabled,
            sleep_insights_enabled: p.sleep_insights_enabled,
            hrm_enabled: hrm.map(|h| h.enabled).unwrap_or(false),
            hrm_measurement_interval: hrm
                .and_then(|h| h.measurement_interval)
                .map(|i| i.code())
                .unwrap_or(255),
            hrm_activity_tracking: hrm.and_then(|h| h.activity_tracking_enabled).unwrap_or(false),
            resting_hr: hr.map(|h| h.resting_hr as u16).unwrap_or(0),
            elevated_hr: hr.map(|h| h.elevated_hr as u16).unwrap_or(0),
            max_hr: hr.map(|h| h.max_hr as u16).unwrap_or(0),
            hr_zone1_threshold: hr.map(|h| h.zone1_threshold as u16).unwrap_or(0),
            hr_zone2_threshold: hr.map(|h| h.zone2_threshold as u16).unwrap_or(0),
            hr_zone3_threshold: hr.map(|h| h.zone3_threshold as u16).unwrap_or(0),
            imperial_units: s.imperial_units.unwrap_or(false),
        })
    }

    /// Called by the supervisor when the watch disconnects.
    pub fn set_disconnected(&self) {
        let mut state = self.state.lock().unwrap();
        state.connected = false;
        state.pebble = None;
        // Drop watch-scoped session state so a different watch reconnecting
        // doesn't serve the previous watch's stale profile/settings until it
        // re-syncs. The cache_* handlers rebuild these from the new session.
        state.health_profile = None;
        state.device_health_activity = None;
        state.hrm_prefs = None;
        state.heart_rate_prefs = None;
        state.imperial_units = None;
        state.watch_settings.clear();
        state.watch_setting_raw.clear();
        state.battery_level = None;
        state.music = MusicState::default();
        let _ = state.event_tx.send(DaemonEvent::ConnectionChanged(false));
        drop(state);
        self.connection_tx.send_replace(false);
        self.reset_device_config(DeviceConfigState::Disconnected);
    }

    pub fn is_stopping(&self) -> bool {
        self.state.lock().unwrap().stopping
    }

    pub fn set_stopping(&self) {
        self.state.lock().unwrap().stopping = true;
    }

    /// Forward a desktop notification to the watch (called by NotificationMonitor).
    pub fn on_desktop_notification(&self, app_name: String, summary: String, body: String) {
        let state = self.state.lock().unwrap();
        if !state.connected {
            debug!("watch down; dropping notification from {app_name:?}");
            return;
        }
        if state.notify_blocklist.iter().any(|b| b.eq_ignore_ascii_case(&app_name)) {
            debug!("filtered notification from {app_name:?}");
            return;
        }
        if summary.is_empty() && body.is_empty() {
            return;
        }
        if let Some(pebble) = state.pebble.clone() {
            drop(state);
            let category = app_name_to_category(&app_name);
            debug!("notification from {app_name:?} -> category {category:?}");
            tokio::spawn(async move {
                if let Err(e) = pebble.send_notification(&summary, &body, &app_name, category).await {
                    warn!("send notification failed: {e}");
                }
            });
        }
    }

    fn publish_config_revision(&self) {
        self.config_revision.send_modify(|revision| *revision += 1);
        let revision = *self.config_revision.borrow();
        let event_tx = self.state.lock().unwrap().event_tx.clone();
        let _ = event_tx.send(DaemonEvent::DaemonConfigChanged(revision));
    }

    fn record_config_error(&self, message: String) {
        let changed = {
            let mut state = self.state.lock().unwrap();
            if state.config_error.as_deref() == Some(message.as_str()) {
                false
            } else {
                state.config_error = Some(message);
                true
            }
        };
        if changed {
            self.publish_config_revision();
        }
    }

    async fn apply_loaded_config(&self, new_cfg: Config) {
        let (previous, cleared_error) = {
            let mut state = self.state.lock().unwrap();
            let previous = state.config.clone();
            let cleared_error = state.config_error.take().is_some();
            (previous, cleared_error)
        };
        if previous == new_cfg {
            if cleared_error {
                self.publish_config_revision();
            }
            return;
        }
        debug!(
            "reload_config: adapter={}, address{}, intervals_icu={:?}",
            new_cfg.adapter,
            if new_cfg.address.is_empty() { " (none)" } else { " set" },
            new_cfg.redacted_intervals_icu(),
        );
        let pebble_to_disconnect = {
            let mut state = self.state.lock().unwrap();
            let changed = state.address != new_cfg.address || state.adapter != new_cfg.adapter;
            state.address = new_cfg.address.clone();
            state.adapter = new_cfg.adapter.clone();
            state.config = new_cfg.clone();
            if changed { state.pebble.clone() } else { None }
        };
        let integration_config = new_cfg.integrations.intervals_icu.clone();
        let integration_changed = self.integration_config.send_if_modified(|current| {
            if *current == integration_config { false } else { *current = integration_config.clone(); true }
        });
        if integration_changed { self.wellness_status_revision.fetch_add(1, Ordering::SeqCst); }
        if let Some(pebble) = pebble_to_disconnect { let _ = pebble.disconnect().await; }
        self.publish_config_revision();
    }
}

// ---------------------------------------------------------------------------
// zbus interface
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
#[interface(name = "org.cobble.Daemon")]
impl CobbleDaemon {
    // ---- Properties ----

    #[zbus(property)]
    fn connected(&self) -> bool {
        self.state.lock().unwrap().connected
    }

    #[zbus(property)]
    fn watch_address(&self) -> String {
        self.state.lock().unwrap().address.clone()
    }

    /// Watch battery percentage (0–100), or -1 if unknown/disconnected.
    #[zbus(property)]
    fn battery_level(&self) -> i16 {
        self.state.lock().unwrap().battery_level.map(i16::from).unwrap_or(-1)
    }

    // ---- Methods ----

    async fn send_app_message(
        &self,
        app_uuid: String,
        data: WireDict,
        wait_ack: bool,
    ) -> Result<u32, DaemonError> {
        let pebble = self.require_pebble()?;
        let decoded = decode_wire_dict(data);
        let txn = pebble
            .send_app_message(&app_uuid, decoded, wait_ack, 5.0)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        debug!("D-Bus SendAppMessage uuid={app_uuid} wait_ack={wait_ack} -> txn={txn}");
        Ok(txn as u32)
    }

    async fn launch_app(&self, app_uuid: String) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.launch_app(&app_uuid).await.map_err(|e| DaemonError::Failed(e.to_string()))
    }

    async fn stop_app(&self, app_uuid: String) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.stop_app(&app_uuid).await.map_err(|e| DaemonError::Failed(e.to_string()))
    }

    async fn update_time(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.update_time().await.map_err(|e| DaemonError::Failed(e.to_string()))
    }

    async fn notify(&self, title: String, body: String, subtitle: String) -> Result<u32, DaemonError> {
        let pebble = self.require_pebble()?;
        // subtitle is conventionally the app_name; use it for category detection.
        let category = app_name_to_category(&subtitle);
        let token = pebble
            .send_notification(&title, &body, &subtitle, category)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        Ok(token as u32)
    }

    fn ping(&self) -> bool {
        true
    }

    /// Request one serialized wellness reconciliation run.
    fn sync_wellness(&self) -> Result<(), DaemonError> {
        let config = self.integration_config.subscribe().borrow().clone();
        if !config.enabled {
            return Err(DaemonError::Failed(
                "Intervals.icu wellness sync is disabled".into(),
            ));
        }
        config
            .validate()
            .map_err(|error| DaemonError::Failed(error.to_string()))?;
        if self.state.lock().unwrap().db.is_none() {
            return Err(DaemonError::Failed("app database is not available".into()));
        }
        self.wellness_running.store(true, Ordering::SeqCst);
        self.wellness_sync_tx.send_modify(|revision| {
            *revision = revision.wrapping_add(1);
        });
        Ok(())
    }

    /// Return the durable wellness export status for the current account.
    /// Error text comes from the sanitized export ledger and never includes
    /// credentials or response bodies.
    async fn get_wellness_sync_status(&self) -> Result<HashMap<String, OwnedValue>, DaemonError> {
        loop {
            let config = self.integration_config.subscribe().borrow().clone();
            let account_id = config.athlete_id.clone();
            let revision = self.wellness_status_revision.load(Ordering::Acquire);
            if let Some(status) = self
                .wellness_status_cache
                .lock()
                .unwrap()
                .as_ref()
                .filter(|cached| {
                    cached.revision == revision && cached.account_id == account_id
                })
                .map(|cached| cached.status.clone())
            {
                let running = self.wellness_running.load(Ordering::SeqCst);
                return Ok(wellness_status_to_dbus_map(&config, status, running));
            }

            let db = self
                .state
                .lock()
                .unwrap()
                .db
                .clone()
                .ok_or_else(|| DaemonError::Failed("app database is not available".into()))?;
            let account_id_for_load = account_id.clone();
            let status = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
                let db = db.lock().unwrap();
                let current_payloads =
                    match (db.oldest_wellness_date()?, db.newest_wellness_date()?) {
                        (Some(start), Some(end)) => db
                            .fetch_daily_wellness(DateRange { start, end })?
                            .into_iter()
                            .map(|daily| {
                                let record =
                                    crate::integrations::intervals_icu::WellnessRecord::from(&daily);
                                Ok((daily.date, record.payload_hash()?))
                            })
                            .collect::<anyhow::Result<Vec<_>>>()?,
                        _ => Vec::new(),
                    };
                db.fetch_wellness_export_status(
                    "intervals_icu",
                    &account_id_for_load,
                    &current_payloads,
                )
            })
            .await
            .map_err(|error| DaemonError::Failed(error.to_string()))?
            .map_err(|error| DaemonError::Failed(error.to_string()))?;

            if self.wellness_status_revision.load(Ordering::Acquire) != revision {
                continue;
            }
            self.wellness_status_cache.lock().unwrap().replace(CachedWellnessStatus {
                revision,
                account_id,
                status: status.clone(),
            });
            let running = self.wellness_running.load(Ordering::SeqCst);
            return Ok(wellness_status_to_dbus_map(&config, status, running));
        }
    }

    async fn scan(&self, timeout_secs: f64) -> Result<Vec<(String, String)>, DaemonError> {
        let adapter = self.state.lock().unwrap().adapter.clone();
        Pebble::scan(&adapter, timeout_secs)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Return the running daemon's effective, redacted configuration snapshot.
    fn get_daemon_config(&self) -> Result<HashMap<String, OwnedValue>, DaemonError> {
        let state = self.state.lock().unwrap();
        daemon_config_map(
            &state.config,
            &state.config_path,
            *self.config_revision.borrow(),
            state.config_error.as_deref(),
            &state.active_database_path,
            state.active_verbose,
        )
    }

    /// Merge a versioned patch into the running daemon's latest configuration,
    /// atomically persist its effective file, and return the applied snapshot.
    async fn update_daemon_config(
        &self,
        expected_revision: u64,
        patch: HashMap<String, OwnedValue>,
    ) -> Result<HashMap<String, OwnedValue>, DaemonError> {
        let _operation = self.config_operation.lock().await;
        const PATCH_FIELDS: &[&str] = &[
            "address",
            "adapter",
            "verbose",
            "database_path",
            "intervals_enabled",
            "intervals_athlete_id",
            "intervals_api_key_clear",
            "intervals_api_key_replace",
        ];
        if let Some(key) = patch.keys().find(|key| !PATCH_FIELDS.contains(&key.as_str())) {
            return Err(DaemonError::Failed(format!("unsupported patch field {key}")));
        }
        if patch_bool(&patch, "intervals_api_key_clear")? == Some(true)
            && patch.contains_key("intervals_api_key_replace")
        {
            return Err(DaemonError::Failed(
                "intervals API key cannot be cleared and replaced in one update".into(),
            ));
        }
        let path = self.state.lock().unwrap().config_path.clone();
        let latest = match crate::config::load(&path) {
            Ok(config) => config,
            Err(error) => {
                let message = error.to_string();
                self.record_config_error(message.clone());
                return Err(DaemonError::Failed(format!(
                    "on-disk config is invalid; correct it and reload before updating: {message}"
                )));
            }
        };
        if let Err(error) = latest.validate() {
            let message = error.to_string();
            self.record_config_error(message.clone());
            return Err(DaemonError::Failed(format!(
                "on-disk config is invalid; correct it and reload before updating: {message}"
            )));
        }
        self.apply_loaded_config(latest).await;

        let actual_revision = *self.config_revision.borrow();
        if expected_revision != actual_revision {
            return Err(DaemonError::Failed(format!(
                "revision conflict: expected {expected_revision}, current {actual_revision}"
            )));
        }
        let mut config = {
            let state = self.state.lock().unwrap();
            state.config.clone()
        };
        let original = config.clone();
        if let Some(value) = patch_string(&patch, "address")? { config.address = value; }
        if let Some(value) = patch_string(&patch, "adapter")? { config.adapter = value; }
        if let Some(value) = patch_bool(&patch, "verbose")? { config.verbose = value; }
        if let Some(value) = patch_string(&patch, "database_path")? {
            config.db = if value.is_empty() { None } else { Some(value) };
        }
        if let Some(value) = patch_bool(&patch, "intervals_enabled")? {
            config.integrations.intervals_icu.enabled = value;
        }
        if let Some(value) = patch_string(&patch, "intervals_athlete_id")? {
            config.integrations.intervals_icu.athlete_id = value;
        }
        if patch_bool(&patch, "intervals_api_key_clear")? == Some(true) {
            config.integrations.intervals_icu.api_key.clear();
        }
        if let Some(value) = patch_string(&patch, "intervals_api_key_replace")? {
            config.integrations.intervals_icu.api_key = value;
        }
        config.validate().map_err(|error| DaemonError::Failed(error.to_string()))?;

        if config != original {
            crate::config::save(&path, &config)
                .map_err(|error| DaemonError::Failed(error.to_string()))?;
            self.apply_loaded_config(config.clone()).await;
        }
        let revision = *self.config_revision.borrow();
        let state = self.state.lock().unwrap();
        let mut result = daemon_config_map(
            &config,
            &path,
            revision,
            None,
            &state.active_database_path,
            state.active_verbose,
        )?;
        drop(state);
        if original.address != config.address { result.insert("apply.address".into(), dbus_val("reconnecting")); }
        if original.adapter != config.adapter { result.insert("apply.adapter".into(), dbus_val("reconnecting")); }
        if original.verbose != config.verbose { result.insert("apply.verbose".into(), dbus_val("daemon_restart_required")); }
        if original.db != config.db { result.insert("apply.database_path".into(), dbus_val("daemon_and_gui_restart_required")); }
        if original.integrations != config.integrations { result.insert("apply.intervals_icu".into(), dbus_val("applied_live")); }
        Ok(result)
    }

    /// Write health user profile to the watch and trigger a DataLog sync.
    /// gender: 0 = female, 1 = male, 2 = other (libpebble3 `HealthGender`).
    async fn activate_health(
        &self,
        height_cm: u16,
        weight_kg: u16,
        age: u8,
        gender: u8,
        hrm_enabled: bool,
    ) -> Result<(), DaemonError> {
        if gender > 2 {
            return Err(DaemonError::Failed(format!(
                "invalid gender={gender}; must be 0 (female), 1 (male), or 2 (other)"
            )));
        }
        let pebble = self.require_pebble()?;
        pebble
            .activate_health(height_cm, weight_kg, age, gender, hrm_enabled)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Ask the watch to flush pending health records via DataLog sessions.
    fn fetch_health_data(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.fetch_health_data().map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// PROTOTYPE: ask the watch to re-sync its HealthParams BlobDB (height,
    /// weight, age, gender, HRM). Decoded records are logged by the daemon; this
    /// call only triggers the request and returns once it has been sent.
    async fn fetch_health_params(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .fetch_health_params()
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Return the last health profile (height/weight/age/gender/HRM) the watch
    /// synced. Fails if no profile has been received yet this session — call
    /// `FetchHealthParams` (or wait for the on-connect sync) first.
    fn get_health_profile(&self) -> Result<HashMap<String, OwnedValue>, DaemonError> {
        Self::merged_profile(&self.state.lock().unwrap())
            .map(HealthProfile::to_dbus_map)
            .ok_or_else(|| DaemonError::Failed("no health profile synced yet".into()))
    }

    /// Return all general watch settings (BlobDB WatchPrefs, db 12) decoded so
    /// far, as a map of key -> variant (bool / uint32 / string). Empty until the
    /// watch syncs settings on connect. See `WatchSettingReceived` for updates.
    fn get_watch_settings(&self) -> HashMap<String, OwnedValue> {
        self.state
            .lock()
            .unwrap()
            .watch_settings
            .iter()
            .map(|(k, v)| (k.clone(), watch_pref_owned_value(v)))
            .collect()
    }

    /// Return one coherent, versioned view of all device-owned configuration.
    fn get_device_config(&self) -> HashMap<String, OwnedValue> {
        device_config_map(&self.state.lock().unwrap())
    }

    /// Clear session values and request a complete WatchPrefs refresh. Modern
    /// watches become Ready only after BlobDB2 SyncDone and after all preceding
    /// preference events have been incorporated into the snapshot.
    async fn refresh_device_config(&self) -> Result<HashMap<String, OwnedValue>, DaemonError> {
        let _operation = self.device_config_operation.lock().await;
        self.refresh_device_config_unlocked().await
    }

    async fn refresh_device_config_unlocked(&self) -> Result<HashMap<String, OwnedValue>, DaemonError> {
        let pebble = self.require_pebble()?;
        self.reset_device_config(DeviceConfigState::Loading);

        let (info, identity_error) = match pebble.get_watch_version().await {
            Ok(info) => (Some(info), None),
            Err(error) => (None, Some(format!("watch identity unavailable: {error}"))),
        };
        let blob_db_version = pebble.blob_db_version();
        let refresh_error = if blob_db_version == 0 {
            Some("watch does not support completeness-confirmed BlobDB2 refresh".to_string())
        } else {
            pebble.refresh_watch_preferences().await.err().map(|error| error.to_string())
        };
        let error = refresh_error.or(identity_error);
        let (done_tx, done_rx) = oneshot::channel();
        self.event_tx()
            .send(DaemonEvent::DeviceConfigRefreshComplete {
                info,
                blob_db_version,
                error,
                done: done_tx,
            })
            .map_err(|_| DaemonError::Failed("device-config event loop is unavailable".into()))?;
        done_rx
            .await
            .map_err(|_| DaemonError::Failed("device-config refresh completion was lost".into()))?;
        Ok(device_config_map(&self.state.lock().unwrap()))
    }

    /// Apply a health-only patch against a coherent watch snapshot. Each record
    /// must be accepted before the next is sent; modern watches are refreshed
    /// afterward so the returned snapshot reflects actual watch state.
    async fn update_device_config(
        &self,
        expected_revision: u64,
        patch: HashMap<String, OwnedValue>,
    ) -> Result<HashMap<String, OwnedValue>, DaemonError> {
        const FIELDS: &[&str] = &[
            "health.height_mm", "health.weight_dag", "health.tracking_enabled",
            "health.activity_insights_enabled", "health.sleep_insights_enabled", "health.age",
            "health.gender", "health.distance_units", "health.hrm.enabled",
            "health.hrm.measurement_interval", "health.hrm.during_activity",
            "health.thresholds.resting", "health.thresholds.elevated", "health.thresholds.maximum",
            "health.thresholds.zone1", "health.thresholds.zone2", "health.thresholds.zone3",
        ];
        if let Some(key) = patch.keys().find(|key| {
            !FIELDS.contains(&key.as_str()) && !key.starts_with("preference.")
        }) {
            return Err(DaemonError::Failed(format!("unsupported patch field {key}")));
        }

        let _operation = self.device_config_operation.lock().await;
        let pebble = self.require_pebble()?;
        let (actual_revision, mut activity, mut activity_raw, units, mut units_raw,
             mut hrm, mut hrm_raw, mut thresholds, mut thresholds_raw, firmware, blob_db_version) = {
            let state = self.state.lock().unwrap();
            (state.device_config_revision, state.device_health_activity,
             state.device_health_activity_raw.clone(), state.imperial_units,
             state.imperial_units_raw.clone(), state.hrm_prefs, state.hrm_prefs_raw.clone(),
             state.heart_rate_prefs, state.heart_rate_prefs_raw.clone(),
             state.device_config_watch.as_ref().map(|info| (
                 info.running.major as u8, info.running.minor as u8, info.running.patch as u16,
             )), state.device_config_blob_db_version)
        };
        if expected_revision != actual_revision {
            return Err(DaemonError::Failed(format!(
                "revision conflict: expected {expected_revision}, current {actual_revision}"
            )));
        }
        if patch.is_empty() { return Ok(device_config_map(&self.state.lock().unwrap())); }

        let activity_changed = patch.keys().any(|key| matches!(key.as_str(),
            "health.height_mm" | "health.weight_dag" | "health.tracking_enabled" |
            "health.activity_insights_enabled" | "health.sleep_insights_enabled" |
            "health.age" | "health.gender"));
        if activity_changed {
            let value = activity.as_mut().ok_or_else(|| DaemonError::Failed(
                "activityPreferences was not received; refresh before editing".into()))?;
            let raw = activity_raw.as_mut().filter(|raw| raw.len() >= 9).ok_or_else(||
                DaemonError::Failed("activityPreferences raw record is unavailable".into()))?;
            if let Some(v) = patch_u16(&patch, "health.height_mm")? {
                if !(1000..=2200).contains(&v) { return Err(DaemonError::Failed("height must be between 1000 and 2200 mm".into())); }
                value.height_mm = v; raw[0..2].copy_from_slice(&v.to_le_bytes());
            }
            if let Some(v) = patch_u16(&patch, "health.weight_dag")? {
                if !(3000..=20_000).contains(&v) { return Err(DaemonError::Failed("weight must be between 3000 and 20000 dag".into())); }
                value.weight_dag = v; raw[2..4].copy_from_slice(&v.to_le_bytes());
            }
            if let Some(v) = patch_bool(&patch, "health.tracking_enabled")? { value.tracking_enabled = v; raw[4] = u8::from(v); }
            if let Some(v) = patch_bool(&patch, "health.activity_insights_enabled")? { value.activity_insights_enabled = v; raw[5] = u8::from(v); }
            if let Some(v) = patch_bool(&patch, "health.sleep_insights_enabled")? { value.sleep_insights_enabled = v; raw[6] = u8::from(v); }
            if let Some(v) = patch_u8(&patch, "health.age")? {
                if !(1..=120).contains(&v) { return Err(DaemonError::Failed("age must be between 1 and 120".into())); }
                value.age = v; raw[7] = v;
            }
            if let Some(v) = patch_u8(&patch, "health.gender")? {
                if v > 2 { return Err(DaemonError::Failed("gender must be Female (0), Male (1), or Other (2)".into())); }
                value.gender = v; raw[8] = v;
            }
        }

        let units_changed = patch.contains_key("health.distance_units");
        let new_units = if units_changed {
            let raw = units_raw.as_mut().filter(|raw| !raw.is_empty()).ok_or_else(||
                DaemonError::Failed("unitsDistance raw record is unavailable".into()))?;
            match patch_u8(&patch, "health.distance_units")?.unwrap() {
                0 => { raw[0] = 0; Some(false) }, 1 => { raw[0] = 1; Some(true) },
                _ => return Err(DaemonError::Failed("distance units must be metric (0) or imperial (1)".into())),
            }
        } else { units };

        let hrm_changed = patch.keys().any(|key| key.starts_with("health.hrm."));
        if hrm_changed {
            let value = hrm.as_mut().ok_or_else(|| DaemonError::Failed(
                "hrmPreferences was not received; refresh before editing".into()))?;
            let raw = hrm_raw.as_mut().filter(|raw| !raw.is_empty()).ok_or_else(||
                DaemonError::Failed("hrmPreferences raw record is unavailable".into()))?;
            let version = firmware.ok_or_else(|| DaemonError::Failed(
                "watch firmware is unavailable; refresh before editing HRM settings".into()))?;
            if patch.contains_key("health.hrm.measurement_interval") && version < (4, 9, 146) {
                return Err(DaemonError::Failed("HRM measurement interval is unsupported by this firmware".into()));
            }
            if patch.contains_key("health.hrm.during_activity") && version < (4, 9, 150) {
                return Err(DaemonError::Failed("HRM during activities is unsupported by this firmware".into()));
            }
            if let Some(v) = patch_bool(&patch, "health.hrm.enabled")? { value.enabled = v; raw[0] = u8::from(v); }
            if let Some(v) = patch_u8(&patch, "health.hrm.measurement_interval")? {
                if raw.len() < 2 { return Err(DaemonError::Failed("watch returned a short hrmPreferences record".into())); }
                value.measurement_interval = Some(HrMonitoringInterval::from_u8(v));
                raw[1] = v;
            }
            if let Some(v) = patch_bool(&patch, "health.hrm.during_activity")? {
                if raw.len() < 3 { return Err(DaemonError::Failed("watch returned a short hrmPreferences record".into())); }
                value.activity_tracking_enabled = Some(v);
                raw[2] = u8::from(v);
            }
        }

        let thresholds_changed = patch.keys().any(|key| key.starts_with("health.thresholds."));
        if thresholds_changed {
            let value = thresholds.as_mut().ok_or_else(|| DaemonError::Failed(
                "heartRatePreferences was not received; refresh before editing".into()))?;
            let raw = thresholds_raw.as_mut().filter(|raw| raw.len() >= 6).ok_or_else(||
                DaemonError::Failed("heartRatePreferences raw record is unavailable".into()))?;
            if let Some(v) = patch_u8(&patch, "health.thresholds.resting")? { value.resting_hr = v; raw[0] = v; }
            if let Some(v) = patch_u8(&patch, "health.thresholds.elevated")? { value.elevated_hr = v; raw[1] = v; }
            if let Some(v) = patch_u8(&patch, "health.thresholds.maximum")? { value.max_hr = v; raw[2] = v; }
            if let Some(v) = patch_u8(&patch, "health.thresholds.zone1")? { value.zone1_threshold = v; raw[3] = v; }
            if let Some(v) = patch_u8(&patch, "health.thresholds.zone2")? { value.zone2_threshold = v; raw[4] = v; }
            if let Some(v) = patch_u8(&patch, "health.thresholds.zone3")? { value.zone3_threshold = v; raw[5] = v; }
        }

        let (watch_type, observed_settings) = {
            let state = self.state.lock().unwrap();
            (
                state.device_config_watch.as_ref().map(|info| info.watch_type()).unwrap_or(WatchType::Unknown),
                state.watch_settings.clone(),
            )
        };
        let model = match watch_type {
            WatchType::Emery => WatchPrefModel::Emery,
            WatchType::Gabbro => WatchPrefModel::Gabbro,
            _ => WatchPrefModel::Standard,
        };

        let mut writes = Vec::new();
        if activity_changed {
            writes.push(("activityPreferences", activity_raw.clone().unwrap()));
        }
        if units_changed { writes.push(("unitsDistance", units_raw.clone().unwrap())); }
        if hrm_changed {
            writes.push(("hrmPreferences", hrm_raw.clone().unwrap()));
        }
        if thresholds_changed {
            writes.push(("heartRatePreferences", thresholds_raw.clone().unwrap()));
        }
        let mut general_updates = Vec::new();
        let mut preference_fields: Vec<_> = patch.iter()
            .filter(|(field, _)| field.starts_with("preference."))
            .collect();
        preference_fields.sort_by_key(|(field, _)| field.as_str());
        for (field, value) in preference_fields {
            let Some(key) = field.strip_prefix("preference.") else { continue };
            if !observed_settings.contains_key(key) {
                return Err(DaemonError::Failed(format!(
                    "preference {key} was not reported by this watch"
                )));
            }
            let value = if let Ok(value) = bool::try_from(value) {
                WatchPrefValue::Bool(value)
            } else if let Ok(value) = u32::try_from(value) {
                WatchPrefValue::Number(value)
            } else if let Ok(value) = <&str>::try_from(value) {
                WatchPrefValue::Text(value.to_owned())
            } else {
                return Err(DaemonError::Failed(format!("invalid preference value for {key}")));
            };
            let encoded = encode_watch_pref(key, &value, model)
                .map_err(|error| DaemonError::Failed(error.to_string()))?;
            let key: &'static str = match key {
                "clock24h" => "clock24h",
                "displayOrientationLeftHanded" => "displayOrientationLeftHanded",
                "textStyle" => "textStyle",
                "lightEnabled" => "lightEnabled",
                "lightAmbientSensorEnabled" => "lightAmbientSensorEnabled",
                "lightMotion" => "lightMotion",
                "lightTimeoutMs" => "lightTimeoutMs",
                "lightTouch" => "lightTouch",
                "lightIntensity" => "lightIntensity",
                "lightDynamicIntensity" => "lightDynamicIntensity",
                "lightPreset" => "lightPreset",
                "lightDynamicMode" => "lightDynamicMode",
                "menuScrollWrapAround" => "menuScrollWrapAround",
                "menuScrollVibeBehavior" => "menuScrollVibeBehavior",
                "mask" => "mask",
                "notifDesignStyle" => "notifDesignStyle",
                "notifVibeDelay" => "notifVibeDelay",
                "notifBacklight" => "notifBacklight",
                "notifWindowTimeout" => "notifWindowTimeout",
                "vibeIntensity" => "vibeIntensity",
                "vibeScoreNotifications" => "vibeScoreNotifications",
                "vibeScoreIncomingCalls" => "vibeScoreIncomingCalls",
                "vibeScoreAlarms" => "vibeScoreAlarms",
                "dndManuallyEnabled" => "dndManuallyEnabled",
                "dndSmartEnabled" => "dndSmartEnabled",
                "dndInterruptionsMask" => "dndInterruptionsMask",
                "dndShowNotifications" => "dndShowNotifications",
                "dndMotionBacklight" => "dndMotionBacklight",
                "dndAutoDismiss" => "dndAutoDismiss",
                "timelineQuickViewEnabled" => "timelineQuickViewEnabled",
                "timelineQuickViewBeforeTimeMin" => "timelineQuickViewBeforeTimeMin",
                "musicShowVolumeControls" => "musicShowVolumeControls",
                "musicShowProgressBar" => "musicShowProgressBar",
                _ => return Err(DaemonError::Failed(format!("preference {key} is not editable in this phase"))),
            };
            writes.push((key, encoded.clone()));
            general_updates.push((key.to_owned(), value, encoded));
        }

        let write_error = write_records_in_order(writes, |key, value| {
            let pebble = pebble.clone();
            async move {
                pebble.write_preference_confirmed(key, &value).await
                    .map(|_| ()).map_err(|error| error.to_string())
            }
        }).await.err();
        if blob_db_version >= 1 {
            let mut snapshot = self.refresh_device_config_unlocked().await?;
            if let Some(message) = write_error {
                let mut state = self.state.lock().unwrap();
                state.device_config_error = Some(message);
                state.device_config_state = completed_write_state(blob_db_version, true);
                state.device_config_revision = state.device_config_revision.wrapping_add(1);
                let _ = state.event_tx.send(DaemonEvent::DeviceConfigChanged {
                    revision: state.device_config_revision, state: state.device_config_state,
                });
                snapshot = device_config_map(&state);
            }
            Ok(snapshot)
        } else if let Some(message) = write_error {
            Err(DaemonError::Failed(message))
        } else {
            let mut state = self.state.lock().unwrap();
            if activity_changed { state.device_health_activity = activity; }
            if activity_changed { state.device_health_activity_raw = activity_raw; }
            if units_changed { state.imperial_units = new_units; }
            if units_changed { state.imperial_units_raw = units_raw; }
            if hrm_changed { state.hrm_prefs = hrm; }
            if hrm_changed { state.hrm_prefs_raw = hrm_raw; }
            if thresholds_changed { state.heart_rate_prefs = thresholds; }
            if thresholds_changed { state.heart_rate_prefs_raw = thresholds_raw; }
            for (key, value, raw) in general_updates {
                state.watch_settings.insert(key.clone(), value);
                state.watch_setting_raw.insert(key, raw);
            }
            state.device_config_state = completed_write_state(blob_db_version, false);
            state.device_config_revision = state.device_config_revision.wrapping_add(1);
            let _ = state.event_tx.send(DaemonEvent::DeviceConfigChanged {
                revision: state.device_config_revision, state: state.device_config_state,
            });
            Ok(device_config_map(&state))
        }
    }

    /// Query the watch's version info (firmware, board, serial, BT address,
    /// language, capabilities, platform) as a key -> variant map.
    async fn get_watch_version(&self) -> Result<HashMap<String, OwnedValue>, DaemonError> {
        let pebble = self.require_pebble()?;
        let info = pebble
            .get_watch_version()
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        Ok(watch_version_to_map(&info))
    }

    /// Query the watch's manufacturing color/variant as a key -> variant map.
    /// Fails if the watch reports an error or an unknown color.
    async fn get_watch_color(&self) -> Result<HashMap<String, OwnedValue>, DaemonError> {
        let pebble = self.require_pebble()?;
        match pebble
            .get_watch_color()
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?
        {
            Some(color) => Ok(watch_color_to_map(color)),
            None => Err(DaemonError::Failed("watch reported an unknown color".into())),
        }
    }

    /// Capture the watch screen and return it as PNG bytes.
    async fn screenshot(&self) -> Result<Vec<u8>, DaemonError> {
        let pebble = self.require_pebble()?;
        let shot = pebble
            .take_screenshot()
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        encode_png(shot.width, shot.height, &shot.pixels)
    }

    /// Tell the watch which media app is playing (now-playing source).
    pub(crate) async fn set_music_player_info(&self, pkg: String, name: String) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .update_music_player_info(&pkg, &name)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        self.state.lock().unwrap().music.player = Some((pkg, name));
        Ok(())
    }

    /// Push the current track metadata. `track_length_ms`/`track_count`/
    /// `track_number` are sent as-is (0 is a valid "unknown" value).
    pub(crate) async fn set_music_track(
        &self,
        artist: String,
        album: String,
        title: String,
        track_length_ms: u32,
        track_count: u32,
        track_number: u32,
    ) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .update_music_track(
                &artist,
                &album,
                &title,
                Some(track_length_ms),
                Some(track_count),
                Some(track_number),
            )
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        self.state.lock().unwrap().music.track =
            Some((artist, album, title, track_length_ms, track_count, track_number));
        Ok(())
    }

    /// Push playback state. `state`: 0=paused 1=playing 2=rewinding
    /// 3=fast-forwarding 4=unknown. `shuffle`: 0=unknown 1=off 2=on.
    /// `repeat`: 0=unknown 1=off 2=one 3=all.
    pub(crate) async fn set_music_playback_state(
        &self,
        state: u8,
        track_position_ms: u32,
        play_rate_pct: u32,
        shuffle: u8,
        repeat: u8,
    ) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .update_music_play_state(
                MusicPlaybackState::from_u8(state),
                track_position_ms,
                play_rate_pct,
                MusicShuffle::from_u8(shuffle),
                MusicRepeat::from_u8(repeat),
            )
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        self.state.lock().unwrap().music.play_state =
            Some((state, track_position_ms, play_rate_pct, shuffle, repeat));
        Ok(())
    }

    /// Push the current volume (0–100).
    pub(crate) async fn set_music_volume(&self, volume_percent: u8) -> Result<(), DaemonError> {
        if volume_percent > 100 {
            return Err(DaemonError::Failed(format!(
                "volume {volume_percent} out of range (0-100)"
            )));
        }
        let pebble = self.require_pebble()?;
        pebble
            .update_music_volume(volume_percent)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?;
        self.state.lock().unwrap().music.volume = Some(volume_percent);
        Ok(())
    }

    /// Reboot the watch. It drops the link and the daemon reconnects.
    async fn reboot_watch(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.reboot_watch().map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Reboot the watch into its recovery (PRF) firmware.
    async fn reset_into_recovery(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.reset_into_recovery().map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Trigger a core dump on the watch.
    async fn create_core_dump(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.create_core_dump().map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Factory-reset the watch. DESTRUCTIVE: wipes all watch data and unpairs.
    /// Requires `confirm = true` so an accidental/no-arg call can't wipe the watch.
    async fn factory_reset(&self, confirm: bool) -> Result<(), DaemonError> {
        if !confirm {
            return Err(DaemonError::Failed(
                "factory_reset is destructive (wipes the watch and unpairs it); \
                 call with confirm=true to proceed"
                    .into(),
            ));
        }
        let pebble = self.require_pebble()?;
        pebble.factory_reset().map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Remove the watch's Bluetooth bond (unpair). The watch re-pairs on the
    /// next reconnect.
    async fn forget(&self) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble.forget().await.map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Push weather data to the Pebble built-in weather app.
    ///
    /// `location_key` must be exactly 16 bytes (a UUID); re-use the same bytes to update
    /// an existing location entry rather than creating a new one.
    ///
    /// `current_weather` / `tomorrow_weather`: 0=PartlyCloudy, 1=CloudyDay, 2=LightSnow,
    ///   3=LightRain, 4=HeavyRain, 5=HeavySnow, 6=Generic, 7=Sun, 8=RainAndSnow, 255=Unknown
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn push_weather(
        &self,
        location_key: Vec<u8>,
        location_name: String,
        forecast_short: String,
        current_temp: i16,
        current_weather: u8,
        today_high: i16,
        today_low: i16,
        tomorrow_weather: u8,
        tomorrow_high: i16,
        tomorrow_low: i16,
        is_current_location: bool,
    ) -> Result<(), DaemonError> {
        if location_key.len() != 16 {
            return Err(DaemonError::Failed(format!(
                "location_key must be 16 bytes, got {}",
                location_key.len()
            )));
        }
        let key: [u8; 16] = location_key.try_into().unwrap();
        let pebble = self.require_pebble()?;
        pebble
            .push_weather(
                &key,
                &location_name,
                &forecast_short,
                current_temp,
                WeatherType::from_u8(current_weather),
                today_high,
                today_low,
                WeatherType::from_u8(tomorrow_weather),
                tomorrow_high,
                tomorrow_low,
                is_current_location,
            )
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    // ── Phone calls ───────────────────────────────────────────────────

    /// Push an incoming call to the watch (shows caller screen).
    /// `cookie` is an arbitrary u32 echoed back in answer/hangup actions.
    pub(crate) async fn push_incoming_call(
        &self,
        cookie: u32,
        caller_number: String,
        caller_name: String,
    ) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .push_incoming_call(cookie, &caller_number, &caller_name)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Push a missed call notification to the watch.
    async fn push_missed_call(
        &self,
        cookie: u32,
        caller_number: String,
        caller_name: String,
    ) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .push_missed_call(cookie, &caller_number, &caller_name)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Notify the watch that the call is now active (answered).
    pub(crate) async fn push_call_start(&self, cookie: u32) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .push_call_start(cookie)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Notify the watch that the call has ended.
    pub(crate) async fn push_call_end(&self, cookie: u32) -> Result<(), DaemonError> {
        let pebble = self.require_pebble()?;
        pebble
            .push_call_end(cookie)
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Rebuild health_activity_minutes and health_activity_sessions from the raw
    /// blobs in health_records. Call this after a schema change or to backfill
    /// utc_offset for rows that were stored before the column existed.
    async fn reprocess_health_data(&self) -> Result<(), DaemonError> {
        let db = self.state.lock().unwrap().db.clone();
        let db = db.ok_or_else(|| DaemonError::Failed("app database not available".into()))?;
        tokio::task::spawn_blocking(move || db.lock().unwrap().reprocess())
            .await
            .map_err(|e| DaemonError::Failed(e.to_string()))?
            .map_err(|e| DaemonError::Failed(e.to_string()))
    }

    /// Re-read the config file from disk and apply changes.
    /// If address or adapter changed, disconnects the current session so the
    /// supervisor reconnects with the new parameters on the next cycle.
    pub(crate) async fn reload_config(&self) -> Result<(), DaemonError> {
        let _operation = self.config_operation.lock().await;
        let config_path = self.state.lock().unwrap().config_path.clone();

        let new_cfg = match crate::config::load(&config_path) {
            Ok(config) => config,
            Err(error) => {
                let message = error.to_string();
                self.record_config_error(message.clone());
                return Err(DaemonError::Failed(message));
            }
        };
        crate::config::warn_if_invalid(&config_path, &new_cfg);
        if let Err(error) = new_cfg.validate() {
            let message = error.to_string();
            self.record_config_error(message.clone());
            return Err(DaemonError::Failed(message));
        }

        self.apply_loaded_config(new_cfg).await;
        Ok(())
    }

    // ---- Signals ----

    #[zbus(signal)]
    pub async fn daemon_config_changed(
        signal_emitter: &SignalEmitter<'_>,
        revision: u64,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn device_config_changed(
        signal_emitter: &SignalEmitter<'_>,
        revision: u64,
        state: &str,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn app_message_received(
        signal_emitter: &SignalEmitter<'_>,
        app_uuid: &str,
        data: WireDict,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn ack_received(signal_emitter: &SignalEmitter<'_>, txn: u32) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn nack_received(signal_emitter: &SignalEmitter<'_>, txn: u32) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn connection_changed(
        signal_emitter: &SignalEmitter<'_>,
        connected: bool,
    ) -> zbus::Result<()>;

    /// Emitted for each batch of health records received from the watch.
    /// tag: data type (81=steps, 83=sleep, 84=activity sessions, 85=HR).
    /// app_uuid: 16 bytes (all-zeros for health sessions).
    /// item_size: bytes per record in `data`.
    /// items_left: records still queued on the watch after this batch.
    /// crc: CRC-32 of `data` as computed by the watch; use for deduplication on reconnect.
    #[zbus(signal)]
    pub async fn health_data_received(
        signal_emitter: &SignalEmitter<'_>,
        tag: u32,
        app_uuid: Vec<u8>,
        session_timestamp: u32,
        items_left: u32,
        crc: u32,
        item_type: u8,
        item_size: u16,
        data: Vec<u8>,
    ) -> zbus::Result<()>;

    /// Emitted when the watch syncs its health profile (height/weight/age/gender/HRM).
    /// Fires on connect and on any subsequent change, including HRM updates.
    #[zbus(signal)]
    pub async fn health_profile_received(
        signal_emitter: &SignalEmitter<'_>,
        profile: HashMap<String, OwnedValue>,
    ) -> zbus::Result<()>;

    /// Emitted for each general watch setting (db 12) as the watch syncs it.
    /// `value` is a variant: bool, uint32, or string depending on the key.
    #[zbus(signal)]
    pub async fn watch_setting_received(
        signal_emitter: &SignalEmitter<'_>,
        key: &str,
        value: OwnedValue,
    ) -> zbus::Result<()>;

    /// Emitted when the watch battery percentage changes. -1 means unknown.
    #[zbus(signal)]
    pub async fn battery_changed(
        signal_emitter: &SignalEmitter<'_>,
        level: i16,
    ) -> zbus::Result<()>;

    /// Emitted when an app opens (running=true) or closes (running=false) on the watch.
    #[zbus(signal)]
    pub async fn app_run_state_changed(
        signal_emitter: &SignalEmitter<'_>,
        uuid: &str,
        running: bool,
    ) -> zbus::Result<()>;

    /// Emitted when the watch sends a media-control action. `action` is one of
    /// play, pause, play_pause, next_track, previous_track, volume_up,
    /// volume_down, get_current_track. The transport actions (play/pause/next/…)
    /// are surfaced but not acted on yet; `get_current_track` is handled by
    /// replaying the cached music state to the watch.
    #[zbus(signal)]
    pub async fn music_action_received(
        signal_emitter: &SignalEmitter<'_>,
        action: &str,
    ) -> zbus::Result<()>;

    /// Emitted when the watch sends a phone control action.
    /// `action` is "answer" or "hangup". `cookie` is the u32 that was sent
    /// with the original incoming/missed call so the client can match it.
    #[zbus(signal)]
    pub async fn phone_action_received(
        signal_emitter: &SignalEmitter<'_>,
        action: &str,
        cookie: u32,
    ) -> zbus::Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_path() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("cobbled-reload-test-{unique}.toml"))
    }

    #[tokio::test]
    async fn integration_reload_wakes_worker_without_replacing_ble_params() {
        let path = test_path();
        let initial = IntervalsIcuConfig {
            enabled: false,
            athlete_id: String::new(),
            api_key: String::new(),
        };
        let updated = crate::config::Config {
            address: "E6:94:0A:D4:D5:DC".into(),
            adapter: "hci0".into(),
            verbose: false,
            db: None,
            integrations: cobble_config::Integrations {
                intervals_icu: IntervalsIcuConfig {
                    enabled: true,
                    athlete_id: "i123456".into(),
                    api_key: "test-only-value".into(),
                },
            },
        };
        std::fs::write(&path, toml::to_string(&updated).unwrap()).unwrap();

        let (event_tx, _) = mpsc::unbounded_channel();
        let (music_tx, _) = mpsc::unbounded_channel();
        let (phone_tx, _) = mpsc::unbounded_channel();
        let daemon = CobbleDaemon::new(
            Config {
                address: "E6:94:0A:D4:D5:DC".into(),
                adapter: "hci0".into(),
                integrations: cobble_config::Integrations { intervals_icu: initial },
                ..Config::default()
            },
            watch::channel(0).0,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
            path.clone(),
            path.clone(),
            false,
            event_tx,
            None,
            music_tx,
            phone_tx,
        );
        let mut integration_rx = daemon.integration_config_changed();
        let revision_rx = daemon.config_changed();

        daemon.reload_config().await.unwrap();
        integration_rx.changed().await.unwrap();
        assert_eq!(integration_rx.borrow().athlete_id, "i123456");
        assert!(integration_rx.borrow().enabled);
        assert!(revision_rx.has_changed().unwrap());
        assert_eq!(
            daemon.current_connection_params(),
            ("E6:94:0A:D4:D5:DC".into(), "hci0".into())
        );
        assert!(!*daemon.watch_connection().borrow());

        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn daemon_config_update_is_revisioned_persisted_and_redacted() {
        let path = test_path();
        let (event_tx, _) = mpsc::unbounded_channel();
        let (music_tx, _) = mpsc::unbounded_channel();
        let (phone_tx, _) = mpsc::unbounded_channel();
        let daemon = CobbleDaemon::new(
            Config::default(),
            watch::channel(0).0,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
            path.clone(),
            path.clone(),
            false,
            event_tx,
            None,
            music_tx,
            phone_tx,
        );
        let patch = HashMap::from([
            ("adapter".into(), dbus_val("hci7")),
            ("database_path".into(), dbus_val("/tmp/new-cobbled.db")),
            ("intervals_api_key_replace".into(), dbus_val("test-secret")),
        ]);
        let snapshot = daemon.update_daemon_config(0, patch).await.unwrap();
        assert_eq!(u64::try_from(snapshot.get("revision").unwrap()).unwrap(), 1);
        assert!(bool::try_from(snapshot.get("intervals_api_key_configured").unwrap()).unwrap());
        assert!(!format!("{snapshot:?}").contains("test-secret"));
        assert_eq!(
            <&str>::try_from(snapshot.get("active_database_path").unwrap()).unwrap(),
            path.to_string_lossy()
        );
        assert_eq!(
            <&str>::try_from(snapshot.get("apply.database_path").unwrap()).unwrap(),
            "daemon_and_gui_restart_required"
        );
        let persisted = crate::config::load(&path).unwrap();
        assert_eq!(persisted.adapter, "hci7");
        assert_eq!(persisted.db.as_deref(), Some("/tmp/new-cobbled.db"));
        assert_eq!(persisted.integrations.intervals_icu.api_key, "test-secret");
        assert!(daemon.update_daemon_config(0, HashMap::new()).await.is_err());
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn daemon_config_preserves_applied_snapshot_when_disk_becomes_invalid() {
        let path = test_path();
        let (event_tx, _) = mpsc::unbounded_channel();
        let (music_tx, _) = mpsc::unbounded_channel();
        let (phone_tx, _) = mpsc::unbounded_channel();
        let daemon = CobbleDaemon::new(
            Config::default(),
            watch::channel(0).0,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
            path.clone(),
            path.clone(),
            false,
            event_tx,
            None,
            music_tx,
            phone_tx,
        );

        daemon
            .update_daemon_config(
                0,
                HashMap::from([("adapter".into(), dbus_val("hci7"))]),
            )
            .await
            .unwrap();
        std::fs::write(&path, "adapter = [not valid TOML").unwrap();

        assert!(daemon.update_daemon_config(1, HashMap::new()).await.is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "adapter = [not valid TOML");
        let snapshot = daemon.get_daemon_config().unwrap();
        assert_eq!(<&str>::try_from(snapshot.get("adapter").unwrap()).unwrap(), "hci7");
        assert_eq!(
            <&str>::try_from(snapshot.get("error_kind").unwrap()).unwrap(),
            "invalid_data"
        );
        assert_eq!(u64::try_from(snapshot.get("revision").unwrap()).unwrap(), 2);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn device_config_snapshot_distinguishes_missing_and_precise_values() {
        let path = test_path();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        let (music_tx, _) = mpsc::unbounded_channel();
        let (phone_tx, _) = mpsc::unbounded_channel();
        let daemon = CobbleDaemon::new(
            Config::default(),
            watch::channel(0).0,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
            path.clone(),
            path,
            false,
            event_tx,
            None,
            music_tx,
            phone_tx,
        );

        let empty = daemon.get_device_config();
        assert_eq!(<&str>::try_from(empty.get("state").unwrap()).unwrap(), "disconnected");
        assert_eq!(
            <&str>::try_from(empty.get("health.activity.availability").unwrap()).unwrap(),
            "not_received"
        );
        assert!(!empty.contains_key("health.height_mm"));

        daemon.cache_health_activity_raw(libpebble_ble::HealthActivityConfig {
            height_mm: 1805,
            weight_dag: 7555,
            tracking_enabled: true,
            activity_insights_enabled: false,
            sleep_insights_enabled: true,
            age: 42,
            gender: 2,
        }, vec![13, 7, 131, 29, 1, 1, 0, 42, 2]);
        {
            let mut state = daemon.state.lock().unwrap();
            state.connected = true;
        }
        let (_, completed_state) = daemon.complete_device_config_refresh(None, 1, None);
        assert_eq!(completed_state, DeviceConfigState::Partial);
        let snapshot = daemon.get_device_config();
        assert_eq!(u16::try_from(snapshot.get("health.height_mm").unwrap()).unwrap(), 1805);
        assert_eq!(u16::try_from(snapshot.get("health.weight_dag").unwrap()).unwrap(), 7555);
        assert_eq!(
            <&str>::try_from(snapshot.get("health.units.availability").unwrap()).unwrap(),
            "not_received"
        );
    }

    #[test]
    fn wellness_status_map_reports_running_without_exposing_credentials() {
        let config = IntervalsIcuConfig {
            enabled: true,
            athlete_id: "i123456".into(),
            api_key: "secret-api-key".into(),
        };
        let map = wellness_status_to_dbus_map(
            &config,
            WellnessExportStatus {
                exported_dates: 3,
                pending_dates: 2,
                ..WellnessExportStatus::default()
            },
            true,
        );

        assert!(bool::try_from(map.get("running").unwrap()).unwrap());
        assert_eq!(u64::try_from(map.get("pending_dates").unwrap()).unwrap(), 2);
        assert!(!format!("{map:?}").contains("secret-api-key"));
    }

    #[test]
    fn manual_sync_marks_running_before_notifying_the_worker() {
        let db_path = test_path().with_extension("db");
        let db = Arc::new(Mutex::new(AppDb::open(&db_path).unwrap()));
        let (sync_tx, sync_rx) = watch::channel(0_u64);
        let running = Arc::new(AtomicBool::new(false));
        let (event_tx, _) = mpsc::unbounded_channel();
        let (music_tx, _) = mpsc::unbounded_channel();
        let (phone_tx, _) = mpsc::unbounded_channel();
        let daemon = CobbleDaemon::new(
            Config {
                address: "watch-address".into(),
                adapter: "hci0".into(),
                integrations: cobble_config::Integrations {
                    intervals_icu: IntervalsIcuConfig {
                        enabled: true,
                        athlete_id: "i123456".into(),
                        api_key: "test-only-value".into(),
                    },
                },
                ..Config::default()
            },
            sync_tx,
            running.clone(),
            Arc::new(AtomicU64::new(0)),
            test_path(),
            db_path.clone(),
            false,
            event_tx,
            Some(db),
            music_tx,
            phone_tx,
        );

        daemon.sync_wellness().unwrap();

        assert!(running.load(Ordering::SeqCst));
        assert_eq!(*sync_rx.borrow(), 1);

        drop(daemon);
        std::fs::remove_file(db_path).unwrap();
    }

    #[test]
    fn modern_and_legacy_writes_have_explicit_confirmation_states() {
        assert_eq!(completed_write_state(1, false), DeviceConfigState::Ready);
        assert_eq!(completed_write_state(0, false), DeviceConfigState::Partial);
        assert_eq!(completed_write_state(1, true), DeviceConfigState::Error);
    }

    #[tokio::test]
    async fn rejected_write_stops_before_later_records() {
        let attempted = Arc::new(Mutex::new(Vec::new()));
        let observed = attempted.clone();
        let error = write_records_in_order(
            vec![
                ("activityPreferences", vec![1]),
                ("unitsDistance", vec![2]),
                ("hrmPreferences", vec![3]),
            ],
            move |key, _| {
                observed.lock().unwrap().push(key);
                std::future::ready(if key == "unitsDistance" {
                    Err("watch rejected preference: InvalidData".to_string())
                } else {
                    Ok(())
                })
            },
        ).await.unwrap_err();

        assert!(error.contains("unitsDistance"));
        assert_eq!(*attempted.lock().unwrap(), vec!["activityPreferences", "unitsDistance"]);
    }

    #[tokio::test]
    async fn disconnect_stops_before_later_records() {
        let attempted = Arc::new(Mutex::new(Vec::new()));
        let observed = attempted.clone();
        let error = write_records_in_order(
            vec![("activityPreferences", vec![1]), ("hrmPreferences", vec![2])],
            move |key, _| {
                observed.lock().unwrap().push(key);
                std::future::ready(Err("watch disconnected".to_string()))
            },
        ).await.unwrap_err();

        assert!(error.contains("disconnected"));
        assert_eq!(*attempted.lock().unwrap(), vec!["activityPreferences"]);
    }
}

// ---------------------------------------------------------------------------
// Signal emission task
// ---------------------------------------------------------------------------

/// Processes `DaemonEvent`s from the reconnect supervisor and emits the
/// corresponding D-Bus signals. Keeps the `Connected` property in sync.
pub async fn run_signal_emitter(
    conn: Connection,
    daemon: CobbleDaemon,
    mut event_rx: mpsc::UnboundedReceiver<DaemonEvent>,
    app_db: Option<Arc<Mutex<AppDb>>>,
    wellness_revision_tx: watch::Sender<u64>,
) {
    while let Some(event) = event_rx.recv().await {
        let iface_result = conn
            .object_server()
            .interface::<_, CobbleDaemon>(OBJECT_PATH)
            .await;
        let iface = match iface_result {
            Ok(i) => i,
            Err(e) => {
                warn!("could not get interface for signal emission: {e}");
                continue;
            }
        };
        let emitter = iface.signal_emitter();
        match event {
            DaemonEvent::DeviceConfigChanged { revision, state } => {
                let _ = CobbleDaemon::device_config_changed(
                    emitter,
                    revision,
                    device_config_state_name(state),
                )
                .await;
            }
            DaemonEvent::DeviceConfigRefreshComplete {
                info,
                blob_db_version,
                error,
                done,
            } => {
                let (revision, state) =
                    daemon.complete_device_config_refresh(info, blob_db_version, error);
                let _ = CobbleDaemon::device_config_changed(
                    emitter,
                    revision,
                    device_config_state_name(state),
                )
                .await;
                let _ = done.send(());
            }
            DaemonEvent::DaemonConfigChanged(revision) => {
                let _ = CobbleDaemon::daemon_config_changed(emitter, revision).await;
            }
            DaemonEvent::ConnectionChanged(c) => {
                let _ = CobbleDaemon::connection_changed(emitter, c).await;
                let _ = iface.get().await.connected_changed(iface.signal_emitter()).await;
                if c {
                    // The connect-time battery read can be queued before this
                    // event and dropped by the disconnected gate; deliver it now.
                    if let Some(level) = daemon.session_battery_level()
                        && daemon.set_battery_level(level)
                    {
                        let _ =
                            iface.get().await.battery_level_changed(iface.signal_emitter()).await;
                        let _ = CobbleDaemon::battery_changed(emitter, i16::from(level)).await;
                    }
                } else {
                    // Battery is unknown while disconnected (state was cleared).
                    let _ = iface.get().await.battery_level_changed(iface.signal_emitter()).await;
                    let _ = CobbleDaemon::battery_changed(emitter, -1).await;
                }
            }
            DaemonEvent::BatteryChanged(level) => {
                // Gated on connected so a late event after disconnect can't
                // resurrect a stale level past the -1 contract.
                if daemon.set_battery_level(level) {
                    let _ = iface.get().await.battery_level_changed(iface.signal_emitter()).await;
                    let _ = CobbleDaemon::battery_changed(emitter, i16::from(level)).await;
                }
            }
            DaemonEvent::AppRunState { uuid, running } => {
                let _ = CobbleDaemon::app_run_state_changed(emitter, &uuid, running).await;
                // This firmware doesn't send GetCurrentTrack, but it does launch
                // the Music app — replay the cached now-playing so it displays.
                if running && uuid == MUSIC_APP_UUID {
                    daemon.replay_music_state().await;
                }
            }
            DaemonEvent::MusicAction(action) => {
                let _ = CobbleDaemon::music_action_received(emitter, &action).await;
                // The watch asks for the now-playing when its music app opens;
                // replay the last pushed state so it actually displays something.
                if action == "get_current_track" {
                    daemon.replay_music_state().await;
                }
                // Forward the action to the MPRIS monitor so it can control
                // the desktop media player (play/pause/next/volume/…).
                let _ = daemon.music_action_tx().send(action);
            }
            DaemonEvent::PhoneAction(action) => {
                let (name, cookie) = match action {
                    libpebble_ble::PhoneAction::Answer { cookie } => ("answer", cookie),
                    libpebble_ble::PhoneAction::Hangup { cookie } => ("hangup", cookie),
                };
                let _ = CobbleDaemon::phone_action_received(emitter, name, cookie).await;
                // Forward to the call monitor — it will call push_call_start
                // only after the modem confirms the answer.
                let _ = daemon.phone_action_tx().send((name.to_string(), cookie));
            }
            DaemonEvent::AppMessageReceived { uuid, data } => {
                let wire = encode_wire_dict(&data);
                let _ = CobbleDaemon::app_message_received(emitter, &uuid, wire).await;
            }
            DaemonEvent::AckReceived(txn) => {
                let _ = CobbleDaemon::ack_received(emitter, txn as u32).await;
            }
            DaemonEvent::NackReceived(txn) => {
                let _ = CobbleDaemon::nack_received(emitter, txn as u32).await;
            }
            DaemonEvent::HealthData(batch) => {
                if let Some(db) = &app_db {
                    let db = db.clone();
                    let batch_for_db = batch.clone();
                    match tokio::task::spawn_blocking(move || {
                        db.lock().unwrap().insert_batch(&batch_for_db)
                    })
                    .await
                    {
                        Ok(Err(e)) => warn!("app DB insert failed: {e}"),
                        Err(e) => warn!("app DB task panicked: {e}"),
                        Ok(Ok(true)) => {
                            daemon
                                .wellness_status_revision
                                .fetch_add(1, Ordering::SeqCst);
                            wellness_revision_tx.send_modify(|revision| {
                                *revision = revision.wrapping_add(1);
                            });
                        }
                        Ok(Ok(false)) => {}
                    }
                }
                let _ = CobbleDaemon::health_data_received(
                    emitter,
                    batch.tag,
                    batch.app_uuid.to_vec(),
                    batch.session_timestamp,
                    batch.items_left,
                    batch.crc,
                    batch.item_type,
                    batch.item_size,
                    batch.data,
                )
                .await;
            }
            DaemonEvent::HealthProfile(prefs) => {
                let profile = daemon.cache_health_profile(prefs);
                daemon.note_device_config_value();
                let _ = CobbleDaemon::health_profile_received(emitter, profile.to_dbus_map()).await;
            }
            DaemonEvent::HealthActivityRaw(config, raw) => {
                daemon.cache_health_activity_raw(config, raw);
                daemon.note_device_config_value();
            }
            DaemonEvent::HealthHrm(hrm, raw) => {
                if let Some(profile) = daemon.cache_hrm(hrm, raw) {
                    let _ = CobbleDaemon::health_profile_received(emitter, profile.to_dbus_map()).await;
                }
                daemon.note_device_config_value();
            }
            DaemonEvent::HealthHeartRate(hr, raw) => {
                if let Some(profile) = daemon.cache_heart_rate(hr, raw) {
                    let _ = CobbleDaemon::health_profile_received(emitter, profile.to_dbus_map()).await;
                }
                daemon.note_device_config_value();
            }
            DaemonEvent::HealthUnits(imperial, raw) => {
                if let Some(profile) = daemon.cache_units(imperial, raw) {
                    let _ = CobbleDaemon::health_profile_received(emitter, profile.to_dbus_map()).await;
                }
                daemon.note_device_config_value();
            }
            DaemonEvent::WatchSetting { key, value, raw } => {
                let variant = watch_pref_owned_value(&value);
                daemon.cache_watch_setting(key.clone(), value, raw);
                daemon.note_device_config_value();
                let _ = CobbleDaemon::watch_setting_received(emitter, &key, variant).await;
            }
        }
    }
}
