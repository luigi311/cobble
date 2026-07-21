//! Daemon state: events, health profile, music, and session data.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use libpebble_ble::{
    ActivityPreferences, AppMessageValue, DatalogData, HeartRatePreferences, HrmPreferences,
    HealthActivityConfig, Pebble, WatchPrefValue, WatchVersionInfo,
};

use cobble_db::AppDb;
use cobble_config::Config;
use cobble_contracts::DeviceConfigState;
use tokio::sync::{mpsc, oneshot};
use zbus::zvariant::{OwnedValue, Value};

pub const BUS_NAME: &str = "org.cobble.Daemon";
pub const OBJECT_PATH: &str = "/org/cobble/Daemon";
pub(crate) const MUSIC_APP_UUID: &str = "1f03293d-47af-4f28-b960-f2b02a6dd757";

#[derive(Debug, zbus::DBusError)]
#[zbus(prefix = "org.cobble.Daemon")]
pub(crate) enum DaemonError {
    NotConnected(String),
    Failed(String),
}

#[derive(Debug)]
pub enum DaemonEvent {
    ConnectionChanged(bool),
    AppMessageReceived { uuid: String, data: HashMap<u32, AppMessageValue> },
    AckReceived(u8),
    NackReceived(u8),
    HealthData(DatalogData),
    BatteryChanged(u8),
    AppRunState { uuid: String, running: bool },
    MusicAction(String),
    PhoneAction(libpebble_ble::PhoneAction),
    HealthProfile(ActivityPreferences),
    HealthActivityRaw(HealthActivityConfig, Vec<u8>),
    HealthHrm(HrmPreferences, Vec<u8>),
    HealthHeartRate(HeartRatePreferences, Vec<u8>),
    HealthUnits(bool, Vec<u8>),
    WatchSetting { key: String, value: WatchPrefValue, raw: Vec<u8> },
    DaemonConfigChanged(u64),
    DeviceConfigChanged { revision: u64, state: DeviceConfigState },
    DeviceConfigRefreshComplete {
        info: Option<WatchVersionInfo>,
        blob_db_version: u8,
        error: Option<String>,
        done: oneshot::Sender<()>,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct HealthProfile {
    pub height_cm: u16, pub weight_kg: u16, pub age: u16, pub gender: u16,
    pub tracking_enabled: bool, pub activity_insights_enabled: bool, pub sleep_insights_enabled: bool,
    pub hrm_enabled: bool, pub hrm_measurement_interval: u8, pub hrm_activity_tracking: bool,
    pub resting_hr: u16, pub elevated_hr: u16, pub max_hr: u16,
    pub hr_zone1_threshold: u16, pub hr_zone2_threshold: u16, pub hr_zone3_threshold: u16,
    pub imperial_units: bool,
}

impl HealthProfile {
    pub(crate) fn to_dbus_map(self) -> HashMap<String, OwnedValue> {
        fn val(v: impl Into<Value<'static>>) -> OwnedValue {
            OwnedValue::try_from(v.into()).expect("primitive converts to OwnedValue")
        }
        HashMap::from([
            ("height_cm".into(), val(self.height_cm)),
            ("weight_kg".into(), val(self.weight_kg)),
            ("age".into(), val(self.age)),
            ("gender".into(), val(self.gender)),
            ("tracking_enabled".into(), val(self.tracking_enabled)),
            ("activity_insights_enabled".into(), val(self.activity_insights_enabled)),
            ("sleep_insights_enabled".into(), val(self.sleep_insights_enabled)),
            ("hrm_enabled".into(), val(self.hrm_enabled)),
            ("hrm_measurement_interval".into(), val(self.hrm_measurement_interval)),
            ("hrm_activity_tracking".into(), val(self.hrm_activity_tracking)),
            ("resting_hr".into(), val(self.resting_hr)),
            ("elevated_hr".into(), val(self.elevated_hr)),
            ("max_hr".into(), val(self.max_hr)),
            ("hr_zone1_threshold".into(), val(self.hr_zone1_threshold)),
            ("hr_zone2_threshold".into(), val(self.hr_zone2_threshold)),
            ("hr_zone3_threshold".into(), val(self.hr_zone3_threshold)),
            ("imperial_units".into(), val(self.imperial_units)),
        ])
    }
}

#[derive(Default, Clone)]
pub(crate) struct MusicState {
    pub(crate) player: Option<(String, String)>,
    pub(crate) track: Option<(String, String, String, u32, u32, u32)>,
    pub(crate) play_state: Option<(u8, u32, u32, u8, u8)>,
    pub(crate) volume: Option<u8>,
}

pub(crate) fn watch_pref_owned_value(v: &WatchPrefValue) -> OwnedValue {
    let value = match v {
        WatchPrefValue::Bool(b) => Value::from(*b),
        WatchPrefValue::Number(n) => Value::from(*n),
        WatchPrefValue::Text(s) => Value::from(s.clone()),
    };
    OwnedValue::try_from(value).expect("primitive value converts to OwnedValue")
}

pub(crate) fn dbus_val(v: impl Into<Value<'static>>) -> OwnedValue {
    OwnedValue::try_from(v.into()).expect("primitive converts to OwnedValue")
}

pub(crate) struct DaemonState {
    pub(crate) address: String,
    pub(crate) adapter: String,
    pub(crate) config_path: PathBuf,
    pub(crate) config: Config,
    pub(crate) config_error: Option<String>,
    pub(crate) active_database_path: PathBuf,
    pub(crate) active_verbose: bool,
    pub(crate) pebble: Option<Arc<Pebble>>,
    pub(crate) connected: bool,
    pub(crate) stopping: bool,
    pub(crate) notify_blocklist: Vec<String>,
    pub(crate) event_tx: mpsc::UnboundedSender<DaemonEvent>,
    pub(crate) db: Option<Arc<Mutex<AppDb>>>,
    pub(crate) health_profile: Option<ActivityPreferences>,
    pub(crate) device_health_activity: Option<HealthActivityConfig>,
    pub(crate) device_health_activity_raw: Option<Vec<u8>>,
    pub(crate) hrm_prefs: Option<HrmPreferences>,
    pub(crate) hrm_prefs_raw: Option<Vec<u8>>,
    pub(crate) heart_rate_prefs: Option<HeartRatePreferences>,
    pub(crate) heart_rate_prefs_raw: Option<Vec<u8>>,
    pub(crate) imperial_units: Option<bool>,
    pub(crate) imperial_units_raw: Option<Vec<u8>>,
    pub(crate) watch_settings: HashMap<String, WatchPrefValue>,
    pub(crate) watch_setting_raw: HashMap<String, Vec<u8>>,
    pub(crate) device_config_revision: u64,
    pub(crate) device_config_state: DeviceConfigState,
    pub(crate) device_config_last_read_at_ms: Option<i64>,
    pub(crate) device_config_watch: Option<WatchVersionInfo>,
    pub(crate) device_config_blob_db_version: u8,
    pub(crate) device_config_error: Option<String>,
    pub(crate) battery_level: Option<u8>,
    pub(crate) music: MusicState,
}
