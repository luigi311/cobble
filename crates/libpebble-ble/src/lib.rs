//! libpebble-ble — talk to a Pebble smartwatch over BLE from Linux.
//!
//! Platform: Linux only. Requires a running BlueZ >= 5.48.

pub mod endpoints;
pub mod error;
pub mod transport;
pub mod uuids;

mod pebble;

pub use endpoints::Endpoint;
pub use endpoints::app_message::AppMessageValue;
pub use endpoints::app_run_state::AppRunStateCmd;
pub use endpoints::blob_db::{
    BlobDBId, BlobDBStatus, NotificationCategory, WeatherType, is_health_preference,
    logical_incoming_database, outgoing_preference_database,
};
pub use endpoints::datalog::DatalogData;
pub use endpoints::health::{
    ActivityPreferences, HealthActivityConfig, HealthConfigError, HeartRatePreferences,
    HrMonitoringInterval, HrmPreferences, encode_activity_preferences,
    encode_heart_rate_preferences, encode_hrm_preferences, encode_units_distance,
    parse_activity_preferences, parse_health_activity_config, parse_heart_rate_preferences,
    parse_hrm_preferences, parse_units_distance,
};
pub use endpoints::music::{MusicAction, MusicPlaybackState, MusicRepeat, MusicShuffle};
pub use endpoints::phone_control::PhoneAction;
pub use endpoints::reset::ResetCommand;
pub use endpoints::screenshot::ScreenshotVersion;
pub use endpoints::system::{
    FirmwareVersion, WatchColorInfo, WatchFirmwareVersion, WatchType, WatchVersionInfo,
    hardware_platform, watch_color,
};
pub use endpoints::watch_pref::{
    WatchPrefDefault, WatchPrefEncodeError, WatchPrefMetadata, WatchPrefModel, WatchPrefOption,
    WatchPrefType, WatchPrefValue, WatchPrefVariant, decode_watch_pref,
    decode_watch_pref_for_model, encode_watch_pref, watch_pref_metadata,
};
pub use error::PebbleError;
pub use pebble::{
    AckHandler, AppMessageHandler, AppRunStateHandler, BatteryHandler, HealthDataHandler,
    MusicActionHandler, NackHandler, Pebble, PhoneActionHandler, PreferenceWriteConfirmation,
    Screenshot, WatchPrefHandler,
};
