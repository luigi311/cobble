//! Transport-neutral contracts for the public Cobble daemon API.
//!
//! This crate deliberately contains no D-Bus, BLE, TOML, or UI code. Client
//! applications should consume these types through `cobble-client`, which
//! re-exports them as its public model layer.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

pub const CONFIG_API_VERSION: u16 = 1;
pub type Revision = u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonConfigSnapshot {
    pub api_version: u16,
    pub revision: Revision,
    pub config_path: String,
    pub resolved_database_path: String,
    pub config: DaemonConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonConfig {
    pub address: String,
    pub adapter: String,
    pub verbose: bool,
    pub database_path: Option<String>,
    pub intervals_icu: IntervalsIcuConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntervalsIcuConfig {
    pub enabled: bool,
    pub athlete_id: String,
    pub api_key_configured: bool,
}

/// Only fields present in a patch are edited.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonConfigPatch {
    pub expected_revision: Revision,
    pub address: Option<String>,
    pub adapter: Option<String>,
    pub verbose: Option<bool>,
    /// Outer `None` leaves it untouched; inner `None` selects the default.
    pub database_path: Option<Option<String>>,
    pub intervals_icu: Option<IntervalsIcuPatch>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntervalsIcuPatch {
    pub enabled: Option<bool>,
    pub athlete_id: Option<String>,
    pub api_key: Option<SecretPatch>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecretPatch {
    Replace(String),
    Clear,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyDisposition {
    AppliedLive,
    Reconnecting,
    GuiDataSourceReopenRequired,
    DaemonRestartRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonConfigUpdate {
    pub snapshot: DaemonConfigSnapshot,
    pub fields: BTreeMap<String, ApplyDisposition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeviceConfigState {
    Disconnected,
    Loading,
    Ready,
    Partial,
    Error,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchIdentity {
    pub watch_id: String,
    pub display_name: Option<String>,
    pub platform: Option<String>,
    pub firmware: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceCapabilities {
    pub blob_db_version: u8,
    pub supported: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceConfigSnapshot {
    pub api_version: u16,
    pub revision: Revision,
    pub state: DeviceConfigState,
    pub watch: Option<WatchIdentity>,
    pub capabilities: DeviceCapabilities,
    /// Unix timestamp in milliseconds for the last confirmed watch read.
    pub last_read_at_ms: Option<i64>,
    pub health: FieldValue<HealthConfig>,
    pub preferences: BTreeMap<String, PreferenceField>,
    pub error: Option<ConfigError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldAvailability {
    Available,
    NotReceived,
    Unsupported,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldValue<T> {
    pub availability: FieldAvailability,
    pub value: Option<T>,
    pub error: Option<ConfigError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthConfig {
    pub height_mm: u16,
    pub weight_dag: u16,
    pub tracking_enabled: bool,
    pub activity_insights_enabled: bool,
    pub sleep_insights_enabled: bool,
    pub age: u8,
    /// Pebble protocol compatibility field: Female=0, Male=1, Other=2.
    pub gender: u8,
    pub distance_units: DistanceUnits,
    pub hrm: FieldValue<HrmConfig>,
    pub heart_rate_thresholds: FieldValue<HeartRateThresholds>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceUnits {
    Metric,
    Imperial,
    Unknown(u8),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HrmConfig {
    pub enabled: bool,
    pub measurement_interval: Option<HrmMeasurementInterval>,
    pub during_activity: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HrmMeasurementInterval {
    TenMinutes,
    ThirtyMinutes,
    OneHour,
    Off,
    Unknown(u8),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartRateThresholds {
    pub resting: u8,
    pub elevated: u8,
    pub maximum: u8,
    pub zone_1: u8,
    pub zone_2: u8,
    pub zone_3: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreferenceField {
    pub availability: FieldAvailability,
    pub value: Option<PreferenceValue>,
    /// Exact record retained for unknown values and lossless reconciliation.
    pub raw: Vec<u8>,
    pub error: Option<ConfigError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferenceValue {
    Bool(bool),
    Unsigned(u32),
    Text(String),
    Color(u32),
    Enum { code: u32, label: Option<String> },
    Unknown,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceConfigPatch {
    pub expected_revision: Revision,
    pub health: Option<HealthConfigPatch>,
    pub preferences: BTreeMap<String, PreferenceValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthConfigPatch {
    pub height_mm: Option<u16>,
    pub weight_dag: Option<u16>,
    pub tracking_enabled: Option<bool>,
    pub activity_insights_enabled: Option<bool>,
    pub sleep_insights_enabled: Option<bool>,
    pub age: Option<u8>,
    pub gender: Option<u8>,
    pub distance_units: Option<DistanceUnits>,
    pub hrm_enabled: Option<bool>,
    pub hrm_measurement_interval: Option<HrmMeasurementInterval>,
    pub hrm_during_activity: Option<bool>,
    pub heart_rate_thresholds: Option<HeartRateThresholds>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigErrorKind {
    InvalidData,
    RevisionConflict,
    NotSupported,
    Disconnected,
    Timeout,
    Rejected,
    ReadbackMismatch,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigError {
    pub kind: ConfigErrorKind,
    pub field: Option<String>,
    pub message: String,
}
