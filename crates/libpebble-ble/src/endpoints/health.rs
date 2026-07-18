//! Pebble Health — activation preferences and sync trigger.
//!
//! Activation (once, after first connect):
//!   Write user profile to BlobDB PREFERENCES key "activityPreferences".
//!   Optionally write "hrmPreferences" to enable heart-rate monitoring.
//!
//! Sync trigger (on demand):
//!   Send a HealthSync request (endpoint 911). The watch ACKs it and then
//!   streams pending records via the DataLog endpoint (0x6778).

/// Build the 9-byte blob for the "activityPreferences" BlobDB PREFERENCES key.
///
/// The watch uses this to configure its health tracking and step calibration.
/// `height_cm`  user height in centimetres.
/// `weight_kg`  user weight in kilograms.
/// `age`        user age in years.
/// `gender`     0 = female, 1 = male, 2 = other (matches libpebble3 `HealthGender`;
///              used for step-length calibration).
pub fn build_activate_health_blob(height_cm: u16, weight_kg: u16, age: u8, gender: u8) -> Vec<u8> {
    let mut blob = Vec::with_capacity(9);
    blob.extend_from_slice(&height_cm.saturating_mul(10).to_le_bytes()); // height in mm (LE u16)
    blob.extend_from_slice(&weight_kg.saturating_mul(100).to_le_bytes()); // weight in dag (LE u16)
    blob.push(0x01); // tracking enabled
    blob.push(0x00); // activity insights disabled
    blob.push(0x00); // sleep insights disabled
    blob.push(age);
    blob.push(gender);
    blob
}

/// Build the 9-byte blob to deactivate health tracking (all zeros).
pub fn build_deactivate_health_blob() -> Vec<u8> {
    vec![0u8; 9]
}

/// Decoded "activityPreferences" health profile, read back from the watch.
///
/// This is the inverse of [`build_activate_health_blob`]: the watch stores the
/// 9-byte blob we wrote and (on a BlobDB2 sync) hands it back unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivityPreferences {
    pub height_cm: u16,
    pub weight_kg: u16,
    pub tracking_enabled: bool,
    pub activity_insights_enabled: bool,
    pub sleep_insights_enabled: bool,
    pub age: u8,
    /// 0 = female, 1 = male, 2 = other (matches libpebble3 `HealthGender`).
    pub gender: u8,
}

/// Lossless writable form of the activity-preferences record in native wire
/// units. This is separate from the legacy centimetre/kilogram compatibility
/// decoder above.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthActivityConfig {
    pub height_mm: u16,
    pub weight_dag: u16,
    pub tracking_enabled: bool,
    pub activity_insights_enabled: bool,
    pub sleep_insights_enabled: bool,
    pub age: u8,
    pub gender: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HealthConfigError {
    #[error("height must be between 1000 and 2200 mm")]
    Height,
    #[error("weight must be between 3000 and 20000 dag")]
    Weight,
    #[error("age must be between 1 and 120")]
    Age,
    #[error("gender must be Female (0), Male (1), or Other (2)")]
    Gender,
}

pub fn encode_activity_preferences(
    config: &HealthActivityConfig,
) -> Result<Vec<u8>, HealthConfigError> {
    if !(1000..=2200).contains(&config.height_mm) {
        return Err(HealthConfigError::Height);
    }
    if !(3000..=20_000).contains(&config.weight_dag) {
        return Err(HealthConfigError::Weight);
    }
    if !(1..=120).contains(&config.age) {
        return Err(HealthConfigError::Age);
    }
    if config.gender > 2 {
        return Err(HealthConfigError::Gender);
    }
    let mut blob = Vec::with_capacity(9);
    blob.extend_from_slice(&config.height_mm.to_le_bytes());
    blob.extend_from_slice(&config.weight_dag.to_le_bytes());
    blob.push(u8::from(config.tracking_enabled));
    blob.push(u8::from(config.activity_insights_enabled));
    blob.push(u8::from(config.sleep_insights_enabled));
    blob.push(config.age);
    blob.push(config.gender);
    Ok(blob)
}

/// Decode a 9-byte "activityPreferences" blob into an [`ActivityPreferences`].
///
/// Returns `None` if the blob is shorter than 9 bytes. Trailing bytes beyond
/// the 9th are ignored so a longer firmware blob still parses.
pub fn parse_activity_preferences(blob: &[u8]) -> Option<ActivityPreferences> {
    if blob.len() < 9 {
        return None;
    }
    let height_mm = u16::from_le_bytes([blob[0], blob[1]]);
    let weight_dag = u16::from_le_bytes([blob[2], blob[3]]);
    Some(ActivityPreferences {
        height_cm: height_mm / 10,   // stored in mm
        weight_kg: weight_dag / 100, // stored in decagrams
        tracking_enabled: blob[4] != 0,
        activity_insights_enabled: blob[5] != 0,
        sleep_insights_enabled: blob[6] != 0,
        age: blob[7],
        gender: blob[8],
    })
}

/// Decode a "unitsDistance" blob (libpebble3 `DistanceUnitsBlobItem`): one byte.
/// Returns `Some(true)` for imperial units (mi/lb), `Some(false)` for metric
/// (km/kg), or `None` if empty.
pub fn parse_units_distance(blob: &[u8]) -> Option<bool> {
    blob.first().map(|&b| b != 0)
}

pub fn encode_units_distance(imperial: bool) -> Vec<u8> {
    vec![u8::from(imperial)]
}

/// Heart-rate monitoring interval (libpebble3 `HRMonitoringInterval`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HrMonitoringInterval {
    TenMin,
    ThirtyMin,
    OneHour,
    Disabled,
    /// A value the firmware reported that we don't have a name for.
    Unknown(u8),
}

impl HrMonitoringInterval {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::TenMin,
            1 => Self::ThirtyMin,
            2 => Self::OneHour,
            3 => Self::Disabled,
            other => Self::Unknown(other),
        }
    }

    /// The on-wire byte value (round-trips `from_u8`).
    pub fn code(self) -> u8 {
        match self {
            Self::TenMin => 0,
            Self::ThirtyMin => 1,
            Self::OneHour => 2,
            Self::Disabled => 3,
            Self::Unknown(v) => v,
        }
    }
}

/// Decoded "hrmPreferences" blob (libpebble3 `ActivityHRMSettings`).
///
/// The struct grew across firmware revisions, so the optional fields are only
/// present when the watch sent a long-enough blob:
///   1 byte  → `enabled` only (legacy hardware)
///   2 bytes → `+ measurement_interval` (fw ≥ v4.9.146)
///   3 bytes → `+ activity_tracking_enabled` (fw ≥ v4.9.150)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HrmPreferences {
    pub enabled: bool,
    pub measurement_interval: Option<HrMonitoringInterval>,
    pub activity_tracking_enabled: Option<bool>,
}

/// Decode an "hrmPreferences" blob. Returns `None` if empty.
pub fn parse_hrm_preferences(blob: &[u8]) -> Option<HrmPreferences> {
    let enabled = *blob.first()? != 0;
    Some(HrmPreferences {
        enabled,
        measurement_interval: blob.get(1).map(|&b| HrMonitoringInterval::from_u8(b)),
        activity_tracking_enabled: blob.get(2).map(|&b| b != 0),
    })
}

/// Build the 1-byte blob for the "hrmPreferences" BlobDB PREFERENCES key.
pub fn build_hrm_blob(enabled: bool) -> Vec<u8> {
    vec![if enabled { 0x01 } else { 0x00 }]
}

/// Encode the firmware-sized ActivityHRMSettings record. Firmware before
/// 4.9.146 accepts one byte, 4.9.146–149 accepts two, and 4.9.150+ accepts three.
pub fn encode_hrm_preferences(
    enabled: bool,
    measurement_interval: HrMonitoringInterval,
    activity_tracking_enabled: bool,
    firmware: (u8, u8, u16),
) -> Vec<u8> {
    let mut blob = vec![u8::from(enabled)];
    if firmware >= (4, 9, 146) {
        blob.push(measurement_interval.code());
    }
    if firmware >= (4, 9, 150) {
        blob.push(u8::from(activity_tracking_enabled));
    }
    blob
}

/// Decoded "heartRatePreferences" blob (libpebble3 `HeartRatePreferencesBlobItem`):
/// six little-endian `u8` BPM/threshold values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartRatePreferences {
    pub resting_hr: u8,
    pub elevated_hr: u8,
    pub max_hr: u8,
    pub zone1_threshold: u8,
    pub zone2_threshold: u8,
    pub zone3_threshold: u8,
}

/// Decode a 6-byte "heartRatePreferences" blob. Returns `None` if too short.
pub fn parse_heart_rate_preferences(blob: &[u8]) -> Option<HeartRatePreferences> {
    if blob.len() < 6 {
        return None;
    }
    Some(HeartRatePreferences {
        resting_hr: blob[0],
        elevated_hr: blob[1],
        max_hr: blob[2],
        zone1_threshold: blob[3],
        zone2_threshold: blob[4],
        zone3_threshold: blob[5],
    })
}

pub fn encode_heart_rate_preferences(config: &HeartRatePreferences) -> Vec<u8> {
    vec![
        config.resting_hr,
        config.elevated_hr,
        config.max_hr,
        config.zone1_threshold,
        config.zone2_threshold,
        config.zone3_threshold,
    ]
}

/// Health sync request command (phone → watch, endpoint 911).
pub const HEALTH_SYNC_CMD_SYNC: u8 = 0x01;
/// Health sync ACK command (watch → phone, endpoint 911).
pub const HEALTH_SYNC_CMD_ACK: u8 = 0x11;

/// Build the 5-byte payload for a HealthSync request (endpoint 911).
///
/// `seconds_since_sync = 0` asks the watch to flush everything in its queue.
pub fn build_health_sync_request() -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    out.push(HEALTH_SYNC_CMD_SYNC);
    out.extend_from_slice(&0u32.to_le_bytes()); // seconds_since_sync = 0
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical_fixture(record: &str, variant: &str) -> Vec<u8> {
        let line = include_str!("../../tests/fixtures/health_canonical.tsv")
            .lines()
            .find(|line| {
                let mut columns = line.split_ascii_whitespace();
                columns.next() == Some(record) && columns.next() == Some(variant)
            })
            .expect("canonical health fixture");
        let hex = line.split_ascii_whitespace().nth(2).expect("fixture hex");
        hex.as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII hex"), 16)
                    .expect("valid fixture hex")
            })
            .collect()
    }

    #[test]
    fn activity_preferences_round_trips() {
        let blob = build_activate_health_blob(180, 75, 30, 1);
        let prefs = parse_activity_preferences(&blob).expect("decodes");
        assert_eq!(prefs.height_cm, 180);
        assert_eq!(prefs.weight_kg, 75);
        assert_eq!(prefs.age, 30);
        assert_eq!(prefs.gender, 1);
        assert!(prefs.tracking_enabled);
        assert!(!prefs.activity_insights_enabled);
        assert!(!prefs.sleep_insights_enabled);
    }

    #[test]
    fn canonical_activity_and_units_fixtures_decode() {
        let raw = canonical_fixture("activityPreferences", "defaults");
        let activity = parse_activity_preferences(&raw).expect("decodes");
        assert_eq!(activity.height_cm, 170);
        assert_eq!(activity.weight_kg, 70);
        assert_eq!(activity.age, 35);
        assert_eq!(
            encode_activity_preferences(&HealthActivityConfig {
                height_mm: 1700,
                weight_dag: 7000,
                tracking_enabled: false,
                activity_insights_enabled: false,
                sleep_insights_enabled: false,
                age: 35,
                gender: 0,
            })
            .expect("encodes"),
            raw
        );
        assert_eq!(
            parse_units_distance(&canonical_fixture("unitsDistance", "metric")),
            Some(false)
        );
        assert_eq!(
            parse_units_distance(&canonical_fixture("unitsDistance", "imperial")),
            Some(true)
        );
        assert_eq!(
            encode_units_distance(false),
            canonical_fixture("unitsDistance", "metric")
        );
        assert_eq!(
            encode_units_distance(true),
            canonical_fixture("unitsDistance", "imperial")
        );
    }

    #[test]
    fn native_activity_validation_rejects_out_of_range_values() {
        let valid = HealthActivityConfig {
            height_mm: 1700,
            weight_dag: 7000,
            tracking_enabled: true,
            activity_insights_enabled: true,
            sleep_insights_enabled: true,
            age: 35,
            gender: 2,
        };
        assert!(encode_activity_preferences(&valid).is_ok());
        assert_eq!(
            encode_activity_preferences(&HealthActivityConfig {
                height_mm: 999,
                ..valid
            }),
            Err(HealthConfigError::Height)
        );
        assert_eq!(
            encode_activity_preferences(&HealthActivityConfig { gender: 3, ..valid }),
            Err(HealthConfigError::Gender)
        );
    }

    #[test]
    fn activity_preferences_rejects_short_blob() {
        assert!(parse_activity_preferences(&[0u8; 8]).is_none());
    }

    #[test]
    fn hrm_preferences_decodes() {
        let one = parse_hrm_preferences(&canonical_fixture("hrmPreferences", "pre-4.9.146"))
            .expect("decodes");
        assert!(one.enabled);
        assert_eq!(one.measurement_interval, None);
        assert_eq!(one.activity_tracking_enabled, None);

        // Canonical current-firmware shape: enabled, interval, activity tracking.
        let three = parse_hrm_preferences(&canonical_fixture("hrmPreferences", "4.9.150+"))
            .expect("decodes");
        assert!(three.enabled);
        assert_eq!(
            three.measurement_interval,
            Some(HrMonitoringInterval::ThirtyMin)
        );
        assert_eq!(three.activity_tracking_enabled, Some(true));

        assert_eq!(parse_hrm_preferences(&[]), None);
        assert_eq!(
            encode_hrm_preferences(true, HrMonitoringInterval::ThirtyMin, true, (4, 9, 145)),
            canonical_fixture("hrmPreferences", "pre-4.9.146")
        );
        assert_eq!(
            encode_hrm_preferences(true, HrMonitoringInterval::ThirtyMin, true, (4, 9, 146)),
            canonical_fixture("hrmPreferences", "4.9.146..4.9.149")
        );
        assert_eq!(
            encode_hrm_preferences(true, HrMonitoringInterval::ThirtyMin, true, (4, 9, 150)),
            canonical_fixture("hrmPreferences", "4.9.150+")
        );
    }

    #[test]
    fn heart_rate_preferences_decodes() {
        let hr =
            parse_heart_rate_preferences(&canonical_fixture("heartRatePreferences", "defaults"))
                .expect("decodes");
        assert_eq!(hr.resting_hr, 70);
        assert_eq!(hr.elevated_hr, 100);
        assert_eq!(hr.max_hr, 190);
        assert_eq!(hr.zone1_threshold, 130);
        assert_eq!(hr.zone2_threshold, 154);
        assert_eq!(hr.zone3_threshold, 172);
        assert_eq!(
            encode_heart_rate_preferences(&hr),
            canonical_fixture("heartRatePreferences", "defaults")
        );
        assert!(parse_heart_rate_preferences(&[1, 2, 3, 4, 5]).is_none());
    }
}
