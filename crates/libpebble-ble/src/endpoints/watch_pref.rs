//! WatchPrefs (BlobDB 12) general device-settings decoding.
//!
//! Mirrors libpebble3's typed `WatchPref` registry (`WatchPrefEntity.kt`): each
//! known key maps to a wire type, and [`decode_watch_pref`] turns the raw blob
//! the watch syncs into a typed [`WatchPrefValue`].
//!
//! Health keys (activityPreferences, hrmPreferences, heartRatePreferences,
//! unitsDistance) are NOT here — they live in [`crate::endpoints::health`].
//! Keys libpebble3 itself leaves raw (dndWeekday/WeekendSchedule, workerId,
//! *AppOpened markers, watchface, automaticTimezoneID) are intentionally absent
//! so [`watch_pref_type`] returns `None` and the caller leaves them untouched.

use uuid::Uuid;

/// Wire encoding of a watch preference value (libpebble3 `WatchPrefType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchPrefType {
    Bool,
    /// One-byte value, including the various single-byte enum settings.
    U8,
    U16,
    U32,
    Str,
    Uuid,
    /// `[enabled: u8][uuid: 16B]` quick-launch binding.
    QuickLaunch,
    /// One-byte Pebble color code.
    Color,
}

/// A decoded watch preference value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchPrefValue {
    Bool(bool),
    /// Unsigned integer (u8/u16/u32 widths and single-byte enum codes).
    Number(u32),
    /// String, UUID, quick-launch, or color rendered as text.
    Text(String),
}

/// Model context needed for symmetric preference transforms.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WatchPrefModel {
    #[default]
    Standard,
    Emery,
    Gabbro,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchPrefDefault {
    Bool(bool),
    Number(u32),
    QuickLaunch {
        enabled: bool,
        uuid: Option<&'static str>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchPrefOption {
    pub code: u32,
    pub label: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchPrefMetadata {
    pub key: &'static str,
    pub wire_type: WatchPrefType,
    pub default: WatchPrefDefault,
    pub range: Option<(u32, u32)>,
    pub options: &'static [WatchPrefOption],
    pub debug_only: bool,
    pub variant: WatchPrefVariant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchPrefVariant {
    Common,
    Legacy,
    Current,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WatchPrefEncodeError {
    #[error("unknown watch preference: {0}")]
    UnknownKey(String),
    #[error("wrong value type for watch preference: {0}")]
    WrongType(String),
    #[error("value {value} is invalid for watch preference {key}")]
    InvalidValue { key: String, value: u32 },
    #[error("invalid UUID for watch preference {0}")]
    InvalidUuid(String),
}

const EMPTY_OPTIONS: &[WatchPrefOption] = &[];
const TEXT_SIZE: &[WatchPrefOption] = &[
    WatchPrefOption {
        code: 0,
        label: "Smaller",
    },
    WatchPrefOption {
        code: 1,
        label: "Default",
    },
    WatchPrefOption {
        code: 2,
        label: "Larger",
    },
];
const ALERT_MASK: &[WatchPrefOption] = &[
    WatchPrefOption {
        code: 0,
        label: "All Off",
    },
    WatchPrefOption {
        code: 2,
        label: "Phone Calls",
    },
    WatchPrefOption {
        code: 15,
        label: "All On",
    },
];
const DND_INTERRUPTIONS_MASK: &[WatchPrefOption] = &[ALERT_MASK[0], ALERT_MASK[1]];
const SHOW_NOTIFICATIONS: &[WatchPrefOption] = &[
    WatchPrefOption {
        code: 0,
        label: "Hide",
    },
    WatchPrefOption {
        code: 1,
        label: "Show",
    },
];
const VIBE_INTENSITY: &[WatchPrefOption] = &[
    WatchPrefOption {
        code: 0,
        label: "Low",
    },
    WatchPrefOption {
        code: 1,
        label: "Medium",
    },
    WatchPrefOption {
        code: 2,
        label: "High",
    },
];
const VIBE_SCORE: &[WatchPrefOption] = &[
    WatchPrefOption {
        code: 1,
        label: "Disabled",
    },
    WatchPrefOption {
        code: 2,
        label: "Standard Short - Low",
    },
    WatchPrefOption {
        code: 3,
        label: "Standard Long - Low",
    },
    WatchPrefOption {
        code: 4,
        label: "Standard Short - High",
    },
    WatchPrefOption {
        code: 5,
        label: "Standard Long - High",
    },
    WatchPrefOption {
        code: 8,
        label: "Pulse",
    },
    WatchPrefOption {
        code: 9,
        label: "Nudge Nudge",
    },
    WatchPrefOption {
        code: 10,
        label: "Jackhammer",
    },
    WatchPrefOption {
        code: 11,
        label: "Reveille",
    },
    WatchPrefOption {
        code: 12,
        label: "Mario",
    },
    WatchPrefOption {
        code: 13,
        label: "ALARMS LPM",
    },
    WatchPrefOption {
        code: 14,
        label: "Gentle",
    },
];
const VIBE_SCORE_NOTIFICATIONS: &[WatchPrefOption] = &[
    VIBE_SCORE[0],
    VIBE_SCORE[1],
    VIBE_SCORE[3],
    VIBE_SCORE[5],
    VIBE_SCORE[6],
    VIBE_SCORE[7],
    VIBE_SCORE[9],
];
const VIBE_SCORE_CALLS: &[WatchPrefOption] = &[
    VIBE_SCORE[0],
    VIBE_SCORE[2],
    VIBE_SCORE[4],
    VIBE_SCORE[5],
    VIBE_SCORE[6],
    VIBE_SCORE[7],
    VIBE_SCORE[9],
];
const VIBE_SCORE_ALARMS: &[WatchPrefOption] = &[
    VIBE_SCORE[2],
    VIBE_SCORE[4],
    VIBE_SCORE[5],
    VIBE_SCORE[6],
    VIBE_SCORE[7],
    VIBE_SCORE[8],
    VIBE_SCORE[9],
    VIBE_SCORE[11],
];
const MENU_VIBE: &[WatchPrefOption] = &[
    WatchPrefOption {
        code: 0,
        label: "No Vibe",
    },
    WatchPrefOption {
        code: 1,
        label: "Vibe On Wrap Around",
    },
    WatchPrefOption {
        code: 2,
        label: "Vibe On Locked",
    },
];
const MOTION: &[WatchPrefOption] = &[
    WatchPrefOption {
        code: 10,
        label: "Very Low",
    },
    WatchPrefOption {
        code: 25,
        label: "Low",
    },
    WatchPrefOption {
        code: 40,
        label: "Medium-Low",
    },
    WatchPrefOption {
        code: 55,
        label: "Medium",
    },
    WatchPrefOption {
        code: 70,
        label: "Medium-High",
    },
    WatchPrefOption {
        code: 85,
        label: "High",
    },
    WatchPrefOption {
        code: 100,
        label: "Very High",
    },
];
const LIGHT_INTENSITY: &[WatchPrefOption] = &[
    WatchPrefOption {
        code: 10,
        label: "Low",
    },
    WatchPrefOption {
        code: 25,
        label: "Medium",
    },
    WatchPrefOption {
        code: 50,
        label: "High",
    },
    WatchPrefOption {
        code: 100,
        label: "Blinding",
    },
];
const LIGHT_TOUCH: &[WatchPrefOption] = &[
    WatchPrefOption {
        code: 0,
        label: "Double Tap",
    },
    WatchPrefOption {
        code: 1,
        label: "Tap",
    },
    WatchPrefOption {
        code: 2,
        label: "Off",
    },
];
const LIGHT_DYNAMIC: &[WatchPrefOption] = &[
    WatchPrefOption {
        code: 0,
        label: "Off",
    },
    WatchPrefOption {
        code: 1,
        label: "Bright",
    },
    WatchPrefOption {
        code: 2,
        label: "Standard",
    },
    WatchPrefOption {
        code: 3,
        label: "Dim",
    },
];
const LIGHT_PRESET: &[WatchPrefOption] = &[
    WatchPrefOption {
        code: 0,
        label: "Max Brightness",
    },
    WatchPrefOption {
        code: 1,
        label: "Standard",
    },
    WatchPrefOption {
        code: 2,
        label: "Battery Saver",
    },
    WatchPrefOption {
        code: 3,
        label: "Advanced",
    },
];
const LANGUAGE: &[WatchPrefOption] = &[
    WatchPrefOption {
        code: 0,
        label: "Custom (Language Pack)",
    },
    WatchPrefOption {
        code: 1,
        label: "English",
    },
    WatchPrefOption {
        code: 2,
        label: "Català",
    },
    WatchPrefOption {
        code: 3,
        label: "Deutsch",
    },
    WatchPrefOption {
        code: 4,
        label: "Español",
    },
    WatchPrefOption {
        code: 5,
        label: "Français",
    },
    WatchPrefOption {
        code: 6,
        label: "Italiano",
    },
    WatchPrefOption {
        code: 7,
        label: "Nederlands",
    },
    WatchPrefOption {
        code: 8,
        label: "Português",
    },
    WatchPrefOption {
        code: 9,
        label: "Polski",
    },
];

macro_rules! bool_pref {
    ($key:literal, $default:literal, $variant:ident) => {
        WatchPrefMetadata {
            key: $key,
            wire_type: WatchPrefType::Bool,
            default: WatchPrefDefault::Bool($default),
            range: None,
            options: EMPTY_OPTIONS,
            debug_only: false,
            variant: WatchPrefVariant::$variant,
        }
    };
}

macro_rules! number_pref {
    ($key:literal, $wire:ident, $default:expr, $range:expr, $options:expr, $debug:literal, $variant:ident) => {
        WatchPrefMetadata {
            key: $key,
            wire_type: WatchPrefType::$wire,
            default: WatchPrefDefault::Number($default),
            range: $range,
            options: $options,
            debug_only: $debug,
            variant: WatchPrefVariant::$variant,
        }
    };
}

macro_rules! quick_launch_pref {
    ($key:literal, $enabled:literal, $uuid:expr) => {
        WatchPrefMetadata {
            key: $key,
            wire_type: WatchPrefType::QuickLaunch,
            default: WatchPrefDefault::QuickLaunch {
                enabled: $enabled,
                uuid: $uuid,
            },
            range: None,
            options: EMPTY_OPTIONS,
            debug_only: false,
            variant: WatchPrefVariant::Common,
        }
    };
}

const WATCH_PREF_METADATA: &[WatchPrefMetadata] = &[
    bool_pref!("timezoneSource", false, Common),
    bool_pref!("clock24h", false, Common),
    bool_pref!("stationaryMode", true, Common),
    bool_pref!("displayOrientationLeftHanded", false, Common),
    bool_pref!("lightEnabled", true, Common),
    bool_pref!("lightAmbientSensorEnabled", true, Common),
    bool_pref!("lightMotion", true, Common),
    bool_pref!("timelineQuickViewEnabled", true, Common),
    bool_pref!("dndManuallyEnabled", false, Common),
    bool_pref!("dndSmartEnabled", false, Common),
    bool_pref!("notifDesignStyle", false, Common),
    bool_pref!("notifVibeDelay", true, Common),
    bool_pref!("notifBacklight", true, Common),
    bool_pref!("menuScrollWrapAround", false, Common),
    bool_pref!("dndMotionBacklight", true, Common),
    bool_pref!("dndAutoDismiss", false, Current),
    bool_pref!("musicShowVolumeControls", true, Common),
    bool_pref!("musicShowProgressBar", true, Common),
    bool_pref!("lightDynamicIntensity", true, Legacy),
    bool_pref!("langEnglish", false, Legacy),
    number_pref!("textStyle", U8, 1, None, TEXT_SIZE, false, Common),
    number_pref!("mask", U8, 15, None, ALERT_MASK, false, Common),
    number_pref!(
        "dndInterruptionsMask",
        U8,
        0,
        None,
        DND_INTERRUPTIONS_MASK,
        false,
        Common
    ),
    number_pref!(
        "dndShowNotifications",
        U8,
        1,
        None,
        SHOW_NOTIFICATIONS,
        false,
        Common
    ),
    number_pref!("vibeIntensity", U8, 2, None, VIBE_INTENSITY, false, Common),
    number_pref!(
        "vibeScoreNotifications",
        U8,
        9,
        None,
        VIBE_SCORE_NOTIFICATIONS,
        false,
        Common
    ),
    number_pref!(
        "vibeScoreIncomingCalls",
        U8,
        8,
        None,
        VIBE_SCORE_CALLS,
        false,
        Common
    ),
    number_pref!(
        "vibeScoreAlarms",
        U8,
        11,
        None,
        VIBE_SCORE_ALARMS,
        false,
        Common
    ),
    number_pref!(
        "menuScrollVibeBehavior",
        U8,
        0,
        None,
        MENU_VIBE,
        false,
        Common
    ),
    number_pref!("motionSensitivity", U8, 55, None, MOTION, true, Common),
    number_pref!("lightPreset", U8, 1, None, LIGHT_PRESET, false, Current),
    number_pref!(
        "lightIntensity",
        U8,
        25,
        None,
        LIGHT_INTENSITY,
        false,
        Common
    ),
    number_pref!(
        "lightDynamicMode",
        U8,
        2,
        None,
        LIGHT_DYNAMIC,
        false,
        Current
    ),
    number_pref!("lightTouch", U8, 0, None, LIGHT_TOUCH, false, Common),
    number_pref!("language", U8, 0, None, LANGUAGE, false, Current),
    number_pref!(
        "lightTimeoutMs",
        U32,
        3000,
        Some((1, 10_000)),
        EMPTY_OPTIONS,
        false,
        Common
    ),
    number_pref!(
        "lightAmbientThreshold",
        U32,
        150,
        Some((1, 4096)),
        EMPTY_OPTIONS,
        true,
        Common
    ),
    number_pref!(
        "dynBacklightMinThreshold",
        U32,
        5,
        Some((0, 4096)),
        EMPTY_OPTIONS,
        true,
        Common
    ),
    number_pref!(
        "timelineQuickViewBeforeTimeMin",
        U16,
        10,
        Some((0, 30)),
        EMPTY_OPTIONS,
        false,
        Common
    ),
    number_pref!(
        "notifWindowTimeout",
        U32,
        180_000,
        Some((0, 600_000)),
        EMPTY_OPTIONS,
        false,
        Common
    ),
    number_pref!(
        "lightColor",
        U32,
        0x00ff_bfa2,
        Some((0, 0x00ff_ffff)),
        EMPTY_OPTIONS,
        false,
        Common
    ),
    quick_launch_pref!("qlUp", false, None),
    quick_launch_pref!("qlDown", false, None),
    quick_launch_pref!("qlSelect", false, None),
    quick_launch_pref!("qlBack", true, Some("2220d805-cf9a-4e12-92b9-5ca778aff6bb")),
    quick_launch_pref!("qlComboBackUp", false, None),
    quick_launch_pref!("qlComboUpDown", false, None),
    quick_launch_pref!(
        "qlSingleClickUp",
        true,
        Some("36d8c6ed-4c83-4fa1-a9e2-8f12dc941f8c")
    ),
    quick_launch_pref!(
        "qlSingleClickDown",
        true,
        Some("79c76b48-6111-4e80-8deb-3119eebef33e")
    ),
];

/// Look up the authoritative metadata for a preference that is safe to model.
/// Support is still gated by keys/capabilities observed from the connected watch.
pub fn watch_pref_metadata(key: &str) -> Option<WatchPrefMetadata> {
    WATCH_PREF_METADATA
        .iter()
        .copied()
        .find(|metadata| metadata.key == key)
}

/// The "no binding" sentinel UUID used by quick-launch settings.
const NULL_UUID: Uuid = Uuid::from_bytes([0xff; 16]);

/// Look up the wire type for a known watch-pref key. Returns `None` for keys not
/// in the registry — the caller should leave those untouched.
pub fn watch_pref_type(key: &str) -> Option<WatchPrefType> {
    watch_pref_metadata(key).map(|metadata| metadata.wire_type)
}

/// Decode a watch-pref blob for a known key. Returns `None` for unknown keys or
/// blobs too short for their type.
///
/// Without model context this uses the standard-model representation. Call
/// [`decode_watch_pref_for_model`] when the watch model is known so `textStyle`
/// receives the Emery/Gabbro offset adjustment.
pub fn decode_watch_pref(key: &str, raw: &[u8]) -> Option<WatchPrefValue> {
    decode_watch_pref_for_model(key, raw, WatchPrefModel::Standard)
}

pub fn decode_watch_pref_for_model(
    key: &str,
    raw: &[u8],
    model: WatchPrefModel,
) -> Option<WatchPrefValue> {
    match watch_pref_type(key)? {
        WatchPrefType::Bool => Some(WatchPrefValue::Bool(*raw.first()? != 0)),
        WatchPrefType::U8 | WatchPrefType::Color => Some(WatchPrefValue::Number(
            receive_model_transform(key, *raw.first()? as u32, model),
        )),
        WatchPrefType::U16 => {
            let b = raw.get(..2)?;
            Some(WatchPrefValue::Number(
                u16::from_le_bytes([b[0], b[1]]) as u32
            ))
        }
        WatchPrefType::U32 => {
            let b = raw.get(..4)?;
            Some(WatchPrefValue::Number(u32::from_le_bytes([
                b[0], b[1], b[2], b[3],
            ])))
        }
        WatchPrefType::Str => Some(WatchPrefValue::Text(
            String::from_utf8_lossy(raw)
                .trim_end_matches('\0')
                .to_owned(),
        )),
        WatchPrefType::Uuid => {
            let b: [u8; 16] = raw.get(..16)?.try_into().ok()?;
            Some(WatchPrefValue::Text(Uuid::from_bytes(b).to_string()))
        }
        WatchPrefType::QuickLaunch => {
            let enabled = *raw.first()? != 0;
            let b: [u8; 16] = raw.get(1..17)?.try_into().ok()?;
            let uuid = Uuid::from_bytes(b);
            let text = if !enabled || uuid == NULL_UUID {
                "off".to_string()
            } else {
                uuid.to_string()
            };
            Some(WatchPrefValue::Text(text))
        }
    }
}

fn text_size_offset(model: WatchPrefModel) -> u32 {
    match model {
        WatchPrefModel::Emery | WatchPrefModel::Gabbro => 1,
        WatchPrefModel::Standard => 0,
    }
}

fn receive_model_transform(key: &str, value: u32, model: WatchPrefModel) -> u32 {
    if key == "textStyle" {
        value.saturating_sub(text_size_offset(model)).min(2)
    } else {
        value
    }
}

fn send_model_transform(key: &str, value: u32, model: WatchPrefModel) -> u32 {
    if key == "textStyle" {
        value + text_size_offset(model)
    } else {
        value
    }
}

/// Encode a known preference after validating its type, range and enum values.
pub fn encode_watch_pref(
    key: &str,
    value: &WatchPrefValue,
    model: WatchPrefModel,
) -> Result<Vec<u8>, WatchPrefEncodeError> {
    let metadata =
        watch_pref_metadata(key).ok_or_else(|| WatchPrefEncodeError::UnknownKey(key.into()))?;
    let wrong_type = || WatchPrefEncodeError::WrongType(key.into());
    match metadata.wire_type {
        WatchPrefType::Bool => match value {
            WatchPrefValue::Bool(v) => Ok(vec![u8::from(*v)]),
            _ => Err(wrong_type()),
        },
        WatchPrefType::U8 | WatchPrefType::Color => {
            let WatchPrefValue::Number(value) = value else {
                return Err(wrong_type());
            };
            if (!metadata.options.is_empty()
                && !metadata.options.iter().any(|option| option.code == *value))
                || metadata
                    .range
                    .is_some_and(|(min, max)| *value < min || *value > max)
            {
                return Err(WatchPrefEncodeError::InvalidValue {
                    key: key.into(),
                    value: *value,
                });
            }
            let wire = send_model_transform(key, *value, model);
            u8::try_from(wire)
                .map(|v| vec![v])
                .map_err(|_| WatchPrefEncodeError::InvalidValue {
                    key: key.into(),
                    value: *value,
                })
        }
        WatchPrefType::U16 | WatchPrefType::U32 => {
            let WatchPrefValue::Number(value) = value else {
                return Err(wrong_type());
            };
            if metadata
                .range
                .is_some_and(|(min, max)| *value < min || *value > max)
            {
                return Err(WatchPrefEncodeError::InvalidValue {
                    key: key.into(),
                    value: *value,
                });
            }
            if metadata.wire_type == WatchPrefType::U16 {
                u16::try_from(*value)
                    .map(|v| v.to_le_bytes().to_vec())
                    .map_err(|_| WatchPrefEncodeError::InvalidValue {
                        key: key.into(),
                        value: *value,
                    })
            } else {
                Ok(value.to_le_bytes().to_vec())
            }
        }
        WatchPrefType::QuickLaunch => {
            let WatchPrefValue::Text(text) = value else {
                return Err(wrong_type());
            };
            let (enabled, uuid) = if text == "off" {
                (false, NULL_UUID)
            } else {
                (
                    true,
                    Uuid::parse_str(text)
                        .map_err(|_| WatchPrefEncodeError::InvalidUuid(key.into()))?,
                )
            };
            let mut raw = Vec::with_capacity(17);
            raw.push(u8::from(enabled));
            raw.extend_from_slice(uuid.as_bytes());
            Ok(raw)
        }
        WatchPrefType::Str | WatchPrefType::Uuid => Err(wrong_type()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_hex(text: &str) -> Vec<u8> {
        text.as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII hex"), 16)
                    .expect("valid fixture hex")
            })
            .collect()
    }

    #[test]
    fn decodes_bool() {
        assert_eq!(
            decode_watch_pref("clock24h", &[1]),
            Some(WatchPrefValue::Bool(true))
        );
        assert_eq!(
            decode_watch_pref("lightEnabled", &[0]),
            Some(WatchPrefValue::Bool(false))
        );
    }

    #[test]
    fn decodes_u32_le() {
        // lightTimeoutMs observed from the watch: [136,19,0,0] = 5000.
        assert_eq!(
            decode_watch_pref("lightTimeoutMs", &[136, 19, 0, 0]),
            Some(WatchPrefValue::Number(5000)),
        );
    }

    #[test]
    fn decodes_u8_enum() {
        assert_eq!(
            decode_watch_pref("vibeScoreNotifications", &[9]),
            Some(WatchPrefValue::Number(9)),
        );
    }

    #[test]
    fn quick_launch_off_when_disabled() {
        let mut blob = vec![0u8]; // disabled
        blob.extend_from_slice(&[0xff; 16]);
        assert_eq!(
            decode_watch_pref("qlUp", &blob),
            Some(WatchPrefValue::Text("off".into()))
        );
    }

    #[test]
    fn unknown_and_health_keys_are_none() {
        assert_eq!(decode_watch_pref("automaticTimezoneID", &[0, 0]), None);
        assert_eq!(decode_watch_pref("activityPreferences", &[0; 9]), None);
        assert_eq!(decode_watch_pref("dndWeekdaySchedule", &[0; 4]), None);
    }

    #[test]
    fn registry_keys_are_unique_and_canonical() {
        let mut keys = std::collections::HashSet::new();
        for metadata in WATCH_PREF_METADATA {
            assert!(keys.insert(metadata.key), "duplicate key: {}", metadata.key);
            assert_eq!(watch_pref_metadata(metadata.key), Some(*metadata));
        }
    }

    #[test]
    fn short_blob_is_none() {
        assert_eq!(decode_watch_pref("lightTimeoutMs", &[1, 2]), None);
    }

    #[test]
    fn canonical_fixtures_round_trip() {
        for line in include_str!("../../tests/fixtures/watch_prefs_canonical.tsv").lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let columns: Vec<_> = line.split_ascii_whitespace().collect();
            assert_eq!(columns.len(), 4, "bad fixture line: {line}");
            let model = match columns[1] {
                "standard" => WatchPrefModel::Standard,
                "emery" => WatchPrefModel::Emery,
                "gabbro" => WatchPrefModel::Gabbro,
                other => panic!("unknown fixture model: {other}"),
            };
            let value = if let Some(value) = columns[2].strip_prefix("bool:") {
                WatchPrefValue::Bool(value.parse().expect("fixture bool"))
            } else if let Some(value) = columns[2].strip_prefix("number:") {
                WatchPrefValue::Number(value.parse().expect("fixture number"))
            } else {
                panic!("unknown fixture value: {}", columns[2]);
            };
            let raw = decode_hex(columns[3]);
            assert_eq!(
                decode_watch_pref_for_model(columns[0], &raw, model),
                Some(value.clone()),
                "decode fixture: {line}"
            );
            assert_eq!(
                encode_watch_pref(columns[0], &value, model).expect("encode fixture"),
                raw,
                "encode fixture: {line}"
            );
        }
    }

    #[test]
    fn registry_rejects_invalid_or_unknown_edits() {
        assert!(matches!(
            encode_watch_pref(
                "timelineQuickViewBeforeTimeMin",
                &WatchPrefValue::Number(31),
                WatchPrefModel::Standard
            ),
            Err(WatchPrefEncodeError::InvalidValue { .. })
        ));
        assert!(matches!(
            encode_watch_pref(
                "language",
                &WatchPrefValue::Number(42),
                WatchPrefModel::Standard
            ),
            Err(WatchPrefEncodeError::InvalidValue { .. })
        ));
        assert!(matches!(
            encode_watch_pref(
                "automaticTimezoneID",
                &WatchPrefValue::Number(0),
                WatchPrefModel::Standard
            ),
            Err(WatchPrefEncodeError::UnknownKey(_))
        ));
    }
}
