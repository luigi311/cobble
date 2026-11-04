//! Map displayed choices to edits without replacing unknown reported values.

use cobble_client::{FieldAvailability, HrmMeasurementInterval, PreferenceField, PreferenceValue};
use std::collections::BTreeMap;

pub const TEXT_SIZES: &[(u32, &str)] = &[(0, "Smaller"), (1, "Default"), (2, "Larger")];
pub const NOTIFICATION_TEXT_SIZES: &[(u32, &str)] = &[
    (0, "Smaller"),
    (1, "Default"),
    (2, "Larger"),
    (4, "Same as system"),
];
pub const LANGUAGES: &[(u32, &str)] = &[(1, "English"), (0, "Installed language pack")];

pub struct NumericChoices {
    pub options: Vec<(u32, String)>,
    pub selected: i32,
}

impl NumericChoices {
    fn new(mut options: Vec<(u32, String)>, current: u32) -> Self {
        let selected = options
            .iter()
            .position(|(value, _)| *value == current)
            .unwrap_or_else(|| {
                options.push((current, format!("Current value ({current})")));
                options.len() - 1
            }) as i32;
        Self { options, selected }
    }

    pub fn value_at(&self, index: i32) -> Option<u32> {
        self.options
            .get(usize::try_from(index).ok()?)
            .map(|(value, _)| *value)
    }
}

pub fn text_size_key(preferences: &BTreeMap<String, PreferenceField>) -> &'static str {
    if preferences.contains_key("systemTextSize") {
        "systemTextSize"
    } else {
        "textStyle"
    }
}

pub fn preference_choices(
    field: Option<&PreferenceField>,
    defaults: &[(u32, &str)],
) -> Option<NumericChoices> {
    let field = field.filter(|field| field.availability == FieldAvailability::Available)?;
    let Some(PreferenceValue::Unsigned(current)) = field.value else {
        return None;
    };
    let options = if field.options.is_empty() {
        defaults
            .iter()
            .map(|(value, label)| (*value, (*label).into()))
            .collect()
    } else {
        field
            .options
            .iter()
            .map(|option| (option.value, option.label.clone()))
            .collect()
    };
    Some(NumericChoices::new(options, current))
}

pub fn preference_patch(
    field: Option<&PreferenceField>,
    defaults: &[(u32, &str)],
    index: i32,
) -> Option<PreferenceValue> {
    let choices = preference_choices(field, defaults)?;
    if choices.selected == index {
        return None;
    }
    choices.value_at(index).map(PreferenceValue::Unsigned)
}

pub fn hrm_choices(
    current: Option<HrmMeasurementInterval>,
    supports_five_minutes: bool,
) -> NumericChoices {
    let mut options = vec![
        (0, "10 minutes".into()),
        (1, "30 minutes".into()),
        (2, "1 hour".into()),
        (3, "Off".into()),
    ];
    if supports_five_minutes || current == Some(HrmMeasurementInterval::FiveMinutes) {
        options.insert(0, (4, "5 minutes".into()));
    }
    let code = match current {
        Some(HrmMeasurementInterval::FiveMinutes) => 4,
        Some(HrmMeasurementInterval::ThirtyMinutes) => 1,
        Some(HrmMeasurementInterval::OneHour) => 2,
        Some(HrmMeasurementInterval::Off) => 3,
        Some(HrmMeasurementInterval::Unknown(code)) => u32::from(code),
        _ => 0,
    };
    NumericChoices::new(options, code)
}

pub fn hrm_interval_patch(
    current: Option<HrmMeasurementInterval>,
    supports_five_minutes: bool,
    index: i32,
) -> Option<HrmMeasurementInterval> {
    let choices = hrm_choices(Some(current?), supports_five_minutes);
    if choices.selected == index {
        return None;
    }
    match choices.value_at(index)? {
        0 => Some(HrmMeasurementInterval::TenMinutes),
        1 => Some(HrmMeasurementInterval::ThirtyMinutes),
        2 => Some(HrmMeasurementInterval::OneHour),
        3 => Some(HrmMeasurementInterval::Off),
        4 => Some(HrmMeasurementInterval::FiveMinutes),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applying_other_settings_preserves_five_minute_and_unknown_hrm_values() {
        for interval in [
            HrmMeasurementInterval::FiveMinutes,
            HrmMeasurementInterval::Unknown(17),
        ] {
            for supported in [false, true] {
                let choices = hrm_choices(Some(interval), supported);
                assert_eq!(
                    hrm_interval_patch(Some(interval), supported, choices.selected),
                    None
                );
                let ten_minutes = choices
                    .options
                    .iter()
                    .position(|(code, _)| *code == 0)
                    .unwrap() as i32;
                assert_eq!(
                    hrm_interval_patch(Some(interval), supported, ten_minutes),
                    Some(HrmMeasurementInterval::TenMinutes)
                );
            }
        }
        assert!(
            !hrm_choices(Some(HrmMeasurementInterval::TenMinutes), false)
                .options
                .iter()
                .any(|(code, _)| *code == 4)
        );
        assert_eq!(
            hrm_interval_patch(Some(HrmMeasurementInterval::Off), true, 0),
            Some(HrmMeasurementInterval::FiveMinutes)
        );
    }

    fn field(value: u32) -> PreferenceField {
        PreferenceField {
            availability: FieldAvailability::Available,
            value: Some(PreferenceValue::Unsigned(value)),
            raw: vec![],
            error: None,
            options: vec![],
        }
    }

    #[test]
    fn notification_system_sentinel_and_legacy_language_values_are_preserved() {
        for (field, defaults) in [(field(4), NOTIFICATION_TEXT_SIZES), (field(7), LANGUAGES)] {
            let choices = preference_choices(Some(&field), defaults).unwrap();
            assert_eq!(
                preference_patch(Some(&field), defaults, choices.selected),
                None
            );
            assert_eq!(preference_patch(Some(&field), defaults, -1), None);
        }
    }

    #[test]
    fn installed_pack_names_and_values_come_from_the_snapshot() {
        let mut language = field(3);
        language.options = vec![
            cobble_client::PreferenceOption {
                value: 1,
                label: "English".into(),
            },
            cobble_client::PreferenceOption {
                value: 3,
                label: "Deutsch".into(),
            },
        ];
        let choices = preference_choices(Some(&language), LANGUAGES).unwrap();
        assert_eq!(
            choices.options[choices.selected as usize],
            (3, "Deutsch".into())
        );
        assert_eq!(
            preference_patch(Some(&language), LANGUAGES, 0),
            Some(PreferenceValue::Unsigned(1))
        );
        let mut preferences = BTreeMap::from([("textStyle".into(), field(1))]);
        assert_eq!(text_size_key(&preferences), "textStyle");
        preferences.insert("systemTextSize".into(), field(2));
        assert_eq!(text_size_key(&preferences), "systemTextSize");
    }
}
