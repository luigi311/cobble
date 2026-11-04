//! Map HRM choices to edits without replacing unknown reported intervals.

use cobble_client::HrmMeasurementInterval;

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
}
