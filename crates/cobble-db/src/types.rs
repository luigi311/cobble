/// Cached IP geolocation result.
#[derive(Debug, Clone)]
pub struct IpLocation {
    pub latitude: f64,
    pub longitude: f64,
    pub city: String,
    pub region: String,
}

pub struct DayStepsData {
    pub label: String,
    pub steps_label: String,
    pub steps_raw: i64,
    pub fraction: f32,
    pub bar_start: i64,
    pub bar_end: i64,
}

pub struct SleepBarData {
    pub label: String,
    pub bar_start: i64,
    pub bar_end: i64,
    pub light_fraction: f32,
    pub deep_fraction: f32,
    pub light_secs: i64,
    pub deep_secs: i64,
    pub total_label: String,
    pub deep_label: String,
}

pub struct HealthSessionData {
    pub type_name: String,
    pub start_label: String,
    pub duration_label: String,
    pub has_metrics: bool,
    pub metrics_label: String,
}

pub struct SleepSegmentData {
    pub start_frac: f32,
    pub width_frac: f32,
    pub is_deep: bool,
}

pub struct SleepNightData {
    pub label: String,
    pub duration_label: String,
    pub bar_start: i64,
    pub segments: Vec<SleepSegmentData>,
}
