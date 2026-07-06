use chrono::NaiveDate;

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

// ─── Domain types ─────────────────────────────────────────────────────────────

/// One sleep or deep-sleep span, in watch-local epoch seconds.
#[derive(Debug, Clone, Copy)]
pub struct NightPhase {
    pub local_start: i64,
    pub local_end: i64,
    /// Deep phases (session types 2/4) overlay the primary phases (1/3);
    /// they don't add to the night's total.
    pub is_deep: bool,
}

/// One night of sleep, keyed by the watch-local date the sleeper woke up on.
/// Phases are sorted by start time.
#[derive(Debug, Clone)]
pub struct Night {
    pub wake_date: NaiveDate,
    pub phases: Vec<NightPhase>,
}

impl Night {
    /// Total slept seconds: the primary (non-deep) phases.
    pub fn total_secs(&self) -> i64 {
        self.phases
            .iter()
            .filter(|p| !p.is_deep)
            .map(|p| p.local_end - p.local_start)
            .sum()
    }

    /// Deep-sleep seconds, clamped to the total.
    pub fn deep_secs(&self) -> i64 {
        let deep: i64 = self
            .phases
            .iter()
            .filter(|p| p.is_deep)
            .map(|p| p.local_end - p.local_start)
            .sum();
        deep.min(self.total_secs())
    }

    pub fn sleep_start(&self) -> i64 {
        self.phases.iter().map(|p| p.local_start).min().unwrap_or(0)
    }

    pub fn sleep_end(&self) -> i64 {
        self.phases.iter().map(|p| p.local_end).max().unwrap_or(0)
    }
}

/// Everything the steps chart needs, computed in one call so the bars and the
/// header labels always agree on bucketing.
pub struct StepsChart {
    pub bars: Vec<DayStepsData>,
    pub summary: String,
    pub avg_label: String,
    pub delta_label: String,
    pub delta_positive: bool,
}

/// Everything the sleep chart needs, computed in one call.
pub struct SleepChart {
    pub bars: Vec<SleepBarData>,
    pub summary: String,
    pub avg_label: String,
    pub delta_label: String,
    pub delta_positive: bool,
}
