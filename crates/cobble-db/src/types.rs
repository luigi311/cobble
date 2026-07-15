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

// ─── Domain types ─────────────────────────────────────────────────────────────

/// One sleep or deep-sleep span, in watch-local epoch seconds.
#[derive(Debug, Clone, Copy)]
pub struct NightPhase {
    pub local_start: i64,
    pub local_end: i64,
    /// Absolute UTC bounds for matching per-minute activity samples.
    pub utc_start: i64,
    pub utc_end: i64,
    /// Deep phases (session types 2/4) overlay the primary phases (1/3);
    /// they don't add to the night's total.
    pub is_deep: bool,
    /// Nap phases (session types 3/4) count toward the night's totals but
    /// are excluded from the overnight block (bedtime / wake-up / in-bed).
    pub is_nap: bool,
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

    /// Primary phases of the overnight block. Naps are excluded so an
    /// afternoon nap doesn't stretch the in-bed span or shift the wake-up
    /// time; nap-only nights fall back to the nap phases themselves.
    pub fn overnight_phases(&self) -> Vec<&NightPhase> {
        let block: Vec<&NightPhase> =
            self.phases.iter().filter(|p| !p.is_deep && !p.is_nap).collect();
        if !block.is_empty() {
            return block;
        }
        self.phases.iter().filter(|p| !p.is_deep).collect()
    }
}

/// Provider-neutral wellness observations for one watch-local calendar date.
/// Missing observations stay absent instead of becoming zero-valued updates.
#[derive(Debug, Clone, PartialEq)]
pub struct DailyWellness {
    pub date: NaiveDate,
    pub steps: Option<u32>,
    pub sleep_secs: Option<u32>,
    pub avg_sleeping_hr: Option<f32>,
    pub resting_hr: Option<u16>,
}

/// Durable state for one provider/account/date wellness export.
///
/// The payload hash is the last successfully uploaded representation. API
/// keys and other credentials are intentionally not part of this record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WellnessExportState {
    pub provider: String,
    pub account_id: String,
    pub wellness_date: NaiveDate,
    pub payload_hash: Option<String>,
    pub attempt_count: i64,
    pub next_attempt_at: Option<i64>,
    pub last_attempt_at: Option<i64>,
    pub last_success_at: Option<i64>,
    pub last_error: Option<String>,
}

/// Aggregated durable status for one provider/account wellness export.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WellnessExportStatus {
    /// Current local dates whose payload matches the last successful export.
    pub exported_dates: i64,
    /// Current local dates that are unseen or differ from the successful hash.
    pub pending_dates: i64,
    pub last_success_at: Option<i64>,
    pub last_error: Option<String>,
    pub last_error_at: Option<i64>,
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

/// Key sleep stats for the stats panel on the Sleep tab.
#[derive(Debug, Clone)]
pub struct SleepStats {
    /// Average deep sleep time label, e.g. "1h 45m"
    pub deep_avg_label: String,
    /// Stage mix percentages over one denominator — slept time plus awake
    /// gaps between the overnight sleep sections — so the three always
    /// total exactly 100.
    pub light_pct: f32,
    pub deep_pct: f32,
    /// Share spent awake between sleep sections; time outside the overnight
    /// block (e.g. between wake-up and an afternoon nap) doesn't count.
    pub awake_pct: f32,
    /// Average bedtime label, e.g. "10:47 PM"
    pub avg_bedtime: String,
    /// Average wakeup time label, e.g. "7:15 AM"
    pub avg_wakeup: String,
    /// Highest sleep duration label, e.g. "8h 30m"
    pub highest_dur: String,
    /// Lowest sleep duration label, e.g. "5h 45m"
    pub lowest_dur: String,
}
