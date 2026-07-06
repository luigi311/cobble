use std::sync::atomic::{AtomicI64, Ordering};

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use rusqlite::Connection;

/// Watch UTC offset in seconds (local = UTC + offset), set from synced data so
/// consumers render the watch's local time regardless of the host system tz.
static WATCH_OFFSET_SECS: AtomicI64 = AtomicI64::new(0);

/// Set the watch timezone offset used for all local-time grouping and labels.
pub fn set_watch_offset(secs: i64) {
    WATCH_OFFSET_SECS.store(secs, Ordering::Relaxed);
}

pub(crate) fn watch_offset() -> i64 {
    WATCH_OFFSET_SECS.load(Ordering::Relaxed)
}

/// Read the watch's current UTC offset from the most recent health record across
/// both tables (whichever has the newer `start_ts`), falling back to UTC (0)
/// when there's no data yet.
pub fn watch_tz_offset(conn: &Connection) -> i64 {
    let latest = |table: &str| -> Option<(i64, i64)> {
        conn.query_row(
            &format!("SELECT start_ts, utc_offset FROM {table} ORDER BY start_ts DESC LIMIT 1"),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok()
    };
    match (
        latest("health_activity_minutes"),
        latest("health_activity_sessions"),
    ) {
        (Some((mt, mo)), Some((st, so))) => {
            if mt >= st {
                mo
            } else {
                so
            }
        }
        (Some((_, mo)), None) => mo,
        (None, Some((_, so))) => so,
        (None, None) => 0,
    }
}

/// Current date in the watch's timezone.
pub(crate) fn watch_today() -> NaiveDate {
    DateTime::from_timestamp(Utc::now().timestamp() + watch_offset(), 0)
        .map(|dt| dt.date_naive())
        .unwrap_or_default()
}

/// Real UTC timestamp of a watch-local wall-clock time (local = UTC + offset).
pub(crate) fn local_ts(date: NaiveDate, h: u32, m: u32, s: u32) -> i64 {
    date.and_hms_opt(h, m, s)
        .map(|naive| naive.and_utc().timestamp() - watch_offset())
        .unwrap_or(0)
}

/// Convert (year, month) + a backward month offset into the target (year, month).
fn offset_ym(base_year: i32, base_month: u32, offset: i32) -> (i32, u32) {
    let total = base_year * 12 + base_month as i32 - 1 - offset;
    (total.div_euclid(12), (total.rem_euclid(12) + 1) as u32)
}

// ─── Period range + label ─────────────────────────────────────────────────────

/// Compute [start, end] Unix timestamps for `period` shifted back by `offset` units.
/// period 0=Day (offset in days), 1=Week (offset in weeks), 2=Month (offset in months).
pub fn period_range_offset(period: i32, offset: i32) -> (i64, i64) {
    let today = watch_today();
    match period {
        0 => {
            let date = today - chrono::Duration::days(offset as i64);
            (local_ts(date, 0, 0, 0), local_ts(date, 23, 59, 59))
        }
        2 => {
            let (year, month) = offset_ym(today.year(), today.month(), offset);
            let first = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
            let last = if month == 12 {
                NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
            } else {
                NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
            } - chrono::Duration::days(1);
            (local_ts(first, 0, 0, 0), local_ts(last, 23, 59, 59))
        }
        _ => {
            let end = today - chrono::Duration::days(offset as i64 * 7);
            let start = end - chrono::Duration::days(6);
            (local_ts(start, 0, 0, 0), local_ts(end, 23, 59, 59))
        }
    }
}

/// Human-readable label for the navigated period (shown between the arrows).
pub fn period_label(period: i32, offset: i32) -> String {
    let today = watch_today();
    match period {
        0 => match offset {
            0 => "Today".to_string(),
            1 => "Yesterday".to_string(),
            n => (today - chrono::Duration::days(n as i64))
                .format("%a, %b %-d")
                .to_string(),
        },
        2 => {
            if offset == 0 {
                "This Month".to_string()
            } else {
                let (year, month) = offset_ym(today.year(), today.month(), offset);
                NaiveDate::from_ymd_opt(year, month, 1)
                    .unwrap()
                    .format("%B %Y")
                    .to_string()
            }
        }
        _ => match offset {
            0 => "This Week".to_string(),
            1 => "Last Week".to_string(),
            n => {
                let end = today - chrono::Duration::days(n as i64 * 7);
                let start = end - chrono::Duration::days(6);
                if start.year() == end.year() {
                    format!("{} \u{2013} {}", start.format("%b %-d"), end.format("%b %-d"))
                } else {
                    format!(
                        "{} \u{2013} {}",
                        start.format("%b %-d, %Y"),
                        end.format("%b %-d, %Y")
                    )
                }
            }
        },
    }
}

// ─── Formatting helpers ───────────────────────────────────────────────────────

/// Format a watch-local-epoch (start_ts + utc_offset) as a date/time string.
pub fn format_ts(local_epoch: i64) -> String {
    DateTime::from_timestamp(local_epoch, 0)
        .map(|dt| dt.format("%b %d, %H:%M").to_string())
        .unwrap_or_else(|| "?".to_string())
}

pub fn format_duration(secs: i64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    if h > 0 {
        format!("{}h {}m", h, m)
    } else {
        format!("{}m", m)
    }
}

pub fn format_distance(meters: i64) -> String {
    if meters >= 1000 {
        format!("{:.1} km", meters as f64 / 1000.0)
    } else {
        format!("{} m", meters)
    }
}

pub fn format_number(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

pub fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}
