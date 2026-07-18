use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

use chrono::{DateTime, Datelike, NaiveDate, Timelike};
use rusqlite::{Connection, params};

use crate::time::{self, *};
use crate::types::*;

// ═══ Data layer ═══════════════════════════════════════════════════════════════
//
// Two invariants shared by every query and label in this file:
//
//  • A step minute belongs to the watch-local date (and hour) its start falls on.
//  • A sleep session belongs to the night of the watch-local date the sleeper
//    WOKE UP on (the session's end). So "sleep for Jul 4" includes a session
//    that started 11pm Jul 3 and ended 7am Jul 4. Deep-sleep segments attach
//    to the night they overlap, so a deep span ending just before midnight
//    still lands with the following morning's wake-up.
//
// Local time always comes from each row's own utc_offset. UTC `start_ts`
// filters in SQL are deliberately generous (they only bound the scan); exact
// membership is decided on watch-local dates in Rust.

/// Step totals per hour (0–23) of one watch-local day. Hours without data are absent.
pub fn fetch_steps_by_hour(conn: &Connection, day: NaiveDate) -> anyhow::Result<Vec<(u32, i64)>> {
    let (utc_start, utc_end) = DateRange::day(day).utc_bounds();
    // Same membership rule as fetch_steps_by_day — the row's own local date
    // must equal `day` — so the hourly bars always sum to the daily total.
    // The UTC bounds (±12h slack) only bound the index scan.
    let mut stmt = conn.prepare(
        "SELECT CAST(strftime('%H', start_ts + utc_offset, 'unixepoch') AS INTEGER) AS hour,
                SUM(steps)
         FROM health_activity_minutes
         WHERE start_ts >= ?1 - 43200 AND start_ts <= ?2 + 43200
           AND date(start_ts + utc_offset, 'unixepoch') = ?3
         GROUP BY hour ORDER BY hour ASC",
    )?;
    let day_key = day.format("%Y-%m-%d").to_string();
    let rows = stmt
        .query_map(params![utc_start, utc_end, day_key], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows)
}

/// Step totals per watch-local date. Dates without data are absent.
pub fn fetch_steps_by_day(
    conn: &Connection,
    range: DateRange,
) -> anyhow::Result<BTreeMap<NaiveDate, i64>> {
    let (utc_start, utc_end) = range.utc_bounds();
    // ±12h slack tolerates rows whose own utc_offset differs from the watch's
    // current offset; the exact date filter below decides membership.
    let mut stmt = conn.prepare(
        "SELECT date(start_ts + utc_offset, 'unixepoch') AS day, SUM(steps)
         FROM health_activity_minutes
         WHERE start_ts >= ?1 - 43200 AND start_ts <= ?2 + 43200
         GROUP BY day",
    )?;
    let totals = stmt
        .query_map(params![utc_start, utc_end], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?
        .filter_map(|r| r.ok())
        .filter_map(|(day, total)| Some((day.parse::<NaiveDate>().ok()?, total)))
        .filter(|(day, _)| range.contains(*day))
        .collect();
    Ok(totals)
}

/// Sleep nights whose wake-up date falls inside `range`, sorted by date.
pub fn fetch_sleep_nights(conn: &Connection, range: DateRange) -> anyhow::Result<Vec<Night>> {
    let (utc_start, utc_end) = range.utc_bounds();
    // A session waking inside the range must start within ~48h before it; the
    // slack also absorbs per-row utc_offset differences. Exact bucketing
    // happens below on wake dates.
    let mut stmt = conn.prepare(
        "SELECT start_ts, utc_offset, duration_secs, session_type
         FROM health_activity_sessions
         WHERE session_type <= 4 AND start_ts >= ?1 AND start_ts <= ?2
         ORDER BY start_ts ASC",
    )?;
    let rows: Vec<(i64, i64, i64, i32)> = stmt
        .query_map(params![utc_start - 48 * 3600, utc_end + 12 * 3600], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })?
        .filter_map(|r| r.ok())
        .collect();

    let wake_date =
        |local_end: i64| DateTime::from_timestamp(local_end, 0).map(|dt| dt.date_naive());

    // Primary sessions (sleep=1, nap=3) define the nights.
    let mut nights: BTreeMap<NaiveDate, Night> = BTreeMap::new();
    for &(utc_start, utc_offset, dur, ty) in rows.iter().filter(|r| matches!(r.3, 1 | 3)) {
        let local_start = utc_start + utc_offset;
        let local_end = local_start + dur;
        let utc_end = utc_start + dur;
        let Some(date) = wake_date(local_end) else { continue };
        nights
            .entry(date)
            .or_insert_with(|| Night { wake_date: date, phases: Vec::new() })
            .phases
            .push(NightPhase {
                local_start,
                local_end,
                utc_start,
                utc_end,
                is_deep: false,
                is_nap: ty == 3,
            });
    }

    // Deep segments (2/4) attach to the night they overlap; a segment ending
    // before midnight belongs with the following morning's wake-up, not its
    // own end date. Orphans (no overlapping primary) fall back to wake date.
    for &(utc_start, utc_offset, dur, ty) in rows.iter().filter(|r| matches!(r.3, 2 | 4)) {
        let local_start = utc_start + utc_offset;
        let local_end = local_start + dur;
        let utc_end = utc_start + dur;
        let date = nights
            .values()
            .find(|n| {
                n.phases
                    .iter()
                    .any(|p| !p.is_deep && p.local_start < local_end && local_start < p.local_end)
            })
            .map(|n| n.wake_date)
            .or_else(|| wake_date(local_end));
        let Some(date) = date else { continue };
        nights
            .entry(date)
            .or_insert_with(|| Night { wake_date: date, phases: Vec::new() })
            .phases
            .push(NightPhase {
                local_start,
                local_end,
                utc_start,
                utc_end,
                is_deep: true,
                is_nap: ty == 4,
            });
    }

    let mut nights: Vec<Night> = nights
        .into_values()
        .filter(|n| range.contains(n.wake_date))
        .collect();
    for n in &mut nights {
        n.phases.sort_by_key(|p| p.local_start);
    }
    Ok(nights)
}

/// Aggregate supported wellness observations by watch-local calendar date.
///
/// Steps use each minute row's own offset. Sleep is grouped by the wake-up date
/// and includes naps, while deep-sleep rows remain overlays. Sleeping heart-rate
/// samples are matched against absolute UTC sleep bounds and are counted once
/// when overlapping primary sleep spans contain the same minute. Resting HR is
/// the lowest sustained qualifying window during primary overnight sleep.
pub fn fetch_daily_wellness(
    conn: &Connection,
    range: DateRange,
) -> anyhow::Result<Vec<DailyWellness>> {
    let mut by_date = BTreeMap::<NaiveDate, DailyWellness>::new();

    for (date, total) in fetch_steps_by_day(conn, range)? {
        let steps = u32::try_from(total)
            .map_err(|_| anyhow::anyhow!("steps total for {date} is outside u32 range: {total}"))?;
        by_date
            .entry(date)
            .or_insert_with(|| DailyWellness {
                date,
                steps: None,
                sleep_secs: None,
                avg_sleeping_hr: None,
                resting_hr: None,
            })
            .steps = Some(steps);
    }

    let nights = fetch_sleep_nights(conn, range)?;
    let mut primary_spans_by_date = BTreeMap::<NaiveDate, Vec<(i64, i64)>>::new();
    for night in &nights {
        let spans: Vec<_> = night
            .phases
            .iter()
            .filter(|phase| !phase.is_deep && phase.utc_start < phase.utc_end)
            .map(|phase| (phase.utc_start, phase.utc_end))
            .collect();
        if !spans.is_empty() {
            primary_spans_by_date
                .entry(night.wake_date)
                .or_default()
                .extend(spans);
        }
    }

    if !primary_spans_by_date.is_empty() {
        // Merge overlaps within each wake date first. This guarantees that a
        // sample in overlapping primary phases contributes once to that date,
        // matching the previous per-night HashSet de-duplication.
        let mut primary_spans = Vec::new();
        for (date, mut spans) in primary_spans_by_date {
            spans.sort_unstable_by_key(|&(start, _)| start);
            let mut merged = Vec::new();
            for (start, end) in spans {
                if let Some((_, merged_end)) = merged.last_mut() {
                    if start <= *merged_end {
                        *merged_end = (*merged_end).max(end);
                        continue;
                    }
                }
                merged.push((start, end));
            }
            let total_secs: i64 = merged.iter().map(|&(start, end)| end - start).sum();
            if total_secs <= 0 {
                continue;
            }
            let sleep_secs = u32::try_from(total_secs).map_err(|_| {
                anyhow::anyhow!(
                    "sleep total for {} is outside u32 range: {total_secs}",
                    date
                )
            })?;
            by_date
                .entry(date)
                .or_insert_with(|| DailyWellness {
                    date,
                    steps: None,
                    sleep_secs: None,
                    avg_sleeping_hr: None,
                    resting_hr: None,
                })
                .sleep_secs = Some(sleep_secs);
            primary_spans.extend(merged.into_iter().map(|(start, end)| (date, start, end)));
        }
        primary_spans.sort_unstable_by_key(|&(_, start, _)| start);

        let utc_start = primary_spans
            .iter()
            .map(|&(_, start, _)| start)
            .min()
            .unwrap();
        let utc_end = primary_spans.iter().map(|&(_, _, end)| end).max().unwrap();

        let mut stmt = conn.prepare(
            "SELECT start_ts, heart_rate_bpm
             FROM health_activity_minutes
             WHERE start_ts >= ?1
               AND start_ts < ?2
               AND heart_rate_bpm IS NOT NULL
               AND heart_rate_bpm > 0
             ORDER BY start_ts ASC",
        )?;
        let mut next_span = 0;
        let mut active = BTreeMap::<NaiveDate, i64>::new();
        let mut expirations = BinaryHeap::<Reverse<(i64, NaiveDate)>>::new();
        let mut hr_totals = BTreeMap::<NaiveDate, (f64, u32)>::new();

        // Both the SQL rows and primary_spans are sorted by UTC start. Each
        // sample advances the interval cursor once; active intervals are
        // visited only when a sample actually overlaps them.
        for row in stmt.query_map(params![utc_start, utc_end], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })? {
            let (sample_ts, bpm) = row?;

            while let Some(&Reverse((end, date))) = expirations.peek() {
                if end > sample_ts {
                    break;
                }
                expirations.pop();
                if active.get(&date) == Some(&end) {
                    active.remove(&date);
                }
            }

            while let Some(&(date, start, end)) = primary_spans.get(next_span) {
                if start > sample_ts {
                    break;
                }
                next_span += 1;
                if end > sample_ts {
                    active.insert(date, end);
                    expirations.push(Reverse((end, date)));
                }
            }

            for &date in active.keys() {
                let (sum, count) = hr_totals.entry(date).or_default();
                *sum += bpm as f64;
                *count += 1;
            }
        }

        for (date, (sum, count)) in hr_totals {
            if let Some(day) = by_date.get_mut(&date) {
                day.avg_sleeping_hr = Some((sum / f64::from(count)) as f32);
            }
        }
    }

    for (date, resting_hr) in estimated_resting_hr_by_wake_date(conn, &nights)? {
        if let Some(day) = by_date.get_mut(&date) {
            day.resting_hr = Some(resting_hr);
        }
    }

    Ok(by_date.into_values().collect())
}

fn wellness_date_bounds(
    conn: &Connection,
) -> anyhow::Result<(Option<NaiveDate>, Option<NaiveDate>)> {
    let (oldest, newest): (Option<String>, Option<String>) = conn.query_row(
        "SELECT MIN(day), MAX(day)
         FROM (
             SELECT date(start_ts + utc_offset, 'unixepoch') AS day
             FROM health_activity_minutes
             UNION ALL
             SELECT date(start_ts + utc_offset + duration_secs, 'unixepoch') AS day
             FROM health_activity_sessions
             WHERE session_type IN (1, 3)
         )",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    let parse = |value: Option<String>| {
        value
            .map(|value| {
                NaiveDate::parse_from_str(&value, "%Y-%m-%d")
                    .map_err(|error| anyhow::anyhow!("invalid wellness date {value:?}: {error}"))
            })
            .transpose()
    };
    Ok((parse(oldest)?, parse(newest)?))
}

/// Return the oldest watch-local date containing steps or primary sleep/nap data.
pub fn oldest_wellness_date(conn: &Connection) -> anyhow::Result<Option<NaiveDate>> {
    Ok(wellness_date_bounds(conn)?.0)
}

/// Return the newest watch-local date containing steps or primary sleep/nap data.
pub fn newest_wellness_date(conn: &Connection) -> anyhow::Result<Option<NaiveDate>> {
    Ok(wellness_date_bounds(conn)?.1)
}

// ═══ Presentation layer ═══════════════════════════════════════════════════════
//
// Pure functions over the fetchers above. Bars, header labels, and
// previous-period deltas are all derived from the same fetched data, so they
// can't disagree about bucketing.

/// Consecutive ISO-week buckets covering `range`, in order.
fn week_buckets(range: DateRange) -> Vec<DateRange> {
    let mut weeks: Vec<DateRange> = Vec::new();
    for day in range.days() {
        match weeks.last_mut() {
            Some(w) if day.iso_week() == w.start.iso_week() => w.end = day,
            _ => weeks.push(DateRange::day(day)),
        }
    }
    weeks
}

fn delta_pct(current: i64, previous: i64) -> String {
    if previous == 0 {
        return "—".to_string();
    }
    let pct = (current - previous) as f64 / previous.abs() as f64 * 100.0;
    if !pct.is_finite() {
        return "—".to_string();
    }
    // Round first so a -0.4% change reads "+0%", not "-0%".
    let pct = pct.round() as i64;
    if pct >= 0 {
        format!("+{pct}%")
    } else {
        format!("{pct}%")
    }
}

// ─── Steps chart ─────────────────────────────────────────────────────────────

pub fn load_steps_chart(conn: &Connection, period: i32, offset: i32) -> anyhow::Result<StepsChart> {
    let range = range_for(period, offset);
    let by_day = fetch_steps_by_day(conn, range)?;

    let bars = match period {
        0 => hourly_step_bars(conn, range.start)?,
        2 => weekly_step_bars(&by_day, range),
        _ => daily_step_bars(&by_day, range),
    };

    let (summary, avg_label) = steps_labels(&by_day, range, period);

    // The delta compares the same metric the header shows: the day's total in
    // day view, average steps per elapsed day otherwise — so a partial
    // current month isn't measured against a full previous month's total.
    let prev_range = range_for(period, offset + 1);
    let prev_by_day = fetch_steps_by_day(conn, prev_range)?;
    let delta_label = delta_pct(
        steps_metric(&by_day, range, period),
        steps_metric(&prev_by_day, prev_range, period),
    );

    Ok(StepsChart {
        bars,
        summary,
        avg_label,
        delta_positive: delta_label.starts_with('+'),
        delta_label,
    })
}

fn hourly_step_bars(conn: &Connection, day: NaiveDate) -> anyhow::Result<Vec<DayStepsData>> {
    let rows = fetch_steps_by_hour(conn, day)?;
    let max_steps = rows.iter().map(|r| r.1).max().unwrap_or(1).max(1);
    Ok(rows
        .into_iter()
        .map(|(h, total)| DayStepsData {
            label: format!("{}", h),
            steps_label: format_number(total),
            steps_raw: total,
            fraction: total as f32 / max_steps as f32,
            bar_start: time::local_ts(day, h, 0, 0),
            bar_end: time::local_ts(day, h, 59, 59),
        })
        .collect())
}

fn daily_step_bars(by_day: &BTreeMap<NaiveDate, i64>, range: DateRange) -> Vec<DayStepsData> {
    let days: Vec<(NaiveDate, i64)> = range
        .days()
        .map(|d| (d, by_day.get(&d).copied().unwrap_or(0)))
        .collect();
    let max_steps = days.iter().map(|(_, t)| *t).max().unwrap_or(1).max(1);
    days.into_iter()
        .map(|(d, total)| DayStepsData {
            label: d.format("%a").to_string(),
            steps_label: format_number(total),
            steps_raw: total,
            fraction: total as f32 / max_steps as f32,
            bar_start: time::local_ts(d, 0, 0, 0),
            bar_end: time::local_ts(d, 23, 59, 59),
        })
        .collect()
}

/// Month view: one bar per ISO week, height and label are the week's average
/// steps per elapsed day. Weeks entirely in the future are skipped, but the
/// W1/W2/… labels stay anchored to the week's position in the month.
fn weekly_step_bars(by_day: &BTreeMap<NaiveDate, i64>, range: DateRange) -> Vec<DayStepsData> {
    let today = watch_today();
    struct Week {
        idx: usize,
        span: DateRange,
        total: i64,
        avg: i64,
    }
    let weeks: Vec<Week> = week_buckets(range)
        .into_iter()
        .enumerate()
        .filter_map(|(idx, span)| {
            let elapsed = span.days().filter(|d| *d <= today).count() as i64;
            if elapsed == 0 {
                return None;
            }
            let total: i64 = span.days().filter_map(|d| by_day.get(&d)).sum();
            Some(Week { idx, span, total, avg: total / elapsed })
        })
        .collect();

    let max_avg = weeks.iter().map(|w| w.avg).max().unwrap_or(1).max(1);
    weeks
        .into_iter()
        .map(|w| {
            let (bar_start, bar_end) = w.span.utc_bounds();
            DayStepsData {
                label: format!("W{}", w.idx + 1),
                steps_label: format_number(w.avg),
                steps_raw: w.total,
                fraction: w.avg as f32 / max_avg as f32,
                bar_start,
                bar_end,
            }
        })
        .collect()
}

/// The headline steps metric: the total in day view, average per elapsed day
/// otherwise. Must stay in sync with `steps_labels`.
fn steps_metric(by_day: &BTreeMap<NaiveDate, i64>, range: DateRange, period: i32) -> i64 {
    let total: i64 = by_day.values().sum();
    if period == 0 {
        total
    } else {
        total / range.days_elapsed().max(1)
    }
}

/// (summary, avg_label) for the steps chart header.
/// Day: total steps. Week/Month: average per elapsed day.
fn steps_labels(
    by_day: &BTreeMap<NaiveDate, i64>,
    range: DateRange,
    period: i32,
) -> (String, String) {
    if by_day.is_empty() {
        return ("0 steps".to_string(), "—".to_string());
    }
    let metric = steps_metric(by_day, range, period);
    let label = if period == 0 {
        format!("{} steps", format_number(metric))
    } else {
        format!("avg {} / day", format_number(metric))
    };
    (label.clone(), label)
}

/// Average steps per elapsed day for a selected chart-bar range.
pub fn load_steps_avg_label_for_range(
    conn: &Connection,
    range: DateRange,
) -> anyhow::Result<String> {
    let by_day = fetch_steps_by_day(conn, range)?;
    Ok(steps_labels(&by_day, range, 1).1)
}

// ─── Sleep chart ─────────────────────────────────────────────────────────────

pub fn load_sleep_chart(conn: &Connection, period: i32, offset: i32) -> anyhow::Result<SleepChart> {
    let range = range_for(period, offset);
    let nights = fetch_sleep_nights(conn, range)?;

    let bars = if period == 2 {
        weekly_sleep_bars(&nights, range)
    } else {
        nightly_sleep_bars(&nights)
    };

    let (summary, avg_label) = sleep_labels(&nights, period);

    // The delta compares the same metric the header shows: the night's total
    // in day view, average per night with data otherwise — so a week that's
    // missing a synced night isn't measured against full-week totals.
    let prev_nights = fetch_sleep_nights(conn, range_for(period, offset + 1))?;
    let delta_label = delta_pct(
        sleep_metric(&nights, period),
        sleep_metric(&prev_nights, period),
    );

    Ok(SleepChart {
        bars,
        summary,
        avg_label,
        delta_positive: delta_label.starts_with('+'),
        delta_label,
    })
}

fn total_sleep(nights: &[Night]) -> i64 {
    nights.iter().map(|n| n.total_secs()).sum()
}

/// The headline sleep metric: the total in day view, average per night with
/// data otherwise. Must stay in sync with `sleep_labels`.
fn sleep_metric(nights: &[Night], period: i32) -> i64 {
    if period == 0 || nights.is_empty() {
        total_sleep(nights)
    } else {
        total_sleep(nights) / nights.len() as i64
    }
}

fn deep_label(deep: i64) -> String {
    if deep > 0 {
        format!("{} deep", format_duration(deep))
    } else {
        String::new()
    }
}

fn nightly_sleep_bars(nights: &[Night]) -> Vec<SleepBarData> {
    let max_total = nights.iter().map(|n| n.total_secs()).max().unwrap_or(1).max(1);
    nights
        .iter()
        .map(|n| {
            let total = n.total_secs();
            let deep = n.deep_secs();
            let light = (total - deep).max(0);
            SleepBarData {
                label: n.wake_date.format("%a").to_string(),
                bar_start: time::local_ts(n.wake_date, 0, 0, 0),
                bar_end: time::local_ts(n.wake_date, 23, 59, 59),
                light_fraction: light as f32 / max_total as f32,
                deep_fraction: deep as f32 / max_total as f32,
                light_secs: light,
                deep_secs: deep,
                total_label: format_duration(total),
                deep_label: deep_label(deep),
            }
        })
        .collect()
}

/// Month view: one bar per ISO week, sized by the week's average per night
/// with data. Weeks without any sleep data are skipped, but W1/W2/… labels
/// stay anchored to the week's position in the month.
fn weekly_sleep_bars(nights: &[Night], range: DateRange) -> Vec<SleepBarData> {
    struct Week {
        idx: usize,
        span: DateRange,
        avg_total: i64,
        avg_deep: i64,
    }
    let weeks: Vec<Week> = week_buckets(range)
        .into_iter()
        .enumerate()
        .filter_map(|(idx, span)| {
            let wn: Vec<&Night> = nights.iter().filter(|n| span.contains(n.wake_date)).collect();
            if wn.is_empty() {
                return None;
            }
            let n = wn.len() as i64;
            let total: i64 = wn.iter().map(|night| night.total_secs()).sum();
            let deep: i64 = wn.iter().map(|night| night.deep_secs()).sum();
            Some(Week { idx, span, avg_total: total / n, avg_deep: deep / n })
        })
        .collect();

    let max_total = weeks.iter().map(|w| w.avg_total).max().unwrap_or(1).max(1);
    weeks
        .into_iter()
        .map(|w| {
            let light = (w.avg_total - w.avg_deep).max(0);
            let (bar_start, bar_end) = w.span.utc_bounds();
            SleepBarData {
                label: format!("W{}", w.idx + 1),
                bar_start,
                bar_end,
                light_fraction: light as f32 / max_total as f32,
                deep_fraction: w.avg_deep as f32 / max_total as f32,
                light_secs: light,
                deep_secs: w.avg_deep,
                total_label: format_duration(w.avg_total),
                deep_label: deep_label(w.avg_deep),
            }
        })
        .collect()
}

/// (summary, avg_label) for the sleep chart header.
/// Day: that night's totals. Week/Month: average per night with data.
fn sleep_labels(nights: &[Night], period: i32) -> (String, String) {
    if nights.is_empty() {
        return ("No sleep data".to_string(), "—".to_string());
    }
    let total = total_sleep(nights);
    let deep: i64 = nights.iter().map(|n| n.deep_secs()).sum();

    if period == 0 {
        let summary = if deep > 0 {
            format!("{} · {} deep", format_duration(total), format_duration(deep))
        } else {
            format_duration(total)
        };
        (summary, format_duration(total))
    } else {
        let n = nights.len() as i64;
        let (avg, avg_deep) = (total / n, deep / n);
        let summary = if avg_deep > 0 {
            format!("avg {} · {} deep", format_duration(avg), format_duration(avg_deep))
        } else {
            format!("avg {}", format_duration(avg))
        };
        (summary, format!("AVG {} / night", format_duration(avg)))
    }
}

/// Average sleep per night for a selected chart-bar range.
pub fn load_sleep_avg_label_for_range(
    conn: &Connection,
    range: DateRange,
) -> anyhow::Result<String> {
    let nights = fetch_sleep_nights(conn, range)?;
    Ok(sleep_labels(&nights, 1).1)
}

// ─── Heart-rate stats ────────────────────────────────────────────────────────

const HEART_RATE_MIN_BPM: i64 = 30;
const HEART_RATE_MAX_BPM: i64 = 220;
const HEART_RATE_UTC_OFFSET_SPREAD_SECS: i64 = 26 * 60 * 60;

#[derive(Debug, Clone, Copy)]
struct HeartSample {
    local_date: NaiveDate,
    local_hour: u32,
    bpm: u16,
}

/// Valid heart-rate samples in a watch-local date range. The SQL bounds only
/// narrow the scan; each row's own UTC offset decides its exact local date.
fn fetch_heart_samples(conn: &Connection, range: DateRange) -> anyhow::Result<Vec<HeartSample>> {
    let (utc_start, utc_end) = range.utc_bounds();
    let mut stmt = conn.prepare(
        "SELECT start_ts, utc_offset, heart_rate_bpm
         FROM health_activity_minutes
         WHERE start_ts >= ?1 - ?3
           AND start_ts <= ?2 + ?3
           AND heart_rate_bpm BETWEEN ?4 AND ?5
         ORDER BY start_ts ASC",
    )?;
    let mut samples = Vec::new();
    for row in stmt.query_map(
        params![
            utc_start,
            utc_end,
            HEART_RATE_UTC_OFFSET_SPREAD_SECS,
            HEART_RATE_MIN_BPM,
            HEART_RATE_MAX_BPM
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        },
    )? {
        let (start_ts, utc_offset, bpm) = row?;
        let Some(timestamp) = DateTime::from_timestamp(start_ts + utc_offset, 0) else {
            continue;
        };
        let local_date = timestamp.date_naive();
        if range.contains(local_date) {
            samples.push(HeartSample {
                local_date,
                local_hour: timestamp.hour(),
                bpm: bpm as u16,
            });
        }
    }
    Ok(samples)
}

fn average_bpm<I>(values: I) -> Option<f64>
where
    I: IntoIterator<Item = f64>,
{
    let (sum, count) = values
        .into_iter()
        .fold((0.0, 0u64), |(sum, count), value| (sum + value, count + 1));
    (count > 0).then_some(sum / count as f64)
}

fn bpm_label(value: Option<f64>) -> String {
    value
        .map(|bpm| format!("{} bpm", bpm.round() as i64))
        .unwrap_or_else(|| "—".to_string())
}

fn mean_bpm<I>(values: I) -> Option<f32>
where
    I: IntoIterator<Item = f32>,
{
    average_bpm(values.into_iter().map(f64::from)).map(|bpm| bpm as f32)
}

fn daily_heart_trend_points(
    samples: &[HeartSample],
    wellness: &[DailyWellness],
    range: DateRange,
) -> Vec<HeartTrendPointData> {
    let mut totals = BTreeMap::<NaiveDate, (f64, u32)>::new();
    for sample in samples {
        let (sum, count) = totals.entry(sample.local_date).or_default();
        *sum += f64::from(sample.bpm);
        *count += 1;
    }
    let resting = wellness
        .iter()
        .filter_map(|day| day.resting_hr.map(|bpm| (day.date, f32::from(bpm))))
        .collect::<BTreeMap<_, _>>();

    range
        .days()
        .map(|date| {
            let average_bpm = totals
                .get(&date)
                .map(|(sum, count)| (*sum / f64::from(*count)) as f32);
            HeartTrendPointData {
                label: date.format("%a").to_string(),
                average_bpm,
                resting_bpm: resting.get(&date).copied(),
            }
        })
        .collect()
}

fn hourly_heart_trend_points(
    samples: &[HeartSample],
    wellness: &[DailyWellness],
    day: NaiveDate,
) -> Vec<HeartTrendPointData> {
    let mut totals = [(0.0f64, 0u32); 24];
    for sample in samples.iter().filter(|sample| sample.local_date == day) {
        let (sum, count) = &mut totals[sample.local_hour as usize];
        *sum += f64::from(sample.bpm);
        *count += 1;
    }
    let resting = wellness
        .iter()
        .find(|entry| entry.date == day)
        .and_then(|entry| entry.resting_hr)
        .map(f32::from);

    (0..24)
        .map(|hour| {
            let (sum, count) = totals[hour];
            let average_bpm = (count > 0).then_some((sum / f64::from(count)) as f32);
            let label = match hour {
                0 => "12a",
                6 => "6a",
                12 => "12p",
                18 => "6p",
                _ => "",
            };
            HeartTrendPointData {
                label: label.to_string(),
                average_bpm,
                // Resting HR is a daily overnight estimate, so show it as a
                // reference line across the selected day.
                resting_bpm: resting,
            }
        })
        .collect()
}

fn weekly_heart_trend_points(
    samples: &[HeartSample],
    wellness: &[DailyWellness],
    range: DateRange,
) -> Vec<HeartTrendPointData> {
    week_buckets(range)
        .into_iter()
        .enumerate()
        .map(|(index, span)| {
            let average_bpm = mean_bpm(
                samples
                    .iter()
                    .filter(|sample| span.contains(sample.local_date))
                    .map(|sample| f32::from(sample.bpm)),
            );
            let resting_bpm = mean_bpm(
                wellness
                    .iter()
                    .filter(|day| span.contains(day.date))
                    .filter_map(|day| day.resting_hr.map(f32::from)),
            );
            HeartTrendPointData {
                label: format!("W{}", index + 1),
                average_bpm,
                resting_bpm,
            }
        })
        .collect()
}

fn heart_trend_scale(points: &[HeartTrendPointData]) -> (f32, f32) {
    let mut min_bpm = f32::INFINITY;
    let mut max_bpm = f32::NEG_INFINITY;
    for value in points
        .iter()
        .flat_map(|point| [point.average_bpm, point.resting_bpm])
        .flatten()
    {
        min_bpm = min_bpm.min(value);
        max_bpm = max_bpm.max(value);
    }

    if !min_bpm.is_finite() || !max_bpm.is_finite() {
        return (40.0, 180.0);
    }

    let min_bpm = ((min_bpm - 5.0).max(0.0) / 10.0).floor() * 10.0;
    let mut max_bpm = ((max_bpm + 5.0) / 10.0).ceil() * 10.0;
    if max_bpm <= min_bpm {
        max_bpm = min_bpm + 10.0;
    }
    (min_bpm, max_bpm)
}

/// Build average and resting heart-rate trend points for the active period.
pub fn load_heart_trend(
    conn: &Connection,
    period: i32,
    offset: i32,
) -> anyhow::Result<HeartTrend> {
    let range = range_for(period, offset);
    let samples = fetch_heart_samples(conn, range)?;
    let wellness = fetch_daily_wellness(conn, range)?;
    let points = match period {
        0 => hourly_heart_trend_points(&samples, &wellness, range.start),
        2 => weekly_heart_trend_points(&samples, &wellness, range),
        _ => daily_heart_trend_points(&samples, &wellness, range),
    };
    let (min_bpm, max_bpm) = heart_trend_scale(&points);
    Ok(HeartTrend {
        points,
        min_bpm,
        max_bpm,
    })
}

/// Compute key heart-rate metrics for one navigated period.
pub fn load_heart_stats(
    conn: &Connection,
    period: i32,
    offset: i32,
) -> anyhow::Result<HeartStats> {
    let range = range_for(period, offset);
    let samples = fetch_heart_samples(conn, range)?;
    let wellness = fetch_daily_wellness(conn, range)?;
    let resting: Vec<f64> = wellness
        .iter()
        .filter_map(|day| day.resting_hr.map(f64::from))
        .collect();
    let sleeping: Vec<f64> = wellness
        .iter()
        .filter_map(|day| day.avg_sleeping_hr.map(f64::from))
        .collect();

    let lowest = samples.iter().map(|sample| sample.bpm).min().map(f64::from);
    let highest = samples.iter().map(|sample| sample.bpm).max().map(f64::from);
    Ok(HeartStats {
        average_label: bpm_label(average_bpm(
            samples.iter().map(|sample| f64::from(sample.bpm)),
        )),
        resting_label: bpm_label(average_bpm(resting)),
        sleeping_label: bpm_label(average_bpm(sleeping)),
        lowest_label: bpm_label(lowest),
        highest_label: bpm_label(highest),
        samples_label: if samples.is_empty() {
            "—".to_string()
        } else {
            format!("{} readings", format_number(samples.len() as i64))
        },
    })
}

// ─── Activity sessions ───────────────────────────────────────────────────────

pub fn load_sessions_filtered(
    conn: &Connection,
    session_filter: i32,
    range_start: i64,
    range_end: i64,
) -> anyhow::Result<Vec<HealthSessionData>> {
    let sql = "SELECT s.start_ts, s.utc_offset, s.duration_secs, t.name,
                      s.has_metrics, s.steps, s.active_kcal, s.distance_m
               FROM (
                   SELECT start_ts, utc_offset, duration_secs, session_type,
                          (steps IS NOT NULL)       AS has_metrics,
                          COALESCE(steps, 0)        AS steps,
                          COALESCE(active_kcal, 0)  AS active_kcal,
                          COALESCE(distance_m, 0)   AS distance_m
                   FROM health_activity_sessions
               ) s
               JOIN session_types t ON s.session_type = t.id
               WHERE (?1 < 0 OR s.start_ts >= ?1)
                 AND (?2 < 0 OR s.start_ts <= ?2)
                 AND (?3 = 0
                      OR (?3 = 1 AND s.session_type >= 5)
                      OR (?3 = 2 AND s.session_type <= 4))
               ORDER BY s.start_ts DESC
               LIMIT 100";

    let mut stmt = conn.prepare(sql)?;
    let sessions = stmt
        .query_map(params![range_start, range_end, session_filter], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, bool>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, i64>(7)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .map(
            |(ts, utc_offset, dur, type_name, has_metrics, steps, active_kcal, distance_m)| {
                let true_ts = ts + utc_offset;
                let metrics_label = if has_metrics {
                    format!(
                        "{} steps · {} kcal · {}",
                        format_number(steps),
                        active_kcal,
                        format_distance(distance_m),
                    )
                } else {
                    String::new()
                };
                HealthSessionData {
                    type_name: capitalize(&type_name),
                    start_label: format_ts(true_ts),
                    duration_label: format_duration(dur),
                    has_metrics,
                    metrics_label,
                }
            },
        )
        .collect();

    Ok(sessions)
}

// ─── Sleep stats ─────────────────────────────────────────────────────────────

const RESTING_HR_WINDOW_SECS: i64 = 60 * 60;
const RESTING_HR_MIN_SPAN_SECS: i64 = 30 * 60;
const RESTING_HR_MAX_GAP_SECS: i64 = 30 * 60;
const RESTING_HR_MIN_SAMPLES: usize = 4;
const RESTING_HR_MIN_BPM: i64 = 30;
const RESTING_HR_MAX_BPM: i64 = 220;

/// Estimate resting HR as the lowest qualifying mean inside any rolling
/// 60-minute window. A window must contain at least four readings, cover at
/// least 30 minutes from first to last reading, and contain no gap over 30
/// minutes. These constraints prevent an isolated low reading from becoming
/// the day's estimate.
fn lowest_sustained_resting_hr(samples: &[(i64, i64)]) -> Option<u16> {
    let mut best: Option<f64> = None;
    let mut earliest_start = 0;

    for end in 0..samples.len() {
        while samples[end].0 - samples[earliest_start].0 > RESTING_HR_WINDOW_SECS {
            earliest_start += 1;
        }
        for start in earliest_start..=end {
            let window = &samples[start..=end];
            if window.len() < RESTING_HR_MIN_SAMPLES {
                break;
            }
            if samples[end].0 - samples[start].0 < RESTING_HR_MIN_SPAN_SECS
                || window
                    .windows(2)
                    .any(|pair| pair[1].0 - pair[0].0 > RESTING_HR_MAX_GAP_SECS)
            {
                continue;
            }

            let mean = window.iter().map(|(_, bpm)| *bpm as f64).sum::<f64>()
                / window.len() as f64;
            best = Some(best.map_or(mean, |current| current.min(mean)));
        }
    }

    best.map(|bpm| bpm.round() as u16)
}

/// Calculate daily estimates from primary overnight sleep.
/// Naps are intentionally excluded.
fn estimated_resting_hr_by_wake_date(
    conn: &Connection,
    nights: &[Night],
) -> anyhow::Result<BTreeMap<NaiveDate, u16>> {
    let mut primary_spans = Vec::new();
    for night in nights {
        let mut spans: Vec<(i64, i64)> = night
            .phases
            .iter()
            .filter(|phase| {
                !phase.is_deep && !phase.is_nap && phase.utc_start < phase.utc_end
            })
            .map(|phase| (phase.utc_start, phase.utc_end))
            .collect();
        spans.sort_unstable_by_key(|&(start, _)| start);

        let mut merged: Vec<(i64, i64)> = Vec::new();
        for (start, end) in spans {
            if let Some((_, merged_end)) = merged.last_mut()
                && start <= *merged_end
            {
                *merged_end = (*merged_end).max(end);
                continue;
            }
            merged.push((start, end));
        }
        primary_spans.extend(
            merged
                .into_iter()
                .map(|(start, end)| (night.wake_date, start, end)),
        );
    }
    if primary_spans.is_empty() {
        return Ok(BTreeMap::new());
    }
    primary_spans.sort_unstable_by_key(|&(_, start, _)| start);

    let query_start = primary_spans
        .iter()
        .map(|&(_, start, _)| start)
        .min()
        .unwrap();
    let query_end = primary_spans
        .iter()
        .map(|&(_, _, end)| end)
        .max()
        .unwrap();
    let mut stmt = conn.prepare(
        "SELECT start_ts, heart_rate_bpm
         FROM health_activity_minutes
         WHERE start_ts >= ?1 AND start_ts < ?2
           AND heart_rate_bpm BETWEEN ?3 AND ?4
         ORDER BY start_ts ASC",
    )?;
    let rows = stmt.query_map(
        params![
            query_start,
            query_end,
            RESTING_HR_MIN_BPM,
            RESTING_HR_MAX_BPM
        ],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
    )?;
    let mut next_span = 0;
    let mut active = BTreeMap::<NaiveDate, i64>::new();
    let mut expirations = BinaryHeap::<Reverse<(i64, NaiveDate)>>::new();
    let mut samples_by_date = BTreeMap::<NaiveDate, Vec<(i64, i64)>>::new();

    for row in rows {
        let (sample_ts, bpm) = row?;

        while let Some(&Reverse((end, date))) = expirations.peek() {
            if end > sample_ts {
                break;
            }
            expirations.pop();
            if active.get(&date) == Some(&end) {
                active.remove(&date);
            }
        }

        while let Some(&(date, start, end)) = primary_spans.get(next_span) {
            if start > sample_ts {
                break;
            }
            next_span += 1;
            if end > sample_ts {
                active.insert(date, end);
                expirations.push(Reverse((end, date)));
            }
        }

        for &date in active.keys() {
            samples_by_date
                .entry(date)
                .or_default()
                .push((sample_ts, bpm));
        }
    }

    let mut estimates = BTreeMap::new();
    for (date, samples) in samples_by_date {
        if let Some(resting_hr) = lowest_sustained_resting_hr(&samples) {
            estimates.insert(date, resting_hr);
        }
    }

    Ok(estimates)
}

/// Return sustained overnight resting-HR estimates keyed by sleep wake date.
pub fn fetch_daily_resting_hr(
    conn: &Connection,
    range: DateRange,
) -> anyhow::Result<BTreeMap<NaiveDate, u16>> {
    let nights = fetch_sleep_nights(conn, range)?;
    estimated_resting_hr_by_wake_date(conn, &nights)
}

/// Compute key statistics from sleep data for the current period.
pub fn load_sleep_stats(conn: &Connection, period: i32, offset: i32) -> anyhow::Result<SleepStats> {
    load_sleep_stats_for_range(conn, range_for(period, offset))
}

/// Compute key statistics from sleep data inside an explicit watch-local date
/// range. This is used when a chart bar is selected, so the stats can describe
/// one day or one month-view week instead of the whole navigated period.
pub fn load_sleep_stats_for_range(
    conn: &Connection,
    range: DateRange,
) -> anyhow::Result<SleepStats> {
    let nights = fetch_sleep_nights(conn, range)?;

    if nights.is_empty() {
        return Ok(SleepStats {
            deep_avg_label: "—".into(),
            light_pct: 0.0,
            deep_pct: 0.0,
            awake_pct: 0.0,
            avg_bedtime: "—".into(),
            avg_wakeup: "—".into(),
            highest_dur: "—".into(),
            lowest_dur: "—".into(),
        });
    }

    // Stage mix: light/deep as a share of all slept time (naps included,
    // matching the chart's totals).
    let total_slept: i64 = nights.iter().map(|n| n.total_secs()).sum();
    let total_deep: i64 = nights.iter().map(|n| n.deep_secs()).sum();

    // Bedtime, wake-up, and awake gaps come from the overnight block only,
    // so an afternoon nap doesn't count the whole day as "in bed" or shift
    // the average wake-up time to the nap's end.
    let mut bedtimes: Vec<i64> = Vec::new();
    let mut wakeups: Vec<i64> = Vec::new();
    let mut in_bed: i64 = 0;
    let mut slept_in_bed: i64 = 0;
    for night in &nights {
        let block = night.overnight_phases();
        let (Some(start), Some(end)) = (
            block.iter().map(|p| p.local_start).min(),
            block.iter().map(|p| p.local_end).max(),
        ) else {
            continue; // deep-only orphan night
        };
        bedtimes.push(start);
        wakeups.push(end);
        in_bed += end - start;
        slept_in_bed += block.iter().map(|p| p.local_end - p.local_start).sum::<i64>();
    }

    // One denominator for the whole stage mix — slept time plus the awake
    // gaps between sleep sections — so light + deep + awake = 100%.
    let awake = (in_bed - slept_in_bed).max(0);
    let (light_pct, deep_pct, awake_pct) =
        stage_percentages(total_slept - total_deep, total_deep, awake);

    // Duration stats: exclude nights with zero primary sleep (orphan deep-only
    // nights) so they don't drag down the average or claim shortest night.
    let with_sleep: Vec<i64> = nights
        .iter()
        .filter_map(|n| {
            let s = n.total_secs();
            if s > 0 { Some(s) } else { None }
        })
        .collect();
    let slept_nights = with_sleep.len().max(1) as i64;
    let slept_deep: i64 = nights
        .iter()
        .filter(|n| n.total_secs() > 0)
        .map(|n| n.deep_secs())
        .sum();
    Ok(SleepStats {
        deep_avg_label: format_duration(slept_deep / slept_nights),
        light_pct,
        deep_pct,
        awake_pct,
        avg_bedtime: avg_time_of_day(&bedtimes),
        avg_wakeup: avg_time_of_day(&wakeups),
        highest_dur: format_duration(with_sleep.iter().copied().max().unwrap_or(0)),
        lowest_dur: format_duration(with_sleep.iter().copied().min().unwrap_or(0)),
    })
}

/// Integer percentages of (light, deep, awake) that total exactly 100.
/// Largest-remainder rounding, so independent rounding can't add up to 101.
fn stage_percentages(light: i64, deep: i64, awake: i64) -> (f32, f32, f32) {
    let parts = [light.max(0), deep.max(0), awake.max(0)];
    let total: i64 = parts.iter().sum();
    if total == 0 {
        return (0.0, 0.0, 0.0);
    }
    let exact = parts.map(|p| p as f64 / total as f64 * 100.0);
    let mut floors = exact.map(|e| e.floor() as i64);
    let mut by_remainder: Vec<usize> = (0..exact.len()).collect();
    by_remainder.sort_by(|&a, &b| {
        (exact[b] - exact[b].floor()).total_cmp(&(exact[a] - exact[a].floor()))
    });
    let deficit = 100 - floors.iter().sum::<i64>();
    for &i in by_remainder.iter().take(deficit as usize) {
        floors[i] += 1;
    }
    (floors[0] as f32, floors[1] as f32, floors[2] as f32)
}

/// Average wall-clock time of a set of watch-local epochs, as a 12-hour
/// label. Uses a circular mean on the 24h clock so bedtimes straddling
/// midnight (11pm, 1am) average to midnight instead of noon.
fn avg_time_of_day(epochs: &[i64]) -> String {
    use chrono::Timelike;
    use std::f64::consts::TAU;

    let angles = epochs.iter().filter_map(|&e| {
        DateTime::from_timestamp(e, 0)
            .map(|dt| (dt.hour() * 60 + dt.minute()) as f64 / 1440.0 * TAU)
    });
    let (sum_sin, sum_cos, n) = angles.fold((0.0f64, 0.0f64, 0u32), |(s, c, n), a| {
        (s + a.sin(), c + a.cos(), n + 1)
    });
    if n == 0 {
        return "—".to_string();
    }
    let mean_mins = (sum_sin.atan2(sum_cos) / TAU * 1440.0 + 1440.0) % 1440.0;
    let (h, m) = (mean_mins as u32 / 60, mean_mins as u32 % 60);
    let ampm = if h < 12 { "AM" } else { "PM" };
    let h12 = match h % 12 {
        0 => 12,
        x => x,
    };
    format!("{}:{:02} {}", h12, m, ampm)
}

// ═══ Tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema;

    /// Watch-local = UTC-4. Every test uses the same value because the watch
    /// offset is process-global.
    const TZ: i64 = -4 * 3600;

    fn setup() -> Connection {
        time::set_watch_offset(TZ);
        let conn = Connection::open_in_memory().unwrap();
        schema::initialize_schema(&conn).unwrap();
        conn.execute(
            "INSERT INTO health_records (id, tag, app_uuid, session_ts, item_type, item_size, crc, data, received_at)
             VALUES (1, 83, x'00', 0, 0, 0, 0, x'00', 0)",
            [],
        )
        .unwrap();
        conn
    }

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    /// Watch-local epoch seconds for a wall-clock time.
    fn local(date: NaiveDate, h: u32, min: u32) -> i64 {
        date.and_hms_opt(h, min, 0).unwrap().and_utc().timestamp()
    }

    fn insert_session(conn: &Connection, ty: i32, local_start: i64, dur: i64) {
        conn.execute(
            "INSERT INTO health_activity_sessions
             (health_record_id, record_version, session_type, utc_offset, start_ts, duration_secs, raw)
             VALUES (1, 3, ?1, ?2, ?3, ?4, x'00')",
            params![ty, TZ, local_start - TZ, dur],
        )
        .unwrap();
    }

    fn insert_minute(conn: &Connection, local_start: i64, steps: i64) {
        conn.execute(
            "INSERT INTO health_activity_minutes
             (health_record_id, record_version, start_ts, utc_offset, steps, orientation, vmc, light, raw)
             VALUES (1, 7, ?1, ?2, ?3, 0, 0, 0, x'00')",
            params![local_start - TZ, TZ, steps],
        )
        .unwrap();
    }

    fn insert_minute_at(
        conn: &Connection,
        utc_start: i64,
        utc_offset: i64,
        steps: i64,
        heart_rate: Option<i64>,
    ) {
        conn.execute(
            "INSERT INTO health_activity_minutes
             (health_record_id, record_version, start_ts, utc_offset, steps, orientation, vmc, light,
              heart_rate_bpm, raw)
             VALUES (1, 7, ?1, ?2, ?3, 0, 0, 0, ?4, x'00')",
            params![utc_start, utc_offset, steps, heart_rate],
        )
        .unwrap();
    }

    /// The whole point of the redesign: sleep starting the night of Jul 3 and
    /// ending the morning of Jul 4 belongs to Jul 4, including a deep segment
    /// that ends before midnight.
    #[test]
    fn overnight_sleep_lands_on_wake_date() {
        let conn = setup();
        let jul3 = d(2026, 7, 3);
        let jul4 = d(2026, 7, 4);

        // 11pm Jul 3 → 7am Jul 4 (8h), with deep spans on both sides of midnight.
        insert_session(&conn, 1, local(jul3, 23, 0), 8 * 3600);
        insert_session(&conn, 2, local(jul3, 23, 10), 40 * 60); // ends 23:50 Jul 3
        insert_session(&conn, 2, local(jul4, 2, 0), 50 * 60);

        let nights = fetch_sleep_nights(&conn, DateRange::day(jul4)).unwrap();
        assert_eq!(nights.len(), 1);
        let night = &nights[0];
        assert_eq!(night.wake_date, jul4);
        assert_eq!(night.phases.len(), 3);
        assert_eq!(night.total_secs(), 8 * 3600);
        assert_eq!(night.deep_secs(), 90 * 60);
        assert_eq!(night.sleep_start(), local(jul3, 23, 0));

        // Nothing woke up on Jul 3, so its day view is empty.
        let jul3_nights = fetch_sleep_nights(&conn, DateRange::day(jul3)).unwrap();
        assert!(jul3_nights.is_empty());
    }

    #[test]
    fn nap_counts_on_its_own_day() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);

        insert_session(&conn, 1, local(d(2026, 7, 3), 23, 0), 8 * 3600);
        insert_session(&conn, 3, local(jul4, 14, 0), 3600); // afternoon nap

        let nights = fetch_sleep_nights(&conn, DateRange::day(jul4)).unwrap();
        assert_eq!(nights.len(), 1);
        assert_eq!(nights[0].total_secs(), 9 * 3600);
    }

    #[test]
    fn week_range_buckets_each_night_by_wake_date() {
        let conn = setup();
        // Nights waking Jul 2, 3, 4.
        for day in 1..=3 {
            insert_session(&conn, 1, local(d(2026, 7, day), 23, 0), 7 * 3600);
        }

        let range = DateRange { start: d(2026, 6, 29), end: d(2026, 7, 5) };
        let nights = fetch_sleep_nights(&conn, range).unwrap();
        let dates: Vec<NaiveDate> = nights.iter().map(|n| n.wake_date).collect();
        assert_eq!(dates, vec![d(2026, 7, 2), d(2026, 7, 3), d(2026, 7, 4)]);

        // A range ending Jul 3 must exclude the night that woke on Jul 4.
        let range = DateRange { start: d(2026, 6, 29), end: d(2026, 7, 3) };
        assert_eq!(fetch_sleep_nights(&conn, range).unwrap().len(), 2);
    }

    #[test]
    fn deep_segment_uses_row_utc_offset() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        // Session stored with a different utc_offset than the watch's current
        // one (e.g. recorded before a DST change): local times still come from
        // the row itself.
        let local_start = local(d(2026, 7, 3), 23, 0);
        let row_tz = TZ + 3600;
        conn.execute(
            "INSERT INTO health_activity_sessions
             (health_record_id, record_version, session_type, utc_offset, start_ts, duration_secs, raw)
             VALUES (1, 3, 1, ?1, ?2, ?3, x'00')",
            params![row_tz, local_start - row_tz, 8 * 3600],
        )
        .unwrap();

        let nights = fetch_sleep_nights(&conn, DateRange::day(jul4)).unwrap();
        assert_eq!(nights.len(), 1);
        assert_eq!(nights[0].wake_date, jul4);
    }

    #[test]
    fn steps_bucket_by_local_date_and_fill_missing_days() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        insert_minute(&conn, local(jul4, 0, 5), 20); // just after local midnight
        insert_minute(&conn, local(jul4, 12, 0), 100);
        insert_minute(&conn, local(jul4, 23, 59), 30);

        let range = DateRange { start: d(2026, 6, 29), end: d(2026, 7, 5) };
        let by_day = fetch_steps_by_day(&conn, range).unwrap();
        assert_eq!(by_day.len(), 1);
        assert_eq!(by_day[&jul4], 150);

        let bars = daily_step_bars(&by_day, range);
        assert_eq!(bars.len(), 7);
        assert_eq!(bars.iter().map(|b| b.steps_raw).sum::<i64>(), 150);

        let hours = fetch_steps_by_hour(&conn, jul4).unwrap();
        assert_eq!(hours, vec![(0, 20), (12, 100), (23, 30)]);
    }

    #[test]
    fn weekly_buckets_follow_iso_weeks() {
        // July 2026: Wed Jul 1 … Fri Jul 31 spans 5 ISO weeks.
        let range = DateRange { start: d(2026, 7, 1), end: d(2026, 7, 31) };
        let weeks = week_buckets(range);
        assert_eq!(weeks.len(), 5);
        assert_eq!(weeks[0], DateRange { start: d(2026, 7, 1), end: d(2026, 7, 5) });
        assert_eq!(weeks[1], DateRange { start: d(2026, 7, 6), end: d(2026, 7, 12) });
        assert_eq!(weeks[4], DateRange { start: d(2026, 7, 27), end: d(2026, 7, 31) });
    }

    #[test]
    fn delta_uses_same_wake_date_bucketing() {
        let conn = setup();
        // Night waking Jul 4: 8h. Night waking Jul 3: 4h.
        insert_session(&conn, 1, local(d(2026, 7, 3), 23, 0), 8 * 3600);
        insert_session(&conn, 1, local(d(2026, 7, 2), 23, 0), 4 * 3600);

        let cur = fetch_sleep_nights(&conn, DateRange::day(d(2026, 7, 4))).unwrap();
        let prev = fetch_sleep_nights(&conn, DateRange::day(d(2026, 7, 3))).unwrap();
        assert_eq!(delta_pct(total_sleep(&cur), total_sleep(&prev)), "+100%");
    }

    /// Hourly bars and the daily total must use the same membership rule
    /// (the row's own local date), even when a row's stored utc_offset
    /// differs from the watch's current offset.
    #[test]
    fn hourly_and_daily_step_bucketing_agree_across_offsets() {
        let conn = setup();
        let jul3 = d(2026, 7, 3);
        let jul4 = d(2026, 7, 4);

        // Row-local 00:30 on Jul 4, stored with utc_offset one hour east of
        // the watch's current offset — its UTC instant falls before the
        // watch-offset start of Jul 4.
        let row_tz = TZ + 3600;
        let local_start = local(jul4, 0, 30);
        conn.execute(
            "INSERT INTO health_activity_minutes
             (health_record_id, record_version, start_ts, utc_offset, steps, orientation, vmc, light, raw)
             VALUES (1, 7, ?1, ?2, 42, 0, 0, 0, x'00')",
            params![local_start - row_tz, row_tz],
        )
        .unwrap();

        let by_day = fetch_steps_by_day(&conn, DateRange::day(jul4)).unwrap();
        assert_eq!(by_day[&jul4], 42);
        assert_eq!(fetch_steps_by_hour(&conn, jul4).unwrap(), vec![(0, 42)]);

        // And Jul 3 must not double-count it under either rule.
        assert!(fetch_steps_by_day(&conn, DateRange::day(jul3)).unwrap().is_empty());
        assert!(fetch_steps_by_hour(&conn, jul3).unwrap().is_empty());
    }

    #[test]
    fn daily_wellness_aggregates_steps_sleep_naps_and_deep_overlays() {
        let conn = setup();
        let jul3 = d(2026, 7, 3);
        let jul4 = d(2026, 7, 4);

        insert_minute(&conn, local(jul4, 9, 0), 125);
        insert_minute(&conn, local(jul4, 18, 0), 75);
        insert_session(&conn, 1, local(jul3, 23, 0), 8 * 3600);
        insert_session(&conn, 2, local(jul3, 23, 10), 40 * 60);
        insert_session(&conn, 2, local(jul4, 2, 0), 50 * 60);
        insert_session(&conn, 3, local(jul4, 14, 0), 3600);

        let wellness = fetch_daily_wellness(&conn, DateRange::day(jul4)).unwrap();
        assert_eq!(
            wellness,
            vec![DailyWellness {
                date: jul4,
                steps: Some(200),
                sleep_secs: Some(9 * 3600),
                avg_sleeping_hr: None,
                resting_hr: None,
            }]
        );
    }

    #[test]
    fn daily_wellness_preserves_missing_fields_and_wake_date_bounds() {
        let conn = setup();
        let jul2 = d(2026, 7, 2);
        let jul4 = d(2026, 7, 4);
        let jul5 = d(2026, 7, 5);
        let jul6 = d(2026, 7, 6);

        insert_minute(&conn, local(jul2, 12, 0), 10);
        insert_session(&conn, 1, local(jul4, 23, 0), 8 * 3600);
        // Deep-only data is not an independently uploadable wellness field.
        insert_session(&conn, 2, local(jul6, 1, 0), 30 * 60);

        let wellness = fetch_daily_wellness(
            &conn,
            DateRange { start: jul2, end: jul5 },
        )
        .unwrap();
        assert_eq!(wellness.len(), 2);
        assert_eq!(wellness[0].date, jul2);
        assert_eq!(wellness[0].steps, Some(10));
        assert_eq!(wellness[0].sleep_secs, None);
        assert_eq!(wellness[0].avg_sleeping_hr, None);
        assert_eq!(wellness[0].resting_hr, None);
        assert_eq!(wellness[1].date, jul5);
        assert_eq!(wellness[1].steps, None);
        assert_eq!(wellness[1].sleep_secs, Some(8 * 3600));
        assert_eq!(wellness[1].avg_sleeping_hr, None);
        assert_eq!(wellness[1].resting_hr, None);
        assert_eq!(oldest_wellness_date(&conn).unwrap(), Some(jul2));
        assert_eq!(newest_wellness_date(&conn).unwrap(), Some(jul5));
    }

    #[test]
    fn daily_wellness_uses_each_activity_row_offset() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        let row_offset = TZ + 3600;
        let row_local_start = local(jul4, 0, 30);
        insert_minute_at(
            &conn,
            row_local_start - row_offset,
            row_offset,
            42,
            None,
        );

        let wellness = fetch_daily_wellness(&conn, DateRange::day(jul4)).unwrap();
        assert_eq!(wellness[0].date, jul4);
        assert_eq!(wellness[0].steps, Some(42));
    }

    #[test]
    fn daily_wellness_matches_sleeping_hr_in_absolute_time_and_omits_zero() {
        let conn = setup();
        let jul3 = d(2026, 7, 3);
        let jul4 = d(2026, 7, 4);
        let sleep_start_utc = local(jul3, 23, 0) - TZ;

        insert_session(&conn, 1, local(jul3, 23, 0), 8 * 3600);
        // This nap overlaps the overnight session, so an HR minute in the
        // overlap must still be counted only once.
        insert_session(&conn, 3, local(jul4, 0, 30), 3600);

        // Keep the minute's absolute timestamp inside sleep while storing a
        // different row offset, proving matching does not use local wall time.
        insert_minute_at(&conn, sleep_start_utc + 2 * 3600, TZ + 3600, 0, Some(50));
        insert_minute_at(
            &conn,
            sleep_start_utc + 2 * 3600 + 60,
            TZ,
            0,
            Some(60),
        );
        insert_minute_at(
            &conn,
            sleep_start_utc + 2 * 3600 + 120,
            TZ,
            0,
            Some(0),
        );

        let wellness = fetch_daily_wellness(&conn, DateRange::day(jul4)).unwrap();
        assert_eq!(wellness.len(), 1);
        assert_eq!(wellness[0].sleep_secs, Some(8 * 3600));
        assert_eq!(wellness[0].steps, Some(0));
        assert_eq!(wellness[0].avg_sleeping_hr, Some(55.0));
        assert_eq!(wellness[0].resting_hr, None);
    }

    #[test]
    fn heart_stats_aggregate_available_samples() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        let sleep_start_utc = local(d(2026, 7, 3), 23, 0) - TZ;
        insert_session(&conn, 1, local(d(2026, 7, 3), 23, 0), 8 * 3600);

        for (minute, bpm) in [(0, 60), (10, 62), (20, 64), (30, 66)] {
            insert_minute_at(
                &conn,
                sleep_start_utc + minute * 60,
                TZ,
                0,
                Some(bpm),
            );
        }
        insert_minute_at(&conn, local(jul4, 12, 0) - TZ, TZ, 0, Some(100));
        insert_minute_at(&conn, local(jul4, 13, 0) - TZ, TZ, 0, Some(120));

        let offset = (watch_today() - jul4).num_days() as i32;
        let stats = load_heart_stats(&conn, 0, offset).unwrap();
        // Overall samples follow their own watch-local calendar dates; the
        // overnight samples belong to Jul 3 while the sleep-derived metrics
        // below are keyed to the Jul 4 wake date.
        assert_eq!(stats.average_label, "110 bpm");
        assert_eq!(stats.resting_label, "63 bpm");
        assert_eq!(stats.sleeping_label, "63 bpm");
        assert_eq!(stats.lowest_label, "100 bpm");
        assert_eq!(stats.highest_label, "120 bpm");
        assert_eq!(stats.samples_label, "2 readings");

        let trend = load_heart_trend(&conn, 0, offset).unwrap();
        assert_eq!(trend.points.len(), 24);
        assert_eq!(trend.points[12].label, "12p");
        assert_eq!(trend.points[12].average_bpm, Some(100.0));
        assert_eq!(trend.points[13].average_bpm, Some(120.0));
        assert_eq!(trend.points[0].resting_bpm, Some(63.0));
        assert_eq!(trend.points[23].resting_bpm, Some(63.0));
    }

    #[test]
    fn heart_samples_include_large_row_offset_differences() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        let row_tz = 10 * 3600; // UTC+10, 14 hours from the watch's UTC-4.
        let local_start = local(jul4, 0, 30);
        insert_minute_at(
            &conn,
            local_start - row_tz,
            row_tz,
            0,
            Some(88),
        );

        let offset = (watch_today() - jul4).num_days() as i32;
        let stats = load_heart_stats(&conn, 0, offset).unwrap();
        assert_eq!(stats.average_label, "88 bpm");
        assert_eq!(stats.samples_label, "1 readings");

        let trend = load_heart_trend(&conn, 0, offset).unwrap();
        assert_eq!(trend.points[0].average_bpm, Some(88.0));
    }

    #[test]
    fn daily_wellness_reflects_late_arriving_activity() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        let range = DateRange::day(jul4);

        insert_minute(&conn, local(jul4, 9, 0), 10);
        assert_eq!(fetch_daily_wellness(&conn, range).unwrap()[0].steps, Some(10));

        insert_minute(&conn, local(jul4, 12, 0), 5);
        assert_eq!(fetch_daily_wellness(&conn, range).unwrap()[0].steps, Some(15));
    }

    #[test]
    fn delta_pct_rounds_sanely() {
        assert_eq!(delta_pct(100, 0), "—");
        assert_eq!(delta_pct(150, 75), "+100%");
        assert_eq!(delta_pct(50, 100), "-50%");
        assert_eq!(delta_pct(100, 100), "+0%");
        assert_eq!(delta_pct(999, 1000), "+0%"); // -0.1% must not read "-0%"
        assert_eq!(delta_pct(199, 200), "-1%");  // -0.5% rounds away from zero
    }

    #[test]
    fn sleep_chart_day_view_end_to_end() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        insert_session(&conn, 1, local(d(2026, 7, 3), 23, 0), 8 * 3600);
        insert_session(&conn, 2, local(d(2026, 7, 3), 23, 30), 90 * 60);
        insert_session(&conn, 1, local(d(2026, 7, 2), 23, 0), 4 * 3600); // wakes Jul 3

        let offset = (watch_today() - jul4).num_days() as i32;
        let chart = load_sleep_chart(&conn, 0, offset).unwrap();
        assert_eq!(chart.bars.len(), 1);
        assert_eq!(chart.summary, "8h 0m · 1h 30m deep");
        assert_eq!(chart.avg_label, "8h 0m");
        assert_eq!(chart.delta_label, "+100%"); // 8h vs the 4h night before
        assert!(chart.delta_positive);
    }

    #[test]
    fn steps_chart_day_view_end_to_end() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        insert_minute(&conn, local(jul4, 9, 0), 100);
        insert_minute(&conn, local(jul4, 12, 0), 50);
        insert_minute(&conn, local(d(2026, 7, 3), 9, 0), 75);

        let offset = (watch_today() - jul4).num_days() as i32;
        let chart = load_steps_chart(&conn, 0, offset).unwrap();
        assert_eq!(chart.summary, "150 steps");
        assert_eq!(chart.bars.len(), 2);
        assert_eq!(chart.bars.iter().map(|b| b.steps_raw).sum::<i64>(), 150);
        assert_eq!(chart.delta_label, "+100%"); // 150 vs 75 the day before
    }

    /// Regression: the weekly delta must compare the per-night averages the
    /// header shows, not week totals — otherwise a week with a missing synced
    /// night reads as a big change even when the averages are identical.
    #[test]
    fn weekly_sleep_delta_compares_per_night_averages() {
        let conn = setup();
        // This week: a 7h night waking on every day of the range.
        // Last week: the same 7h nights, but one night missing.
        let this_week = range_for(1, 0);
        let last_week = range_for(1, 1);
        for wake in this_week.days() {
            insert_session(&conn, 1, local(wake, 6, 0) - 7 * 3600, 7 * 3600);
        }
        for wake in last_week.days().skip(1) {
            insert_session(&conn, 1, local(wake, 6, 0) - 7 * 3600, 7 * 3600);
        }

        let chart = load_sleep_chart(&conn, 1, 0).unwrap();
        assert_eq!(chart.avg_label, "AVG 7h 0m / night");
        assert_eq!(chart.delta_label, "+0%");
    }

    /// Regression: the month delta must compare average steps per elapsed
    /// day, not a partial month's total against a full previous month.
    #[test]
    fn monthly_steps_delta_compares_daily_averages() {
        let conn = setup();
        let today = watch_today();
        let this_month = range_for(2, 0);
        let last_month = range_for(2, 1);
        for day in this_month.days().filter(|d| *d <= today) {
            insert_minute(&conn, local(day, 12, 0), 1000);
        }
        for day in last_month.days() {
            insert_minute(&conn, local(day, 12, 0), 1000);
        }

        let chart = load_steps_chart(&conn, 2, 0).unwrap();
        assert_eq!(chart.summary, "avg 1,000 / day");
        assert_eq!(chart.delta_label, "+0%");
    }

    #[test]
    fn sleep_stats_stage_and_awake_percentages() {
        let conn = setup();
        let jul3 = d(2026, 7, 3);
        let jul4 = d(2026, 7, 4);
        // In bed 23:00–07:00 (8h) with a 1h awake gap: slept 7h, 2h deep.
        insert_session(&conn, 1, local(jul3, 23, 0), 3 * 3600); // 23:00–02:00
        insert_session(&conn, 1, local(jul4, 3, 0), 4 * 3600); // 03:00–07:00
        insert_session(&conn, 2, local(jul4, 0, 0), 2 * 3600); // deep 00:00–02:00

        let offset = (watch_today() - jul4).num_days() as i32;
        let stats = load_sleep_stats(&conn, 0, offset).unwrap();
        assert_eq!(stats.deep_avg_label, "2h 0m");
        // Shares of the 8h in bed: 5h light, 2h deep, 1h awake — the tied
        // .5 remainders resolve to light, and the three total exactly 100.
        assert_eq!(stats.light_pct, 63.0);
        assert_eq!(stats.deep_pct, 25.0);
        assert_eq!(stats.awake_pct, 12.0);
        assert_eq!(stats.avg_bedtime, "11:00 PM");
        assert_eq!(stats.avg_wakeup, "7:00 AM");
        assert_eq!(stats.highest_dur, "7h 0m");
        assert_eq!(stats.lowest_dur, "7h 0m");
    }

    #[test]
    fn sleep_stats_explicit_range_scopes_selected_bar() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        let jul5 = d(2026, 7, 5);

        insert_session(&conn, 1, local(d(2026, 7, 3), 23, 0), 8 * 3600);
        insert_session(&conn, 1, local(jul4, 22, 0), 6 * 3600);
        insert_session(&conn, 1, local(jul5, 21, 0), 10 * 3600);

        let selected_day = load_sleep_stats_for_range(&conn, DateRange::day(jul5)).unwrap();
        assert_eq!(selected_day.highest_dur, "6h 0m");
        assert_eq!(selected_day.lowest_dur, "6h 0m");
        assert_eq!(selected_day.avg_bedtime, "10:00 PM");
        assert_eq!(
            load_sleep_avg_label_for_range(&conn, DateRange::day(jul5)).unwrap(),
            "AVG 6h 0m / night"
        );

        // This represents a month-view bar whose week span covers Jul 4–5;
        // the neighboring night's data must not leak into its key stats.
        let selected_week = load_sleep_stats_for_range(
            &conn,
            DateRange {
                start: jul4,
                end: jul5,
            },
        )
        .unwrap();
        assert_eq!(selected_week.highest_dur, "8h 0m");
        assert_eq!(selected_week.lowest_dur, "6h 0m");
        assert_eq!(
            load_sleep_avg_label_for_range(
                &conn,
                DateRange {
                    start: jul4,
                    end: jul5,
                },
            )
            .unwrap(),
            "AVG 7h 0m / night"
        );
    }

    #[test]
    fn steps_avg_label_explicit_range_scopes_selected_bar() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        let jul5 = d(2026, 7, 5);
        insert_minute(&conn, local(jul4, 12, 0), 100);
        insert_minute(&conn, local(jul5, 12, 0), 300);

        assert_eq!(
            load_steps_avg_label_for_range(&conn, DateRange::day(jul5)).unwrap(),
            "avg 300 / day"
        );
        assert_eq!(
            load_steps_avg_label_for_range(
                &conn,
                DateRange {
                    start: jul4,
                    end: jul5,
                },
            )
            .unwrap(),
            "avg 200 / day"
        );
    }

    #[test]
    fn resting_hr_uses_the_lowest_sustained_window() {
        let samples = [
            (0, 80),
            (10 * 60, 79),
            (20 * 60, 81),
            (30 * 60, 80),
            (2 * 3600, 60),
            (2 * 3600 + 10 * 60, 59),
            (2 * 3600 + 20 * 60, 61),
            (2 * 3600 + 30 * 60, 60),
        ];

        assert_eq!(lowest_sustained_resting_hr(&samples), Some(60));
        // The older high sample remains inside the trailing hour, but the
        // four-reading suffix is independently sustained and lower.
        assert_eq!(
            lowest_sustained_resting_hr(&[
                (0, 100),
                (10 * 60, 60),
                (20 * 60, 60),
                (30 * 60, 60),
                (40 * 60, 60),
            ]),
            Some(60)
        );
        assert_eq!(
            lowest_sustained_resting_hr(&[(0, 50), (600, 80), (1200, 80), (1800, 80)]),
            Some(73)
        );
    }

    #[test]
    fn resting_hr_requires_sustained_sample_coverage() {
        assert_eq!(
            lowest_sustained_resting_hr(&[(0, 60), (600, 60), (1800, 60)]),
            None
        );
        assert_eq!(
            lowest_sustained_resting_hr(&[(0, 60), (600, 60), (1200, 60), (1799, 60)]),
            None
        );
        assert_eq!(
            lowest_sustained_resting_hr(&[(0, 60), (600, 60), (1200, 60), (3001, 60)]),
            None
        );
    }

    #[test]
    fn daily_resting_hr_uses_overnight_sleep_and_excludes_naps() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        let sleep_start = local(d(2026, 7, 3), 23, 0);
        let nap_start = local(jul4, 14, 0);
        insert_session(&conn, 1, sleep_start, 8 * 3600);
        insert_session(&conn, 3, nap_start, 3600);

        for (minute, bpm) in [(0, 60), (10, 59), (20, 61), (30, 60)] {
            insert_minute_at(
                &conn,
                sleep_start - TZ + minute * 60,
                TZ,
                0,
                Some(bpm),
            );
        }
        for (minute, bpm) in [(0, 40), (10, 40), (20, 40), (30, 40)] {
            insert_minute_at(
                &conn,
                nap_start - TZ + minute * 60,
                TZ,
                0,
                Some(bpm),
            );
        }

        let resting_hr = fetch_daily_resting_hr(&conn, DateRange::day(jul4)).unwrap();
        assert_eq!(resting_hr.get(&jul4), Some(&60));
        let wellness = fetch_daily_wellness(&conn, DateRange::day(jul4)).unwrap();
        assert_eq!(wellness[0].resting_hr, Some(60));
    }

    #[test]
    fn daily_resting_hr_batch_assigns_samples_to_each_wake_date() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        let jul5 = d(2026, 7, 5);
        let first_start = local(d(2026, 7, 3), 23, 0);
        let second_start = local(jul4, 23, 0);
        insert_session(&conn, 1, first_start, 8 * 3600);
        insert_session(&conn, 1, second_start, 8 * 3600);

        for (start, bpm) in [(first_start, 60), (second_start, 65)] {
            for minute in [0, 10, 20, 30] {
                insert_minute_at(
                    &conn,
                    start - TZ + minute * 60,
                    TZ,
                    0,
                    Some(bpm),
                );
            }
        }
        // These lower daytime readings fall inside the batched SQL bounds but
        // outside every merged overnight span and must not affect either day.
        let midday = local(jul4, 12, 0);
        for minute in [0, 10, 20, 30] {
            insert_minute_at(&conn, midday - TZ + minute * 60, TZ, 0, Some(40));
        }

        let resting_hr = fetch_daily_resting_hr(
            &conn,
            DateRange {
                start: jul4,
                end: jul5,
            },
        )
        .unwrap();
        assert_eq!(resting_hr.get(&jul4), Some(&60));
        assert_eq!(resting_hr.get(&jul5), Some(&65));
    }

    #[test]
    fn stage_percentages_always_total_100() {
        assert_eq!(stage_percentages(0, 0, 0), (0.0, 0.0, 0.0));
        assert_eq!(stage_percentages(9, 0, 0), (100.0, 0.0, 0.0));
        assert_eq!(stage_percentages(1, 1, 1), (34.0, 33.0, 33.0));
        // 62.5 / 25 / 12.5 would independently round to 63 + 25 + 13 = 101.
        assert_eq!(stage_percentages(5, 2, 1), (63.0, 25.0, 12.0));
        for (l, d, a) in [(7, 2, 1), (12345, 6789, 42), (1, 1, 998)] {
            let (lp, dp, ap) = stage_percentages(l, d, a);
            assert_eq!(lp + dp + ap, 100.0);
        }
    }

    /// A nap counts toward the night's totals but must not stretch the
    /// in-bed span or shift the wake-up time to the nap's end.
    #[test]
    fn nap_does_not_skew_wakeup_or_awake_stats() {
        let conn = setup();
        let jul4 = d(2026, 7, 4);
        insert_session(&conn, 1, local(d(2026, 7, 3), 23, 0), 8 * 3600); // wake 7am
        insert_session(&conn, 3, local(jul4, 14, 0), 3600); // 2pm–3pm nap

        let offset = (watch_today() - jul4).num_days() as i32;
        let stats = load_sleep_stats(&conn, 0, offset).unwrap();
        assert_eq!(stats.avg_wakeup, "7:00 AM"); // not 3:00 PM
        assert_eq!(stats.awake_pct, 0.0); // the 7h before the nap isn't "in bed"
        assert_eq!(stats.highest_dur, "9h 0m"); // totals still include the nap
    }

    /// Bedtimes straddling midnight must average on the clock circle:
    /// 11pm and 1am → midnight, not noon.
    #[test]
    fn average_bedtime_wraps_midnight() {
        let conn = setup();
        let today = watch_today();
        let yesterday = today - chrono::Duration::days(1);
        insert_session(&conn, 1, local(yesterday, 23, 0), 8 * 3600); // bed 11pm, wake 7am today
        insert_session(&conn, 1, local(yesterday, 1, 0), 6 * 3600); // bed 1am, wake 7am yesterday

        let stats = load_sleep_stats(&conn, 1, 0).unwrap();
        assert_eq!(stats.avg_bedtime, "12:00 AM");
        assert_eq!(stats.avg_wakeup, "7:00 AM");
    }

    #[test]
    fn sleep_labels_average_per_night_with_data() {
        let nights = vec![
            Night {
                wake_date: d(2026, 7, 3),
                phases: vec![NightPhase {
                    local_start: 0,
                    local_end: 6 * 3600,
                    utc_start: 0,
                    utc_end: 6 * 3600,
                    is_deep: false,
                    is_nap: false,
                }],
            },
            Night {
                wake_date: d(2026, 7, 4),
                phases: vec![
                    NightPhase {
                        local_start: 0,
                        local_end: 8 * 3600,
                        utc_start: 0,
                        utc_end: 8 * 3600,
                        is_deep: false,
                        is_nap: false,
                    },
                    NightPhase {
                        local_start: 0,
                        local_end: 2 * 3600,
                        utc_start: 0,
                        utc_end: 2 * 3600,
                        is_deep: true,
                        is_nap: false,
                    },
                ],
            },
        ];
        let (summary, avg) = sleep_labels(&nights, 1);
        assert_eq!(summary, "avg 7h 0m · 1h 0m deep");
        assert_eq!(avg, "AVG 7h 0m / night");

        let (summary, avg) = sleep_labels(&nights[1..], 0);
        assert_eq!(summary, "8h 0m · 2h 0m deep");
        assert_eq!(avg, "8h 0m");
    }
}
