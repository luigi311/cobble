//! Background wellness reconciliation worker.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use chrono::NaiveDate;
use cobble_config::IntervalsIcuConfig;
use cobble_db::{AppDb, DateRange, WellnessExportState};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

const RECONCILIATION_INTERVAL: Duration = Duration::from_secs(60 * 60);
const WELLNESS_BATCH_SIZE: usize = 90;
const MAX_UPLOAD_ATTEMPTS: u32 = 5;
const BASE_RETRY_DELAY: Duration = Duration::from_secs(30);
const MAX_BACKOFF_DELAY: Duration = Duration::from_secs(60 * 60);
const MAX_INLINE_RETRY_DELAY: Duration = Duration::from_secs(5 * 60);
/// Bound the extra requests used to isolate records rejected with HTTP 400/422.
/// Each split creates two additional requests, so this caps amplification at
/// 32 requests per reconciliation while still allowing a few bad dates to be
/// isolated from an otherwise valid 90-record batch.
const MAX_PAYLOAD_ISOLATION_SPLITS: usize = 16;
const PROVIDER: &str = "intervals_icu";

type PayloadHash = (NaiveDate, String);
type PendingBatch<'a> = (
    &'a [super::intervals_icu::WellnessRecord],
    &'a [PayloadHash],
);

/// Reasons that cause the serialized exporter loop to wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WellnessWake {
    Startup,
    HealthData,
    Manual,
    ConfigChanged,
    Timer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileOutcome {
    Complete,
    AuthenticationFailure,
}

#[derive(Debug)]
enum BatchOutcome {
    Success,
    Failure {
        error: String,
        next_attempt_at: Option<i64>,
        authentication_failure: bool,
        payload_failure: bool,
    },
}

/// Run one exporter task for the lifetime of the daemon.
///
pub(crate) async fn run(
    db: Arc<Mutex<AppDb>>,
    mut integration_rx: watch::Receiver<IntervalsIcuConfig>,
    mut health_rx: watch::Receiver<u64>,
    mut manual_sync_rx: watch::Receiver<u64>,
    mut shutdown_rx: watch::Receiver<bool>,
    running: Arc<AtomicBool>,
) {
    let _running_reset = RunningReset(running.clone());
    let mut config = integration_rx.borrow().clone();
    let mut authentication_blocked_config = None;
    let mut active = start_reconciliation(
        &db,
        &config,
        WellnessWake::Startup,
        &authentication_blocked_config,
        &running,
    );
    let mut pending_wake = None;
    let mut health_rx_open = true;
    let mut manual_sync_rx_open = true;

    let mut timer = tokio::time::interval(RECONCILIATION_INTERVAL);
    timer.tick().await; // startup wake already covers the immediate run

    loop {
        tokio::select! {
            result = async {
                active
                    .as_mut()
                    .expect("active wellness reconciliation task")
                    .await
            }, if active.is_some() => {
                active = None;
                match result {
                    Ok(ReconcileOutcome::AuthenticationFailure) => {
                        authentication_blocked_config = Some(config.clone());
                    }
                    Ok(ReconcileOutcome::Complete) => {}
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        warn!("wellness reconciliation task failed: {error}");
                    }
                }
                if let Some(reason) = pending_wake.take() {
                    active = start_reconciliation(
                        &db,
                        &config,
                        reason,
                        &authentication_blocked_config,
                        &running,
                    );
                } else {
                    running.store(false, Ordering::SeqCst);
                }
            }
            changed = integration_rx.changed() => {
                if changed.is_err() {
                    cancel_reconciliation(&mut active).await;
                    break;
                }
                cancel_reconciliation(&mut active).await;
                config = integration_rx.borrow().clone();
                authentication_blocked_config = None;
                pending_wake = None;
                active = start_reconciliation(
                    &db,
                    &config,
                    WellnessWake::ConfigChanged,
                    &authentication_blocked_config,
                    &running,
                );
            }
            changed = health_rx.changed(), if health_rx_open => {
                if changed.is_err() {
                    health_rx_open = false;
                } else if let Some(reason) = queue_or_start_wake(
                    active.is_some(),
                    &mut pending_wake,
                    WellnessWake::HealthData,
                ) {
                    active = start_reconciliation(
                        &db,
                        &config,
                        reason,
                        &authentication_blocked_config,
                        &running,
                    );
                }
            }
            changed = manual_sync_rx.changed(), if manual_sync_rx_open => {
                if changed.is_err() {
                    manual_sync_rx_open = false;
                } else if let Some(reason) = queue_or_start_wake(
                    active.is_some(),
                    &mut pending_wake,
                    WellnessWake::Manual,
                ) {
                    active = start_reconciliation(
                        &db,
                        &config,
                        reason,
                        &authentication_blocked_config,
                        &running,
                    );
                }
            }
            _ = timer.tick() => {
                if let Some(reason) = queue_or_start_wake(
                    active.is_some(),
                    &mut pending_wake,
                    WellnessWake::Timer,
                ) {
                    active = start_reconciliation(
                        &db,
                        &config,
                        reason,
                        &authentication_blocked_config,
                        &running,
                    );
                }
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    cancel_reconciliation(&mut active).await;
                    break;
                }
            }
        }
    }

    running.store(false, Ordering::SeqCst);
    debug!("wellness exporter stopped");
}

/// Return a wake that should start immediately, or retain at most one wake for
/// the end of the active reconciliation. All wake reasons perform the same
/// full hash reconciliation, so preserving the first one loses no work.
fn queue_or_start_wake(
    active: bool,
    pending_wake: &mut Option<WellnessWake>,
    reason: WellnessWake,
) -> Option<WellnessWake> {
    if active {
        pending_wake.get_or_insert(reason);
        None
    } else {
        Some(reason)
    }
}

fn start_reconciliation(
    db: &Arc<Mutex<AppDb>>,
    config: &IntervalsIcuConfig,
    reason: WellnessWake,
    authentication_blocked_config: &Option<IntervalsIcuConfig>,
    running: &Arc<AtomicBool>,
) -> Option<JoinHandle<ReconcileOutcome>> {
    if authentication_blocked_config
        .as_ref()
        .is_some_and(|blocked| blocked == config)
    {
        debug!(
            ?reason,
            "wellness exporter paused after authentication failure"
        );
        running.store(false, Ordering::SeqCst);
        return None;
    }

    let db = db.clone();
    let config = config.clone();
    running.store(true, Ordering::SeqCst);
    Some(tokio::spawn(async move {
        reconcile(&db, &config, reason).await
    }))
}

struct RunningReset(Arc<AtomicBool>);

impl Drop for RunningReset {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

async fn cancel_reconciliation(active: &mut Option<JoinHandle<ReconcileOutcome>>) {
    let Some(task) = active.take() else {
        return;
    };
    task.abort();
    if let Err(error) = task.await {
        if !error.is_cancelled() {
            warn!("wellness reconciliation cancellation failed: {error}");
        }
    }
}

async fn reconcile(
    db: &Arc<Mutex<AppDb>>,
    config: &IntervalsIcuConfig,
    reason: WellnessWake,
) -> ReconcileOutcome {
    if !config.enabled {
        debug!(?reason, "wellness exporter wake ignored while disabled");
        return ReconcileOutcome::Complete;
    }
    if let Err(error) = config.validate() {
        warn!(?reason, "wellness exporter wake ignored: {error}");
        return ReconcileOutcome::Complete;
    }

    let provider = PROVIDER.to_string();
    let account_id = config.athlete_id.clone();
    let db_for_load = db.clone();
    let loaded = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let db = db_for_load.lock().unwrap();
        let (Some(start), Some(end)) = (db.oldest_wellness_date()?, db.newest_wellness_date()?)
        else {
            return Ok((Vec::new(), Vec::new()));
        };
        let range = DateRange { start, end };
        let wellness = db.fetch_daily_wellness(range)?;
        let states = db.fetch_wellness_export_states(&provider, &account_id)?;
        Ok((wellness, states))
    })
    .await
    .context("load wellness reconciliation data")
    .and_then(|result| result);

    let (wellness, states) = match loaded {
        Ok(data) => data,
        Err(error) => {
            warn!(?reason, "wellness reconciliation load failed: {error}");
            return ReconcileOutcome::Complete;
        }
    };
    if wellness.is_empty() {
        debug!(?reason, "wellness reconciliation found no local data");
        return ReconcileOutcome::Complete;
    }

    let successful_hashes: HashMap<String, WellnessExportState> = states
        .into_iter()
        .map(|state| (state.wellness_date.format("%Y-%m-%d").to_string(), state))
        .collect();
    let mut records = Vec::new();
    let mut payloads = Vec::new();
    let now = unix_now();
    for daily in wellness {
        let record = super::intervals_icu::WellnessRecord::from(&daily);
        let hash = match record.payload_hash() {
            Ok(hash) => hash,
            Err(error) => {
                warn!(date = %record.id, "wellness payload hash failed: {error}");
                continue;
            }
        };
        let state = successful_hashes.get(&record.id);
        if let Some(state) = state {
            let retry_not_due = state
                .next_attempt_at
                .is_some_and(|next_attempt_at| next_attempt_at > now);
            if retry_not_due && !matches!(reason, WellnessWake::ConfigChanged) {
                continue;
            }
            if state.payload_hash.as_deref() == Some(hash.as_str()) && !retry_not_due {
                continue;
            }
        }
        payloads.push((daily.date, hash));
        records.push(record);
    }

    if records.is_empty() {
        debug!(?reason, athlete_id = %config.athlete_id, "wellness reconciliation is already current");
        return ReconcileOutcome::Complete;
    }

    let client = match super::intervals_icu::IntervalsIcuClient::new() {
        Ok(client) => client,
        Err(error) => {
            warn!("could not create wellness client: {error}");
            record_failure(
                db,
                config,
                &payloads,
                Some(retry_at(RECONCILIATION_INTERVAL)),
                "wellness client initialization failed",
            )
            .await;
            return ReconcileOutcome::Complete;
        }
    };

    let mut pending_batches = VecDeque::new();
    for batch in records
        .chunks(WELLNESS_BATCH_SIZE)
        .zip(payloads.chunks(WELLNESS_BATCH_SIZE))
    {
        pending_batches.push_back(batch);
    }
    let mut remaining_payload_splits = MAX_PAYLOAD_ISOLATION_SPLITS;

    while let Some((record_chunk, payload_chunk)) = pending_batches.pop_front() {
        let start_date = record_chunk
            .first()
            .map(|record| record.id.as_str())
            .unwrap_or("?");
        let end_date = record_chunk
            .last()
            .map(|record| record.id.as_str())
            .unwrap_or("?");
        info!(
            provider = PROVIDER,
            athlete_id = %config.athlete_id,
            start_date,
            end_date,
            record_count = record_chunk.len(),
            ?reason,
            "uploading wellness batch"
        );

        match upload_batch(&client, config, record_chunk).await {
            BatchOutcome::Success => {
                if let Err(error) = record_success(db, config, payload_chunk).await {
                    warn!("recording wellness upload success failed: {error}");
                    return ReconcileOutcome::Complete;
                }
                info!(
                    provider = PROVIDER,
                    athlete_id = %config.athlete_id,
                    record_count = record_chunk.len(),
                    "wellness upload succeeded"
                );
            }
            BatchOutcome::Failure {
                error,
                next_attempt_at,
                authentication_failure,
                payload_failure,
            } => {
                warn!(
                    provider = PROVIDER,
                    athlete_id = %config.athlete_id,
                    record_count = record_chunk.len(),
                    error = %error,
                    "wellness upload failed"
                );
                if authentication_failure {
                    record_failure(db, config, payload_chunk, next_attempt_at, &error).await;
                    return ReconcileOutcome::AuthenticationFailure;
                }
                if payload_failure
                    && let Some((left, right)) = split_rejected_batch(
                        record_chunk,
                        payload_chunk,
                        &mut remaining_payload_splits,
                    )
                {
                    warn!(
                        provider = PROVIDER,
                        athlete_id = %config.athlete_id,
                        record_count = record_chunk.len(),
                        left_count = left.0.len(),
                        right_count = right.0.len(),
                        remaining_splits = remaining_payload_splits,
                        "splitting rejected wellness batch"
                    );
                    pending_batches.push_front(right);
                    pending_batches.push_front(left);
                    continue;
                }

                if payload_failure && record_chunk.len() > 1 {
                    warn!(
                        provider = PROVIDER,
                        athlete_id = %config.athlete_id,
                        record_count = record_chunk.len(),
                        "wellness payload-isolation budget exhausted"
                    );
                }
                record_failure(db, config, payload_chunk, next_attempt_at, &error).await;
                if payload_failure {
                    continue;
                }
                return ReconcileOutcome::Complete;
            }
        }
    }

    ReconcileOutcome::Complete
}

fn split_rejected_batch<'a>(
    records: &'a [super::intervals_icu::WellnessRecord],
    payloads: &'a [PayloadHash],
    remaining_splits: &mut usize,
) -> Option<(PendingBatch<'a>, PendingBatch<'a>)> {
    debug_assert_eq!(records.len(), payloads.len());
    if records.len() <= 1 || *remaining_splits == 0 {
        return None;
    }

    *remaining_splits -= 1;
    let midpoint = records.len() / 2;
    let (record_left, record_right) = records.split_at(midpoint);
    let (payload_left, payload_right) = payloads.split_at(midpoint);
    Some(((record_left, payload_left), (record_right, payload_right)))
}

async fn upload_batch(
    client: &super::intervals_icu::IntervalsIcuClient,
    config: &IntervalsIcuConfig,
    records: &[super::intervals_icu::WellnessRecord],
) -> BatchOutcome {
    for attempt in 0..MAX_UPLOAD_ATTEMPTS {
        let response = match client.send_wellness(config, records).await {
            Ok(response) => response,
            Err(error) => {
                let delay = backoff_delay(attempt);
                if attempt + 1 == MAX_UPLOAD_ATTEMPTS {
                    return BatchOutcome::Failure {
                        error: "wellness network request failed".to_string(),
                        next_attempt_at: Some(retry_at(delay)),
                        authentication_failure: false,
                        payload_failure: false,
                    };
                }
                if delay > MAX_INLINE_RETRY_DELAY {
                    return BatchOutcome::Failure {
                        error: "wellness network request failed".to_string(),
                        next_attempt_at: Some(retry_at(delay)),
                        authentication_failure: false,
                        payload_failure: false,
                    };
                }
                warn!(
                    attempt = attempt + 1,
                    delay_secs = delay.as_secs_f64(),
                    error = %error,
                    "retrying wellness network request"
                );
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        let classification = super::intervals_icu::classify_response(&response);
        if classification.class == super::intervals_icu::ResponseClass::Success {
            return BatchOutcome::Success;
        }

        if !classification.should_retry() {
            return BatchOutcome::Failure {
                error: classification.summary(),
                next_attempt_at: if classification.class
                    == super::intervals_icu::ResponseClass::Authentication
                {
                    None
                } else {
                    Some(retry_at(RECONCILIATION_INTERVAL))
                },
                authentication_failure: classification.class
                    == super::intervals_icu::ResponseClass::Authentication,
                payload_failure: matches!(classification.status, 400 | 422),
            };
        }

        let delay = classification
            .retry_after
            .unwrap_or_else(|| backoff_delay(attempt));
        if attempt + 1 == MAX_UPLOAD_ATTEMPTS {
            return BatchOutcome::Failure {
                error: classification.summary(),
                next_attempt_at: Some(retry_at(delay)),
                authentication_failure: false,
                payload_failure: false,
            };
        }
        if delay > MAX_INLINE_RETRY_DELAY {
            return BatchOutcome::Failure {
                error: classification.summary(),
                next_attempt_at: Some(retry_at(delay)),
                authentication_failure: false,
                payload_failure: false,
            };
        }
        warn!(
            attempt = attempt + 1,
            status = classification.status,
            delay_secs = delay.as_secs_f64(),
            "retrying wellness HTTP response"
        );
        tokio::time::sleep(delay).await;
    }

    unreachable!("upload attempts always return a result")
}

async fn record_success(
    db: &Arc<Mutex<AppDb>>,
    config: &IntervalsIcuConfig,
    payloads: &[PayloadHash],
) -> anyhow::Result<()> {
    let db = db.clone();
    let provider = PROVIDER.to_string();
    let account_id = config.athlete_id.clone();
    let payloads = payloads.to_vec();
    let completed_at = unix_now();
    tokio::task::spawn_blocking(move || {
        db.lock().unwrap().record_wellness_export_success(
            &provider,
            &account_id,
            &payloads,
            completed_at,
        )
    })
    .await
    .context("record wellness upload success")?
}

async fn record_failure(
    db: &Arc<Mutex<AppDb>>,
    config: &IntervalsIcuConfig,
    payloads: &[PayloadHash],
    next_attempt_at: Option<i64>,
    error: &str,
) {
    let dates: Vec<_> = payloads.iter().map(|(date, _)| *date).collect();
    let db = db.clone();
    let provider = PROVIDER.to_string();
    let account_id = config.athlete_id.clone();
    let attempted_at = unix_now();
    let error = error.to_string();
    if let Err(db_error) = tokio::task::spawn_blocking(move || {
        db.lock().unwrap().record_wellness_export_failure(
            &provider,
            &account_id,
            &dates,
            attempted_at,
            next_attempt_at,
            &error,
        )
    })
    .await
    .context("record wellness upload failure")
    .and_then(|result| result)
    {
        warn!("recording wellness upload failure failed: {db_error}");
    }
}

fn backoff_delay(attempt: u32) -> Duration {
    let exponent = attempt.min(6);
    let multiplier = 1_u64 << exponent;
    let base_secs = BASE_RETRY_DELAY
        .as_secs()
        .saturating_mul(multiplier)
        .min(MAX_BACKOFF_DELAY.as_secs());
    let jitter_millis = fastrand::u64(0..1001);
    Duration::from_secs(base_secs) + Duration::from_millis(jitter_millis)
}

fn retry_at(delay: Duration) -> i64 {
    unix_now().saturating_add(delay.as_secs().min(i64::MAX as u64) as i64)
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_wakes_are_coalesced_without_overwriting_the_pending_run() {
        let mut pending = None;

        assert_eq!(
            queue_or_start_wake(true, &mut pending, WellnessWake::HealthData),
            None
        );
        assert_eq!(
            queue_or_start_wake(true, &mut pending, WellnessWake::Timer),
            None
        );
        assert_eq!(pending, Some(WellnessWake::HealthData));

        assert_eq!(
            queue_or_start_wake(false, &mut None, WellnessWake::Timer),
            Some(WellnessWake::Timer)
        );
    }

    #[tokio::test]
    async fn cancellation_aborts_and_joins_the_active_reconciliation() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_task = dropped.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _dropped = Dropped(dropped_in_task);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
            ReconcileOutcome::Complete
        });
        started_rx.await.unwrap();

        let mut active = Some(task);
        cancel_reconciliation(&mut active).await;

        assert!(active.is_none());
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn dropping_the_worker_resets_running_state() {
        let running = Arc::new(AtomicBool::new(true));
        drop(RunningReset(running.clone()));
        assert!(!running.load(Ordering::SeqCst));
    }

    #[test]
    fn rejected_batch_isolation_is_bounded_and_keeps_later_batches() {
        let date = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let records: Vec<_> = (0..180)
            .map(|day| super::super::intervals_icu::WellnessRecord {
                id: format!("record-{day}"),
                steps: Some(day),
                sleep_secs: None,
                avg_sleeping_hr: None,
            })
            .collect();
        let payloads: Vec<_> = records
            .iter()
            .map(|record| (date, record.id.clone()))
            .collect();
        let mut pending = VecDeque::new();
        for batch in records
            .chunks(WELLNESS_BATCH_SIZE)
            .zip(payloads.chunks(WELLNESS_BATCH_SIZE))
        {
            pending.push_back(batch);
        }
        let mut remaining_splits = MAX_PAYLOAD_ISOLATION_SPLITS;
        let mut request_count = 0;
        let mut terminal_record_count = 0;

        // Model a global 400/422 response for every attempted batch.
        while let Some((record_chunk, payload_chunk)) = pending.pop_front() {
            request_count += 1;
            if let Some((left, right)) =
                split_rejected_batch(record_chunk, payload_chunk, &mut remaining_splits)
            {
                pending.push_front(right);
                pending.push_front(left);
            } else {
                terminal_record_count += record_chunk.len();
            }
        }

        assert_eq!(remaining_splits, 0);
        assert_eq!(
            request_count,
            records.len() / WELLNESS_BATCH_SIZE + 2 * MAX_PAYLOAD_ISOLATION_SPLITS
        );
        assert_eq!(terminal_record_count, records.len());
    }
}
