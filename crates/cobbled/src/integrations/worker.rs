//! Background wellness reconciliation worker.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use chrono::NaiveDate;
use cobble_config::IntervalsIcuConfig;
use cobble_db::{AppDb, DateRange, WellnessExportState};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

const RECONCILIATION_INTERVAL: Duration = Duration::from_secs(60 * 60);
const WELLNESS_BATCH_SIZE: usize = 90;
const MAX_UPLOAD_ATTEMPTS: u32 = 5;
const BASE_RETRY_DELAY: Duration = Duration::from_secs(30);
const MAX_BACKOFF_DELAY: Duration = Duration::from_secs(60 * 60);
const PROVIDER: &str = "intervals_icu";

/// Reasons that cause the serialized exporter loop to wake.
#[derive(Debug, Clone, Copy)]
pub(crate) enum WellnessWake {
    Startup,
    HealthData,
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
    },
}

/// Run one exporter task for the lifetime of the daemon.
///
pub(crate) async fn run(
    db: Arc<Mutex<AppDb>>,
    mut integration_rx: watch::Receiver<IntervalsIcuConfig>,
    mut wake_rx: mpsc::UnboundedReceiver<WellnessWake>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut config = integration_rx.borrow().clone();
    let mut authentication_blocked_config = None;
    reconcile_if_allowed(
        &db,
        &config,
        WellnessWake::Startup,
        &mut authentication_blocked_config,
    )
    .await;

    let mut timer = tokio::time::interval(RECONCILIATION_INTERVAL);
    timer.tick().await; // startup wake already covers the immediate run

    loop {
        tokio::select! {
            changed = integration_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                config = integration_rx.borrow().clone();
                authentication_blocked_config = None;
                reconcile_if_allowed(
                    &db,
                    &config,
                    WellnessWake::ConfigChanged,
                    &mut authentication_blocked_config,
                )
                .await;
            }
            Some(reason) = wake_rx.recv() => {
                reconcile_if_allowed(
                    &db,
                    &config,
                    reason,
                    &mut authentication_blocked_config,
                )
                .await;
            }
            _ = timer.tick() => {
                reconcile_if_allowed(
                    &db,
                    &config,
                    WellnessWake::Timer,
                    &mut authentication_blocked_config,
                )
                .await;
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }

    debug!("wellness exporter stopped");
}

async fn reconcile_if_allowed(
    db: &Arc<Mutex<AppDb>>,
    config: &IntervalsIcuConfig,
    reason: WellnessWake,
    authentication_blocked_config: &mut Option<IntervalsIcuConfig>,
) {
    if authentication_blocked_config
        .as_ref()
        .is_some_and(|blocked| blocked == config)
    {
        debug!(
            ?reason,
            "wellness exporter paused after authentication failure"
        );
        return;
    }

    if matches!(
        reconcile(db, config, reason).await,
        ReconcileOutcome::AuthenticationFailure
    ) {
        *authentication_blocked_config = Some(config.clone());
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

    for (record_chunk, payload_chunk) in records
        .chunks(WELLNESS_BATCH_SIZE)
        .zip(payloads.chunks(WELLNESS_BATCH_SIZE))
    {
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
            } => {
                warn!(
                    provider = PROVIDER,
                    athlete_id = %config.athlete_id,
                    record_count = record_chunk.len(),
                    error = %error,
                    "wellness upload failed"
                );
                record_failure(db, config, payload_chunk, next_attempt_at, &error).await;
                if authentication_failure {
                    return ReconcileOutcome::AuthenticationFailure;
                }
                return ReconcileOutcome::Complete;
            }
        }
    }

    ReconcileOutcome::Complete
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
    payloads: &[(NaiveDate, String)],
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
    payloads: &[(NaiveDate, String)],
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
