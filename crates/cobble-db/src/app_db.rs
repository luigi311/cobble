use std::collections::HashMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use anyhow::Context;
use chrono::NaiveDate;
use libpebble_ble::endpoints::datalog::tag as datalog_tag;
use libpebble_ble::DatalogData;
use rusqlite::{Connection, params};
use tracing::{debug, warn};

use crate::schema;
use crate::time::DateRange;
use crate::types::{
    DailyWellness, IpLocation, WellnessExportState, WellnessExportStatus,
};

// Pebble firmware version constants (from RecordVersion enum in dataloggingendpoint.cpp).
const VERSION_FW_3_10_AND_BELOW: u16 = 5;
const VERSION_FW_3_11: u16 = 6;
const VERSION_FW_4_0: u16 = 7;
const VERSION_FW_4_1: u16 = 8;
const VERSION_FW_4_3: u16 = 13;

pub struct AppDb {
    conn: Connection,
}

struct RawRecord {
    id: i64,
    data: Vec<u8>,
    item_size: usize,
}

impl AppDb {
    /// Open the database with full write capability. Creates the directory,
    /// sets file permissions, applies PRAGMAs, and initializes the schema
    /// (tables + views). Safe to call alongside a read-only consumer because
    /// `CREATE TABLE IF NOT EXISTS` is idempotent.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create DB directory {}", parent.display()))?;
            #[cfg(unix)]
            if let Err(e) = std::fs::set_permissions(
                parent,
                std::fs::Permissions::from_mode(0o700),
            ) {
                warn!("could not set permissions on {}: {e}", parent.display());
            }
        }
        let conn = Connection::open(path)
            .with_context(|| format!("open app DB at {}", path.display()))?;
        #[cfg(unix)]
        if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            warn!("could not set permissions on {}: {e}", path.display());
        }

        schema::apply_pragmas(&conn)?;
        schema::initialize_schema(&conn)?;

        Ok(Self { conn })
    }

    /// Aggregate supported wellness observations for a watch-local date range.
    pub fn fetch_daily_wellness(&self, range: DateRange) -> anyhow::Result<Vec<DailyWellness>> {
        crate::queries::fetch_daily_wellness(&self.conn, range)
    }

    /// Return the oldest watch-local date with steps or primary sleep/nap data.
    pub fn oldest_wellness_date(&self) -> anyhow::Result<Option<chrono::NaiveDate>> {
        crate::queries::oldest_wellness_date(&self.conn)
    }

    /// Return the newest watch-local date with steps or primary sleep/nap data.
    pub fn newest_wellness_date(&self) -> anyhow::Result<Option<chrono::NaiveDate>> {
        crate::queries::newest_wellness_date(&self.conn)
    }

    /// Return all durable export state for one provider/account pair.
    pub fn fetch_wellness_export_states(
        &self,
        provider: &str,
        account_id: &str,
    ) -> anyhow::Result<Vec<WellnessExportState>> {
        let mut stmt = self.conn.prepare(
            "SELECT wellness_date, payload_hash, attempt_count, next_attempt_at,
                    last_attempt_at, last_success_at, last_error
             FROM wellness_export_state
             WHERE provider = ?1 AND account_id = ?2
             ORDER BY wellness_date ASC",
        )?;
        let rows = stmt.query_map(params![provider, account_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })?;

        let mut states = Vec::new();
        for row in rows {
            let (
                wellness_date,
                payload_hash,
                attempt_count,
                next_attempt_at,
                last_attempt_at,
                last_success_at,
                last_error,
            ) = row?;
            let wellness_date = NaiveDate::parse_from_str(&wellness_date, "%Y-%m-%d")
                .with_context(|| format!("parse wellness export date {wellness_date}"))?;
            states.push(WellnessExportState {
                provider: provider.to_string(),
                account_id: account_id.to_string(),
                wellness_date,
                payload_hash,
                attempt_count,
                next_attempt_at,
                last_attempt_at,
                last_success_at,
                last_error,
            });
        }
        Ok(states)
    }

    /// Return an aggregate status snapshot for one provider/account pair.
    /// Error text is already sanitized when it enters the ledger.
    pub fn fetch_wellness_export_status(
        &self,
        provider: &str,
        account_id: &str,
        current_payloads: &[(NaiveDate, String)],
    ) -> anyhow::Result<WellnessExportStatus> {
        let states = self.fetch_wellness_export_states(provider, account_id)?;
        let successful_hashes: HashMap<NaiveDate, &str> = states
            .iter()
            .filter_map(|state| {
                state
                    .payload_hash
                    .as_deref()
                    .map(|hash| (state.wellness_date, hash))
            })
            .collect();
        let exported_dates = current_payloads
            .iter()
            .filter(|(date, hash)| successful_hashes.get(date) == Some(&hash.as_str()))
            .count() as i64;
        let pending_dates = current_payloads.len() as i64 - exported_dates;
        let last_success_at = states.iter().filter_map(|state| state.last_success_at).max();
        let latest_error = states
            .iter()
            .filter_map(|state| {
                state.last_error.as_ref().map(|error| {
                    (
                        state.last_attempt_at.unwrap_or(i64::MIN),
                        error.clone(),
                        state.last_attempt_at,
                    )
                })
            })
            .max_by_key(|(attempted_at, _, _)| *attempted_at);
        let (last_error, last_error_at) = latest_error
            .map(|(_, error, attempted_at)| (Some(error), attempted_at))
            .unwrap_or((None, None));
        Ok(WellnessExportStatus {
            exported_dates,
            pending_dates,
            last_success_at,
            last_error,
            last_error_at,
        })
    }

    /// Record one successfully uploaded batch atomically.
    ///
    /// The hashes are the exact per-date payload hashes sent in the batch.
    /// Success resets the retry counter and clears prior failure state.
    pub fn record_wellness_export_success(
        &self,
        provider: &str,
        account_id: &str,
        payloads: &[(NaiveDate, String)],
        completed_at: i64,
    ) -> anyhow::Result<()> {
        let txn = self.conn.unchecked_transaction()?;
        for (date, payload_hash) in payloads {
            txn.execute(
                "INSERT INTO wellness_export_state
                     (provider, account_id, wellness_date, payload_hash,
                      attempt_count, next_attempt_at, last_attempt_at,
                      last_success_at, last_error)
                 VALUES (?1, ?2, ?3, ?4, 0, NULL, ?5, ?5, NULL)
                 ON CONFLICT(provider, account_id, wellness_date) DO UPDATE SET
                     payload_hash = excluded.payload_hash,
                     attempt_count = 0,
                     next_attempt_at = NULL,
                     last_attempt_at = excluded.last_attempt_at,
                     last_success_at = excluded.last_success_at,
                     last_error = NULL",
                params![
                    provider,
                    account_id,
                    date.format("%Y-%m-%d").to_string(),
                    payload_hash,
                    completed_at,
                ],
            )?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Mark every date in a failed batch for the next retry in one transaction.
    /// The last successful hash is intentionally preserved until a whole batch
    /// succeeds, so a partial response cannot claim individual success.
    pub fn record_wellness_export_failure(
        &self,
        provider: &str,
        account_id: &str,
        dates: &[NaiveDate],
        attempted_at: i64,
        next_attempt_at: Option<i64>,
        error_summary: &str,
    ) -> anyhow::Result<()> {
        let error_summary = sanitize_export_error(error_summary);
        let txn = self.conn.unchecked_transaction()?;
        for date in dates {
            txn.execute(
                "INSERT INTO wellness_export_state
                     (provider, account_id, wellness_date, payload_hash,
                      attempt_count, next_attempt_at, last_attempt_at,
                      last_success_at, last_error)
                 VALUES (?1, ?2, ?3, NULL, 1, ?4, ?5, NULL, ?6)
                 ON CONFLICT(provider, account_id, wellness_date) DO UPDATE SET
                     attempt_count = wellness_export_state.attempt_count + 1,
                     next_attempt_at = excluded.next_attempt_at,
                     last_attempt_at = excluded.last_attempt_at,
                     last_error = excluded.last_error",
                params![
                    provider,
                    account_id,
                    date.format("%Y-%m-%d").to_string(),
                    next_attempt_at,
                    attempted_at,
                    &error_summary,
                ],
            )?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Insert a raw batch into health_records and parse individual records into the
    /// per-tag tables. Returns whether the batch was new and supported.
    pub fn insert_batch(&self, batch: &DatalogData) -> anyhow::Result<bool> {
        let received_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let rows_changed = self.conn.execute(
            "INSERT OR IGNORE INTO health_records
                 (tag, app_uuid, session_ts, item_type, item_size, crc, data, received_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                batch.tag as i64,
                batch.app_uuid.as_slice(),
                batch.session_timestamp as i64,
                batch.item_type as i64,
                batch.item_size as i64,
                batch.crc as i64,
                &batch.data,
                received_at,
            ],
        )?;

        if rows_changed == 0 {
            // Duplicate batch; child records already stored on the first receipt.
            return Ok(false);
        }

        let record_id = self.conn.last_insert_rowid();
        let item_size = batch.item_size as usize;

        match batch.tag {
            datalog_tag::ACTIVITY_STEPS => {
                self.insert_activity_minutes(record_id, &batch.data, item_size)
            }
            // Tags 83 (SLEEP) and 84 (ACTIVITY_SESSIONS) both use overlay format.
            // Tag 83 carries only sleep-type sessions; duplicates are silently ignored.
            datalog_tag::SLEEP | datalog_tag::ACTIVITY_SESSIONS => {
                self.insert_activity_sessions(record_id, &batch.data, item_size)
            }
            // tag 85 (HR) is protobuf — skip until schema is known.
            // tag 87 is device/firmware summary — not health data.
            _ => return Ok(false),
        }?;

        Ok(true)
    }

    /// Parse tag 81 per-minute activity chunks.
    fn insert_activity_minutes(
        &self,
        record_id: i64,
        data: &[u8],
        item_size: usize,
    ) -> anyhow::Result<()> {
        const CHUNK_HEADER: usize = 9; // u16 ver + u32 ts + i8 utc_off + u8 rec_len + u8 rec_num

        if item_size < CHUNK_HEADER {
            warn!("activity item_size={item_size} too small; skipping");
            return Ok(());
        }
        if data.is_empty() || !data.len().is_multiple_of(item_size) {
            return Ok(());
        }

        let mut stmt = self.conn.prepare_cached(
            "INSERT OR IGNORE INTO health_activity_minutes
                 (health_record_id, record_version, start_ts, utc_offset, steps, orientation,
                  vmc, light, flags, resting_gram_calories, active_gram_calories, distance_cm,
                  heart_rate_bpm, heart_rate_weight, heart_rate_zone, raw)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        )?;

        for item in data.chunks_exact(item_size) {
            let record_version = u16::from_le_bytes([item[0], item[1]]);
            let mut ts = u32::from_le_bytes([item[2], item[3], item[4], item[5]]) as i64;
            let utc_offset = (item[6] as i8) as i64 * 900;
            let record_length = item[7] as usize;
            let record_num = item[8] as usize;

            if record_length == 0 {
                continue;
            }

            let sub_data = &item[CHUNK_HEADER..];
            let count = (sub_data.len() / record_length).min(record_num);

            for i in 0..count {
                let rec = &sub_data[i * record_length..(i + 1) * record_length];
                let start_ts = ts;
                ts += 60;

                // Minimum: steps(1) + orientation(1) + vmc(2) + light(1) = 5 bytes
                if rec.len() < 5 {
                    continue;
                }

                let steps = rec[0] as i64;
                let orientation = rec[1] as i64;
                let vmc = u16::from_le_bytes([rec[2], rec[3]]) as i64;
                let light = rec[4] as i64;
                let mut off = 5usize;

                let flags: Option<i64> =
                    if record_version >= VERSION_FW_3_10_AND_BELOW && off < rec.len() {
                        let v = rec[off] as i64;
                        off += 1;
                        Some(v)
                    } else {
                        None
                    };

                let (resting_gram_cal, active_gram_cal, distance_cm) =
                    if record_version >= VERSION_FW_3_11 && off + 5 < rec.len() {
                        let r = u16::from_le_bytes([rec[off], rec[off + 1]]) as i64;
                        off += 2;
                        let a = u16::from_le_bytes([rec[off], rec[off + 1]]) as i64;
                        off += 2;
                        let d = u16::from_le_bytes([rec[off], rec[off + 1]]) as i64;
                        off += 2;
                        (Some(r), Some(a), Some(d))
                    } else {
                        (None, None, None)
                    };

                let heart_rate: Option<i64> =
                    if record_version >= VERSION_FW_4_0 && off < rec.len() {
                        let v = rec[off] as i64;
                        off += 1;
                        Some(v)
                    } else {
                        None
                    };

                let heart_rate_weight: Option<i64> =
                    if record_version >= VERSION_FW_4_1 && off + 1 < rec.len() {
                        let v = u16::from_le_bytes([rec[off], rec[off + 1]]) as i64;
                        off += 2;
                        Some(v)
                    } else {
                        None
                    };

                let heart_rate_zone: Option<i64> =
                    if record_version >= VERSION_FW_4_3 && off < rec.len() {
                        Some(rec[off] as i64)
                    } else {
                        None
                    };

                stmt.execute(params![
                    record_id,
                    record_version as i64,
                    start_ts,
                    utc_offset,
                    steps,
                    orientation,
                    vmc,
                    light,
                    flags,
                    resting_gram_cal,
                    active_gram_cal,
                    distance_cm,
                    heart_rate,
                    heart_rate_weight,
                    heart_rate_zone,
                    rec,
                ])?;
            }
        }
        Ok(())
    }

    fn insert_activity_sessions(
        &self,
        record_id: i64,
        data: &[u8],
        item_size: usize,
    ) -> anyhow::Result<()> {
        Self::do_insert_activity_sessions(&self.conn, record_id, data, item_size)
    }

    /// Parse overlay session records (tags 83 and 84).
    ///
    /// Base (18 bytes): u16 version, u16 skip, u16 session_type, u32 utc_offset,
    ///   u32 start_ts, u32 duration_secs.
    ///
    /// Walk/run extension (version >= 3, session_type 5 or 6, 8 extra bytes):
    ///   u16 steps, u16 active_kcal, u16 resting_kcal, u16 distance_m.
    ///   Note: for version == 3 non-walk/run sessions the 8 bytes are present
    ///   in the payload but contain no useful data and are skipped.
    fn do_insert_activity_sessions(
        conn: &Connection,
        record_id: i64,
        data: &[u8],
        item_size: usize,
    ) -> anyhow::Result<()> {
        const MIN_ITEM: usize = 18;
        const WALK_RUN_EXT: usize = 26; // 18 base + 8 walk/run fields

        if item_size < MIN_ITEM {
            warn!(
                "session item_size={item_size} (expected >={MIN_ITEM}); \
                 raw bytes stored in health_records for reprocessing"
            );
            return Ok(());
        }
        if data.is_empty() || !data.len().is_multiple_of(item_size) {
            return Ok(());
        }

        let mut stmt = conn.prepare_cached(
            "INSERT OR IGNORE INTO health_activity_sessions
                 (health_record_id, record_version, session_type, start_ts, utc_offset,
                  duration_secs, steps, active_kcal, resting_kcal, distance_m, raw)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        )?;

        for item in data.chunks_exact(item_size) {
            let version = u16::from_le_bytes([item[0], item[1]]);
            // item[2..4]: skip (u16)
            let session_type = u16::from_le_bytes([item[4], item[5]]);
            let utc_offset =
                u32::from_le_bytes([item[6], item[7], item[8], item[9]]) as i32 as i64;
            let start_ts = u32::from_le_bytes([item[10], item[11], item[12], item[13]]) as i64;
            let duration = u32::from_le_bytes([item[14], item[15], item[16], item[17]]) as i64;

            let is_walk_run = session_type == 5 || session_type == 6;
            let (steps, active_kcal, resting_kcal, distance_m) =
                if version >= 3 && is_walk_run && item_size >= WALK_RUN_EXT {
                    // Wire order: steps, active_kcal, resting_kcal, distance_m
                    let s = u16::from_le_bytes([item[18], item[19]]) as i64;
                    let a = u16::from_le_bytes([item[20], item[21]]) as i64;
                    let r = u16::from_le_bytes([item[22], item[23]]) as i64;
                    let d = u16::from_le_bytes([item[24], item[25]]) as i64;
                    (Some(s), Some(a), Some(r), Some(d))
                } else {
                    (None, None, None, None)
                };

            stmt.execute(params![
                record_id,
                version as i64,
                session_type as i64,
                start_ts,
                utc_offset,
                duration,
                steps,
                active_kcal,
                resting_kcal,
                distance_m,
                &item[..item_size],
            ])?;
        }
        Ok(())
    }

    // ── IP geolocation cache ────────────────────────────────────────────

    /// Look up a cached IP geolocation result.
    pub fn lookup_ip_location(&self, ip: &str) -> Option<IpLocation> {
        self.conn
            .query_row(
                "SELECT latitude, longitude, city, region FROM ip_locations WHERE ip = ?1",
                params![ip],
                |row| {
                    Ok(IpLocation {
                        latitude: row.get(0)?,
                        longitude: row.get(1)?,
                        city: row.get(2)?,
                        region: row.get(3)?,
                    })
                },
            )
            .ok()
    }

    /// Store an IP geolocation result in the cache.
    pub fn store_ip_location(&self, ip: &str, loc: &IpLocation) -> anyhow::Result<()> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.conn.execute(
            "INSERT OR REPLACE INTO ip_locations (ip, latitude, longitude, city, region, fetched_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![ip, loc.latitude, loc.longitude, loc.city, loc.region, ts],
        )?;
        Ok(())
    }

    /// Rebuild both derived tables from the raw blobs in `health_records`.
    /// Use this to populate `utc_offset` for rows inserted before the column existed,
    /// or to pick up any parser fix without re-syncing from the watch.
    pub fn reprocess(&self) -> anyhow::Result<()> {
        // Load raw records before opening the write transaction.
        let steps_records =
            Self::load_raw_records(&self.conn, datalog_tag::ACTIVITY_STEPS as i64)?;
        let sleep_records = Self::load_raw_records(&self.conn, datalog_tag::SLEEP as i64)?;
        let session_records =
            Self::load_raw_records(&self.conn, datalog_tag::ACTIVITY_SESSIONS as i64)?;

        // Rebuild both tables atomically: a failure mid-loop leaves neither table
        // partially cleared (unchecked_transaction auto-rolls back on drop).
        let txn = self.conn.unchecked_transaction()?;
        self.conn
            .execute("DELETE FROM health_activity_minutes", [])?;
        for rec in &steps_records {
            self.insert_activity_minutes(rec.id, &rec.data, rec.item_size)?;
        }
        self.conn
            .execute("DELETE FROM health_activity_sessions", [])?;
        for rec in sleep_records.iter().chain(&session_records) {
            Self::do_insert_activity_sessions(
                &self.conn,
                rec.id,
                &rec.data,
                rec.item_size,
            )?;
        }
        txn.commit()?;
        debug!(
            "db reprocess: {} steps records, {} sleep + {} session records",
            steps_records.len(),
            sleep_records.len(),
            session_records.len(),
        );
        Ok(())
    }

    fn load_raw_records(conn: &Connection, tag: i64) -> anyhow::Result<Vec<RawRecord>> {
        let mut stmt = conn.prepare(
            "SELECT id, data, item_size FROM health_records WHERE tag = ?1 ORDER BY id ASC",
        )?;
        let rows: Vec<RawRecord> = stmt
            .query_map(params![tag], |r| {
                Ok(RawRecord {
                    id: r.get(0)?,
                    data: r.get(1)?,
                    item_size: r.get::<_, i64>(2)? as usize,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }
}

fn sanitize_export_error(error: &str) -> String {
    const MAX_ERROR_LENGTH: usize = 512;
    let sanitized: String = error
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .take(MAX_ERROR_LENGTH)
        .collect();
    if sanitized.is_empty() {
        "unknown export error".to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_db() -> AppDb {
        let conn = Connection::open_in_memory().unwrap();
        schema::initialize_schema(&conn).unwrap();
        AppDb { conn }
    }

    #[test]
    fn wellness_export_state_is_isolated_by_account_and_records_success() {
        let db = memory_db();
        let date = NaiveDate::from_ymd_opt(2026, 7, 14).unwrap();

        db.record_wellness_export_success(
            "intervals_icu",
            "athlete-a",
            &[(date, "hash-a".to_string())],
            100,
        )
        .unwrap();

        let states = db
            .fetch_wellness_export_states("intervals_icu", "athlete-a")
            .unwrap();
        assert_eq!(
            states,
            vec![WellnessExportState {
                provider: "intervals_icu".into(),
                account_id: "athlete-a".into(),
                wellness_date: date,
                payload_hash: Some("hash-a".into()),
                attempt_count: 0,
                next_attempt_at: None,
                last_attempt_at: Some(100),
                last_success_at: Some(100),
                last_error: None,
            }]
        );
        assert!(db
            .fetch_wellness_export_states("intervals_icu", "athlete-b")
            .unwrap()
            .is_empty());
        assert!(db
            .fetch_wellness_export_states("other_provider", "athlete-a")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn wellness_export_failure_preserves_success_and_increments_attempts() {
        let db = memory_db();
        let date = NaiveDate::from_ymd_opt(2026, 7, 14).unwrap();

        db.record_wellness_export_success(
            "intervals_icu",
            "athlete-a",
            &[(date, "old-hash".to_string())],
            100,
        )
        .unwrap();
        db.record_wellness_export_failure(
            "intervals_icu",
            "athlete-a",
            &[date],
            200,
            Some(500),
            "HTTP 503:\ntransient response",
        )
        .unwrap();

        let state = &db
            .fetch_wellness_export_states("intervals_icu", "athlete-a")
            .unwrap()[0];
        assert_eq!(state.payload_hash.as_deref(), Some("old-hash"));
        assert_eq!(state.attempt_count, 1);
        assert_eq!(state.next_attempt_at, Some(500));
        assert_eq!(state.last_attempt_at, Some(200));
        assert_eq!(state.last_success_at, Some(100));
        assert_eq!(state.last_error.as_deref(), Some("HTTP 503: transient response"));

        db.record_wellness_export_failure(
            "intervals_icu",
            "athlete-a",
            &[date],
            600,
            None,
            "HTTP 401: authentication failure",
        )
        .unwrap();
        let state = &db
            .fetch_wellness_export_states("intervals_icu", "athlete-a")
            .unwrap()[0];
        assert_eq!(state.attempt_count, 2);
        assert_eq!(state.next_attempt_at, None);
        assert_eq!(state.last_attempt_at, Some(600));
        assert_eq!(state.last_success_at, Some(100));
        assert_eq!(state.last_error.as_deref(), Some("HTTP 401: authentication failure"));

        db.record_wellness_export_success(
            "intervals_icu",
            "athlete-a",
            &[(date, "new-hash".to_string())],
            700,
        )
        .unwrap();
        let state = &db
            .fetch_wellness_export_states("intervals_icu", "athlete-a")
            .unwrap()[0];
        assert_eq!(state.payload_hash.as_deref(), Some("new-hash"));
        assert_eq!(state.attempt_count, 0);
        assert_eq!(state.next_attempt_at, None);
        assert_eq!(state.last_attempt_at, Some(700));
        assert_eq!(state.last_success_at, Some(700));
        assert_eq!(state.last_error, None);
    }

    #[test]
    fn wellness_export_status_compares_current_payloads_with_successful_hashes() {
        let db = memory_db();
        let exported = NaiveDate::from_ymd_opt(2026, 7, 14).unwrap();
        let changed = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let unseen = NaiveDate::from_ymd_opt(2026, 7, 16).unwrap();

        db.record_wellness_export_success(
            "intervals_icu",
            "athlete-a",
            &[
                (exported, "hash-exported".into()),
                (changed, "hash-before-change".into()),
            ],
            100,
        )
        .unwrap();

        let status = db
            .fetch_wellness_export_status(
                "intervals_icu",
                "athlete-a",
                &[
                    (exported, "hash-exported".into()),
                    (changed, "hash-after-change".into()),
                    (unseen, "hash-unseen".into()),
                ],
            )
            .unwrap();

        assert_eq!(status.exported_dates, 1);
        assert_eq!(status.pending_dates, 2);
        assert_eq!(status.last_success_at, Some(100));
        assert_eq!(status.last_error, None);
    }
}
