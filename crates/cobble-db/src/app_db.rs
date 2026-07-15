use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use anyhow::Context;
use libpebble_ble::endpoints::datalog::tag as datalog_tag;
use libpebble_ble::DatalogData;
use rusqlite::{params, Connection};
use tracing::{debug, warn};

use crate::schema;
use crate::time::DateRange;
use crate::types::{DailyWellness, IpLocation};

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

    /// Insert a raw batch into health_records and parse individual records into the
    /// per-tag tables. Silently skips duplicate batches (same CRC).
    pub fn insert_batch(&self, batch: &DatalogData) -> anyhow::Result<()> {
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
            return Ok(());
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
            _ => Ok(()),
        }
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
