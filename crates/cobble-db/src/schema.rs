use rusqlite::Connection;

/// Create all tables, indexes, and views. Idempotent — safe to call on
/// every open because it uses `IF NOT EXISTS` for tables and drops/recreates
/// views so definition changes take effect immediately.
pub fn initialize_schema(conn: &Connection) -> anyhow::Result<()> {
    // Drop views before recreating so definition changes take effect on every open.
    conn.execute_batch(
        "DROP VIEW IF EXISTS v_sleep;
         DROP VIEW IF EXISTS v_workouts;",
    )?;

    conn.execute_batch(SCHEMA_DDL)?;
    conn.execute_batch(VIEWS_DDL)?;

    Ok(())
}

/// PRAGMA statements applied unconditionally on every open.
pub fn apply_pragmas(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA foreign_keys=ON;",
    )?;
    Ok(())
}

/// PRAGMAs for read-only consumers that may race with the daemon's writes.
pub fn apply_read_pragmas(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        "PRAGMA busy_timeout=5000;
         PRAGMA foreign_keys=ON;",
    )?;
    Ok(())
}

pub const SCHEMA_DDL: &str = r#"
-- Raw DataLog batches (one row per SENDDATA message).
-- data + item_size allow reprocessing if a parser needs fixing.
CREATE TABLE IF NOT EXISTS health_records (
    id          INTEGER PRIMARY KEY,
    tag         INTEGER NOT NULL,
    app_uuid    BLOB    NOT NULL,
    session_ts  INTEGER NOT NULL,
    item_type   INTEGER NOT NULL,
    item_size   INTEGER NOT NULL,
    crc         INTEGER NOT NULL,
    data        BLOB    NOT NULL,
    received_at INTEGER NOT NULL,
    UNIQUE(tag, app_uuid, session_ts, crc)
);
CREATE INDEX IF NOT EXISTS idx_health_tag        ON health_records(tag);
CREATE INDEX IF NOT EXISTS idx_health_session_ts ON health_records(session_ts);

-- Per-minute activity data (tag 81).
--
-- Wire format: 9-byte chunk header + record_num × record_length sub-records.
--   [chunk header, 9 bytes]
--     u16 record_version
--     u32 timestamp         unix ts of first minute in chunk
--     i8  utc_offset        15-min segments, stored as utc_offset (× 900 s)
--     u8  record_length     bytes per sub-record
--     u8  record_num        count of sub-records
--   [sub-record, record_length bytes each]
--     u8  steps
--     u8  orientation
--     u16 vmc               (intensity / vector magnitude count)
--     u8  light
--     u8  flags             (version >= 5)
--     u16 resting_gram_cal  (version >= 6)
--     u16 active_gram_cal   (version >= 6)
--     u16 distance_cm       (version >= 6)
--     u8  heart_rate_bpm    (version >= 7)
--     u16 heart_rate_weight (version >= 8)
--     u8  heart_rate_zone   (version >= 13)
CREATE TABLE IF NOT EXISTS health_activity_minutes (
    id                    INTEGER PRIMARY KEY,
    health_record_id      INTEGER NOT NULL REFERENCES health_records(id),
    record_version        INTEGER NOT NULL,
    start_ts              INTEGER NOT NULL,
    utc_offset            INTEGER NOT NULL,
    steps                 INTEGER NOT NULL,
    orientation           INTEGER NOT NULL,
    vmc                   INTEGER NOT NULL,
    light                 INTEGER NOT NULL,
    flags                 INTEGER,
    resting_gram_calories INTEGER,
    active_gram_calories  INTEGER,
    distance_cm           INTEGER,
    heart_rate_bpm        INTEGER,
    heart_rate_weight     INTEGER,
    heart_rate_zone       INTEGER,
    raw                   BLOB    NOT NULL,
    UNIQUE(start_ts)
);
CREATE INDEX IF NOT EXISTS idx_activity_min_ts ON health_activity_minutes(start_ts);

-- Overlay session records (tags 83 and 84).
--
-- Tag 83 (SLEEP) and tag 84 (ACTIVITY_SESSIONS) both use this format.
-- Tag 83 contains only sleep-type sessions; tag 84 contains all types.
-- Duplicates across tags are silently ignored via UNIQUE(start_ts, session_type).
--
-- Wire format (per item, base 18 bytes):
--   u16 version
--   u16 (unused)
--   u16 session_type   1=sleep 2=deep_sleep 3=nap 4=deep_nap 5=walk 6=run
--   u32 start_ts       unix timestamp
--   u32 utc_offset     seconds west of UTC (negative for east zones, signed i32 on wire)
--   u32 duration_secs
--   [walk/run extension, version >= 3, session_type 5 or 6, 8 extra bytes]
--   u16 steps
--   u16 active_kcal
--   u16 resting_kcal
--   u16 distance_m
CREATE TABLE IF NOT EXISTS health_activity_sessions (
    id               INTEGER PRIMARY KEY,
    health_record_id INTEGER NOT NULL REFERENCES health_records(id),
    record_version   INTEGER NOT NULL,
    session_type     INTEGER NOT NULL,
    utc_offset       INTEGER NOT NULL,
    start_ts         INTEGER NOT NULL,
    duration_secs    INTEGER NOT NULL,
    steps            INTEGER,
    active_kcal      INTEGER,
    resting_kcal     INTEGER,
    distance_m       INTEGER,
    raw              BLOB    NOT NULL,
    UNIQUE(start_ts, session_type)
);
CREATE INDEX IF NOT EXISTS idx_sessions_start ON health_activity_sessions(start_ts);

-- Lookup table for overlay session types.
-- Join with health_activity_sessions on session_type = id.
CREATE TABLE IF NOT EXISTS session_types (
    id   INTEGER PRIMARY KEY,
    name TEXT    NOT NULL
);
INSERT OR IGNORE INTO session_types VALUES
    (1, 'sleep'),
    (2, 'deep_sleep'),
    (3, 'nap'),
    (4, 'deep_nap'),
    (5, 'walk'),
    (6, 'run');

-- Cached IP geolocation
CREATE TABLE IF NOT EXISTS ip_locations (
    ip         TEXT    PRIMARY KEY,
    latitude   REAL    NOT NULL,
    longitude  REAL    NOT NULL,
    city       TEXT    NOT NULL,
    region     TEXT    NOT NULL,
    fetched_at INTEGER NOT NULL
);
"#;

pub const VIEWS_DDL: &str = r#"
CREATE VIEW v_sleep AS
SELECT s.id, s.start_ts, s.utc_offset, s.duration_secs, t.name AS type
FROM health_activity_sessions s
JOIN session_types t ON s.session_type = t.id
WHERE s.session_type <= 4;

CREATE VIEW v_workouts AS
SELECT s.id, s.start_ts, s.utc_offset, s.duration_secs, t.name AS type,
       s.steps, s.active_kcal, s.resting_kcal, s.distance_m
FROM health_activity_sessions s
JOIN session_types t ON s.session_type = t.id
WHERE s.session_type >= 5;
"#;
