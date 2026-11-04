//! Synthetic protocol fixtures based on mobileapp 5a0a1873's
//! HealthDataParser.kt / HealthDataParserTest.kt, not watch captures.

use super::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

const START: u32 = 1_700_000_000;

fn memory_db() -> AppDb {
    let conn = Connection::open_in_memory().unwrap();
    schema::apply_pragmas(&conn).unwrap();
    schema::initialize_schema(&conn).unwrap();
    AppDb { conn }
}

fn sample(steps: u8, size: usize) -> Vec<u8> {
    let mut raw = vec![steps, 1];
    raw.extend_from_slice(&500_u16.to_le_bytes());
    raw.extend_from_slice(&[10, 2]);
    for value in [10_u16, 50, 7000] {
        raw.extend_from_slice(&value.to_le_bytes());
    }
    raw.push(60);
    raw.extend_from_slice(&513_u16.to_le_bytes());
    raw.push(3);
    raw.resize(size, 0xaa);
    raw
}

fn activity_item(version: u16, timestamp: u32, samples: &[Vec<u8>]) -> Vec<u8> {
    let size = samples.first().map_or(0, Vec::len);
    assert!(samples.iter().all(|sample| sample.len() == size));
    let mut raw = version.to_le_bytes().to_vec();
    raw.extend_from_slice(&timestamp.to_le_bytes());
    raw.extend_from_slice(&[(-28_i8) as u8, size as u8, samples.len() as u8]);
    for sample in samples {
        raw.extend_from_slice(sample);
    }
    raw
}

fn overlay_item(version: u16, kind: u16, timestamp: u32, size: usize) -> Vec<u8> {
    let mut raw = version.to_le_bytes().to_vec();
    raw.extend_from_slice(&(size as u16).to_le_bytes());
    raw.extend_from_slice(&kind.to_le_bytes());
    raw.extend_from_slice(&(-25_200_i32).to_le_bytes());
    raw.extend_from_slice(&timestamp.to_le_bytes());
    raw.extend_from_slice(&900_u32.to_le_bytes());
    for value in [1200_u16, 40, 15, 800] {
        raw.extend_from_slice(&value.to_le_bytes());
    }
    raw.resize(size, 0xaa);
    raw
}

fn batch(tag: u32, data: Vec<u8>, size: usize) -> DatalogData {
    static NEXT_CRC: AtomicU32 = AtomicU32::new(1);
    DatalogData {
        tag,
        app_uuid: SYSTEM_APP_UUID,
        session_timestamp: START,
        items_left: 0,
        // Unique synthetic batch identities; CRC validation is outside this parser.
        crc: NEXT_CRC.fetch_add(1, Ordering::Relaxed),
        item_type: 0,
        item_size: size.try_into().unwrap(),
        data,
    }
}

fn count(db: &AppDb, table: &str) -> u32 {
    db.conn
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
}

#[test]
fn third_party_health_tag_collisions_remain_raw_before_and_after_reprocessing() {
    for tag in [
        datalog_tag::ACTIVITY_STEPS,
        datalog_tag::SLEEP,
        datalog_tag::ACTIVITY_SESSIONS,
    ] {
        let db = memory_db();
        let (foreign, legitimate, table) = if tag == datalog_tag::ACTIVITY_STEPS {
            (
                activity_item(13, START, &[sample(200, 16)]),
                activity_item(13, START, &[sample(11, 16)]),
                "health_activity_minutes",
            )
        } else {
            let legitimate = overlay_item(3, 1, START, 26);
            let mut foreign = legitimate.clone();
            foreign[14..18].copy_from_slice(&9000_u32.to_le_bytes());
            (foreign, legitimate, "health_activity_sessions")
        };
        let mut foreign = batch(tag, foreign, legitimate.len());
        foreign.app_uuid[15] = 1; // Even an almost-zero UUID is not the system UUID.
        assert!(!db.insert_batch(&foreign).unwrap());
        assert_eq!(count(&db, table), 0);
        let legitimate = batch(tag, legitimate.clone(), legitimate.len());
        assert!(db.insert_batch(&legitimate).unwrap());
        assert!(!db.insert_batch(&legitimate).unwrap());
        assert_eq!(count(&db, "health_records"), 2);
        assert_eq!(count(&db, table), 1);
        db.reprocess().unwrap();
        let (column, expected) = if tag == datalog_tag::ACTIVITY_STEPS {
            ("steps", 11)
        } else {
            ("duration_secs", 900)
        };
        let value: i64 = db
            .conn
            .query_row(&format!("SELECT {column} FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(value, expected);
        assert_eq!(count(&db, "health_records"), 2);
        assert_eq!(count(&db, table), 1);
    }
}

#[test]
fn non_byte_records_and_unrecognized_system_tags_are_retained_without_health_rows() {
    let db = memory_db();
    let data = activity_item(13, START, &[sample(11, 16)]);
    for item_type in [1, 2, 0xff] {
        let mut record = batch(datalog_tag::ACTIVITY_STEPS, data.clone(), data.len());
        record.item_type = item_type;
        assert!(!db.insert_batch(&record).unwrap());
    }
    assert!(
        !db.insert_batch(&batch(86, data.clone(), data.len()))
            .unwrap()
    );
    db.reprocess().unwrap();
    assert_eq!(count(&db, "health_records"), 4);
    assert_eq!(count(&db, "health_activity_minutes"), 0);
    assert_eq!(count(&db, "health_activity_sessions"), 0);
}

#[test]
fn known_step_versions_decode_their_actual_fields() {
    for (version, size) in [(5, 6), (6, 12), (7, 13), (7, 16), (12, 15), (13, 16)] {
        let db = memory_db();
        let data = activity_item(version, START, &[sample(11, size)]);
        assert!(
            db.insert_batch(&batch(
                datalog_tag::ACTIVITY_STEPS,
                data.clone(),
                data.len()
            ))
            .unwrap()
        );
        let values = db
            .conn
            .query_row(
                "SELECT steps, flags, resting_gram_calories, active_gram_calories, distance_cm,
                    heart_rate_bpm, heart_rate_weight, heart_rate_zone, utc_offset
             FROM health_activity_minutes",
                [],
                |row| {
                    Ok((
                        row.get::<_, u32>(0)?,
                        row.get::<_, u32>(1)?,
                        row.get::<_, Option<u32>>(2)?,
                        row.get::<_, Option<u32>>(3)?,
                        row.get::<_, Option<u32>>(4)?,
                        row.get::<_, Option<u32>>(5)?,
                        row.get::<_, Option<u32>>(6)?,
                        row.get::<_, Option<u32>>(7)?,
                        row.get::<_, i32>(8)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(values.0, 11);
        assert_eq!(values.1, 2);
        assert_eq!(values.2, (version >= 6).then_some(10));
        assert_eq!(values.3, (version >= 6).then_some(50));
        assert_eq!(values.4, (version >= 6).then_some(7000));
        assert_eq!(values.5, (version >= 7).then_some(60));
        assert_eq!(values.6, (version >= 12).then_some(513));
        assert_eq!(values.7, (version >= 13).then_some(3));
        assert_eq!(values.8, -25_200);
    }
}

#[test]
fn appended_step_fields_and_item_padding_do_not_shift_following_samples() {
    let db = memory_db();
    let mut first = activity_item(14, START, &[sample(100, 18), sample(101, 18)]);
    first.extend_from_slice(&[0xee; 7]);
    let mut second = activity_item(14, START + 3600, &[sample(100, 18), sample(101, 18)]);
    second.extend_from_slice(&[0xee; 7]);
    let size = first.len();
    let record = batch(datalog_tag::ACTIVITY_STEPS, [first, second].concat(), size);
    assert!(db.insert_batch(&record).unwrap());
    for _ in 0..2 {
        let mut stmt = db.conn.prepare(
            "SELECT steps, heart_rate_bpm, heart_rate_weight, heart_rate_zone, distance_cm, length(raw)
             FROM health_activity_minutes ORDER BY start_ts",
        ).unwrap();
        let values: Vec<(u32, u32, u32, u32, u32, u32)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            values,
            [
                (100, 60, 513, 3, 7000, 18),
                (101, 60, 513, 3, 7000, 18),
                (100, 60, 513, 3, 7000, 18),
                (101, 60, 513, 3, 7000, 18)
            ]
        );
        db.reprocess().unwrap();
    }
    assert_eq!(count(&db, "health_records"), 1);
}

#[test]
fn unsupported_step_versions_are_skipped_without_losing_the_next_item() {
    for version in [0, 4, 8, 9, 10, 11] {
        let db = memory_db();
        let bad = activity_item(version, START, &[sample(200, 16)]);
        assert!(
            !db.insert_batch(&batch(datalog_tag::ACTIVITY_STEPS, bad.clone(), bad.len()))
                .unwrap()
        );
        let good = activity_item(13, START + 60, &[sample(11, 16)]);
        assert!(
            db.insert_batch(&batch(
                datalog_tag::ACTIVITY_STEPS,
                [bad, good.clone()].concat(),
                good.len()
            ))
            .unwrap()
        );
        db.reprocess().unwrap();
        assert_eq!(count(&db, "health_activity_minutes"), 1);
        assert_eq!(count(&db, "health_records"), 2);
        assert_eq!(
            db.conn
                .query_row("SELECT steps FROM health_activity_minutes", [], |row| row
                    .get::<_, u32>(
                    0
                ))
                .unwrap(),
            11
        );
    }
}

#[test]
fn malformed_step_lengths_and_incomplete_samples_stay_raw() {
    let mut cases = vec![];
    for (version, size) in [
        (5, 5),
        (6, 11),
        (7, 12),
        (12, 14),
        (13, 15),
        (14, 15),
        (13, 0),
    ] {
        let data = activity_item(version, START, &[sample(200, size)]);
        cases.push((data.clone(), data.len()));
    }
    let good = activity_item(13, START, &[sample(11, 16)]);
    cases.push((good[..good.len() - 1].to_vec(), good.len()));
    cases.push((vec![0; 8], 8));
    cases.push((good.clone(), 0));
    let mut bad_count = good;
    bad_count[8] = 2;
    cases.push((bad_count.clone(), bad_count.len()));
    for (data, size) in cases {
        let db = memory_db();
        assert!(
            !db.insert_batch(&batch(datalog_tag::ACTIVITY_STEPS, data, size))
                .unwrap()
        );
        db.reprocess().unwrap();
        assert_eq!(count(&db, "health_activity_minutes"), 0);
        assert_eq!(count(&db, "health_records"), 1);
    }
}

#[test]
fn compatible_overlay_versions_preserve_known_types_and_workout_field_order() {
    for version in [1, 3, 5] {
        for kind in 1..=7 {
            let db = memory_db();
            let size = if version == 3 || (version >= 3 && matches!(kind, 5 | 6)) {
                32
            } else {
                18
            };
            let data = overlay_item(version, kind, START, size);
            assert!(
                db.insert_batch(&batch(
                    datalog_tag::ACTIVITY_SESSIONS,
                    data.clone(),
                    data.len()
                ))
                .unwrap()
            );
            let fields = db.conn.query_row(
                "SELECT session_type, steps, active_kcal, resting_kcal, distance_m, utc_offset, duration_secs
                 FROM health_activity_sessions",
                [],
                |row| {
                    Ok((
                        row.get::<_, u16>(0)?,
                        row.get::<_, Option<u16>>(1)?,
                        row.get::<_, Option<u16>>(2)?,
                        row.get::<_, Option<u16>>(3)?,
                        row.get::<_, Option<u16>>(4)?,
                        row.get::<_, i32>(5)?,
                        row.get::<_, u32>(6)?,
                    ))
                },
            ).unwrap();
            let metrics = version >= 3 && matches!(kind, 5 | 6);
            assert_eq!(
                fields,
                (
                    kind,
                    metrics.then_some(1200),
                    metrics.then_some(40),
                    metrics.then_some(15),
                    metrics.then_some(800),
                    -25_200,
                    900
                )
            );
            if kind == 7 {
                assert_eq!(
                    db.conn
                        .query_row("SELECT name FROM session_types WHERE id = 7", [], |row| row
                            .get::<_, String>(0))
                        .unwrap(),
                    "open_workout"
                );
            }
            db.reprocess().unwrap();
            assert_eq!(count(&db, "health_activity_sessions"), 1);
        }
    }
}

#[test]
fn incompatible_overlay_versions_and_unknown_types_do_not_poison_following_items() {
    for (version, kind) in [
        (0, 5),
        (2, 5),
        (4, 5),
        (6, 5),
        (3, 0),
        (3, 8),
        (3, u16::MAX),
    ] {
        let db = memory_db();
        let invalid = overlay_item(version, kind, START, 28);
        assert!(
            !db.insert_batch(&batch(datalog_tag::ACTIVITY_SESSIONS, invalid.clone(), 28))
                .unwrap()
        );
        let valid = overlay_item(3, 5, START + 60, 28);
        assert!(
            db.insert_batch(&batch(
                datalog_tag::ACTIVITY_SESSIONS,
                [invalid, valid].concat(),
                28
            ))
            .unwrap()
        );
        db.reprocess().unwrap();
        assert_eq!(count(&db, "health_activity_sessions"), 1);
        assert_eq!(count(&db, "health_records"), 2);
    }
}

#[test]
fn truncated_overlays_stay_raw() {
    for (version, kind, size) in [(1, 1, 17), (3, 1, 18), (3, 5, 25), (5, 6, 25)] {
        let db = memory_db();
        let data = overlay_item(version, kind, START, size);
        assert!(
            !db.insert_batch(&batch(datalog_tag::SLEEP, data, size))
                .unwrap()
        );
        db.reprocess().unwrap();
        assert_eq!(count(&db, "health_activity_sessions"), 0);
        assert_eq!(count(&db, "health_records"), 1);
    }
}

/// Simulate an old importer letting a third-party minute shadow a genuine
/// system minute, and accepting a layout-incompatible overlay.
fn seed_old_derived_rows(db: &AppDb) {
    let foreign_data = activity_item(13, START, &[sample(200, 16)]);
    let mut foreign = batch(
        datalog_tag::ACTIVITY_STEPS,
        foreign_data.clone(),
        foreign_data.len(),
    );
    foreign.app_uuid[0] = 1;
    assert!(!db.insert_batch(&foreign).unwrap());
    let foreign_id = db.conn.last_insert_rowid();
    db.insert_activity_minutes(foreign_id, &foreign.data, usize::from(foreign.item_size))
        .unwrap();
    let real_data = activity_item(13, START, &[sample(11, 16)]);
    assert!(
        !db.insert_batch(&batch(
            datalog_tag::ACTIVITY_STEPS,
            real_data.clone(),
            real_data.len()
        ))
        .unwrap()
    );
    let invalid_overlay = overlay_item(4, 1, START, 28);
    assert!(
        !db.insert_batch(&batch(
            datalog_tag::SLEEP,
            invalid_overlay.clone(),
            invalid_overlay.len()
        ))
        .unwrap()
    );
    db.conn.execute(
        "INSERT INTO health_activity_sessions
            (health_record_id, record_version, session_type, start_ts, utc_offset, duration_secs, raw)
         VALUES (?1, 4, 1, ?2, 0, 9000, ?3)",
        params![db.conn.last_insert_rowid(), START, invalid_overlay],
    ).unwrap();
}

#[test]
fn reprocessing_and_maintenance_marker_are_atomic_and_preserve_raw_data() {
    let db = memory_db();
    seed_old_derived_rows(&db);
    db.conn
        .execute_batch(
            "CREATE TRIGGER fail_parser_migration BEFORE INSERT ON maintenance_versions
         WHEN NEW.name = 'health_parser' BEGIN SELECT RAISE(FAIL, 'synthetic write failure'); END;",
        )
        .unwrap();
    assert!(db.reprocess().is_err());
    assert_eq!(
        db.maintenance_version(HEALTH_PARSER_MAINTENANCE).unwrap(),
        0
    );
    assert_eq!(
        db.conn
            .query_row("SELECT steps FROM health_activity_minutes", [], |row| row
                .get::<_, u32>(
                0
            ))
            .unwrap(),
        200
    );
    assert_eq!(count(&db, "health_activity_sessions"), 1);
    assert_eq!(count(&db, "health_records"), 3);
    db.conn
        .execute_batch("DROP TRIGGER fail_parser_migration")
        .unwrap();
    db.reprocess().unwrap();
    assert_eq!(
        db.maintenance_version(HEALTH_PARSER_MAINTENANCE).unwrap(),
        HEALTH_PARSER_VERSION
    );
    assert_eq!(
        db.conn
            .query_row("SELECT steps FROM health_activity_minutes", [], |row| row
                .get::<_, u32>(
                0
            ))
            .unwrap(),
        11
    );
    assert_eq!(count(&db, "health_activity_sessions"), 0);
    assert_eq!(count(&db, "health_records"), 3);
}

struct TestDatabaseDirectory(PathBuf);

impl TestDatabaseDirectory {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "cobble-health-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        Self(directory)
    }
    fn path(&self) -> PathBuf {
        self.0.join("health.db")
    }
}

impl Drop for TestDatabaseDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn opening_an_old_database_repairs_cached_health_once() {
    let directory = TestDatabaseDirectory::new();
    let conn = Connection::open(directory.path()).unwrap();
    schema::apply_pragmas(&conn).unwrap();
    schema::initialize_schema(&conn).unwrap();
    let old = AppDb { conn };
    seed_old_derived_rows(&old);
    drop(old);
    let db = AppDb::open(&directory.path()).unwrap();
    assert_eq!(
        db.conn
            .query_row("SELECT steps FROM health_activity_minutes", [], |row| row
                .get::<_, u32>(
                0
            ))
            .unwrap(),
        11
    );
    assert_eq!(count(&db, "health_activity_sessions"), 0);
    assert_eq!(count(&db, "health_records"), 3);
    assert_eq!(
        db.maintenance_version(HEALTH_PARSER_MAINTENANCE).unwrap(),
        HEALTH_PARSER_VERSION
    );
    db.conn
        .execute_batch(
            "CREATE TRIGGER fail_repeated_rebuild BEFORE DELETE ON health_activity_minutes
             BEGIN SELECT RAISE(FAIL, 'unexpected repeated rebuild'); END;",
        )
        .unwrap();
    drop(db);
    let reopened = AppDb::open(&directory.path()).unwrap();
    assert_eq!(count(&reopened, "health_activity_minutes"), 1);
    assert_eq!(count(&reopened, "health_records"), 3);
}

#[test]
fn reprocessing_requires_the_full_system_uuid_and_valid_database_rows() {
    let db = memory_db();
    let data = activity_item(13, START, &[sample(200, 16)]);
    db.conn.execute(
        "INSERT INTO health_records (tag, app_uuid, session_ts, item_type, item_size, crc, data, received_at)
         VALUES (81, x'00', 0, 0, ?1, 1, ?2, 0)", params![u16::try_from(data.len()).unwrap(), data],
    ).unwrap();
    let id = db.conn.last_insert_rowid();
    db.insert_activity_minutes(id, &data, data.len()).unwrap();
    db.reprocess().unwrap();
    assert_eq!(count(&db, "health_activity_minutes"), 0);
    assert_eq!(count(&db, "health_records"), 1);
    db.conn.execute(
        "INSERT INTO health_records (tag, app_uuid, session_ts, item_type, item_size, crc, data, received_at)
         VALUES (81, ?1, 0, 0, 'invalid size', 1, ?2, 0)", params![SYSTEM_APP_UUID.as_slice(), data],
    ).unwrap();
    assert!(db.reprocess().is_err());
    assert_eq!(count(&db, "health_records"), 2);
}
