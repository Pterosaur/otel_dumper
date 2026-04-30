use crate::converter::FlatDataPoint;
use crate::retention::{self, RetentionPolicy, RetentionStats};
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub struct Storage {
    conn: Mutex<Connection>,
    db_path: PathBuf,
}

impl Storage {
    pub fn new(db_path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(db_path)?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -64000;
             PRAGMA temp_store = MEMORY;
             PRAGMA mmap_size = 268435456;
             PRAGMA wal_autocheckpoint = 0;",
        )?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS metric_data_points (
                id                      INTEGER PRIMARY KEY,
                timestamp_ns            INTEGER NOT NULL,
                metric_name             TEXT NOT NULL,
                metric_type             TEXT NOT NULL,
                resource_attrs          TEXT,
                scope_name              TEXT,
                scope_version           TEXT,
                dp_attrs                TEXT,
                value_double            REAL,
                value_int               INTEGER,
                is_monotonic            INTEGER,
                aggregation_temporality INTEGER,
                hist_count              INTEGER,
                hist_sum                REAL,
                hist_min                REAL,
                hist_max                REAL,
                hist_bounds             TEXT,
                hist_counts             TEXT,
                extra_data              TEXT,
                start_timestamp_ns      INTEGER,
                flags                   INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_ts ON metric_data_points(timestamp_ns);",
        )?;

        Ok(Storage {
            conn: Mutex::new(conn),
            db_path: db_path.to_path_buf(),
        })
    }

    pub fn insert_batch(&self, points: &[FlatDataPoint]) -> rusqlite::Result<usize> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO metric_data_points (
                    timestamp_ns, metric_name, metric_type, resource_attrs,
                    scope_name, scope_version, dp_attrs,
                    value_double, value_int, is_monotonic, aggregation_temporality,
                    hist_count, hist_sum, hist_min, hist_max, hist_bounds, hist_counts,
                    extra_data, start_timestamp_ns, flags
                ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
            )?;
            for p in points {
                stmt.execute(params![
                    p.timestamp_ns,
                    p.metric_name,
                    p.metric_type,
                    p.resource_attrs,
                    p.scope_name,
                    p.scope_version,
                    p.dp_attrs,
                    p.value_double,
                    p.value_int,
                    p.is_monotonic.map(|b| b as i32),
                    p.aggregation_temporality,
                    p.hist_count,
                    p.hist_sum,
                    p.hist_min,
                    p.hist_max,
                    p.hist_bounds,
                    p.hist_counts,
                    p.extra_data,
                    p.start_timestamp_ns,
                    p.flags,
                ])?;
            }
        }
        tx.commit()?;
        Ok(points.len())
    }

    /// Create additional indexes for faster Grafana queries. Call after data collection is done.
    ///
    /// When `dp_attr_keys` is non-empty, expression indexes are built on the
    /// specified `dp_attrs` JSON keys so that queries using
    /// `json_extract(dp_attrs, '$.key')` can leverage index lookups instead of
    /// full-table scans.
    pub fn create_analysis_indexes(&self, dp_attr_keys: &[String]) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_name_ts ON metric_data_points(metric_name, timestamp_ns);
             CREATE INDEX IF NOT EXISTS idx_type ON metric_data_points(metric_type);",
        )?;

        if !dp_attr_keys.is_empty() {
            // Composite index on all specified keys + timestamp for panel queries
            let exprs: Vec<String> = dp_attr_keys
                .iter()
                .map(|k| format!("json_extract(dp_attrs, '$.{k}')"))
                .collect();

            let composite = format!(
                "CREATE INDEX IF NOT EXISTS idx_dp_attrs_composite \
                 ON metric_data_points({}, timestamp_ns);",
                exprs.join(", ")
            );
            conn.execute_batch(&composite)?;

            // Individual indexes on each key for variable/filter queries
            for key in dp_attr_keys {
                let idx = format!(
                    "CREATE INDEX IF NOT EXISTS idx_dp_attr_{key} \
                     ON metric_data_points(json_extract(dp_attrs, '$.{key}'));"
                );
                conn.execute_batch(&idx)?;
            }

            conn.execute_batch("ANALYZE;")?;
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }

    pub fn apply_retention(&self, policy: RetentionPolicy) -> rusqlite::Result<RetentionStats> {
        if !policy.is_enabled() {
            return Ok(RetentionStats::default());
        }

        let mut conn = self.conn.lock().unwrap();
        let mut stats = RetentionStats {
            bytes_before: retention::sqlite_database_size_bytes(&self.db_path),
            ..RetentionStats::default()
        };

        if let Some(window_ns) = policy.time_window_ns() {
            let latest_ts: Option<i64> = conn.query_row(
                "SELECT MAX(timestamp_ns) FROM metric_data_points",
                [],
                |row| row.get(0),
            )?;
            if let Some(latest_ts) = latest_ts {
                let cutoff = latest_ts.saturating_sub(window_ns);
                let deleted = conn.execute(
                    "DELETE FROM metric_data_points WHERE timestamp_ns < ?1",
                    params![cutoff],
                )?;
                stats.rows_deleted += deleted as u64;
            }
        }

        if let Some(target_bytes) = policy.size_target_bytes() {
            let current_bytes = retention::sqlite_database_size_bytes(&self.db_path);
            if current_bytes > policy.max_bytes {
                let row_count: u64 =
                    conn.query_row("SELECT COUNT(*) FROM metric_data_points", [], |row| {
                        row.get::<_, i64>(0)
                    })? as u64;
                let rows_to_delete =
                    retention::rows_to_delete_for_size(row_count, current_bytes, target_bytes);
                if rows_to_delete > 0 {
                    let tx = conn.transaction()?;
                    let deleted = tx.execute(
                        "DELETE FROM metric_data_points
                         WHERE id IN (
                             SELECT id FROM metric_data_points
                             ORDER BY timestamp_ns, id
                             LIMIT ?1
                         )",
                        params![rows_to_delete.min(i64::MAX as u64) as i64],
                    )?;
                    tx.commit()?;
                    stats.rows_deleted += deleted as u64;

                    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); VACUUM;")?;
                }
            }
        } else if stats.rows_deleted > 0 {
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        }

        stats.bytes_after = retention::sqlite_database_size_bytes(&self.db_path);
        Ok(stats)
    }

    /// Query row count (for testing).
    #[cfg(test)]
    pub fn count_rows(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM metric_data_points", [], |row| {
            row.get(0)
        })
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::converter::FlatDataPoint;

    fn make_point(name: &str, ts: i64, value: f64) -> FlatDataPoint {
        FlatDataPoint {
            timestamp_ns: ts,
            metric_name: name.to_string(),
            metric_type: "gauge",
            resource_attrs: Some(r#"{"service.name":"test"}"#.to_string()),
            scope_name: Some("test-scope".to_string()),
            scope_version: None,
            dp_attrs: None,
            value_double: Some(value),
            value_int: None,
            is_monotonic: None,
            aggregation_temporality: None,
            hist_count: None,
            hist_sum: None,
            hist_min: None,
            hist_max: None,
            hist_bounds: None,
            hist_counts: None,
            extra_data: None,
            start_timestamp_ns: None,
            flags: 0,
        }
    }

    #[test]
    fn test_storage_init_and_insert() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = Storage::new(&db_path).unwrap();

        let points = vec![
            make_point("cpu.usage", 1_000_000_000, 45.0),
            make_point("cpu.usage", 2_000_000_000, 55.0),
            make_point("mem.usage", 1_000_000_000, 72.0),
        ];

        let written = storage.insert_batch(&points).unwrap();
        assert_eq!(written, 3);
        assert_eq!(storage.count_rows(), 3);
    }

    #[test]
    fn test_storage_empty_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = Storage::new(&db_path).unwrap();

        let written = storage.insert_batch(&[]).unwrap();
        assert_eq!(written, 0);
        assert_eq!(storage.count_rows(), 0);
    }

    #[test]
    fn test_storage_multiple_batches() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = Storage::new(&db_path).unwrap();

        for i in 0..5 {
            let points: Vec<_> = (0..100)
                .map(|j| make_point("metric", (i * 100 + j) as i64, j as f64))
                .collect();
            storage.insert_batch(&points).unwrap();
        }

        assert_eq!(storage.count_rows(), 500);
    }

    #[test]
    fn test_storage_time_retention_deletes_old_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = Storage::new(&db_path).unwrap();

        let points = vec![
            make_point("metric", 1_000_000_000, 1.0),
            make_point("metric", 2_000_000_000, 2.0),
            make_point("metric", 3_000_000_000, 3.0),
        ];
        storage.insert_batch(&points).unwrap();

        let stats = storage
            .apply_retention(RetentionPolicy::new(0, std::time::Duration::from_secs(1)))
            .unwrap();

        assert_eq!(stats.rows_deleted, 1);
        assert_eq!(storage.count_rows(), 2);
    }

    #[test]
    fn test_storage_size_retention_deletes_oldest_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = Storage::new(&db_path).unwrap();

        let points: Vec<_> = (0..100)
            .map(|i| make_point("metric", i, i as f64))
            .collect();
        storage.insert_batch(&points).unwrap();

        let mut policy = RetentionPolicy::new(1, std::time::Duration::ZERO);
        policy.cleanup_interval = std::time::Duration::ZERO;
        let stats = storage.apply_retention(policy).unwrap();

        assert!(stats.rows_deleted > 0);
        assert!(storage.count_rows() < 100);
    }

    #[test]
    fn test_storage_histogram_fields() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = Storage::new(&db_path).unwrap();

        let points = vec![FlatDataPoint {
            timestamp_ns: 1_000_000_000,
            metric_name: "req.duration".to_string(),
            metric_type: "histogram",
            resource_attrs: None,
            scope_name: None,
            scope_version: None,
            dp_attrs: None,
            value_double: None,
            value_int: None,
            is_monotonic: None,
            aggregation_temporality: Some(2),
            hist_count: Some(100),
            hist_sum: Some(5000.0),
            hist_min: Some(0.5),
            hist_max: Some(99.0),
            hist_bounds: Some("[1.0,5.0,10.0]".to_string()),
            hist_counts: Some("[20,50,25,5]".to_string()),
            extra_data: None,
            start_timestamp_ns: Some(500_000_000),
            flags: 0,
        }];

        storage.insert_batch(&points).unwrap();

        let conn = storage.conn.lock().unwrap();
        let (count, sum, min, max): (i64, f64, f64, f64) = conn
            .query_row(
                "SELECT hist_count, hist_sum, hist_min, hist_max FROM metric_data_points WHERE metric_name = 'req.duration'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(count, 100);
        assert_eq!(sum, 5000.0);
        assert_eq!(min, 0.5);
        assert_eq!(max, 99.0);
    }

    #[test]
    fn test_create_analysis_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = Storage::new(&db_path).unwrap();
        // Should not panic on empty DB (no attr keys)
        storage.create_analysis_indexes(&[]).unwrap();
        // Should be idempotent
        storage.create_analysis_indexes(&[]).unwrap();
    }

    #[test]
    fn test_create_analysis_indexes_with_attr_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let storage = Storage::new(&db_path).unwrap();

        let points = vec![FlatDataPoint {
            timestamp_ns: 1_000_000_000,
            metric_name: "test_metric".to_string(),
            metric_type: "gauge",
            resource_attrs: None,
            scope_name: None,
            scope_version: None,
            dp_attrs: Some(r#"{"host":"node-1","region":"us-west"}"#.to_string()),
            value_double: Some(42.0),
            value_int: None,
            is_monotonic: None,
            aggregation_temporality: None,
            hist_count: None,
            hist_sum: None,
            hist_min: None,
            hist_max: None,
            hist_bounds: None,
            hist_counts: None,
            extra_data: None,
            start_timestamp_ns: None,
            flags: 0,
        }];
        storage.insert_batch(&points).unwrap();

        let keys = vec!["host".to_string(), "region".to_string()];
        storage.create_analysis_indexes(&keys).unwrap();
        // Should be idempotent
        storage.create_analysis_indexes(&keys).unwrap();
    }
}
