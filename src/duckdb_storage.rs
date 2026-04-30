use crate::converter::FlatDataPoint;
use crate::retention::{self, RetentionPolicy, RetentionStats};
use duckdb::{params, Connection};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub struct DuckDbStorage {
    conn: Mutex<Connection>,
    db_path: PathBuf,
}

impl DuckDbStorage {
    pub fn new(db_path: &Path) -> duckdb::Result<Self> {
        let conn = Connection::open(db_path)?;

        conn.execute_batch(
            "CREATE SEQUENCE IF NOT EXISTS seq_mdp_id START 1;
             CREATE TABLE IF NOT EXISTS metric_data_points (
                timestamp_ns            BIGINT NOT NULL,
                metric_name             VARCHAR NOT NULL,
                metric_type             VARCHAR NOT NULL,
                resource_attrs          VARCHAR,
                scope_name              VARCHAR,
                scope_version           VARCHAR,
                dp_attrs                VARCHAR,
                value_double            DOUBLE,
                value_int               BIGINT,
                is_monotonic            INTEGER,
                aggregation_temporality INTEGER,
                hist_count              BIGINT,
                hist_sum                DOUBLE,
                hist_min                DOUBLE,
                hist_max                DOUBLE,
                hist_bounds             VARCHAR,
                hist_counts             VARCHAR,
                extra_data              VARCHAR,
                start_timestamp_ns      BIGINT,
                flags                   INTEGER DEFAULT 0
            );",
        )?;

        Ok(DuckDbStorage {
            conn: Mutex::new(conn),
            db_path: db_path.to_path_buf(),
        })
    }

    pub fn insert_batch(&self, points: &[FlatDataPoint]) -> duckdb::Result<usize> {
        if points.is_empty() {
            return Ok(0);
        }
        let conn = self.conn.lock().unwrap();
        let mut appender = conn.appender("metric_data_points")?;

        for p in points {
            appender.append_row(params![
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
        appender.flush()?;
        Ok(points.len())
    }

    /// DuckDB uses columnar storage and doesn't need expression indexes.
    /// This method is kept for API compatibility.
    pub fn create_analysis_indexes(&self, _dp_attr_keys: &[String]) -> duckdb::Result<()> {
        // DuckDB's columnar engine handles analytical queries efficiently
        // without explicit indexes. No-op here.
        Ok(())
    }

    pub fn apply_retention(&self, policy: RetentionPolicy) -> duckdb::Result<RetentionStats> {
        if !policy.is_enabled() {
            return Ok(RetentionStats::default());
        }

        let conn = self.conn.lock().unwrap();
        let mut stats = RetentionStats {
            bytes_before: retention::duckdb_database_size_bytes(&self.db_path),
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
            let current_bytes = retention::duckdb_database_size_bytes(&self.db_path);
            if current_bytes > policy.max_bytes {
                let row_count: u64 =
                    conn.query_row("SELECT COUNT(*) FROM metric_data_points", [], |row| {
                        row.get::<_, i64>(0)
                    })? as u64;
                let rows_to_delete =
                    retention::rows_to_delete_for_size(row_count, current_bytes, target_bytes);
                if rows_to_delete > 0 {
                    let deleted = if rows_to_delete >= row_count {
                        conn.execute("DELETE FROM metric_data_points", [])?
                    } else {
                        let cutoff: i64 = conn.query_row(
                            "SELECT timestamp_ns
                             FROM metric_data_points
                             ORDER BY timestamp_ns
                             LIMIT 1 OFFSET ?1",
                            params![rows_to_delete.saturating_sub(1).min(i64::MAX as u64) as i64],
                            |row| row.get(0),
                        )?;
                        conn.execute(
                            "DELETE FROM metric_data_points WHERE timestamp_ns <= ?1",
                            params![cutoff],
                        )?
                    };
                    stats.rows_deleted += deleted as u64;

                    // Force a checkpoint so the file-size gate observes pending WAL data.
                    conn.execute_batch("CHECKPOINT;")?;
                }
            }
        } else if stats.rows_deleted > 0 {
            conn.execute_batch("CHECKPOINT;")?;
        }

        stats.bytes_after = retention::duckdb_database_size_bytes(&self.db_path);
        Ok(stats)
    }

    /// Query row count (for testing).
    #[cfg(test)]
    pub fn count_rows(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM metric_data_points")
            .unwrap();
        stmt.query_row([], |row| row.get(0)).unwrap()
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
    fn test_duckdb_init_and_insert() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        let storage = DuckDbStorage::new(&db_path).unwrap();

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
    fn test_duckdb_empty_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        let storage = DuckDbStorage::new(&db_path).unwrap();

        let written = storage.insert_batch(&[]).unwrap();
        assert_eq!(written, 0);
        assert_eq!(storage.count_rows(), 0);
    }

    #[test]
    fn test_duckdb_multiple_batches() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        let storage = DuckDbStorage::new(&db_path).unwrap();

        for i in 0..5 {
            let points: Vec<_> = (0..100)
                .map(|j| make_point("metric", (i * 100 + j) as i64, j as f64))
                .collect();
            storage.insert_batch(&points).unwrap();
        }

        assert_eq!(storage.count_rows(), 500);
    }

    #[test]
    fn test_duckdb_time_retention_deletes_old_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        let storage = DuckDbStorage::new(&db_path).unwrap();

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
    fn test_duckdb_size_retention_deletes_old_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        let storage = DuckDbStorage::new(&db_path).unwrap();

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
    fn test_duckdb_histogram_fields() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        let storage = DuckDbStorage::new(&db_path).unwrap();

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
        let mut stmt = conn
            .prepare("SELECT hist_count, hist_sum, hist_min, hist_max FROM metric_data_points WHERE metric_name = 'req.duration'")
            .unwrap();
        let (count, sum, min, max): (i64, f64, f64, f64) = stmt
            .query_row([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap();
        assert_eq!(count, 100);
        assert_eq!(sum, 5000.0);
        assert_eq!(min, 0.5);
        assert_eq!(max, 99.0);
    }

    #[test]
    fn test_duckdb_analysis_indexes_noop() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");
        let storage = DuckDbStorage::new(&db_path).unwrap();
        // Should not panic
        storage.create_analysis_indexes(&[]).unwrap();
        storage
            .create_analysis_indexes(&["key1".to_string(), "key2".to_string()])
            .unwrap();
    }

    #[test]
    fn test_duckdb_reopen_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.duckdb");

        // Write data
        {
            let storage = DuckDbStorage::new(&db_path).unwrap();
            let points = vec![make_point("cpu", 1_000_000_000, 42.0)];
            storage.insert_batch(&points).unwrap();
        }

        // Reopen and verify
        {
            let storage = DuckDbStorage::new(&db_path).unwrap();
            assert_eq!(storage.count_rows(), 1);
        }
    }
}
