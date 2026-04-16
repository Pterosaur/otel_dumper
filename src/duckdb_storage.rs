use crate::converter::FlatDataPoint;
use duckdb::{params, Connection};
use std::path::Path;
use std::sync::Mutex;

pub struct DuckDbStorage {
    conn: Mutex<Connection>,
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

    /// Query row count (for testing).
    #[cfg(test)]
    pub fn count_rows(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM metric_data_points")
            .unwrap();
        stmt.query_row([], |row| row.get(0)).unwrap()
    }

    /// Return the database file path for diagnostics.
    pub fn is_duckdb(&self) -> bool {
        true
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
