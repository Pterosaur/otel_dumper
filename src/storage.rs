use crate::converter::FlatDataPoint;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;

pub struct Storage {
    conn: Mutex<Connection>,
}

impl Storage {
    pub fn new(db_path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(db_path)?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -64000;
             PRAGMA temp_store = MEMORY;
             PRAGMA mmap_size = 268435456;",
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
    pub fn create_analysis_indexes(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_name_ts ON metric_data_points(metric_name, timestamp_ns);
             CREATE INDEX IF NOT EXISTS idx_type ON metric_data_points(metric_type);",
        )?;
        Ok(())
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
        // Should not panic on empty DB
        storage.create_analysis_indexes().unwrap();
        // Should be idempotent
        storage.create_analysis_indexes().unwrap();
    }
}
