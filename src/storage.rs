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
}
