use rusqlite::Connection;
use std::path::Path;
use std::sync::Mutex;

/// Read-only SQLite query server for Grafana Infinity plugin.
pub struct QueryServer {
    conn: Mutex<Connection>,
}

impl QueryServer {
    /// Open a separate read-only connection to the database.
    pub fn new(db_path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open_with_flags(
            db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.execute_batch(
            "PRAGMA query_only = ON;
             PRAGMA wal_autocheckpoint = 0;
             PRAGMA cache_size = -64000;",
        )?;
        Ok(QueryServer {
            conn: Mutex::new(conn),
        })
    }

    /// Execute a read-only SQL query and return results as JSON.
    pub fn query(&self, sql: &str, limit: usize) -> Result<QueryResult, String> {
        // Safety: only allow SELECT
        let trimmed = sql.trim();
        let first_word = trimmed
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_uppercase();
        if first_word != "SELECT" {
            return Err("Only SELECT queries are allowed".to_string());
        }

        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(trimmed)
            .map_err(|e| format!("SQL error: {e}"))?;

        let col_count = stmt.column_count();
        let columns: Vec<String> = (0..col_count)
            .map(|i| stmt.column_name(i).unwrap().to_string())
            .collect();

        let mut rows = Vec::new();
        let mut raw_rows = stmt.query([]).map_err(|e| format!("Query error: {e}"))?;
        let mut count = 0;
        while let Some(row) = raw_rows.next().map_err(|e| format!("Row error: {e}"))? {
            if count >= limit {
                break;
            }
            let mut obj = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                let val = row_value_to_json(row, i);
                obj.insert(col.clone(), val);
            }
            rows.push(serde_json::Value::Object(obj));
            count += 1;
        }

        Ok(QueryResult { columns, rows })
    }
}

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
}

fn row_value_to_json(row: &rusqlite::Row, idx: usize) -> serde_json::Value {
    // Try f64 first to preserve decimal precision (e.g. timestamp_ns/1e9)
    // then i64 for pure integers, then String as fallback
    if let Ok(v) = row.get::<_, f64>(idx) {
        // Check if it's actually an integer value (no fractional part)
        if v.fract() == 0.0 && v.abs() < i64::MAX as f64 {
            return serde_json::Value::Number((v as i64).into());
        }
        return serde_json::json!(v);
    }
    if let Ok(v) = row.get::<_, i64>(idx) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = row.get::<_, String>(idx) {
        return serde_json::Value::String(v);
    }
    serde_json::Value::Null
}

/// Start the SQLite query HTTP server.
pub async fn run(
    query_server: std::sync::Arc<QueryServer>,
    port: u16,
) -> Result<(), std::io::Error> {
    use axum::{routing::get, Router};

    let app = Router::new()
        .route("/api/query", get(handle_query).post(handle_query))
        .route("/api/tables", get(handle_tables))
        .route("/api/schema", get(handle_schema))
        .route("/health", get(handle_health))
        .with_state(query_server);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!("SQLite query API listening on 0.0.0.0:{port}");

    axum::serve(listener, app).await
}

#[derive(serde::Deserialize)]
struct QueryParams {
    #[serde(default)]
    sql: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    10000
}

async fn handle_query(
    axum::extract::State(qs): axum::extract::State<std::sync::Arc<QueryServer>>,
    axum::extract::Query(params): axum::extract::Query<QueryParams>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    if params.sql.is_empty() {
        return (StatusCode::BAD_REQUEST, "Missing 'sql' parameter").into_response();
    }

    let sql = params.sql.clone();
    let limit = params.limit;
    let qs = qs.clone();

    let result = tokio::task::spawn_blocking(move || qs.query(&sql, limit)).await;

    match result {
        Ok(Ok(qr)) => axum::Json(serde_json::json!({
            "columns": qr.columns,
            "rows": qr.rows,
            "count": qr.rows.len(),
        }))
        .into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, e).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Internal error: {e}"),
        )
            .into_response(),
    }
}

async fn handle_tables(
    axum::extract::State(qs): axum::extract::State<std::sync::Arc<QueryServer>>,
) -> axum::Json<serde_json::Value> {
    let qs = qs.clone();
    let result = tokio::task::spawn_blocking(move || {
        qs.query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
            100,
        )
    })
    .await;
    match result {
        Ok(Ok(qr)) => axum::Json(serde_json::json!({
            "tables": qr.rows.iter().map(|r| r["name"].as_str().unwrap_or("")).collect::<Vec<_>>(),
        })),
        _ => axum::Json(serde_json::json!({"tables": []})),
    }
}

async fn handle_schema(
    axum::extract::State(qs): axum::extract::State<std::sync::Arc<QueryServer>>,
) -> axum::Json<serde_json::Value> {
    let qs = qs.clone();
    let result =
        tokio::task::spawn_blocking(move || qs.query("PRAGMA table_info(metric_data_points)", 100))
            .await;
    match result {
        Ok(Ok(qr)) => axum::Json(serde_json::json!({"columns": qr.rows})),
        _ => axum::Json(serde_json::json!({"columns": []})),
    }
}

async fn handle_health() -> &'static str {
    "ok"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::converter::FlatDataPoint;
    use crate::storage::Storage;

    fn make_point(name: &str, ts: i64, value: i64) -> FlatDataPoint {
        FlatDataPoint {
            timestamp_ns: ts,
            metric_name: name.to_string(),
            metric_type: "gauge",
            resource_attrs: None,
            scope_name: None,
            scope_version: None,
            dp_attrs: Some(r#"{"port":"Ethernet16"}"#.to_string()),
            value_double: None,
            value_int: Some(value),
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

    fn setup_db(dir: &tempfile::TempDir) -> (Storage, QueryServer) {
        let db_path = dir.path().join("test.db");
        let storage = Storage::new(&db_path).unwrap();
        let points = vec![
            make_point("cpu", 1_000_000_001, 42),
            make_point("cpu", 1_000_000_002, 43),
            make_point("cpu", 1_000_000_003, 44),
            make_point("mem", 2_000_000_001, 70),
        ];
        storage.insert_batch(&points).unwrap();
        let qs = QueryServer::new(&db_path).unwrap();
        (storage, qs)
    }

    #[test]
    fn test_query_select() {
        let dir = tempfile::tempdir().unwrap();
        let (_storage, qs) = setup_db(&dir);

        let result = qs.query("SELECT COUNT(*) as cnt FROM metric_data_points", 100);
        let qr = result.unwrap();
        assert_eq!(qr.columns, vec!["cnt"]);
        assert_eq!(qr.rows[0]["cnt"], 4);
    }

    #[test]
    fn test_query_with_nanosecond_precision() {
        let dir = tempfile::tempdir().unwrap();
        let (_storage, qs) = setup_db(&dir);

        let result = qs
            .query(
                "SELECT timestamp_ns FROM metric_data_points WHERE metric_name = 'cpu' ORDER BY timestamp_ns",
                100,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0]["timestamp_ns"], 1_000_000_001i64);
        assert_eq!(result.rows[1]["timestamp_ns"], 1_000_000_002i64);
        assert_eq!(result.rows[2]["timestamp_ns"], 1_000_000_003i64);
    }

    #[test]
    fn test_query_reject_non_select() {
        let dir = tempfile::tempdir().unwrap();
        let (_storage, qs) = setup_db(&dir);

        assert!(qs.query("DROP TABLE metric_data_points", 100).is_err());
        assert!(qs
            .query("INSERT INTO metric_data_points VALUES (1)", 100)
            .is_err());
        assert!(qs.query("DELETE FROM metric_data_points", 100).is_err());
    }

    #[test]
    fn test_query_limit() {
        let dir = tempfile::tempdir().unwrap();
        let (_storage, qs) = setup_db(&dir);

        let result = qs.query("SELECT * FROM metric_data_points", 2).unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_query_metric_names() {
        let dir = tempfile::tempdir().unwrap();
        let (_storage, qs) = setup_db(&dir);

        let result = qs
            .query(
                "SELECT DISTINCT metric_name, COUNT(*) as cnt FROM metric_data_points GROUP BY metric_name ORDER BY cnt DESC",
                100,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0]["metric_name"], "cpu");
        assert_eq!(result.rows[0]["cnt"], 3);
    }

    #[test]
    fn test_query_filters_by_attrs() {
        let dir = tempfile::tempdir().unwrap();
        let (_storage, qs) = setup_db(&dir);

        let result = qs
            .query(
                "SELECT value_int FROM metric_data_points WHERE dp_attrs LIKE '%Ethernet16%'",
                100,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 4);
    }
}
