use crate::converter::FlatDataPoint;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Key for a unique metric time series: (metric_name, sorted_label_pairs)
type SeriesKey = (String, Vec<(String, String)>);

struct SeriesValue {
    metric_type: &'static str,
    value_double: Option<f64>,
    value_int: Option<i64>,
    hist_count: Option<i64>,
    hist_sum: Option<f64>,
    timestamp: f64, // Unix seconds
}

/// Thread-safe store of latest metric values, rendered as Prometheus text on demand.
pub struct MetricsStore {
    series: RwLock<HashMap<SeriesKey, SeriesValue>>,
}

impl Default for MetricsStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsStore {
    pub fn new() -> Self {
        MetricsStore {
            series: RwLock::new(HashMap::new()),
        }
    }

    /// Update the store with a batch of data points (keeps latest value per series).
    pub fn update(&self, points: &[FlatDataPoint]) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let mut map = self.series.write().unwrap();
        for p in points {
            let labels = parse_labels(p.dp_attrs.as_deref());
            let key = (p.metric_name.clone(), labels);
            map.insert(
                key,
                SeriesValue {
                    metric_type: p.metric_type,
                    value_double: p.value_double,
                    value_int: p.value_int,
                    hist_count: p.hist_count,
                    hist_sum: p.hist_sum,
                    timestamp: now,
                },
            );
        }
    }

    /// Render all metrics in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let map = self.series.read().unwrap();
        if map.is_empty() {
            return String::new();
        }

        // Group by metric name for TYPE/HELP lines
        type SeriesList<'a> = Vec<(&'a [(String, String)], &'a SeriesValue)>;
        let mut by_name: HashMap<&str, SeriesList<'_>> = HashMap::new();
        for ((name, labels), val) in map.iter() {
            by_name
                .entry(name.as_str())
                .or_default()
                .push((labels, val));
        }

        let mut out = String::with_capacity(4096);
        let mut names: Vec<&&str> = by_name.keys().collect();
        names.sort();

        for name in names {
            let series_list = &by_name[name];
            let prom_name = sanitize_metric_name(name);
            let prom_type = match series_list[0].1.metric_type {
                "gauge" => "gauge",
                "sum" => "counter",
                "histogram" => "gauge", // we expose hist_sum/hist_count as gauges
                _ => "gauge",
            };

            if series_list[0].1.metric_type == "histogram" {
                // Expose as two metrics: _sum and _count
                out.push_str(&format!("# TYPE {prom_name}_sum {prom_type}\n"));
                out.push_str(&format!("# TYPE {prom_name}_count {prom_type}\n"));
                for (labels, val) in series_list {
                    let label_str = format_labels(labels);
                    if let Some(s) = val.hist_sum {
                        out.push_str(&format!("{prom_name}_sum{label_str} {s}\n"));
                    }
                    if let Some(c) = val.hist_count {
                        out.push_str(&format!("{prom_name}_count{label_str} {c}\n"));
                    }
                }
            } else {
                out.push_str(&format!("# TYPE {prom_name} {prom_type}\n"));
                for (labels, val) in series_list {
                    let label_str = format_labels(labels);
                    let value = val
                        .value_double
                        .map(|v| format!("{v}"))
                        .or_else(|| val.value_int.map(|v| format!("{v}")))
                        .unwrap_or_else(|| "0".to_string());
                    out.push_str(&format!("{prom_name}{label_str} {value}\n"));
                }
            }
        }
        out
    }

    /// Build instant query result for Prometheus API.
    pub fn query_instant(&self, query: &str) -> serde_json::Value {
        let map = self.series.read().unwrap();
        let mut results = Vec::new();

        // Parse simple PromQL: metric_name or metric_name{label="value",...}
        let (metric_name, label_filters) = parse_promql(query);
        let prom_name = sanitize_metric_name(&metric_name);

        for ((name, labels), val) in map.iter() {
            let sname = sanitize_metric_name(name);

            // Match metric name
            if !prom_name.is_empty() && sname != prom_name {
                continue;
            }

            // Match label filters
            if !label_filters
                .iter()
                .all(|(fk, fv)| labels.iter().any(|(lk, lv)| lk == fk && lv == fv))
            {
                continue;
            }

            if val.metric_type == "histogram" {
                // Expose _sum and _count as separate results
                if let Some(s) = val.hist_sum {
                    let m = build_metric_map(&format!("{sname}_sum"), labels);
                    results.push(serde_json::json!({
                        "metric": m,
                        "value": [val.timestamp, s.to_string()]
                    }));
                }
                if let Some(c) = val.hist_count {
                    let m = build_metric_map(&format!("{sname}_count"), labels);
                    results.push(serde_json::json!({
                        "metric": m,
                        "value": [val.timestamp, c.to_string()]
                    }));
                }
            } else {
                let value_str = val
                    .value_double
                    .map(|v| v.to_string())
                    .or_else(|| val.value_int.map(|v| v.to_string()))
                    .unwrap_or_else(|| "0".to_string());
                let m = build_metric_map(&sname, labels);
                results.push(serde_json::json!({
                    "metric": m,
                    "value": [val.timestamp, value_str]
                }));
            }
        }

        serde_json::json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": results
            }
        })
    }

    /// Return all known metric names.
    pub fn label_values_name(&self) -> Vec<String> {
        let map = self.series.read().unwrap();
        let mut names: Vec<String> = map
            .keys()
            .map(|(name, _)| sanitize_metric_name(name))
            .collect();
        names.sort();
        names.dedup();
        names
    }
}

fn build_metric_map(
    name: &str,
    labels: &[(String, String)],
) -> serde_json::Map<String, serde_json::Value> {
    let mut m = serde_json::Map::new();
    m.insert(
        "__name__".to_string(),
        serde_json::Value::String(name.to_string()),
    );
    for (k, v) in labels {
        m.insert(k.clone(), serde_json::Value::String(v.clone()));
    }
    m
}

/// Parse simple PromQL like `metric_name{label="value",label2="value2"}`
fn parse_promql(query: &str) -> (String, Vec<(String, String)>) {
    let query = query.trim();
    if let Some(brace_start) = query.find('{') {
        let name = query[..brace_start].to_string();
        let brace_end = query.rfind('}').unwrap_or(query.len());
        let labels_str = &query[brace_start + 1..brace_end];
        let filters: Vec<(String, String)> = labels_str
            .split(',')
            .filter_map(|pair| {
                let pair = pair.trim();
                let eq = pair.find('=')?;
                let key = pair[..eq].trim().to_string();
                let val = pair[eq + 1..].trim().trim_matches('"').to_string();
                Some((key, val))
            })
            .collect();
        (name, filters)
    } else {
        (query.to_string(), vec![])
    }
}

fn parse_labels(dp_attrs: Option<&str>) -> Vec<(String, String)> {
    let Some(s) = dp_attrs else {
        return vec![];
    };
    let Ok(map) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(s) else {
        return vec![];
    };
    let mut labels: Vec<(String, String)> = map
        .into_iter()
        .map(|(k, v)| {
            let val = match v {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            (sanitize_label_name(&k), val)
        })
        .collect();
    labels.sort_by(|a, b| a.0.cmp(&b.0));
    labels
}

fn format_labels(labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let pairs: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v)))
        .collect();
    format!("{{{}}}", pairs.join(","))
}

/// Replace non-alphanumeric chars with underscores (Prometheus naming rules).
fn sanitize_metric_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn sanitize_label_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn escape_label_value(val: &str) -> String {
    val.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Start the Prometheus-compatible HTTP server.
pub async fn run(store: Arc<MetricsStore>, port: u16) -> Result<(), std::io::Error> {
    use axum::{routing::get, Router};

    let app = Router::new()
        .route("/metrics", get(handle_metrics))
        .route("/api/v1/query", get(handle_query).post(handle_query))
        .route(
            "/api/v1/query_range",
            get(handle_query_range).post(handle_query_range),
        )
        .route(
            "/api/v1/label/__name__/values",
            get(handle_label_values_name),
        )
        .route("/api/v1/labels", get(handle_labels))
        .route("/api/v1/status/buildinfo", get(handle_buildinfo))
        .with_state(store);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!("Prometheus exporter listening on 0.0.0.0:{port}/metrics");

    axum::serve(listener, app).await
}

async fn handle_metrics(
    axum::extract::State(store): axum::extract::State<Arc<MetricsStore>>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    let body = store.render();
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

#[derive(serde::Deserialize)]
struct QueryParams {
    query: Option<String>,
}

async fn handle_query(
    axum::extract::State(store): axum::extract::State<Arc<MetricsStore>>,
    axum::extract::Query(params): axum::extract::Query<QueryParams>,
) -> axum::Json<serde_json::Value> {
    let query = params.query.unwrap_or_default();
    axum::Json(store.query_instant(&query))
}

#[derive(serde::Deserialize)]
struct QueryRangeParams {
    query: Option<String>,
    #[allow(dead_code)]
    start: Option<String>,
    #[allow(dead_code)]
    end: Option<String>,
    #[allow(dead_code)]
    step: Option<String>,
}

async fn handle_query_range(
    axum::extract::State(store): axum::extract::State<Arc<MetricsStore>>,
    axum::extract::Query(params): axum::extract::Query<QueryRangeParams>,
) -> axum::Json<serde_json::Value> {
    // We only have latest values, so return them as a single-point matrix
    let query = params.query.unwrap_or_default();
    let instant = store.query_instant(&query);

    // Convert vector result to matrix format
    let results = instant["data"]["result"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let matrix_results: Vec<serde_json::Value> = results
        .into_iter()
        .map(|mut r| {
            if let Some(val) = r.get("value").cloned() {
                r.as_object_mut()
                    .unwrap()
                    .insert("values".to_string(), serde_json::json!([val]));
                r.as_object_mut().unwrap().remove("value");
            }
            r
        })
        .collect();

    axum::Json(serde_json::json!({
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": matrix_results
        }
    }))
}

async fn handle_label_values_name(
    axum::extract::State(store): axum::extract::State<Arc<MetricsStore>>,
) -> axum::Json<serde_json::Value> {
    let names = store.label_values_name();
    axum::Json(serde_json::json!({
        "status": "success",
        "data": names
    }))
}

async fn handle_labels(
    axum::extract::State(store): axum::extract::State<Arc<MetricsStore>>,
) -> axum::Json<serde_json::Value> {
    let mut labels = vec!["__name__".to_string()];
    let map = store.series.read().unwrap();
    let mut seen = std::collections::HashSet::new();
    for ((_, series_labels), _) in map.iter() {
        for (k, _) in series_labels {
            if seen.insert(k.clone()) {
                labels.push(k.clone());
            }
        }
    }
    labels.sort();
    axum::Json(serde_json::json!({
        "status": "success",
        "data": labels
    }))
}

async fn handle_buildinfo() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "status": "success",
        "data": {
            "version": "otel_dumper",
            "revision": "",
            "branch": "",
            "buildUser": "",
            "buildDate": "",
            "goVersion": ""
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_gauge(name: &str, val: f64, attrs: Option<&str>) -> FlatDataPoint {
        FlatDataPoint {
            timestamp_ns: 1_000_000_000,
            metric_name: name.to_string(),
            metric_type: "gauge",
            resource_attrs: None,
            scope_name: None,
            scope_version: None,
            dp_attrs: attrs.map(|s| s.to_string()),
            value_double: Some(val),
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

    fn make_counter(name: &str, val: i64, attrs: Option<&str>) -> FlatDataPoint {
        FlatDataPoint {
            timestamp_ns: 1_000_000_000,
            metric_name: name.to_string(),
            metric_type: "sum",
            resource_attrs: None,
            scope_name: None,
            scope_version: None,
            dp_attrs: attrs.map(|s| s.to_string()),
            value_double: None,
            value_int: Some(val),
            is_monotonic: Some(true),
            aggregation_temporality: Some(2),
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

    fn make_histogram(name: &str, count: i64, sum: f64, attrs: Option<&str>) -> FlatDataPoint {
        FlatDataPoint {
            timestamp_ns: 1_000_000_000,
            metric_name: name.to_string(),
            metric_type: "histogram",
            resource_attrs: None,
            scope_name: None,
            scope_version: None,
            dp_attrs: attrs.map(|s| s.to_string()),
            value_double: None,
            value_int: None,
            is_monotonic: None,
            aggregation_temporality: Some(2),
            hist_count: Some(count),
            hist_sum: Some(sum),
            hist_min: Some(0.1),
            hist_max: Some(99.0),
            hist_bounds: None,
            hist_counts: None,
            extra_data: None,
            start_timestamp_ns: None,
            flags: 0,
        }
    }

    #[test]
    fn test_empty_store() {
        let store = MetricsStore::new();
        assert_eq!(store.render(), "");
    }

    #[test]
    fn test_gauge_no_labels() {
        let store = MetricsStore::new();
        store.update(&[make_gauge("cpu.usage", 72.5, None)]);
        let output = store.render();
        assert!(output.contains("# TYPE cpu_usage gauge"));
        assert!(output.contains("cpu_usage 72.5"));
    }

    #[test]
    fn test_gauge_with_labels() {
        let store = MetricsStore::new();
        store.update(&[make_gauge(
            "cpu.usage",
            45.0,
            Some(r#"{"host":"node-1","core":"0"}"#),
        )]);
        let output = store.render();
        assert!(output.contains("# TYPE cpu_usage gauge"));
        assert!(output.contains(r#"cpu_usage{core="0",host="node-1"} 45"#));
    }

    #[test]
    fn test_counter() {
        let store = MetricsStore::new();
        store.update(&[make_counter(
            "http.requests",
            42,
            Some(r#"{"method":"GET"}"#),
        )]);
        let output = store.render();
        assert!(output.contains("# TYPE http_requests counter"));
        assert!(output.contains(r#"http_requests{method="GET"} 42"#));
    }

    #[test]
    fn test_histogram_sum_count() {
        let store = MetricsStore::new();
        store.update(&[make_histogram(
            "req.duration",
            100,
            5000.0,
            Some(r#"{"endpoint":"/api"}"#),
        )]);
        let output = store.render();
        assert!(output.contains("# TYPE req_duration_sum gauge"));
        assert!(output.contains("# TYPE req_duration_count gauge"));
        assert!(output.contains(r#"req_duration_sum{endpoint="/api"} 5000"#));
        assert!(output.contains(r#"req_duration_count{endpoint="/api"} 100"#));
    }

    #[test]
    fn test_update_overwrites_latest() {
        let store = MetricsStore::new();
        store.update(&[make_gauge("temp", 10.0, None)]);
        store.update(&[make_gauge("temp", 20.0, None)]);
        let output = store.render();
        assert!(output.contains("temp 20"));
        assert!(!output.contains("temp 10"));
    }

    #[test]
    fn test_multiple_series() {
        let store = MetricsStore::new();
        store.update(&[
            make_gauge("cpu", 50.0, Some(r#"{"host":"a"}"#)),
            make_gauge("cpu", 70.0, Some(r#"{"host":"b"}"#)),
            make_counter("reqs", 100, None),
        ]);
        let output = store.render();
        assert!(output.contains("# TYPE cpu gauge"));
        assert!(output.contains(r#"cpu{host="a"} 50"#));
        assert!(output.contains(r#"cpu{host="b"} 70"#));
        assert!(output.contains("# TYPE reqs counter"));
        assert!(output.contains("reqs 100"));
    }

    #[test]
    fn test_sanitize_metric_name() {
        assert_eq!(sanitize_metric_name("foo.bar-baz"), "foo_bar_baz");
        assert_eq!(sanitize_metric_name("a:b_c"), "a:b_c");
    }

    #[test]
    fn test_escape_label_value() {
        assert_eq!(escape_label_value(r#"a"b"#), r#"a\"b"#);
        assert_eq!(escape_label_value("a\\b"), r#"a\\b"#);
        assert_eq!(escape_label_value("a\nb"), r#"a\nb"#);
    }

    #[test]
    fn test_labels_sorted() {
        let labels = parse_labels(Some(r#"{"z":"1","a":"2","m":"3"}"#));
        assert_eq!(labels[0].0, "a");
        assert_eq!(labels[1].0, "m");
        assert_eq!(labels[2].0, "z");
    }
}
