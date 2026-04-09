use crate::converter::FlatDataPoint;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Key for a unique metric time series: (metric_name, sorted_label_pairs)
type SeriesKey = (String, Vec<(String, String)>);

#[derive(Clone)]
struct Sample {
    timestamp: f64,
    value: f64,
}

struct SeriesData {
    metric_type: &'static str,
    /// For histogram, we store _sum and _count as separate "virtual" series,
    /// so this is used for gauge/sum only.
    samples: VecDeque<Sample>,
}

/// Thread-safe store of metric values with optional history retention.
pub struct MetricsStore {
    series: Mutex<HashMap<SeriesKey, SeriesData>>,
    retention: Option<Duration>,
}

impl Default for MetricsStore {
    fn default() -> Self {
        Self::new(None)
    }
}

impl MetricsStore {
    /// Create a new store. If `retention` is Some, keep historical samples within that window.
    /// If None, only keep the latest value per series.
    pub fn new(retention: Option<Duration>) -> Self {
        MetricsStore {
            series: Mutex::new(HashMap::new()),
            retention,
        }
    }

    /// Update the store with a batch of data points.
    pub fn update(&self, points: &[FlatDataPoint]) {
        let cutoff = self.retention.map(|r| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs_f64()
                - r.as_secs_f64()
        });
        let keep_history = self.retention.is_some();

        // Pre-parse labels with caching (same dp_attrs string → same labels)
        let mut label_cache: HashMap<Option<String>, Vec<(String, String)>> = HashMap::new();

        struct Prepared {
            key: (String, Vec<(String, String)>),
            metric_type: &'static str,
            value: f64,
            timestamp: f64,
        }
        let mut batch: Vec<Prepared> = Vec::with_capacity(points.len() * 2);
        for p in points {
            let labels = label_cache
                .entry(p.dp_attrs.clone())
                .or_insert_with(|| parse_labels(p.dp_attrs.as_deref()))
                .clone();
            let ts = p.timestamp_ns as f64 / 1_000_000_000.0;

            if p.metric_type == "histogram" {
                if let Some(s) = p.hist_sum {
                    batch.push(Prepared {
                        key: (format!("{}_sum", p.metric_name), labels.clone()),
                        metric_type: p.metric_type,
                        value: s,
                        timestamp: ts,
                    });
                }
                if let Some(c) = p.hist_count {
                    batch.push(Prepared {
                        key: (format!("{}_count", p.metric_name), labels),
                        metric_type: p.metric_type,
                        value: c as f64,
                        timestamp: ts,
                    });
                }
            } else {
                let value = p
                    .value_double
                    .unwrap_or_else(|| p.value_int.unwrap_or(0) as f64);
                batch.push(Prepared {
                    key: (p.metric_name.clone(), labels),
                    metric_type: p.metric_type,
                    value,
                    timestamp: ts,
                });
            }
        }

        // Hold the lock only for fast HashMap insertions
        let mut map = self.series.lock().unwrap();
        for prep in batch {
            push_sample(
                &mut map,
                prep.key,
                prep.metric_type,
                prep.timestamp,
                prep.value,
                cutoff,
                keep_history,
            );
        }
    }

    /// Render all metrics in Prometheus text exposition format (latest value only).
    pub fn render(&self) -> String {
        let map = self.series.lock().unwrap();
        if map.is_empty() {
            return String::new();
        }

        // Group by base metric name
        type NameGroup<'a> = Vec<(&'a [(String, String)], &'a SeriesData)>;
        let mut by_name: HashMap<&str, NameGroup<'_>> = HashMap::new();
        for ((name, labels), data) in map.iter() {
            by_name
                .entry(name.as_str())
                .or_default()
                .push((labels, data));
        }

        let mut out = String::with_capacity(4096);
        let mut names: Vec<&&str> = by_name.keys().collect();
        names.sort();

        for name in names {
            let series_list = &by_name[name];
            let prom_name = sanitize_metric_name(name);
            let prom_type = match series_list[0].1.metric_type {
                "sum" => "counter",
                _ => "gauge",
            };
            out.push_str(&format!("# TYPE {prom_name} {prom_type}\n"));
            for (labels, data) in series_list {
                if let Some(sample) = data.samples.back() {
                    let label_str = format_labels(labels);
                    out.push_str(&format!("{prom_name}{label_str} {}\n", sample.value));
                }
            }
        }
        out
    }

    /// Build instant query result for Prometheus API.
    pub fn query_instant(&self, query: &str) -> serde_json::Value {
        let map = self.series.lock().unwrap();
        let (metric_name, label_filters) = parse_promql(query);
        let prom_name = sanitize_metric_name(&metric_name);

        let mut results = Vec::new();
        for ((name, labels), data) in map.iter() {
            let sname = sanitize_metric_name(name);
            if !prom_name.is_empty() && sname != prom_name {
                continue;
            }
            if !matches_filters(labels, &label_filters) {
                continue;
            }
            if let Some(sample) = data.samples.back() {
                let m = build_metric_map(&sname, labels);
                results.push(serde_json::json!({
                    "metric": m,
                    "value": [sample.timestamp, sample.value.to_string()]
                }));
            }
        }

        serde_json::json!({
            "status": "success",
            "data": { "resultType": "vector", "result": results }
        })
    }

    /// Build range query result for Prometheus API (returns full history if retention is enabled).
    pub fn query_range(&self, query: &str, start: f64, end: f64, _step: f64) -> serde_json::Value {
        let (metric_name, label_filters) = parse_promql(query);
        let prom_name = sanitize_metric_name(&metric_name);

        // Copy matching series data out of the lock quickly
        struct MatchedSeries {
            name: String,
            labels: Vec<(String, String)>,
            samples: Vec<Sample>,
            has_history: bool,
        }

        let matched: Vec<MatchedSeries> = {
            let map = self.series.lock().unwrap();
            map.iter()
                .filter(|((name, labels), _)| {
                    let sname = sanitize_metric_name(name);
                    (prom_name.is_empty() || sname == prom_name)
                        && matches_filters(labels, &label_filters)
                })
                .map(|((name, labels), data)| MatchedSeries {
                    name: sanitize_metric_name(name),
                    labels: labels.clone(),
                    samples: data.samples.iter().cloned().collect(),
                    has_history: self.retention.is_some(),
                })
                .collect()
        };
        // Lock released here

        let mut results = Vec::new();
        for series in &matched {
            let values: Vec<serde_json::Value> = if series.has_history && !series.samples.is_empty()
            {
                // Return samples in [start, end] range (already time-ordered)
                series
                    .samples
                    .iter()
                    .filter(|s| s.timestamp >= start && s.timestamp <= end)
                    .map(|s| serde_json::json!([s.timestamp, s.value.to_string()]))
                    .collect()
            } else {
                // No history — return latest as single point
                series
                    .samples
                    .last()
                    .map(|s| vec![serde_json::json!([s.timestamp, s.value.to_string()])])
                    .unwrap_or_default()
            };

            if !values.is_empty() {
                let m = build_metric_map(&series.name, &series.labels);
                results.push(serde_json::json!({
                    "metric": m,
                    "values": values
                }));
            }
        }

        serde_json::json!({
            "status": "success",
            "data": { "resultType": "matrix", "result": results }
        })
    }

    /// Return all known metric names.
    pub fn label_values_name(&self) -> Vec<String> {
        let map = self.series.lock().unwrap();
        let mut names: Vec<String> = map
            .keys()
            .map(|(name, _)| sanitize_metric_name(name))
            .collect();
        names.sort();
        names.dedup();
        names
    }
}

fn push_sample(
    map: &mut HashMap<SeriesKey, SeriesData>,
    key: SeriesKey,
    metric_type: &'static str,
    timestamp: f64,
    value: f64,
    cutoff: Option<f64>,
    keep_history: bool,
) {
    let entry = map.entry(key).or_insert_with(|| SeriesData {
        metric_type,
        samples: VecDeque::new(),
    });
    if keep_history {
        entry.samples.push_back(Sample { timestamp, value });
        // Evict old samples
        if let Some(cutoff) = cutoff {
            while entry.samples.front().is_some_and(|s| s.timestamp < cutoff) {
                entry.samples.pop_front();
            }
        }
    } else {
        // Latest-only mode
        if entry.samples.is_empty() {
            entry.samples.push_back(Sample { timestamp, value });
        } else {
            let s = entry.samples.back_mut().unwrap();
            s.timestamp = timestamp;
            s.value = value;
        }
    }
}

fn matches_filters(labels: &[(String, String)], filters: &[(String, String)]) -> bool {
    filters
        .iter()
        .all(|(fk, fv)| labels.iter().any(|(lk, lv)| lk == fk && lv == fv))
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

/// Parse a human-readable duration string like "30 mins", "24 hours", "5 days", "1h", "30m".
pub fn parse_duration_str(s: &str) -> Option<Duration> {
    let s = s.trim().to_lowercase();
    // Try to split into number and unit
    let (num_str, unit) = if let Some(pos) = s.find(|c: char| c.is_alphabetic()) {
        (s[..pos].trim(), s[pos..].trim())
    } else {
        // Bare number = seconds
        return s.parse::<f64>().ok().map(Duration::from_secs_f64);
    };

    let num: f64 = num_str.parse().ok()?;
    let secs = match unit {
        "s" | "sec" | "secs" | "second" | "seconds" => num,
        "m" | "min" | "mins" | "minute" | "minutes" => num * 60.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => num * 3600.0,
        "d" | "day" | "days" => num * 86400.0,
        "w" | "week" | "weeks" => num * 604800.0,
        _ => return None,
    };
    Some(Duration::from_secs_f64(secs))
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
    start: Option<String>,
    end: Option<String>,
    step: Option<String>,
}

async fn handle_query_range(
    axum::extract::State(store): axum::extract::State<Arc<MetricsStore>>,
    axum::extract::Query(params): axum::extract::Query<QueryRangeParams>,
) -> axum::Json<serde_json::Value> {
    let query = params.query.unwrap_or_default();
    let start = params
        .start
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let end = params
        .end
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs_f64()
        });
    let step = params
        .step
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(15.0);
    axum::Json(store.query_range(&query, start, end, step))
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
    let map = store.series.lock().unwrap();
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
        let store = MetricsStore::new(None);
        assert_eq!(store.render(), "");
    }

    #[test]
    fn test_gauge_no_labels() {
        let store = MetricsStore::new(None);
        store.update(&[make_gauge("cpu.usage", 72.5, None)]);
        let output = store.render();
        assert!(output.contains("# TYPE cpu_usage gauge"));
        assert!(output.contains("cpu_usage 72.5"));
    }

    #[test]
    fn test_gauge_with_labels() {
        let store = MetricsStore::new(None);
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
        let store = MetricsStore::new(None);
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
        let store = MetricsStore::new(None);
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
        let store = MetricsStore::new(None);
        store.update(&[make_gauge("temp", 10.0, None)]);
        store.update(&[make_gauge("temp", 20.0, None)]);
        let output = store.render();
        assert!(output.contains("temp 20"));
        assert!(!output.contains("temp 10"));
    }

    #[test]
    fn test_multiple_series() {
        let store = MetricsStore::new(None);
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

    #[test]
    fn test_parse_duration_str() {
        assert_eq!(
            parse_duration_str("30 mins"),
            Some(Duration::from_secs(1800))
        );
        assert_eq!(
            parse_duration_str("24 hours"),
            Some(Duration::from_secs(86400))
        );
        assert_eq!(
            parse_duration_str("5 days"),
            Some(Duration::from_secs(432000))
        );
        assert_eq!(parse_duration_str("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration_str("30m"), Some(Duration::from_secs(1800)));
        assert_eq!(parse_duration_str("90s"), Some(Duration::from_secs(90)));
        assert_eq!(
            parse_duration_str("1 week"),
            Some(Duration::from_secs(604800))
        );
        assert_eq!(
            parse_duration_str("2weeks"),
            Some(Duration::from_secs(1209600))
        );
        assert!(parse_duration_str("invalid").is_none());
        assert!(parse_duration_str("abc hours").is_none());
    }

    fn make_gauge_at(name: &str, val: f64, attrs: Option<&str>, ts_ns: i64) -> FlatDataPoint {
        FlatDataPoint {
            timestamp_ns: ts_ns,
            ..make_gauge(name, val, attrs)
        }
    }

    #[test]
    fn test_history_mode_keeps_samples() {
        let store = MetricsStore::new(Some(Duration::from_secs(3600)));
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        store.update(&[make_gauge_at("temp", 10.0, None, now_ns)]);
        store.update(&[make_gauge_at("temp", 20.0, None, now_ns + 1_000_000_000)]);
        store.update(&[make_gauge_at("temp", 30.0, None, now_ns + 2_000_000_000)]);

        let now = now_ns as f64 / 1e9;
        let result = store.query_range("temp", now - 1.0, now + 10.0, 0.001);
        let values = result["data"]["result"][0]["values"].as_array().unwrap();
        assert!(
            values.len() >= 3,
            "Expected at least 3 samples, got {}",
            values.len()
        );
    }

    #[test]
    fn test_latest_only_mode_single_point() {
        let store = MetricsStore::new(None);
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        store.update(&[make_gauge_at("temp", 10.0, None, now_ns)]);
        store.update(&[make_gauge_at("temp", 20.0, None, now_ns + 1_000_000_000)]);
        store.update(&[make_gauge_at("temp", 30.0, None, now_ns + 2_000_000_000)]);

        // render should show only latest
        let output = store.render();
        assert!(output.contains("temp 30"));
        assert!(!output.contains("temp 10"));
        assert!(!output.contains("temp 20"));

        // query_range should return only 1 point
        let now = now_ns as f64 / 1e9;
        let result = store.query_range("temp", now - 1.0, now + 10.0, 1.0);
        let values = result["data"]["result"][0]["values"].as_array().unwrap();
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn test_histogram_in_store() {
        let store = MetricsStore::new(None);
        store.update(&[make_histogram(
            "req.duration",
            100,
            5000.0,
            Some(r#"{"ep":"/api"}"#),
        )]);
        let output = store.render();
        // Histograms stored as _sum and _count
        assert!(output.contains("req_duration_sum"));
        assert!(output.contains("req_duration_count"));
        assert!(output.contains("5000"));
        assert!(output.contains("100"));
    }

    #[test]
    fn test_query_instant_with_filter() {
        let store = MetricsStore::new(None);
        store.update(&[
            make_gauge("cpu", 50.0, Some(r#"{"host":"a"}"#)),
            make_gauge("cpu", 70.0, Some(r#"{"host":"b"}"#)),
        ]);
        let result = store.query_instant(r#"cpu{host="a"}"#);
        let results = result["data"]["result"].as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["value"][1], "50");
    }
}
