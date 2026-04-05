//! End-to-end test: start full server stack, send OTLP metrics via both
//! gRPC and HTTP, verify data lands correctly in SQLite.

use otel_dumper::{grpc_server, http_server, storage, writer};

use opentelemetry_proto::tonic::{
    collector::metrics::v1::{
        metrics_service_client::MetricsServiceClient, ExportMetricsServiceRequest,
    },
    common::v1::{any_value::Value as AnyValueKind, AnyValue, InstrumentationScope, KeyValue},
    metrics::v1::{
        metric::Data, number_data_point::Value as NumberValue, Gauge, Histogram,
        HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    },
    resource::v1::Resource,
};
use prost::Message;
use rusqlite::Connection;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

fn get_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn make_kv(key: &str, val: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueKind::StringValue(val.to_string())),
        }),
    }
}

fn make_resource() -> Resource {
    Resource {
        attributes: vec![
            make_kv("service.name", "e2e-test-service"),
            make_kv("host.name", "test-host"),
        ],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    }
}

fn make_scope() -> InstrumentationScope {
    InstrumentationScope {
        name: "e2e-test-meter".into(),
        version: "1.0.0".into(),
        attributes: vec![],
        dropped_attributes_count: 0,
    }
}

fn build_gauge_request(name: &str, values: &[(u64, f64, &str)]) -> ExportMetricsServiceRequest {
    let data_points: Vec<NumberDataPoint> = values
        .iter()
        .map(|(ts, val, attr_val)| NumberDataPoint {
            attributes: vec![make_kv("source", attr_val)],
            start_time_unix_nano: 0,
            time_unix_nano: *ts,
            value: Some(NumberValue::AsDouble(*val)),
            exemplars: vec![],
            flags: 0,
        })
        .collect();

    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(make_resource()),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(make_scope()),
                metrics: vec![Metric {
                    name: name.into(),
                    description: "E2E test gauge".into(),
                    unit: "%".into(),
                    metadata: Default::default(),
                    data: Some(Data::Gauge(Gauge { data_points })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn build_counter_request(name: &str, value: i64, ts: u64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(make_resource()),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(make_scope()),
                metrics: vec![Metric {
                    name: name.into(),
                    description: "E2E test counter".into(),
                    unit: "1".into(),
                    metadata: Default::default(),
                    data: Some(Data::Sum(Sum {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![make_kv("method", "GET")],
                            start_time_unix_nano: 1_000_000_000,
                            time_unix_nano: ts,
                            value: Some(NumberValue::AsInt(value)),
                            exemplars: vec![],
                            flags: 0,
                        }],
                        aggregation_temporality: 2, // cumulative
                        is_monotonic: true,
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn build_histogram_request(name: &str, ts: u64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(make_resource()),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(make_scope()),
                metrics: vec![Metric {
                    name: name.into(),
                    description: "E2E test histogram".into(),
                    unit: "ms".into(),
                    metadata: Default::default(),
                    data: Some(Data::Histogram(Histogram {
                        data_points: vec![HistogramDataPoint {
                            attributes: vec![make_kv("endpoint", "/api/data")],
                            start_time_unix_nano: 1_000_000_000,
                            time_unix_nano: ts,
                            count: 150,
                            sum: Some(7500.0),
                            bucket_counts: vec![10, 30, 50, 40, 15, 5],
                            explicit_bounds: vec![1.0, 5.0, 10.0, 50.0, 100.0],
                            exemplars: vec![],
                            flags: 0,
                            min: Some(0.2),
                            max: Some(250.0),
                        }],
                        aggregation_temporality: 2, // cumulative
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Send an ExportMetricsServiceRequest through the HTTP handler via tower
async fn send_via_http(
    tx: &mpsc::Sender<Vec<otel_dumper::converter::FlatDataPoint>>,
    request: &ExportMetricsServiceRequest,
) {
    use axum::body::Body;
    use tower::ServiceExt;

    let app = http_server::app(tx.clone());
    let body = request.encode_to_vec();

    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/metrics")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status().as_u16(),
        200,
        "HTTP request failed with status {}",
        response.status()
    );
}

/// Wait until a TCP port is accepting connections
async fn wait_for_port(port: u16, timeout: Duration) {
    let start = tokio::time::Instant::now();
    loop {
        if tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .is_ok()
        {
            return;
        }
        if start.elapsed() > timeout {
            panic!("Timed out waiting for port {port}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_e2e_full_pipeline() {
    let grpc_port = get_free_port();

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("e2e_test.db");

    // --- Start the full server stack ---
    let storage = Arc::new(storage::Storage::new(&db_path).unwrap());
    let (tx, rx) = mpsc::channel(1000);

    let writer_handle = writer::start_writer(
        rx,
        storage.clone(),
        100, // small batch for testing
        Duration::from_millis(100),
        0,
    );

    // Start gRPC server (network E2E)
    let grpc_tx = tx.clone();
    let grpc_handle = tokio::spawn(async move {
        if let Err(e) = grpc_server::run(grpc_tx, grpc_port).await {
            eprintln!("E2E: gRPC server error: {e}");
        }
    });

    wait_for_port(grpc_port, Duration::from_secs(5)).await;

    // --- Send metrics via HTTP (tower oneshot — full handler pipeline) ---
    let gauge_req = build_gauge_request(
        "e2e.cpu_usage",
        &[
            (1_000_000_000, 45.0, "http"),
            (2_000_000_000, 62.5, "http"),
            (3_000_000_000, 51.2, "http"),
        ],
    );
    send_via_http(&tx, &gauge_req).await;

    let counter_req = build_counter_request("e2e.http_requests", 42, 2_000_000_000);
    send_via_http(&tx, &counter_req).await;

    let hist_req = build_histogram_request("e2e.request_duration", 3_000_000_000);
    send_via_http(&tx, &hist_req).await;

    // --- Send metrics via gRPC (network E2E) ---
    let mut grpc_client = MetricsServiceClient::connect(format!("http://127.0.0.1:{grpc_port}"))
        .await
        .expect("gRPC client connect failed");

    let grpc_gauge_req = build_gauge_request(
        "e2e.mem_usage",
        &[(4_000_000_000, 70.0, "grpc"), (5_000_000_000, 75.5, "grpc")],
    );
    let grpc_resp = grpc_client
        .export(grpc_gauge_req)
        .await
        .expect("gRPC gauge export failed");
    assert_eq!(grpc_resp.into_inner(), Default::default());

    let grpc_counter_req = build_counter_request("e2e.grpc_calls", 100, 5_000_000_000);
    grpc_client
        .export(grpc_counter_req)
        .await
        .expect("gRPC counter export failed");

    // --- Shutdown ---
    // Wait for writer to flush
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Drop the tx we hold, abort gRPC server to drop its tx
    drop(tx);
    drop(grpc_client);
    grpc_handle.abort();

    // Writer finishes when all senders are dropped
    writer_handle.await.unwrap();

    // Create analysis indexes
    storage.create_analysis_indexes().unwrap();

    // --- Verify SQLite data ---
    let conn = Connection::open(&db_path).unwrap();

    // 1. Check total row count: 3 gauge + 1 counter + 1 histogram + 2 gauge + 1 counter = 8
    let total: i64 = conn
        .query_row("SELECT COUNT(*) FROM metric_data_points", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total, 8, "Expected 8 total rows, got {total}");

    // 2. Verify gauge data (HTTP)
    let cpu_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM metric_data_points WHERE metric_name = 'e2e.cpu_usage' AND metric_type = 'gauge'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(cpu_count, 3);

    let (cpu_val, cpu_attrs): (f64, String) = conn
        .query_row(
            "SELECT value_double, dp_attrs FROM metric_data_points WHERE metric_name = 'e2e.cpu_usage' ORDER BY timestamp_ns LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(cpu_val, 45.0);
    assert!(cpu_attrs.contains("http"));

    // 3. Verify counter data (HTTP)
    let (counter_val, counter_mono, counter_temp): (i64, i32, i32) = conn
        .query_row(
            "SELECT value_int, is_monotonic, aggregation_temporality FROM metric_data_points WHERE metric_name = 'e2e.http_requests'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(counter_val, 42);
    assert_eq!(counter_mono, 1); // true
    assert_eq!(counter_temp, 2); // cumulative

    // 4. Verify histogram data (HTTP)
    let (h_count, h_sum, h_min, h_max, h_bounds, h_counts): (
        i64,
        f64,
        f64,
        f64,
        String,
        String,
    ) = conn
        .query_row(
            "SELECT hist_count, hist_sum, hist_min, hist_max, hist_bounds, hist_counts FROM metric_data_points WHERE metric_name = 'e2e.request_duration'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
        )
        .unwrap();
    assert_eq!(h_count, 150);
    assert_eq!(h_sum, 7500.0);
    assert_eq!(h_min, 0.2);
    assert_eq!(h_max, 250.0);
    let bounds: Vec<f64> = serde_json::from_str(&h_bounds).unwrap();
    assert_eq!(bounds, vec![1.0, 5.0, 10.0, 50.0, 100.0]);
    let counts: Vec<u64> = serde_json::from_str(&h_counts).unwrap();
    assert_eq!(counts, vec![10, 30, 50, 40, 15, 5]);

    // 5. Verify gRPC gauge data
    let mem_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM metric_data_points WHERE metric_name = 'e2e.mem_usage' AND metric_type = 'gauge'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(mem_count, 2);

    let grpc_attrs: String = conn
        .query_row(
            "SELECT dp_attrs FROM metric_data_points WHERE metric_name = 'e2e.mem_usage' LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(grpc_attrs.contains("grpc"));

    // 6. Verify resource attributes are preserved
    let res_attrs: String = conn
        .query_row(
            "SELECT resource_attrs FROM metric_data_points LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(res_attrs.contains("e2e-test-service"));
    assert!(res_attrs.contains("test-host"));

    // 7. Verify scope metadata
    let (scope_name, scope_ver): (String, String) = conn
        .query_row(
            "SELECT scope_name, scope_version FROM metric_data_points LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(scope_name, "e2e-test-meter");
    assert_eq!(scope_ver, "1.0.0");

    // 8. Verify all metric types are present
    let mut stmt = conn
        .prepare("SELECT DISTINCT metric_type FROM metric_data_points ORDER BY metric_type")
        .unwrap();
    let types: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(types, vec!["gauge", "histogram", "sum"]);

    // 9. Verify timestamps are ordered correctly
    let timestamps: Vec<i64> = {
        let mut stmt = conn
            .prepare(
                "SELECT timestamp_ns FROM metric_data_points WHERE metric_name = 'e2e.cpu_usage' ORDER BY timestamp_ns",
            )
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    assert_eq!(
        timestamps,
        vec![1_000_000_000i64, 2_000_000_000, 3_000_000_000]
    );

    // 10. Verify analysis indexes were created
    let index_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='metric_data_points'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        index_count >= 3,
        "Expected at least 3 indexes (idx_ts + 2 analysis), got {index_count}"
    );
}
