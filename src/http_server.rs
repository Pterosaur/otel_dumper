use crate::converter::{self, FlatDataPoint};
use axum::{
    body::Bytes,
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;
use tokio::sync::mpsc;

pub async fn run(tx: mpsc::Sender<Vec<FlatDataPoint>>, port: u16) -> Result<(), std::io::Error> {
    let router = app(tx);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!("HTTP OTLP server listening on 0.0.0.0:{port}");

    axum::serve(listener, router).await
}

async fn handle_metrics(
    State(tx): State<mpsc::Sender<Vec<FlatDataPoint>>>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let request = ExportMetricsServiceRequest::decode(body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("protobuf decode error: {e}"),
        )
    })?;

    let points = converter::convert_request(request);
    if !points.is_empty() {
        match tx.try_send(points) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("Channel full, dropping batch");
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err((StatusCode::INTERNAL_SERVER_ERROR, "writer closed".into()));
            }
        }
    }

    let response = ExportMetricsServiceResponse::default();
    let response_bytes = response.encode_to_vec();

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/x-protobuf")],
        response_bytes,
    ))
}

/// Build the axum Router (exposed for testing).
pub fn app(tx: mpsc::Sender<Vec<FlatDataPoint>>) -> Router {
    Router::new()
        .route("/v1/metrics", post(handle_metrics))
        .with_state(tx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::converter::FlatDataPoint;
    use axum::body::Body;
    use opentelemetry_proto::tonic::{
        common::v1::{any_value::Value as AnyValueKind, AnyValue, KeyValue},
        metrics::v1::{
            metric::Data, number_data_point::Value as NumberValue, Gauge, Metric, NumberDataPoint,
            ResourceMetrics, ScopeMetrics,
        },
        resource::v1::Resource,
    };
    use tower::ServiceExt;

    fn build_test_request() -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(AnyValueKind::StringValue("test".into())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "test.gauge".into(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: Default::default(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 0,
                                time_unix_nano: 1_000_000_000,
                                value: Some(NumberValue::AsDouble(42.0)),
                                exemplars: vec![],
                                flags: 0,
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[tokio::test]
    async fn test_http_endpoint_success() {
        let (tx, mut rx) = mpsc::channel::<Vec<FlatDataPoint>>(10);
        let app = app(tx);

        let request_proto = build_test_request();
        let body = request_proto.encode_to_vec();

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

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/x-protobuf"
        );

        // Verify data was sent to channel
        let points = rx.recv().await.unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].metric_name, "test.gauge");
        assert_eq!(points[0].value_double, Some(42.0));
    }

    #[tokio::test]
    async fn test_http_endpoint_invalid_protobuf() {
        let (tx, _rx) = mpsc::channel::<Vec<FlatDataPoint>>(10);
        let app = app(tx);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/metrics")
                    .header("content-type", "application/x-protobuf")
                    .body(Body::from(vec![0xFF, 0xFF, 0xFF]))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_http_endpoint_empty_request() {
        let (tx, _rx) = mpsc::channel::<Vec<FlatDataPoint>>(10);
        let app = app(tx);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/metrics")
                    .header("content-type", "application/x-protobuf")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Empty protobuf is valid (all fields optional)
        assert_eq!(response.status(), StatusCode::OK);
    }
}
