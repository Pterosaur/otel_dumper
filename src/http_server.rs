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

pub async fn run(
    tx: mpsc::Sender<Vec<FlatDataPoint>>,
    port: u16,
) -> Result<(), std::io::Error> {
    let app = Router::new()
        .route("/v1/metrics", post(handle_metrics))
        .with_state(tx);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!("HTTP OTLP server listening on 0.0.0.0:{port}");

    axum::serve(listener, app).await
}

async fn handle_metrics(
    State(tx): State<mpsc::Sender<Vec<FlatDataPoint>>>,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let request = ExportMetricsServiceRequest::decode(body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("protobuf decode error: {e}")))?;

    let points = converter::convert_request(request);
    if !points.is_empty() {
        tx.send(points)
            .await
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "writer closed".into()))?;
    }

    let response = ExportMetricsServiceResponse::default();
    let response_bytes = response.encode_to_vec();

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/x-protobuf")],
        response_bytes,
    ))
}
