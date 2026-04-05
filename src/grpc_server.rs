use crate::converter::{self, FlatDataPoint};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_server::{MetricsService, MetricsServiceServer},
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

pub struct OtlpMetricsService {
    tx: mpsc::Sender<Vec<FlatDataPoint>>,
}

#[tonic::async_trait]
impl MetricsService for OtlpMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let inner = request.into_inner();
        let points = converter::convert_request(inner);
        if !points.is_empty() {
            self.tx
                .send(points)
                .await
                .map_err(|_| Status::internal("writer channel closed"))?;
        }
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

pub async fn run(
    tx: mpsc::Sender<Vec<FlatDataPoint>>,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: std::net::SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
    let service = OtlpMetricsService { tx };

    // Verify we can bind before claiming success
    let incoming = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("gRPC OTLP server listening on {addr}");

    let mut server = tonic::transport::Server::builder();
    let router = server.add_service(MetricsServiceServer::new(service));
    router
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(incoming))
        .await?;
    Ok(())
}
