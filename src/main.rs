use clap::Parser;
use otel_dumper::config::Config;
use otel_dumper::{grpc_server, http_server, storage, writer};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "otel_dumper=info".parse().unwrap()),
        )
        .init();

    let config = Config::parse();

    tracing::info!(
        "Starting otel_dumper: gRPC={}, HTTP={}, db={}, batch_size={}, flush_interval={}ms",
        config.grpc_port,
        config.http_port,
        config.db_path.display(),
        config.batch_size,
        config.flush_interval_ms,
    );

    let storage = Arc::new(
        storage::Storage::new(&config.db_path).expect("Failed to initialize SQLite database"),
    );

    let (tx, rx) = tokio::sync::mpsc::channel(config.channel_capacity);

    let writer_handle = writer::start_writer(
        rx,
        storage.clone(),
        config.batch_size,
        Duration::from_millis(config.flush_interval_ms),
        config.max_rows,
    );

    let grpc_handle = tokio::spawn(grpc_server::run(tx.clone(), config.grpc_port));
    let http_handle = tokio::spawn(http_server::run(tx.clone(), config.http_port));

    // Drop original sender so only the server tasks hold senders
    drop(tx);

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    tracing::info!("Received Ctrl+C, shutting down...");

    // Abort server tasks — this drops their channel senders
    grpc_handle.abort();
    http_handle.abort();

    // Writer will see channel closed (all senders dropped) and flush remaining data
    if let Err(e) = writer_handle.await {
        tracing::error!("Writer task error: {e}");
    }

    // Build analysis indexes after writing is complete
    tracing::info!("Building analysis indexes...");
    if let Err(e) = storage.create_analysis_indexes() {
        tracing::error!("Failed to create indexes: {e}");
    }

    tracing::info!("Shutdown complete.");
    Ok(())
}
