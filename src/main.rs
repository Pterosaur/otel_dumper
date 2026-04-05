use clap::Parser;
use otel_dumper::config::Config;
use otel_dumper::{grpc_server, http_server, jsonl_writer, storage, writer};
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
        "Starting otel_dumper: gRPC={}, HTTP={}, db={}, batch_size={}, flush_interval={}ms{}",
        config.grpc_port,
        config.http_port,
        config.db_path.display(),
        config.batch_size,
        config.flush_interval_ms,
        config
            .jsonl_path
            .as_ref()
            .map(|p| format!(", jsonl={}", p.display()))
            .unwrap_or_default(),
    );

    let storage = Arc::new(
        storage::Storage::new(&config.db_path).expect("Failed to initialize SQLite database"),
    );

    let jsonl = config.jsonl_path.as_ref().map(|path| {
        Arc::new(jsonl_writer::JsonlWriter::new(path).expect("Failed to open JSONL output file"))
    });

    let (tx, rx) = tokio::sync::mpsc::channel(config.channel_capacity);

    let writer_handle = writer::start_writer(
        rx,
        storage.clone(),
        jsonl,
        config.batch_size,
        Duration::from_millis(config.flush_interval_ms),
        config.max_rows,
    );

    let mut grpc_handle = tokio::spawn({
        let tx = tx.clone();
        let port = config.grpc_port;
        async move {
            if let Err(e) = grpc_server::run(tx, port).await {
                tracing::error!("gRPC server failed: {e}");
            }
        }
    });
    let mut http_handle = tokio::spawn({
        let tx = tx.clone();
        let port = config.http_port;
        async move {
            if let Err(e) = http_server::run(tx, port).await {
                tracing::error!("HTTP server failed: {e}");
            }
        }
    });

    // Wait for Ctrl+C, or both servers dying (e.g. port conflict)
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down...");
        }
        _ = async {
            let (_, _) = tokio::join!(&mut grpc_handle, &mut http_handle);
        } => {
            tracing::error!("All servers exited, shutting down...");
        }
    }

    // Abort any still-running server tasks, then drop all senders
    grpc_handle.abort();
    http_handle.abort();
    drop(tx);

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
