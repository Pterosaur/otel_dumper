use clap::Parser;
use otel_dumper::config::Config;
use otel_dumper::retention::RetentionPolicy;
use otel_dumper::{
    grpc_server, http_server, jsonl_writer, prom_exporter, sqlite_api, storage_backend, writer,
};
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
        "Starting otel_dumper: gRPC={}, HTTP={}, db={}, batch_size={}, flush_interval={}ms, db_size_limit={} bytes, db_time_window={}s{}",
        config.grpc_port,
        config.http_port,
        config.db_path.display(),
        config.batch_size,
        config.flush_interval_ms,
        config.db_size,
        config.db_time_window.as_secs_f64(),
        config
            .jsonl_path
            .as_ref()
            .map(|p| format!(", jsonl={}", p.display()))
            .unwrap_or_default(),
    );

    let storage = Arc::new(
        match config.db_format.as_deref() {
            #[cfg(feature = "duckdb")]
            Some("duckdb") => storage_backend::StorageBackend::duckdb(&config.db_path),
            #[cfg(not(feature = "duckdb"))]
            Some("duckdb") => {
                eprintln!(
                    "Error: --db-format duckdb requires the 'duckdb' feature. \
                 Rebuild with: cargo build --features duckdb"
                );
                std::process::exit(1);
            }
            Some("sqlite") => storage_backend::StorageBackend::sqlite(&config.db_path),
            _ => storage_backend::StorageBackend::auto(&config.db_path),
        }
        .expect("Failed to initialize database"),
    );

    tracing::info!("Storage backend: {}", storage.backend_name());

    // Warn if --sqlite-port is used with DuckDB
    if config.sqlite_port.is_some() && storage.backend_name() == "DuckDB" {
        tracing::warn!(
            "--sqlite-port is only supported with SQLite backend; \
             the SQLite query API will not be started"
        );
    }

    let jsonl = config.jsonl_path.as_ref().map(|path| {
        Arc::new(jsonl_writer::JsonlWriter::new(path).expect("Failed to open JSONL output file"))
    });

    let prom_store = config.prom_port.map(|_| {
        let retention = config.prom_history.as_ref().and_then(|s| {
            let d = prom_exporter::parse_duration_str(s);
            if d.is_none() {
                tracing::error!("Invalid --prom-history value: {s:?}");
            }
            d
        });
        if let Some(r) = retention {
            tracing::info!("Prometheus history retention: {}s", r.as_secs());
        }
        Arc::new(prom_exporter::MetricsStore::new(retention))
    });

    let (tx, rx) = tokio::sync::mpsc::channel(config.channel_capacity);
    let retention_policy = RetentionPolicy::new(config.db_size, config.db_time_window);

    let (writer_handle, writer_shutdown) = writer::start_writer(
        rx,
        storage.clone(),
        jsonl,
        prom_store.clone(),
        config.batch_size,
        Duration::from_millis(config.flush_interval_ms),
        config.max_rows,
        retention_policy,
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
    let prom_handle = config.prom_port.map(|port| {
        let store = prom_store.clone().unwrap();
        tokio::spawn(async move {
            if let Err(e) = prom_exporter::run(store, port).await {
                tracing::error!("Prometheus exporter failed: {e}");
            }
        })
    });
    let sqlite_handle = config.sqlite_port.and_then(|port| {
        if storage.backend_name() == "DuckDB" {
            return None;
        }
        let qs = Arc::new(
            sqlite_api::QueryServer::new(&config.db_path)
                .expect("Failed to open read-only SQLite connection for query API"),
        );
        Some(tokio::spawn(async move {
            if let Err(e) = sqlite_api::run(qs, port).await {
                tracing::error!("SQLite query API failed: {e}");
            }
        }))
    });

    // Wait for Ctrl+C, or both servers dying (e.g. port conflict)
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down...");
            // 1. Stop accepting new requests
            grpc_handle.abort();
            http_handle.abort();
            // Wait briefly for servers to stop (don't block forever)
            let _ = tokio::time::timeout(Duration::from_secs(2), grpc_handle).await;
            let _ = tokio::time::timeout(Duration::from_secs(2), http_handle).await;
            if let Some(h) = prom_handle { h.abort(); }
            if let Some(h) = sqlite_handle { h.abort(); }
        }
        _ = async {
            let (_, _) = tokio::join!(&mut grpc_handle, &mut http_handle);
        } => {
            tracing::error!("All servers exited, shutting down...");
        }
    }

    // 2. Signal writer to flush and stop (doesn't depend on channel close)
    let _ = writer_shutdown.send(true);

    // 3. Close channel
    drop(tx);

    // 4. Wait for writer to drain remaining buffer
    tracing::info!("Waiting for writer to flush remaining data...");
    match tokio::time::timeout(Duration::from_secs(10), writer_handle).await {
        Ok(Ok(())) => {
            tracing::info!("Writer drained successfully");
        }
        Ok(Err(e)) => tracing::error!("Writer task error: {e}"),
        Err(_) => tracing::warn!("Writer shutdown timed out after 10s, dropping remaining data"),
    }

    // 5. Build analysis indexes
    tracing::info!("Building analysis indexes...");
    if !config.index_attrs.is_empty() {
        tracing::info!("Indexing dp_attrs keys: {:?}", config.index_attrs);
    }
    if let Err(e) = storage.create_analysis_indexes(&config.index_attrs) {
        tracing::error!("Failed to create indexes: {e}");
    }

    tracing::info!("Shutdown complete.");
    Ok(())
}
