use crate::converter::FlatDataPoint;
use crate::storage::Storage;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub fn start_writer(
    rx: mpsc::Receiver<Vec<FlatDataPoint>>,
    storage: Arc<Storage>,
    batch_size: usize,
    flush_interval: Duration,
    max_rows: u64,
) -> JoinHandle<()> {
    tokio::spawn(run_writer(rx, storage, batch_size, flush_interval, max_rows))
}

async fn run_writer(
    mut rx: mpsc::Receiver<Vec<FlatDataPoint>>,
    storage: Arc<Storage>,
    batch_size: usize,
    flush_interval: Duration,
    max_rows: u64,
) {
    let mut buffer: Vec<FlatDataPoint> = Vec::with_capacity(batch_size * 2);
    let mut interval = tokio::time::interval(flush_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut total_rows: u64 = 0;

    loop {
        tokio::select! {
            batch = rx.recv() => {
                match batch {
                    Some(points) => {
                        buffer.extend(points);
                        if buffer.len() >= batch_size {
                            total_rows += flush(&storage, &mut buffer).await;
                            if max_rows > 0 && total_rows >= max_rows {
                                tracing::info!("Reached max_rows limit ({max_rows}), stopping writer");
                                break;
                            }
                        }
                    }
                    None => {
                        // Channel closed — flush remaining and exit
                        if !buffer.is_empty() {
                            total_rows += flush(&storage, &mut buffer).await;
                        }
                        break;
                    }
                }
            }
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    total_rows += flush(&storage, &mut buffer).await;
                    if max_rows > 0 && total_rows >= max_rows {
                        tracing::info!("Reached max_rows limit ({max_rows}), stopping writer");
                        break;
                    }
                }
            }
        }
    }

    tracing::info!("Writer finished. Total rows written: {total_rows}");
}

async fn flush(storage: &Arc<Storage>, buffer: &mut Vec<FlatDataPoint>) -> u64 {
    let batch = std::mem::replace(buffer, Vec::with_capacity(buffer.capacity()));
    let count = batch.len() as u64;
    let storage = storage.clone();

    let result = tokio::task::spawn_blocking(move || storage.insert_batch(&batch)).await;

    match result {
        Ok(Ok(n)) => {
            tracing::debug!("Flushed {n} data points to SQLite");
            count
        }
        Ok(Err(e)) => {
            tracing::error!("SQLite write error: {e}");
            0
        }
        Err(e) => {
            tracing::error!("Spawn blocking error: {e}");
            0
        }
    }
}
