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
    tokio::spawn(run_writer(
        rx,
        storage,
        batch_size,
        flush_interval,
        max_rows,
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::converter::FlatDataPoint;

    fn make_points(n: usize) -> Vec<FlatDataPoint> {
        (0..n)
            .map(|i| FlatDataPoint {
                timestamp_ns: i as i64,
                metric_name: "test.metric".to_string(),
                metric_type: "gauge",
                resource_attrs: None,
                scope_name: None,
                scope_version: None,
                dp_attrs: None,
                value_double: Some(i as f64),
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
            })
            .collect()
    }

    #[tokio::test]
    async fn test_writer_flushes_on_channel_close() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::new(&dir.path().join("test.db")).unwrap());
        let (tx, rx) = mpsc::channel(100);

        let handle = start_writer(rx, storage.clone(), 10_000, Duration::from_secs(60), 0);

        tx.send(make_points(50)).await.unwrap();
        tx.send(make_points(30)).await.unwrap();
        drop(tx); // close channel

        handle.await.unwrap();
        assert_eq!(storage.count_rows(), 80);
    }

    #[tokio::test]
    async fn test_writer_flushes_on_batch_size() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::new(&dir.path().join("test.db")).unwrap());
        let (tx, rx) = mpsc::channel(100);

        // batch_size=50, so sending 60 points should trigger a flush
        let handle = start_writer(rx, storage.clone(), 50, Duration::from_secs(60), 0);

        tx.send(make_points(60)).await.unwrap();
        // Give writer a moment to process
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Should have flushed at least the first 60
        assert!(storage.count_rows() >= 60);

        drop(tx);
        handle.await.unwrap();
        assert_eq!(storage.count_rows(), 60);
    }

    #[tokio::test]
    async fn test_writer_max_rows() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::new(&dir.path().join("test.db")).unwrap());
        let (tx, rx) = mpsc::channel(100);

        let handle = start_writer(rx, storage.clone(), 10, Duration::from_secs(60), 25);

        tx.send(make_points(15)).await.unwrap();
        tx.send(make_points(15)).await.unwrap();

        handle.await.unwrap();
        // Writer should stop at or after reaching max_rows=25
        let rows = storage.count_rows();
        assert!(rows >= 25, "expected at least 25 rows, got {rows}");
    }

    #[tokio::test]
    async fn test_writer_timer_flush() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::new(&dir.path().join("test.db")).unwrap());
        let (tx, rx) = mpsc::channel(100);

        // Large batch_size but short flush interval
        let handle = start_writer(rx, storage.clone(), 100_000, Duration::from_millis(50), 0);

        tx.send(make_points(10)).await.unwrap();
        // Wait for timer flush
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(storage.count_rows(), 10);

        drop(tx);
        handle.await.unwrap();
    }
}
