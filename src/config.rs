use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "otel_dumper",
    about = "OTLP Metrics receiver that dumps data to SQLite"
)]
pub struct Config {
    /// gRPC server port (OTLP/gRPC)
    #[arg(long, default_value_t = 4317)]
    pub grpc_port: u16,

    /// HTTP server port (OTLP/HTTP)
    #[arg(long, default_value_t = 4318)]
    pub http_port: u16,

    /// SQLite database file path
    #[arg(long, default_value = "metrics.db")]
    pub db_path: PathBuf,

    /// Batch size: flush to SQLite when this many data points accumulate
    #[arg(long, default_value_t = 50_000)]
    pub batch_size: usize,

    /// Flush interval in milliseconds (flush even if batch is not full)
    #[arg(long, default_value_t = 500)]
    pub flush_interval_ms: u64,

    /// Channel capacity (number of batched messages in flight)
    #[arg(long, default_value_t = 10_000)]
    pub channel_capacity: usize,

    /// Maximum total rows to write (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    pub max_rows: u64,
}
