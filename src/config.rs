use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "otel_dumper",
    about = "OTLP Metrics receiver that dumps data to SQLite and/or JSONL"
)]
pub struct Config {
    /// gRPC server port (OTLP/gRPC)
    #[arg(long, default_value_t = 4317)]
    pub grpc_port: u16,

    /// HTTP server port (OTLP/HTTP)
    #[arg(long, default_value_t = 4318)]
    pub http_port: u16,

    /// Database file path (.db/.sqlite → SQLite, default/other → DuckDB)
    #[arg(long, default_value = "metrics.duckdb")]
    pub db_path: PathBuf,

    /// JSONL output file path (optional, for human-readable local inspection)
    #[arg(long)]
    pub jsonl_path: Option<PathBuf>,

    /// Prometheus exporter port (optional, exposes /metrics endpoint)
    #[arg(long)]
    pub prom_port: Option<u16>,

    /// Prometheus history retention window (e.g. "30 mins", "24 hours", "5 days").
    /// When set, the exporter keeps historical data points in memory for time-range queries.
    /// Without this, only the latest value per series is kept.
    #[arg(long)]
    pub prom_history: Option<String>,

    /// SQLite query API port (optional, exposes HTTP endpoint for remote SQL queries)
    #[arg(long)]
    pub sqlite_port: Option<u16>,

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

    /// JSON keys in dp_attrs to index for faster queries (comma-separated).
    /// Example: --index-attrs object_name,sai_type_id,sai_stat_id
    #[arg(long, value_delimiter = ',')]
    pub index_attrs: Vec<String>,

    /// Storage backend format: sqlite or duckdb.
    /// Default: auto-detect from db_path extension (.duckdb/.ddb → DuckDB, otherwise SQLite)
    #[arg(long, value_parser = ["sqlite", "duckdb"])]
    pub db_format: Option<String>,
}
