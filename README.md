# otel_dumper

An OpenTelemetry Collector simulator that receives OTLP Metrics data and dumps it into a SQLite database for offline analysis and Grafana visualization.

[中文文档](README_CN.md)

---

## Features

- **Dual protocol support**: gRPC (`:4317`) and HTTP (`:4318`) OTLP endpoints
- **High throughput**: Designed for ~100K data points/sec with batched SQLite writes
- **Grafana ready**: Use the [SQLite datasource plugin](https://grafana.com/grafana/plugins/frser-sqlite-datasource/) to query and visualize directly — no custom code needed
- **Single static binary**: Statically linked with musl, just `scp` to any Linux machine and run
- **All metric types**: Gauge, Sum (Counter), Histogram, Exponential Histogram, Summary

## Architecture

```
Client (OTLP)
    │
    ├── gRPC :4317 ──► tonic MetricsService
    │                        │
    └── HTTP :4318 ──► axum /v1/metrics
                             │
                        tokio::mpsc channel (bounded)
                             │
                        Batch Writer (background task)
                             │
                        SQLite (WAL mode, batch transactions)
```

## Quick Start

### Build

```bash
# Requires Rust toolchain with musl target
rustup target add x86_64-unknown-linux-musl
cargo build --release

# Output binary (statically linked, ~7.4MB)
ls target/x86_64-unknown-linux-musl/release/otel_dumper
```

### Run

```bash
# Default ports: gRPC=4317, HTTP=4318
./otel_dumper

# Custom configuration
./otel_dumper \
  --grpc-port 14317 \
  --http-port 14318 \
  --db-path ./metrics.db \
  --batch-size 50000 \
  --flush-interval-ms 500 \
  --max-rows 100000000
```

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--grpc-port` | `4317` | gRPC OTLP server port |
| `--http-port` | `4318` | HTTP OTLP server port |
| `--db-path` | `metrics.db` | SQLite database file path |
| `--batch-size` | `50000` | Flush to SQLite after this many data points |
| `--flush-interval-ms` | `500` | Flush interval even if batch is not full |
| `--channel-capacity` | `10000` | Internal channel buffer size |
| `--max-rows` | `0` | Max rows to write, 0=unlimited |

## Grafana Integration

### Setup

1. Install the [SQLite datasource plugin](https://grafana.com/grafana/plugins/frser-sqlite-datasource/):
   ```bash
   grafana-cli plugins install frser-sqlite-datasource
   ```
2. Add a new SQLite datasource in Grafana, point it to your `metrics.db` file.

### Example Queries

**Time series chart:**
```sql
SELECT timestamp_ns / 1000000000 AS time, value_double AS value
FROM metric_data_points
WHERE metric_name = 'cpu.usage'
  AND timestamp_ns BETWEEN ${__from:date:seconds} * 1000000000
                       AND ${__to:date:seconds} * 1000000000
ORDER BY timestamp_ns
```

**Histogram heatmap:**
```sql
SELECT timestamp_ns / 1000000000 AS time,
       hist_bounds, hist_counts
FROM metric_data_points
WHERE metric_name = 'request.duration' AND metric_type = 'histogram'
  AND timestamp_ns BETWEEN ${__from:date:seconds} * 1000000000
                       AND ${__to:date:seconds} * 1000000000
ORDER BY timestamp_ns
```

**List all metric names:**
```sql
SELECT DISTINCT metric_name, metric_type, COUNT(*) as count
FROM metric_data_points
GROUP BY metric_name, metric_type
ORDER BY count DESC
```

### Post-collection Index Optimization

The program automatically creates analysis indexes on shutdown. If you need to create them manually:

```bash
sqlite3 metrics.db "CREATE INDEX IF NOT EXISTS idx_name_ts ON metric_data_points(metric_name, timestamp_ns);"
```

## SQLite Schema

```sql
CREATE TABLE metric_data_points (
    id                      INTEGER PRIMARY KEY,
    timestamp_ns            INTEGER NOT NULL,       -- Unix nanoseconds
    metric_name             TEXT NOT NULL,           -- e.g. "cpu.usage"
    metric_type             TEXT NOT NULL,           -- gauge, sum, histogram, exp_histogram, summary
    resource_attrs          TEXT,                    -- JSON string
    scope_name              TEXT,
    scope_version           TEXT,
    dp_attrs                TEXT,                    -- JSON string, data point attributes
    value_double            REAL,                    -- Gauge/Sum double value
    value_int               INTEGER,                -- Gauge/Sum int value
    is_monotonic            INTEGER,                -- Sum: 0 or 1
    aggregation_temporality INTEGER,                -- 1=delta, 2=cumulative
    hist_count              INTEGER,                -- Histogram/Summary count
    hist_sum                REAL,                    -- Histogram/Summary sum
    hist_min                REAL,
    hist_max                REAL,
    hist_bounds             TEXT,                    -- JSON array: explicit bucket boundaries
    hist_counts             TEXT,                    -- JSON array: bucket counts
    extra_data              TEXT,                    -- JSON for exp_histogram/summary specifics
    start_timestamp_ns      INTEGER,
    flags                   INTEGER DEFAULT 0
);
```

## Performance Tuning

SQLite is configured with the following PRAGMAs for maximum write throughput:

- `journal_mode = WAL` — Write-Ahead Logging, concurrent reads during writes
- `synchronous = NORMAL` — Acceptable for a dump tool
- `cache_size = -64000` — 64MB page cache
- `mmap_size = 268435456` — 256MB memory-mapped I/O

For sustained 100K dp/s ingestion, recommended settings:

```bash
./otel_dumper --batch-size 50000 --flush-interval-ms 500 --channel-capacity 10000
```

## License

MIT
