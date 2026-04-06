# otel_dumper

An OpenTelemetry Collector simulator that receives OTLP Metrics data and dumps it into a SQLite database and/or JSONL file for offline analysis and Grafana visualization.

[中文文档](README_CN.md)

---

## Features

- **Dual protocol support**: gRPC (`:4317`) and HTTP (`:4318`) OTLP endpoints
- **High throughput**: Designed for ~100K data points/sec with batched SQLite writes
- **Dual output format**: SQLite for Grafana queries + optional JSONL for human-readable local inspection
- **Prometheus exporter**: Optional `/metrics` endpoint for real-time Grafana monitoring via SSH tunnel
- **Grafana ready**: Use the [SQLite datasource plugin](https://grafana.com/grafana/plugins/frser-sqlite-datasource/) or built-in Prometheus datasource
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

### Download

Pre-built static binaries are available from [GitHub Releases](https://github.com/Pterosaur/otel_dumper/releases):

```bash
curl -LO https://github.com/Pterosaur/otel_dumper/releases/download/latest/otel_dumper-x86_64-linux
chmod +x otel_dumper-x86_64-linux
./otel_dumper-x86_64-linux --help
```

### Build from source

```bash
# Requires Rust toolchain with musl target
rustup target add x86_64-unknown-linux-musl
cargo build --release

# Output binary (statically linked, ~7.4MB)
ls target/x86_64-unknown-linux-musl/release/otel_dumper
```

### Run

```bash
# Default: SQLite only
./otel_dumper

# With JSONL output for local inspection
./otel_dumper --jsonl-path ./metrics.jsonl

# With Prometheus exporter for real-time Grafana
./otel_dumper --prom-port 9090

# Custom configuration
./otel_dumper \
  --grpc-port 14317 \
  --http-port 14318 \
  --db-path ./metrics.db \
  --jsonl-path ./metrics.jsonl \
  --prom-port 9090 \
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
| `--jsonl-path` | *(none)* | JSONL output file path (optional, for local inspection) |
| `--prom-port` | *(none)* | Prometheus exporter port (optional, exposes `/metrics`) |
| `--prom-history` | *(none)* | Prometheus history retention (e.g. "30 mins", "24 hours") |
| `--sqlite-port` | *(none)* | SQLite query API port (optional, for remote SQL queries) |
| `--batch-size` | `50000` | Flush to SQLite after this many data points |
| `--flush-interval-ms` | `500` | Flush interval even if batch is not full |
| `--channel-capacity` | `10000` | Internal channel buffer size |
| `--max-rows` | `0` | Max rows to write, 0=unlimited |

## SQLite Query API

When `--sqlite-port` is specified, otel_dumper exposes a read-only HTTP API for remote SQL queries against the SQLite database. This gives you **nanosecond precision** timestamps over the network — useful when Grafana is on a different machine.

```bash
# On the target machine (dut)
./otel_dumper --sqlite-port 8080

# On your dev machine, create an SSH tunnel
ssh -L 8080:127.0.0.1:8080 user@dut

# Query from anywhere
curl "http://localhost:8080/api/query?sql=SELECT+timestamp_ns,value_int+FROM+metric_data_points+LIMIT+5"
```

### API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/query?sql=...&limit=10000` | Execute a read-only SELECT query, returns JSON |
| `GET /api/tables` | List all tables |
| `GET /api/schema` | Show metric_data_points column info |
| `GET /health` | Health check |

Only `SELECT` queries are allowed — `INSERT`, `UPDATE`, `DELETE`, `DROP` are rejected.

### Grafana Integration (Infinity Plugin)

1. Install the [Infinity datasource plugin](https://grafana.com/grafana/plugins/yesoreyeram-infinity-datasource/)
2. Add a new Infinity datasource
3. Use **URL** type, set base URL to `http://localhost:8080`
4. Create a panel with URL: `/api/query?sql=SELECT timestamp_ns/1000000000.0 AS time, value_int AS value FROM metric_data_points WHERE metric_name='your_metric' ORDER BY timestamp_ns`

## Prometheus Exporter

When `--prom-port` is specified, otel_dumper exposes a `/metrics` endpoint with the latest values in Prometheus text format. This is useful for real-time Grafana dashboards, especially when Grafana is on a different machine that can only reach the target via SSH.

```bash
# On the target machine (dut)
./otel_dumper --prom-port 9090

# On your dev machine (where Grafana runs), create an SSH tunnel
ssh -L 9090:127.0.0.1:9090 user@dut

# In Grafana: add Prometheus datasource → http://localhost:9090
```

Then query with PromQL:
```
sai_counter_type_1_stat_0{object_name="Ethernet32"}
```

## JSONL Output

When `--jsonl-path` is specified, every data point is also written as a JSON line for easy local inspection:

```bash
./otel_dumper --jsonl-path metrics.jsonl
```

Each line is a self-contained JSON object:

```json
{"timestamp_ns":1712345678000000000,"metric_name":"cpu.usage","metric_type":"gauge","resource_attrs":"{\"service.name\":\"my-app\"}","scope_name":"my-meter","value_double":72.5,"flags":0}
```

Useful for quick inspection with standard tools:

```bash
# Pretty-print the last 5 entries
tail -5 metrics.jsonl | jq .

# Filter by metric name
grep '"metric_name":"cpu.usage"' metrics.jsonl | jq .value_double

# Count data points per metric
jq -r .metric_name metrics.jsonl | sort | uniq -c | sort -rn
```

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
