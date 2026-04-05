# otel_dumper

An OpenTelemetry Collector simulator that receives OTLP Metrics data and dumps it into a SQLite database for offline analysis and Grafana visualization.

一个 OpenTelemetry Collector 模拟器，接收 OTLP Metrics 数据并落盘到 SQLite 数据库，用于离线分析和 Grafana 可视化。

---

## Features / 功能特性

- **Dual protocol support / 双协议支持**: gRPC (`:4317`) and HTTP (`:4318`) OTLP endpoints
- **High throughput / 高吞吐**: Designed for ~100K data points/sec with batched SQLite writes
- **Grafana ready / Grafana 就绪**: Use the [SQLite datasource plugin](https://grafana.com/grafana/plugins/frser-sqlite-datasource/) to query and visualize directly — no custom code needed
- **Single static binary / 单文件静态二进制**: Statically linked with musl, just `scp` to any Linux machine and run
- **All metric types / 全指标类型**: Gauge, Sum (Counter), Histogram, Exponential Histogram, Summary

## Architecture / 架构

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

## Quick Start / 快速开始

### Build / 构建

```bash
# Requires Rust toolchain with musl target
# 需要 Rust 工具链和 musl 目标
rustup target add x86_64-unknown-linux-musl
cargo build --release

# Output binary (statically linked, ~7.4MB)
# 输出二进制（全静态链接，约 7.4MB）
ls target/x86_64-unknown-linux-musl/release/otel_dumper
```

### Run / 运行

```bash
# Default ports: gRPC=4317, HTTP=4318
# 默认端口：gRPC=4317, HTTP=4318
./otel_dumper

# Custom configuration / 自定义配置
./otel_dumper \
  --grpc-port 14317 \
  --http-port 14318 \
  --db-path ./metrics.db \
  --batch-size 50000 \
  --flush-interval-ms 500 \
  --max-rows 100000000
```

### CLI Options / 命令行参数

| Option | Default | Description / 描述 |
|--------|---------|-------------------|
| `--grpc-port` | `4317` | gRPC OTLP server port / gRPC 端口 |
| `--http-port` | `4318` | HTTP OTLP server port / HTTP 端口 |
| `--db-path` | `metrics.db` | SQLite database file path / 数据库文件路径 |
| `--batch-size` | `50000` | Flush to SQLite after this many data points / 批量写入阈值 |
| `--flush-interval-ms` | `500` | Flush interval even if batch is not full / 定时刷盘间隔(ms) |
| `--channel-capacity` | `10000` | Internal channel buffer size / 内部通道缓冲大小 |
| `--max-rows` | `0` | Max rows to write, 0=unlimited / 最大行数，0=不限 |

## Grafana Integration / Grafana 集成

### Setup / 设置

1. Install the [SQLite datasource plugin](https://grafana.com/grafana/plugins/frser-sqlite-datasource/):
   安装 [SQLite 数据源插件](https://grafana.com/grafana/plugins/frser-sqlite-datasource/)：
   ```bash
   grafana-cli plugins install frser-sqlite-datasource
   ```
2. Add a new SQLite datasource in Grafana, point it to your `metrics.db` file.
   在 Grafana 中添加 SQLite 数据源，指向你的 `metrics.db` 文件。

### Example Queries / 查询示例

**Time series chart / 时序曲线图:**
```sql
SELECT timestamp_ns / 1000000000 AS time, value_double AS value
FROM metric_data_points
WHERE metric_name = 'cpu.usage'
  AND timestamp_ns BETWEEN ${__from:date:seconds} * 1000000000
                       AND ${__to:date:seconds} * 1000000000
ORDER BY timestamp_ns
```

**Histogram heatmap / 直方图热力图:**
```sql
SELECT timestamp_ns / 1000000000 AS time,
       hist_bounds, hist_counts
FROM metric_data_points
WHERE metric_name = 'request.duration' AND metric_type = 'histogram'
  AND timestamp_ns BETWEEN ${__from:date:seconds} * 1000000000
                       AND ${__to:date:seconds} * 1000000000
ORDER BY timestamp_ns
```

**List all metric names / 列出所有指标名:**
```sql
SELECT DISTINCT metric_name, metric_type, COUNT(*) as count
FROM metric_data_points
GROUP BY metric_name, metric_type
ORDER BY count DESC
```

### Post-collection Index Optimization / 采集后索引优化

The program automatically creates analysis indexes on shutdown. If you need to create them manually:
程序在关闭时会自动创建分析索引。如需手动创建：

```bash
sqlite3 metrics.db "CREATE INDEX IF NOT EXISTS idx_name_ts ON metric_data_points(metric_name, timestamp_ns);"
```

## SQLite Schema / 数据库表结构

```sql
CREATE TABLE metric_data_points (
    id                      INTEGER PRIMARY KEY,
    timestamp_ns            INTEGER NOT NULL,       -- Unix nanoseconds / 纳秒时间戳
    metric_name             TEXT NOT NULL,           -- e.g. "cpu.usage"
    metric_type             TEXT NOT NULL,           -- gauge, sum, histogram, exp_histogram, summary
    resource_attrs          TEXT,                    -- JSON string
    scope_name              TEXT,
    scope_version           TEXT,
    dp_attrs                TEXT,                    -- JSON string, data point attributes / 数据点属性
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

## Performance Tuning / 性能调优

SQLite is configured with the following PRAGMAs for maximum write throughput:
SQLite 使用以下 PRAGMA 配置以最大化写入吞吐：

- `journal_mode = WAL` — Write-Ahead Logging, concurrent reads during writes / 写前日志，写入时可并发读取
- `synchronous = NORMAL` — Acceptable for a dump tool / dump 工具可接受的同步级别
- `cache_size = -64000` — 64MB page cache / 64MB 页缓存
- `mmap_size = 268435456` — 256MB memory-mapped I/O

For sustained 100K dp/s ingestion, recommended settings:
持续 10 万 dp/s 写入时的推荐配置：

```bash
./otel_dumper --batch-size 50000 --flush-interval-ms 500 --channel-capacity 10000
```

## License / 许可证

MIT
