# otel_dumper

一个 OpenTelemetry Collector 模拟器，接收 OTLP Metrics 数据并落盘到 SQLite 数据库和/或 JSONL 文件，用于离线分析和 Grafana 可视化。

[English](README.md)

---

## 功能特性

- **双协议支持**: gRPC (`:4317`) 和 HTTP (`:4318`) OTLP 端点
- **高吞吐**: 针对 ~10万 data points/秒 设计，批量写入 SQLite
- **双输出格式**: SQLite 用于 Grafana 查询 + 可选 JSONL 用于本地直观阅读
- **Grafana 就绪**: 使用 [SQLite 数据源插件](https://grafana.com/grafana/plugins/frser-sqlite-datasource/) 直接查询和可视化，无需额外代码
- **单文件静态二进制**: 使用 musl 全静态链接，直接 `scp` 到任意 Linux 机器运行
- **全指标类型**: Gauge、Sum (Counter)、Histogram、Exponential Histogram、Summary

## 架构

```
Client (OTLP)
    │
    ├── gRPC :4317 ──► tonic MetricsService
    │                        │
    └── HTTP :4318 ──► axum /v1/metrics
                             │
                        tokio::mpsc channel (有界缓冲)
                             │
                        Batch Writer (后台任务)
                             │
                        SQLite (WAL 模式, 批量事务)
```

## 快速开始

### 构建

```bash
# 需要 Rust 工具链和 musl 目标
rustup target add x86_64-unknown-linux-musl
cargo build --release

# 输出二进制（全静态链接，约 7.4MB）
ls target/x86_64-unknown-linux-musl/release/otel_dumper
```

### 运行

```bash
# 默认：仅 SQLite 输出
./otel_dumper

# 同时输出 JSONL，方便本地阅读
./otel_dumper --jsonl-path ./metrics.jsonl

# 自定义配置
./otel_dumper \
  --grpc-port 14317 \
  --http-port 14318 \
  --db-path ./metrics.db \
  --jsonl-path ./metrics.jsonl \
  --batch-size 50000 \
  --flush-interval-ms 500 \
  --max-rows 100000000
```

### 命令行参数

| 参数 | 默认值 | 描述 |
|------|--------|------|
| `--grpc-port` | `4317` | gRPC OTLP 服务端口 |
| `--http-port` | `4318` | HTTP OTLP 服务端口 |
| `--db-path` | `metrics.db` | SQLite 数据库文件路径 |
| `--jsonl-path` | *（无）* | JSONL 输出文件路径（可选，用于本地直观阅读） |
| `--batch-size` | `50000` | 积累多少数据点后批量写入 SQLite |
| `--flush-interval-ms` | `500` | 定时刷盘间隔（毫秒），即使批次未满也会写入 |
| `--channel-capacity` | `10000` | 内部通道缓冲大小 |
| `--max-rows` | `0` | 最大写入行数，0 表示不限制 |

## JSONL 输出

指定 `--jsonl-path` 后，每个数据点会同时以 JSON 行的形式写入文件，方便本地直观阅读：

```bash
./otel_dumper --jsonl-path metrics.jsonl
```

每行是一个完整的 JSON 对象：

```json
{"timestamp_ns":1712345678000000000,"metric_name":"cpu.usage","metric_type":"gauge","resource_attrs":"{\"service.name\":\"my-app\"}","scope_name":"my-meter","value_double":72.5,"flags":0}
```

可用标准工具快速检索：

```bash
# 美化打印最后 5 条
tail -5 metrics.jsonl | jq .

# 按指标名过滤
grep '"metric_name":"cpu.usage"' metrics.jsonl | jq .value_double

# 统计每个指标的数据点数
jq -r .metric_name metrics.jsonl | sort | uniq -c | sort -rn
```

## Grafana 集成

### 设置

1. 安装 [SQLite 数据源插件](https://grafana.com/grafana/plugins/frser-sqlite-datasource/)：
   ```bash
   grafana-cli plugins install frser-sqlite-datasource
   ```
2. 在 Grafana 中添加 SQLite 数据源，指向你的 `metrics.db` 文件。

### 查询示例

**时序曲线图：**
```sql
SELECT timestamp_ns / 1000000000 AS time, value_double AS value
FROM metric_data_points
WHERE metric_name = 'cpu.usage'
  AND timestamp_ns BETWEEN ${__from:date:seconds} * 1000000000
                       AND ${__to:date:seconds} * 1000000000
ORDER BY timestamp_ns
```

**直方图热力图：**
```sql
SELECT timestamp_ns / 1000000000 AS time,
       hist_bounds, hist_counts
FROM metric_data_points
WHERE metric_name = 'request.duration' AND metric_type = 'histogram'
  AND timestamp_ns BETWEEN ${__from:date:seconds} * 1000000000
                       AND ${__to:date:seconds} * 1000000000
ORDER BY timestamp_ns
```

**列出所有指标名：**
```sql
SELECT DISTINCT metric_name, metric_type, COUNT(*) as count
FROM metric_data_points
GROUP BY metric_name, metric_type
ORDER BY count DESC
```

### 采集后索引优化

程序在关闭时会自动创建分析索引。如需手动创建：

```bash
sqlite3 metrics.db "CREATE INDEX IF NOT EXISTS idx_name_ts ON metric_data_points(metric_name, timestamp_ns);"
```

## 数据库表结构

```sql
CREATE TABLE metric_data_points (
    id                      INTEGER PRIMARY KEY,
    timestamp_ns            INTEGER NOT NULL,       -- 纳秒时间戳
    metric_name             TEXT NOT NULL,           -- 如 "cpu.usage"
    metric_type             TEXT NOT NULL,           -- gauge, sum, histogram, exp_histogram, summary
    resource_attrs          TEXT,                    -- JSON 字符串
    scope_name              TEXT,
    scope_version           TEXT,
    dp_attrs                TEXT,                    -- JSON 字符串，数据点属性
    value_double            REAL,                    -- Gauge/Sum 浮点值
    value_int               INTEGER,                -- Gauge/Sum 整数值
    is_monotonic            INTEGER,                -- Sum: 0 或 1
    aggregation_temporality INTEGER,                -- 1=delta, 2=cumulative
    hist_count              INTEGER,                -- Histogram/Summary 计数
    hist_sum                REAL,                    -- Histogram/Summary 总和
    hist_min                REAL,
    hist_max                REAL,
    hist_bounds             TEXT,                    -- JSON 数组：桶边界
    hist_counts             TEXT,                    -- JSON 数组：桶计数
    extra_data              TEXT,                    -- JSON，exp_histogram/summary 专用字段
    start_timestamp_ns      INTEGER,
    flags                   INTEGER DEFAULT 0
);
```

## 性能调优

SQLite 使用以下 PRAGMA 配置以最大化写入吞吐：

- `journal_mode = WAL` — 写前日志，写入时可并发读取
- `synchronous = NORMAL` — dump 工具可接受的同步级别
- `cache_size = -64000` — 64MB 页缓存
- `mmap_size = 268435456` — 256MB 内存映射 I/O

持续 10 万 dp/s 写入时的推荐配置：

```bash
./otel_dumper --batch-size 50000 --flush-interval-ms 500 --channel-capacity 10000
```

## 许可证

MIT
