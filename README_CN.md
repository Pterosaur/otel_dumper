# otel_dumper

一个 OpenTelemetry Collector 模拟器，接收 OTLP Metrics 数据并落盘到 SQLite 或 DuckDB 数据库和/或 JSONL 文件，用于离线分析和 Grafana 可视化。

[English](README.md)

---

## 功能特性

- **双协议支持**: gRPC (`:4317`) 和 HTTP (`:4318`) OTLP 端点
- **高吞吐**: 针对 ~10万 data points/秒 设计，批量写入
- **双存储后端**: SQLite（行存储，便携）或 DuckDB（列存储，压缩，文件体积缩小约 100 倍）
- **双输出格式**: 数据库用于 Grafana 查询 + 可选 JSONL 用于本地直观阅读
- **Prometheus 导出**: 可选 `/metrics` 端点，通过 SSH 隧道实现远程实时 Grafana 监控
- **Grafana 就绪**: 使用 [SQLite 数据源插件](https://grafana.com/grafana/plugins/frser-sqlite-datasource/) 或 [DuckDB 数据源插件](https://grafana.com/grafana/plugins/grafana-duckdb-datasource/) 或内置 Prometheus 数据源
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
                        SQLite / DuckDB
```

## 快速开始

### 下载

预编译的静态二进制可从 [GitHub Releases](https://github.com/Pterosaur/otel_dumper/releases) 获取：

```bash
curl -LO https://github.com/Pterosaur/otel_dumper/releases/download/latest/otel_dumper-x86_64-linux
chmod +x otel_dumper-x86_64-linux
./otel_dumper-x86_64-linux --help
```

### 从源码构建

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

# 使用 DuckDB 后端（根据文件扩展名自动检测）
./otel_dumper --db-path ./metrics.duckdb

# 显式指定存储后端
./otel_dumper --db-path ./data.db --db-format duckdb

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
  --db-size 5G \
  --db-time-window 30m \
  --max-rows 100000000
```

### 命令行参数

| 参数 | 默认值 | 描述 |
|------|--------|------|
| `--grpc-port` | `4317` | gRPC OTLP 服务端口 |
| `--http-port` | `4318` | HTTP OTLP 服务端口 |
| `--db-path` | `metrics.duckdb` | 数据库文件路径（`.duckdb`/`.ddb` 自动选择 DuckDB） |
| `--db-format` | *（自动）* | 存储后端：`sqlite` 或 `duckdb`（根据扩展名自动检测） |
| `--db-size` | `5G` | 近似滚动数据库大小窗口，0 表示不限制；别名：`--size` |
| `--db-time-window` | `30m` | 近似滚动数据库时间窗口，0 表示不限制；别名：`--time-window` |
| `--index-attrs` | *（无）* | dp_attrs 中需要索引的 JSON key（逗号分隔，仅 SQLite） |
| `--jsonl-path` | *（无）* | JSONL 输出文件路径（可选，用于本地直观阅读） |
| `--prom-port` | *（无）* | Prometheus 导出端口（可选，暴露 `/metrics` 端点） |
| `--prom-history` | *（无）* | Prometheus 历史保留窗口（如 "30 mins"、"24 hours"） |
| `--sqlite-port` | *（无）* | SQLite 查询 API 端口（可选，远程 SQL 查询） |
| `--batch-size` | `50000` | 积累多少数据点后批量写入 SQLite |
| `--flush-interval-ms` | `500` | 定时刷盘间隔（毫秒），即使批次未满也会写入 |
| `--channel-capacity` | `10000` | 内部通道缓冲大小 |
| `--max-rows` | `0` | 最大写入行数，0 表示不限制 |

### 数据库滚动窗口

otel_dumper 只保留配置窗口内的最新数据库数据。窗口有两个 gate：

- `--db-time-window`：删除早于“最新写入时间戳减去窗口”的行。
- `--db-size`：当数据库文件超过限制时，按估算删除一部分最旧数据，并执行 checkpoint/压缩。

两个 gate 都是近似控制，会在批量写入后按间隔触发，避免影响写入热路径。任一 gate 设置为 `0` 表示关闭，例如 `--db-size 0` 或 `--db-time-window 0`。默认值是 `--db-size 5G` 和 `--db-time-window 30m`，也就是数据库文件大致不超过 5 GB，并保留最近 30 分钟的数据。

## SQLite 查询 API

指定 `--sqlite-port` 后，otel_dumper 会暴露一个只读 HTTP API，支持远程 SQL 查询 SQLite 数据库。可以获得**纳秒精度**的时间戳。

```bash
# 在目标机器 (dut) 上
./otel_dumper --sqlite-port 8080

# 在开发机建立 SSH 隧道
ssh -L 8080:127.0.0.1:8080 user@dut

# 远程查询
curl "http://localhost:8080/api/query?sql=SELECT+timestamp_ns,value_int+FROM+metric_data_points+LIMIT+5"
```

### API 端点

| 端点 | 描述 |
|------|------|
| `GET /api/query?sql=...&limit=10000` | 执行只读 SELECT 查询，返回 JSON |
| `GET /api/tables` | 列出所有表 |
| `GET /api/schema` | 显示 metric_data_points 列信息 |
| `GET /health` | 健康检查 |

只允许 `SELECT` 查询，`INSERT`、`UPDATE`、`DELETE`、`DROP` 会被拒绝。

## Prometheus 导出

指定 `--prom-port` 后，otel_dumper 会暴露一个 `/metrics` 端点，以 Prometheus 文本格式输出最新指标值。特别适合 Grafana 在另一台机器上、只能通过 SSH 访问目标机器的场景。

```bash
# 在目标机器 (dut) 上
./otel_dumper --prom-port 9090

# 在你的开发机（Grafana 所在的机器），建立 SSH 隧道
ssh -L 9090:127.0.0.1:9090 user@dut

# 在 Grafana 中：添加 Prometheus 数据源 → http://localhost:9090
```

然后用 PromQL 查询：
```
sai_counter_type_1_stat_0{object_name="Ethernet32"}
```

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

## DuckDB 后端

DuckDB 是列式分析数据库，在指标存储方面有显著优势：

| | SQLite | DuckDB |
|---|---|---|
| **文件大小** | ~117 MB (46万行) | **~1.5 MB** (缩小 77 倍) |
| **查询速度** | 225ms (需要索引) | **24ms** (无需索引) |
| **时间精度** | 秒级（插件限制） | **纳秒级**（原生支持） |

### 何时使用 DuckDB

- 需要**小文件**方便在机器间拷贝
- 需要 Grafana 中的**纳秒时间精度**
- 主要做**分析查询**（聚合、过滤、GROUP BY）

### DuckDB + Grafana 配置

1. 安装 [DuckDB 数据源插件](https://grafana.com/grafana/plugins/grafana-duckdb-datasource/)：
   ```bash
   grafana-cli plugins install grafana-duckdb-datasource
   ```
2. 在 Grafana 中添加 DuckDB 数据源，指向 `.duckdb` 文件。

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
