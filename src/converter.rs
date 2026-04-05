use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{any_value::Value as AnyValueKind, AnyValue, KeyValue},
    metrics::v1::{
        metric::Data, number_data_point::Value as NumberValue, ExponentialHistogramDataPoint,
        HistogramDataPoint, Metric, NumberDataPoint, SummaryDataPoint,
    },
};

/// Flattened data point ready for SQLite insertion.
#[derive(serde::Serialize)]
pub struct FlatDataPoint {
    pub timestamp_ns: i64,
    pub metric_name: String,
    pub metric_type: &'static str,
    pub resource_attrs: Option<String>,
    pub scope_name: Option<String>,
    pub scope_version: Option<String>,
    pub dp_attrs: Option<String>,
    pub value_double: Option<f64>,
    pub value_int: Option<i64>,
    pub is_monotonic: Option<bool>,
    pub aggregation_temporality: Option<i32>,
    pub hist_count: Option<i64>,
    pub hist_sum: Option<f64>,
    pub hist_min: Option<f64>,
    pub hist_max: Option<f64>,
    pub hist_bounds: Option<String>,
    pub hist_counts: Option<String>,
    pub extra_data: Option<String>,
    pub start_timestamp_ns: Option<i64>,
    pub flags: i32,
}

/// Convert an OTLP ExportMetricsServiceRequest into flat data points.
pub fn convert_request(request: ExportMetricsServiceRequest) -> Vec<FlatDataPoint> {
    let mut points = Vec::new();
    for rm in request.resource_metrics {
        let resource_attrs = rm
            .resource
            .as_ref()
            .and_then(|r| kvs_to_json(&r.attributes));

        for sm in rm.scope_metrics {
            let scope_name = sm.scope.as_ref().map(|s| s.name.clone());
            let scope_version = sm.scope.as_ref().and_then(|s| {
                if s.version.is_empty() {
                    None
                } else {
                    Some(s.version.clone())
                }
            });

            for metric in sm.metrics {
                let ctx = MetricContext {
                    resource_attrs: resource_attrs.clone(),
                    scope_name: scope_name.clone(),
                    scope_version: scope_version.clone(),
                };
                flatten_metric(&metric, &ctx, &mut points);
            }
        }
    }
    points
}

fn flatten_metric(metric: &Metric, ctx: &MetricContext, out: &mut Vec<FlatDataPoint>) {
    let name = &metric.name;
    let data = match &metric.data {
        Some(d) => d,
        None => return,
    };

    match data {
        Data::Gauge(gauge) => {
            for dp in &gauge.data_points {
                out.push(number_dp_to_flat(dp, name, "gauge", ctx, None, None));
            }
        }
        Data::Sum(sum) => {
            for dp in &sum.data_points {
                out.push(number_dp_to_flat(
                    dp,
                    name,
                    "sum",
                    ctx,
                    Some(sum.is_monotonic),
                    Some(sum.aggregation_temporality),
                ));
            }
        }
        Data::Histogram(hist) => {
            for dp in &hist.data_points {
                out.push(histogram_dp_to_flat(
                    dp,
                    name,
                    ctx,
                    Some(hist.aggregation_temporality),
                ));
            }
        }
        Data::ExponentialHistogram(exp_hist) => {
            for dp in &exp_hist.data_points {
                out.push(exp_histogram_dp_to_flat(
                    dp,
                    name,
                    ctx,
                    Some(exp_hist.aggregation_temporality),
                ));
            }
        }
        Data::Summary(summary) => {
            for dp in &summary.data_points {
                out.push(summary_dp_to_flat(dp, name, ctx));
            }
        }
    }
}

struct MetricContext {
    resource_attrs: Option<String>,
    scope_name: Option<String>,
    scope_version: Option<String>,
}

fn number_dp_to_flat(
    dp: &NumberDataPoint,
    metric_name: &str,
    metric_type: &'static str,
    ctx: &MetricContext,
    is_monotonic: Option<bool>,
    aggregation_temporality: Option<i32>,
) -> FlatDataPoint {
    let (value_double, value_int) = match &dp.value {
        Some(NumberValue::AsDouble(v)) => (Some(*v), None),
        Some(NumberValue::AsInt(v)) => (None, Some(*v)),
        None => (None, None),
    };
    FlatDataPoint {
        timestamp_ns: dp.time_unix_nano as i64,
        metric_name: metric_name.to_string(),
        metric_type,
        resource_attrs: ctx.resource_attrs.clone(),
        scope_name: ctx.scope_name.clone(),
        scope_version: ctx.scope_version.clone(),
        dp_attrs: kvs_to_json(&dp.attributes),
        value_double,
        value_int,
        is_monotonic,
        aggregation_temporality,
        hist_count: None,
        hist_sum: None,
        hist_min: None,
        hist_max: None,
        hist_bounds: None,
        hist_counts: None,
        extra_data: None,
        start_timestamp_ns: if dp.start_time_unix_nano > 0 {
            Some(dp.start_time_unix_nano as i64)
        } else {
            None
        },
        flags: dp.flags as i32,
    }
}

fn histogram_dp_to_flat(
    dp: &HistogramDataPoint,
    metric_name: &str,
    ctx: &MetricContext,
    aggregation_temporality: Option<i32>,
) -> FlatDataPoint {
    FlatDataPoint {
        timestamp_ns: dp.time_unix_nano as i64,
        metric_name: metric_name.to_string(),
        metric_type: "histogram",
        resource_attrs: ctx.resource_attrs.clone(),
        scope_name: ctx.scope_name.clone(),
        scope_version: ctx.scope_version.clone(),
        dp_attrs: kvs_to_json(&dp.attributes),
        value_double: None,
        value_int: None,
        is_monotonic: None,
        aggregation_temporality,
        hist_count: Some(dp.count as i64),
        hist_sum: dp.sum,
        hist_min: dp.min,
        hist_max: dp.max,
        hist_bounds: Some(serde_json::to_string(&dp.explicit_bounds).unwrap()),
        hist_counts: Some(serde_json::to_string(&dp.bucket_counts).unwrap()),
        extra_data: None,
        start_timestamp_ns: if dp.start_time_unix_nano > 0 {
            Some(dp.start_time_unix_nano as i64)
        } else {
            None
        },
        flags: dp.flags as i32,
    }
}

fn exp_histogram_dp_to_flat(
    dp: &ExponentialHistogramDataPoint,
    metric_name: &str,
    ctx: &MetricContext,
    aggregation_temporality: Option<i32>,
) -> FlatDataPoint {
    let extra = serde_json::json!({
        "scale": dp.scale,
        "zero_count": dp.zero_count,
        "zero_threshold": dp.zero_threshold,
        "positive": dp.positive.as_ref().map(|b| serde_json::json!({
            "offset": b.offset,
            "bucket_counts": b.bucket_counts,
        })),
        "negative": dp.negative.as_ref().map(|b| serde_json::json!({
            "offset": b.offset,
            "bucket_counts": b.bucket_counts,
        })),
    });

    FlatDataPoint {
        timestamp_ns: dp.time_unix_nano as i64,
        metric_name: metric_name.to_string(),
        metric_type: "exp_histogram",
        resource_attrs: ctx.resource_attrs.clone(),
        scope_name: ctx.scope_name.clone(),
        scope_version: ctx.scope_version.clone(),
        dp_attrs: kvs_to_json(&dp.attributes),
        value_double: None,
        value_int: None,
        is_monotonic: None,
        aggregation_temporality,
        hist_count: Some(dp.count as i64),
        hist_sum: dp.sum,
        hist_min: dp.min,
        hist_max: dp.max,
        hist_bounds: None,
        hist_counts: None,
        extra_data: Some(extra.to_string()),
        start_timestamp_ns: if dp.start_time_unix_nano > 0 {
            Some(dp.start_time_unix_nano as i64)
        } else {
            None
        },
        flags: dp.flags as i32,
    }
}

fn summary_dp_to_flat(
    dp: &SummaryDataPoint,
    metric_name: &str,
    ctx: &MetricContext,
) -> FlatDataPoint {
    let quantiles: Vec<_> = dp
        .quantile_values
        .iter()
        .map(|q| {
            serde_json::json!({
                "quantile": q.quantile,
                "value": q.value,
            })
        })
        .collect();

    FlatDataPoint {
        timestamp_ns: dp.time_unix_nano as i64,
        metric_name: metric_name.to_string(),
        metric_type: "summary",
        resource_attrs: ctx.resource_attrs.clone(),
        scope_name: ctx.scope_name.clone(),
        scope_version: ctx.scope_version.clone(),
        dp_attrs: kvs_to_json(&dp.attributes),
        value_double: None,
        value_int: None,
        is_monotonic: None,
        aggregation_temporality: None,
        hist_count: Some(dp.count as i64),
        hist_sum: Some(dp.sum),
        hist_min: None,
        hist_max: None,
        hist_bounds: None,
        hist_counts: None,
        extra_data: Some(serde_json::to_string(&quantiles).unwrap()),
        start_timestamp_ns: if dp.start_time_unix_nano > 0 {
            Some(dp.start_time_unix_nano as i64)
        } else {
            None
        },
        flags: dp.flags as i32,
    }
}

fn kvs_to_json(kvs: &[KeyValue]) -> Option<String> {
    if kvs.is_empty() {
        return None;
    }
    let map: serde_json::Map<String, serde_json::Value> = kvs
        .iter()
        .map(|kv| {
            let val = kv
                .value
                .as_ref()
                .map(any_value_to_json)
                .unwrap_or(serde_json::Value::Null);
            (kv.key.clone(), val)
        })
        .collect();
    Some(serde_json::to_string(&map).unwrap())
}

fn any_value_to_json(av: &AnyValue) -> serde_json::Value {
    match &av.value {
        Some(AnyValueKind::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(AnyValueKind::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(AnyValueKind::IntValue(i)) => serde_json::json!(*i),
        Some(AnyValueKind::DoubleValue(d)) => serde_json::json!(*d),
        Some(AnyValueKind::ArrayValue(arr)) => {
            let vals: Vec<_> = arr.values.iter().map(any_value_to_json).collect();
            serde_json::Value::Array(vals)
        }
        Some(AnyValueKind::KvlistValue(kvlist)) => {
            let map: serde_json::Map<_, _> = kvlist
                .values
                .iter()
                .map(|kv| {
                    let val = kv
                        .value
                        .as_ref()
                        .map(any_value_to_json)
                        .unwrap_or(serde_json::Value::Null);
                    (kv.key.clone(), val)
                })
                .collect();
            serde_json::Value::Object(map)
        }
        Some(AnyValueKind::BytesValue(b)) => {
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            serde_json::Value::String(hex)
        }
        None => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        common::v1::InstrumentationScope,
        metrics::v1::{
            exponential_histogram_data_point::Buckets, summary_data_point::ValueAtQuantile, Gauge,
            Histogram, ResourceMetrics, ScopeMetrics, Sum, SummaryDataPoint,
        },
        resource::v1::Resource,
    };

    fn make_kv(key: &str, str_val: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueKind::StringValue(str_val.to_string())),
            }),
        }
    }

    fn wrap_metric(metric: Metric) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![make_kv("service.name", "test-svc")],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "test-scope".into(),
                        version: "1.0".into(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    metrics: vec![metric],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[test]
    fn test_convert_gauge() {
        let request = wrap_metric(Metric {
            name: "cpu.usage".into(),
            description: String::new(),
            unit: "%".into(),
            metadata: Default::default(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![make_kv("host", "node-1")],
                    start_time_unix_nano: 0,
                    time_unix_nano: 1_000_000_000,
                    value: Some(NumberValue::AsDouble(72.5)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        });

        let points = convert_request(request);
        assert_eq!(points.len(), 1);
        let p = &points[0];
        assert_eq!(p.metric_name, "cpu.usage");
        assert_eq!(p.metric_type, "gauge");
        assert_eq!(p.timestamp_ns, 1_000_000_000);
        assert_eq!(p.value_double, Some(72.5));
        assert_eq!(p.value_int, None);
        assert!(p.resource_attrs.as_ref().unwrap().contains("test-svc"));
        assert_eq!(p.scope_name.as_deref(), Some("test-scope"));
        assert_eq!(p.scope_version.as_deref(), Some("1.0"));
        assert!(p.dp_attrs.as_ref().unwrap().contains("node-1"));
    }

    #[test]
    fn test_convert_sum_counter() {
        let request = wrap_metric(Metric {
            name: "http.requests".into(),
            description: String::new(),
            unit: "1".into(),
            metadata: Default::default(),
            data: Some(Data::Sum(Sum {
                data_points: vec![
                    NumberDataPoint {
                        attributes: vec![make_kv("method", "GET")],
                        start_time_unix_nano: 500_000_000,
                        time_unix_nano: 1_000_000_000,
                        value: Some(NumberValue::AsInt(42)),
                        exemplars: vec![],
                        flags: 0,
                    },
                    NumberDataPoint {
                        attributes: vec![make_kv("method", "POST")],
                        start_time_unix_nano: 500_000_000,
                        time_unix_nano: 1_000_000_000,
                        value: Some(NumberValue::AsInt(10)),
                        exemplars: vec![],
                        flags: 0,
                    },
                ],
                aggregation_temporality: 2, // cumulative
                is_monotonic: true,
            })),
        });

        let points = convert_request(request);
        assert_eq!(points.len(), 2);

        let p0 = &points[0];
        assert_eq!(p0.metric_type, "sum");
        assert_eq!(p0.value_int, Some(42));
        assert_eq!(p0.is_monotonic, Some(true));
        assert_eq!(p0.aggregation_temporality, Some(2));
        assert_eq!(p0.start_timestamp_ns, Some(500_000_000));

        let p1 = &points[1];
        assert_eq!(p1.value_int, Some(10));
        assert!(p1.dp_attrs.as_ref().unwrap().contains("POST"));
    }

    #[test]
    fn test_convert_histogram() {
        let request = wrap_metric(Metric {
            name: "request.duration".into(),
            description: String::new(),
            unit: "ms".into(),
            metadata: Default::default(),
            data: Some(Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: 2_000_000_000,
                    count: 100,
                    sum: Some(5000.0),
                    bucket_counts: vec![10, 30, 40, 15, 5],
                    explicit_bounds: vec![1.0, 5.0, 10.0, 50.0],
                    exemplars: vec![],
                    flags: 0,
                    min: Some(0.5),
                    max: Some(99.0),
                }],
                aggregation_temporality: 1, // delta
            })),
        });

        let points = convert_request(request);
        assert_eq!(points.len(), 1);
        let p = &points[0];
        assert_eq!(p.metric_type, "histogram");
        assert_eq!(p.hist_count, Some(100));
        assert_eq!(p.hist_sum, Some(5000.0));
        assert_eq!(p.hist_min, Some(0.5));
        assert_eq!(p.hist_max, Some(99.0));
        assert_eq!(p.aggregation_temporality, Some(1));

        let bounds: Vec<f64> = serde_json::from_str(p.hist_bounds.as_ref().unwrap()).unwrap();
        assert_eq!(bounds, vec![1.0, 5.0, 10.0, 50.0]);

        let counts: Vec<u64> = serde_json::from_str(p.hist_counts.as_ref().unwrap()).unwrap();
        assert_eq!(counts, vec![10, 30, 40, 15, 5]);
    }

    #[test]
    fn test_convert_exp_histogram() {
        let request = wrap_metric(Metric {
            name: "latency".into(),
            description: String::new(),
            unit: "ms".into(),
            metadata: Default::default(),
            data: Some(Data::ExponentialHistogram(
                opentelemetry_proto::tonic::metrics::v1::ExponentialHistogram {
                    data_points: vec![ExponentialHistogramDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 0,
                        time_unix_nano: 3_000_000_000,
                        count: 50,
                        sum: Some(250.0),
                        scale: 3,
                        zero_count: 2,
                        positive: Some(Buckets {
                            offset: 0,
                            bucket_counts: vec![5, 10, 15],
                        }),
                        negative: Some(Buckets {
                            offset: -1,
                            bucket_counts: vec![3, 7],
                        }),
                        flags: 0,
                        exemplars: vec![],
                        min: Some(0.1),
                        max: Some(20.0),
                        zero_threshold: 0.0,
                    }],
                    aggregation_temporality: 2,
                },
            )),
        });

        let points = convert_request(request);
        assert_eq!(points.len(), 1);
        let p = &points[0];
        assert_eq!(p.metric_type, "exp_histogram");
        assert_eq!(p.hist_count, Some(50));
        assert_eq!(p.hist_sum, Some(250.0));
        assert!(p.extra_data.is_some());

        let extra: serde_json::Value =
            serde_json::from_str(p.extra_data.as_ref().unwrap()).unwrap();
        assert_eq!(extra["scale"], 3);
        assert_eq!(extra["zero_count"], 2);
    }

    #[test]
    fn test_convert_summary() {
        let request = wrap_metric(Metric {
            name: "rpc.duration".into(),
            description: String::new(),
            unit: "ms".into(),
            metadata: Default::default(),
            data: Some(Data::Summary(
                opentelemetry_proto::tonic::metrics::v1::Summary {
                    data_points: vec![SummaryDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 0,
                        time_unix_nano: 4_000_000_000,
                        count: 200,
                        sum: 10000.0,
                        quantile_values: vec![
                            ValueAtQuantile {
                                quantile: 0.5,
                                value: 50.0,
                            },
                            ValueAtQuantile {
                                quantile: 0.99,
                                value: 150.0,
                            },
                        ],
                        flags: 0,
                    }],
                },
            )),
        });

        let points = convert_request(request);
        assert_eq!(points.len(), 1);
        let p = &points[0];
        assert_eq!(p.metric_type, "summary");
        assert_eq!(p.hist_count, Some(200));
        assert_eq!(p.hist_sum, Some(10000.0));
        assert!(p.extra_data.is_some());

        let quantiles: Vec<serde_json::Value> =
            serde_json::from_str(p.extra_data.as_ref().unwrap()).unwrap();
        assert_eq!(quantiles.len(), 2);
        assert_eq!(quantiles[0]["quantile"], 0.5);
    }

    #[test]
    fn test_convert_empty_request() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![],
        };
        let points = convert_request(request);
        assert!(points.is_empty());
    }

    #[test]
    fn test_kvs_to_json_various_types() {
        let kvs = vec![
            KeyValue {
                key: "str_key".into(),
                value: Some(AnyValue {
                    value: Some(AnyValueKind::StringValue("hello".into())),
                }),
            },
            KeyValue {
                key: "int_key".into(),
                value: Some(AnyValue {
                    value: Some(AnyValueKind::IntValue(42)),
                }),
            },
            KeyValue {
                key: "bool_key".into(),
                value: Some(AnyValue {
                    value: Some(AnyValueKind::BoolValue(true)),
                }),
            },
            KeyValue {
                key: "double_key".into(),
                value: Some(AnyValue {
                    value: Some(AnyValueKind::DoubleValue(3.14)),
                }),
            },
        ];
        let json_str = kvs_to_json(&kvs).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["str_key"], "hello");
        assert_eq!(parsed["int_key"], 42);
        assert_eq!(parsed["bool_key"], true);
        assert_eq!(parsed["double_key"], 3.14);
    }

    #[test]
    fn test_kvs_to_json_empty() {
        assert!(kvs_to_json(&[]).is_none());
    }
}
