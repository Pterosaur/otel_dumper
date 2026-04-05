use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{any_value::Value as AnyValueKind, AnyValue, KeyValue},
    metrics::v1::{
        metric::Data, number_data_point::Value as NumberValue, ExponentialHistogramDataPoint,
        HistogramDataPoint, Metric, NumberDataPoint, SummaryDataPoint,
    },
};

/// Flattened data point ready for SQLite insertion.
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
            .map(|r| kvs_to_json(&r.attributes))
            .flatten();

        for sm in rm.scope_metrics {
            let scope_name = sm.scope.as_ref().map(|s| s.name.clone());
            let scope_version = sm
                .scope
                .as_ref()
                .and_then(|s| if s.version.is_empty() { None } else { Some(s.version.clone()) });

            for metric in sm.metrics {
                flatten_metric(
                    &metric,
                    &resource_attrs,
                    &scope_name,
                    &scope_version,
                    &mut points,
                );
            }
        }
    }
    points
}

fn flatten_metric(
    metric: &Metric,
    resource_attrs: &Option<String>,
    scope_name: &Option<String>,
    scope_version: &Option<String>,
    out: &mut Vec<FlatDataPoint>,
) {
    let name = &metric.name;
    let data = match &metric.data {
        Some(d) => d,
        None => return,
    };

    match data {
        Data::Gauge(gauge) => {
            for dp in &gauge.data_points {
                out.push(number_dp_to_flat(
                    dp,
                    name,
                    "gauge",
                    resource_attrs,
                    scope_name,
                    scope_version,
                    None,
                    None,
                ));
            }
        }
        Data::Sum(sum) => {
            for dp in &sum.data_points {
                out.push(number_dp_to_flat(
                    dp,
                    name,
                    "sum",
                    resource_attrs,
                    scope_name,
                    scope_version,
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
                    resource_attrs,
                    scope_name,
                    scope_version,
                    Some(hist.aggregation_temporality),
                ));
            }
        }
        Data::ExponentialHistogram(exp_hist) => {
            for dp in &exp_hist.data_points {
                out.push(exp_histogram_dp_to_flat(
                    dp,
                    name,
                    resource_attrs,
                    scope_name,
                    scope_version,
                    Some(exp_hist.aggregation_temporality),
                ));
            }
        }
        Data::Summary(summary) => {
            for dp in &summary.data_points {
                out.push(summary_dp_to_flat(
                    dp,
                    name,
                    resource_attrs,
                    scope_name,
                    scope_version,
                ));
            }
        }
    }
}

fn number_dp_to_flat(
    dp: &NumberDataPoint,
    metric_name: &str,
    metric_type: &'static str,
    resource_attrs: &Option<String>,
    scope_name: &Option<String>,
    scope_version: &Option<String>,
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
        resource_attrs: resource_attrs.clone(),
        scope_name: scope_name.clone(),
        scope_version: scope_version.clone(),
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
    resource_attrs: &Option<String>,
    scope_name: &Option<String>,
    scope_version: &Option<String>,
    aggregation_temporality: Option<i32>,
) -> FlatDataPoint {
    FlatDataPoint {
        timestamp_ns: dp.time_unix_nano as i64,
        metric_name: metric_name.to_string(),
        metric_type: "histogram",
        resource_attrs: resource_attrs.clone(),
        scope_name: scope_name.clone(),
        scope_version: scope_version.clone(),
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
    resource_attrs: &Option<String>,
    scope_name: &Option<String>,
    scope_version: &Option<String>,
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
        resource_attrs: resource_attrs.clone(),
        scope_name: scope_name.clone(),
        scope_version: scope_version.clone(),
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
    resource_attrs: &Option<String>,
    scope_name: &Option<String>,
    scope_version: &Option<String>,
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
        resource_attrs: resource_attrs.clone(),
        scope_name: scope_name.clone(),
        scope_version: scope_version.clone(),
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
                .map(|v| any_value_to_json(v))
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
                        .map(|v| any_value_to_json(v))
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
