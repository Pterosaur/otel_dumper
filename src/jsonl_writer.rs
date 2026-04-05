use crate::converter::FlatDataPoint;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

pub struct JsonlWriter {
    writer: Mutex<BufWriter<File>>,
}

impl JsonlWriter {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(JsonlWriter {
            writer: Mutex::new(BufWriter::with_capacity(256 * 1024, file)),
        })
    }

    pub fn write_batch(&self, points: &[FlatDataPoint]) -> std::io::Result<usize> {
        let mut w = self.writer.lock().unwrap();
        for point in points {
            serde_json::to_writer(&mut *w, point).map_err(std::io::Error::other)?;
            w.write_all(b"\n")?;
        }
        w.flush()?;
        Ok(points.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_point(name: &str, ts: i64, value: f64) -> FlatDataPoint {
        FlatDataPoint {
            timestamp_ns: ts,
            metric_name: name.to_string(),
            metric_type: "gauge",
            resource_attrs: Some(r#"{"service.name":"test"}"#.to_string()),
            scope_name: Some("test-scope".to_string()),
            scope_version: None,
            dp_attrs: Some(r#"{"host":"node-1"}"#.to_string()),
            value_double: Some(value),
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
        }
    }

    #[test]
    fn test_jsonl_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.jsonl");
        let writer = JsonlWriter::new(&path).unwrap();

        let points = vec![
            make_point("cpu.usage", 1_000_000_000, 45.0),
            make_point("cpu.usage", 2_000_000_000, 62.5),
            make_point("mem.usage", 1_000_000_000, 72.0),
        ];

        let written = writer.write_batch(&points).unwrap();
        assert_eq!(written, 3);

        // Read back and verify each line is valid JSON
        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);

        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["metric_name"], "cpu.usage");
        assert_eq!(first["timestamp_ns"], 1_000_000_000i64);
        assert_eq!(first["value_double"], 45.0);
        assert_eq!(first["metric_type"], "gauge");
        assert_eq!(first["scope_name"], "test-scope");
        assert!(first["scope_version"].is_null());

        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second["value_double"], 62.5);
    }

    #[test]
    fn test_jsonl_empty_batch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.jsonl");
        let writer = JsonlWriter::new(&path).unwrap();

        let written = writer.write_batch(&[]).unwrap();
        assert_eq!(written, 0);

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.is_empty());
    }

    #[test]
    fn test_jsonl_append_multiple_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.jsonl");
        let writer = JsonlWriter::new(&path).unwrap();

        writer.write_batch(&[make_point("m1", 1, 1.0)]).unwrap();
        writer
            .write_batch(&[make_point("m2", 2, 2.0), make_point("m3", 3, 3.0)])
            .unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content.lines().count(), 3);
    }

    #[test]
    fn test_jsonl_histogram_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.jsonl");
        let writer = JsonlWriter::new(&path).unwrap();

        let point = FlatDataPoint {
            timestamp_ns: 1_000_000_000,
            metric_name: "req.duration".to_string(),
            metric_type: "histogram",
            resource_attrs: None,
            scope_name: None,
            scope_version: None,
            dp_attrs: None,
            value_double: None,
            value_int: None,
            is_monotonic: None,
            aggregation_temporality: Some(2),
            hist_count: Some(100),
            hist_sum: Some(5000.0),
            hist_min: Some(0.5),
            hist_max: Some(99.0),
            hist_bounds: Some("[1.0,5.0,10.0]".to_string()),
            hist_counts: Some("[20,50,25,5]".to_string()),
            extra_data: None,
            start_timestamp_ns: Some(500_000_000),
            flags: 0,
        };

        writer.write_batch(&[point]).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(parsed["metric_type"], "histogram");
        assert_eq!(parsed["hist_count"], 100);
        assert_eq!(parsed["hist_sum"], 5000.0);
        assert_eq!(parsed["aggregation_temporality"], 2);
    }
}
