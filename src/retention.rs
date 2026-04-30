use std::path::{Path, PathBuf};
use std::time::Duration;

pub const DEFAULT_DB_SIZE_BYTES: u64 = 5 * 1024 * 1024 * 1024;
pub const DEFAULT_DB_TIME_WINDOW: Duration = Duration::from_secs(30 * 60);
pub const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct RetentionPolicy {
    pub max_bytes: u64,
    pub time_window: Duration,
    pub cleanup_interval: Duration,
}

impl RetentionPolicy {
    pub fn new(max_bytes: u64, time_window: Duration) -> Self {
        Self {
            max_bytes,
            time_window,
            cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
        }
    }

    pub fn disabled() -> Self {
        Self {
            max_bytes: 0,
            time_window: Duration::ZERO,
            cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.max_bytes > 0 || !self.time_window.is_zero()
    }

    pub fn time_window_ns(&self) -> Option<i64> {
        if self.time_window.is_zero() {
            return None;
        }
        Some(self.time_window.as_nanos().min(i64::MAX as u128) as i64)
    }

    pub fn size_target_bytes(&self) -> Option<u64> {
        if self.max_bytes == 0 {
            return None;
        }
        // Delete a little extra when the DB crosses the cap so cleanup does not
        // run on every interval while ingestion is near the limit.
        Some((self.max_bytes as u128 * 90 / 100) as u64)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetentionStats {
    pub rows_deleted: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

pub fn parse_duration_arg(s: &str) -> Result<Duration, String> {
    parse_duration_str(s).ok_or_else(|| {
        format!("invalid duration {s:?}; examples: 0, 30m, 30 mins, 24 hours, 5 days")
    })
}

/// Parse a human-readable duration string like "30 mins", "24 hours", "5 days", "1h", "30m".
pub fn parse_duration_str(s: &str) -> Option<Duration> {
    let s = s.trim().to_lowercase();
    if s.is_empty() {
        return None;
    }

    let (num_str, unit) = if let Some(pos) = s.find(|c: char| c.is_ascii_alphabetic()) {
        (s[..pos].trim(), s[pos..].trim())
    } else {
        return parse_non_negative_duration_secs(&s);
    };

    let num: f64 = num_str.parse().ok()?;
    if !num.is_finite() || num < 0.0 {
        return None;
    }

    let secs = match unit {
        "s" | "sec" | "secs" | "second" | "seconds" => num,
        "m" | "min" | "mins" | "minute" | "minutes" => num * 60.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => num * 3600.0,
        "d" | "day" | "days" => num * 86400.0,
        "w" | "week" | "weeks" => num * 604800.0,
        _ => return None,
    };

    parse_non_negative_duration_secs(&secs.to_string())
}

fn parse_non_negative_duration_secs(s: &str) -> Option<Duration> {
    let secs: f64 = s.parse().ok()?;
    if !secs.is_finite() || secs < 0.0 {
        return None;
    }
    Some(Duration::from_secs_f64(secs))
}

pub fn parse_size_bytes(s: &str) -> Result<u64, String> {
    let s = s.trim().to_lowercase().replace(' ', "");
    if s.is_empty() {
        return Err("size cannot be empty".to_string());
    }

    let unit_pos = s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len());
    let (num_str, unit) = s.split_at(unit_pos);
    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid size {s:?}; examples: 0, 512M, 5G, 1.5GiB"))?;
    if !num.is_finite() || num < 0.0 {
        return Err(format!("invalid size {s:?}; value must be non-negative"));
    }

    let multiplier = match unit {
        "" | "b" | "byte" | "bytes" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" | "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return Err(format!("invalid size unit {unit:?}; use B, K, M, G, or T")),
    };

    let bytes = num * multiplier;
    if bytes > u64::MAX as f64 {
        return Err(format!("size {s:?} is too large"));
    }
    Ok(bytes.round() as u64)
}

pub fn sqlite_database_size_bytes(db_path: &Path) -> u64 {
    path_size(db_path)
        + path_size(&sidecar_path(db_path, "-wal"))
        + path_size(&sidecar_path(db_path, "-shm"))
}

pub fn duckdb_database_size_bytes(db_path: &Path) -> u64 {
    path_size(db_path) + path_size(&sidecar_path(db_path, ".wal"))
}

pub fn rows_to_delete_for_size(row_count: u64, current_bytes: u64, target_bytes: u64) -> u64 {
    if row_count == 0 || current_bytes <= target_bytes {
        return 0;
    }
    let keep_rows = ((row_count as u128 * target_bytes as u128) / current_bytes as u128) as u64;
    row_count.saturating_sub(keep_rows.min(row_count))
}

fn sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut path = db_path.as_os_str().to_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

fn path_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size_bytes() {
        assert_eq!(parse_size_bytes("0").unwrap(), 0);
        assert_eq!(parse_size_bytes("1024").unwrap(), 1024);
        assert_eq!(parse_size_bytes("1K").unwrap(), 1024);
        assert_eq!(parse_size_bytes("1.5M").unwrap(), 1_572_864);
        assert_eq!(parse_size_bytes("5G").unwrap(), DEFAULT_DB_SIZE_BYTES);
        assert_eq!(parse_size_bytes("2 GiB").unwrap(), 2 * 1024 * 1024 * 1024);
        assert!(parse_size_bytes("-1G").is_err());
        assert!(parse_size_bytes("10Q").is_err());
    }

    #[test]
    fn test_parse_duration_str() {
        assert_eq!(parse_duration_str("0"), Some(Duration::ZERO));
        assert_eq!(parse_duration_str("30m"), Some(Duration::from_secs(1800)));
        assert_eq!(
            parse_duration_str("30 mins"),
            Some(Duration::from_secs(1800))
        );
        assert_eq!(parse_duration_str("1h"), Some(Duration::from_secs(3600)));
        assert!(parse_duration_str("invalid").is_none());
        assert!(parse_duration_str("-1m").is_none());
    }

    #[test]
    fn test_rows_to_delete_for_size() {
        assert_eq!(rows_to_delete_for_size(100, 1000, 900), 10);
        assert_eq!(rows_to_delete_for_size(100, 1000, 0), 100);
        assert_eq!(rows_to_delete_for_size(0, 1000, 900), 0);
        assert_eq!(rows_to_delete_for_size(100, 800, 900), 0);
    }
}
