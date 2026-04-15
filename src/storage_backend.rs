use crate::converter::FlatDataPoint;
use crate::duckdb_storage::DuckDbStorage;
use crate::storage::Storage as SqliteStorage;
use std::fmt;
use std::path::Path;

/// Unified error type for storage operations.
#[derive(Debug)]
pub enum StorageError {
    Sqlite(rusqlite::Error),
    DuckDb(duckdb::Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Sqlite(e) => write!(f, "SQLite error: {e}"),
            StorageError::DuckDb(e) => write!(f, "DuckDB error: {e}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<rusqlite::Error> for StorageError {
    fn from(e: rusqlite::Error) -> Self {
        StorageError::Sqlite(e)
    }
}

impl From<duckdb::Error> for StorageError {
    fn from(e: duckdb::Error) -> Self {
        StorageError::DuckDb(e)
    }
}

/// Storage backend selector.
pub enum StorageBackend {
    Sqlite(SqliteStorage),
    DuckDb(DuckDbStorage),
}

impl StorageBackend {
    /// Open a SQLite backend.
    pub fn sqlite(db_path: &Path) -> Result<Self, StorageError> {
        Ok(StorageBackend::Sqlite(SqliteStorage::new(db_path)?))
    }

    /// Open a DuckDB backend.
    pub fn duckdb(db_path: &Path) -> Result<Self, StorageError> {
        Ok(StorageBackend::DuckDb(DuckDbStorage::new(db_path)?))
    }

    /// Auto-detect backend from file extension.
    /// `.duckdb` or `.ddb` → DuckDB, everything else → SQLite.
    pub fn auto(db_path: &Path) -> Result<Self, StorageError> {
        match db_path.extension().and_then(|e| e.to_str()) {
            Some("duckdb") | Some("ddb") => Self::duckdb(db_path),
            _ => Self::sqlite(db_path),
        }
    }

    pub fn insert_batch(&self, points: &[FlatDataPoint]) -> Result<usize, StorageError> {
        match self {
            StorageBackend::Sqlite(s) => Ok(s.insert_batch(points)?),
            StorageBackend::DuckDb(s) => Ok(s.insert_batch(points)?),
        }
    }

    pub fn create_analysis_indexes(
        &self,
        dp_attr_keys: &[String],
    ) -> Result<(), StorageError> {
        match self {
            StorageBackend::Sqlite(s) => Ok(s.create_analysis_indexes(dp_attr_keys)?),
            StorageBackend::DuckDb(s) => Ok(s.create_analysis_indexes(dp_attr_keys)?),
        }
    }

    pub fn backend_name(&self) -> &'static str {
        match self {
            StorageBackend::Sqlite(_) => "SQLite",
            StorageBackend::DuckDb(_) => "DuckDB",
        }
    }

    #[cfg(test)]
    pub fn count_rows(&self) -> i64 {
        match self {
            StorageBackend::Sqlite(s) => s.count_rows(),
            StorageBackend::DuckDb(s) => s.count_rows(),
        }
    }
}
