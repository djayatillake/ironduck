//! Persistent storage for IronDuck
//!
//! This module provides save/load functionality for the database state.
//! The format is:
//! - metadata.json: Catalog metadata (schemas, tables, columns, views, sequences, indexes)
//! - tables/*.bincode: Table data in bincode format

use ironduck_common::{LogicalType, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Database metadata for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMetadata {
    /// Version of the storage format
    pub version: u32,
    /// Schemas in the database
    pub schemas: Vec<SchemaMetadata>,
}

/// Schema metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetadata {
    pub name: String,
    pub tables: Vec<TableMetadata>,
    pub views: Vec<ViewMetadata>,
    pub sequences: Vec<SequenceMetadata>,
    pub indexes: Vec<IndexMetadata>,
}

/// Table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
}

/// Column metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub logical_type: LogicalType,
    pub nullable: bool,
    pub default_value: Option<Value>,
}

/// View metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewMetadata {
    pub name: String,
    pub sql: String,
    pub column_names: Vec<String>,
}

/// Sequence metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceMetadata {
    pub name: String,
    pub current_value: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cycle: bool,
}

/// Index metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

/// Table data for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableData {
    /// Column names
    pub columns: Vec<String>,
    /// Rows of values
    pub rows: Vec<Vec<Value>>,
}

/// Result type for persistence operations
pub type PersistenceResult<T> = Result<T, PersistenceError>;

/// Error type for persistence operations
#[derive(Debug)]
pub enum PersistenceError {
    IoError(std::io::Error),
    SerdeError(String),
    InvalidFormat(String),
}

impl From<std::io::Error> for PersistenceError {
    fn from(e: std::io::Error) -> Self {
        PersistenceError::IoError(e)
    }
}

impl From<serde_json::Error> for PersistenceError {
    fn from(e: serde_json::Error) -> Self {
        PersistenceError::SerdeError(e.to_string())
    }
}

impl From<bincode::Error> for PersistenceError {
    fn from(e: bincode::Error) -> Self {
        PersistenceError::SerdeError(e.to_string())
    }
}

impl std::fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersistenceError::IoError(e) => write!(f, "I/O error: {}", e),
            PersistenceError::SerdeError(e) => write!(f, "Serialization error: {}", e),
            PersistenceError::InvalidFormat(e) => write!(f, "Invalid format: {}", e),
        }
    }
}

impl std::error::Error for PersistenceError {}

/// Storage format version
pub const STORAGE_VERSION: u32 = 1;

/// Save database metadata to a directory
pub fn save_metadata(path: &Path, metadata: &DatabaseMetadata) -> PersistenceResult<()> {
    fs::create_dir_all(path)?;
    let metadata_path = path.join("metadata.json");
    let file = File::create(metadata_path)?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, metadata)?;
    Ok(())
}

/// Load database metadata from a directory
pub fn load_metadata(path: &Path) -> PersistenceResult<DatabaseMetadata> {
    let metadata_path = path.join("metadata.json");
    let file = File::open(metadata_path)?;
    let reader = BufReader::new(file);
    let metadata: DatabaseMetadata = serde_json::from_reader(reader)?;

    if metadata.version != STORAGE_VERSION {
        return Err(PersistenceError::InvalidFormat(format!(
            "Unsupported storage version: {} (expected {})",
            metadata.version, STORAGE_VERSION
        )));
    }

    Ok(metadata)
}

/// Save table data to a file
pub fn save_table_data(path: &Path, schema: &str, table: &str, data: &TableData) -> PersistenceResult<()> {
    let tables_dir = path.join("tables").join(schema);
    fs::create_dir_all(&tables_dir)?;
    let table_path = tables_dir.join(format!("{}.bincode", table));
    let file = File::create(table_path)?;
    let mut writer = BufWriter::new(file);
    bincode::serialize_into(&mut writer, data)?;
    writer.flush()?;
    Ok(())
}

/// Load table data from a file
pub fn load_table_data(path: &Path, schema: &str, table: &str) -> PersistenceResult<TableData> {
    let table_path = path.join("tables").join(schema).join(format!("{}.bincode", table));
    let file = File::open(table_path)?;
    let reader = BufReader::new(file);
    let data: TableData = bincode::deserialize_from(reader)?;
    Ok(data)
}

/// Check if a database exists at the given path
pub fn database_exists(path: &Path) -> bool {
    path.join("metadata.json").exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_save_load_metadata() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        let metadata = DatabaseMetadata {
            version: STORAGE_VERSION,
            schemas: vec![SchemaMetadata {
                name: "main".to_string(),
                tables: vec![TableMetadata {
                    name: "test".to_string(),
                    columns: vec![
                        ColumnMetadata {
                            name: "id".to_string(),
                            logical_type: LogicalType::Integer,
                            nullable: false,
                            default_value: None,
                        },
                        ColumnMetadata {
                            name: "name".to_string(),
                            logical_type: LogicalType::Varchar,
                            nullable: true,
                            default_value: None,
                        },
                    ],
                }],
                views: vec![],
                sequences: vec![],
                indexes: vec![],
            }],
        };

        save_metadata(path, &metadata).unwrap();
        let loaded = load_metadata(path).unwrap();

        assert_eq!(loaded.version, STORAGE_VERSION);
        assert_eq!(loaded.schemas.len(), 1);
        assert_eq!(loaded.schemas[0].tables.len(), 1);
        assert_eq!(loaded.schemas[0].tables[0].name, "test");
    }

    #[test]
    fn test_save_load_table_data() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        let data = TableData {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec![Value::Integer(1), Value::Varchar("Alice".to_string())],
                vec![Value::Integer(2), Value::Varchar("Bob".to_string())],
            ],
        };

        save_table_data(path, "main", "users", &data).unwrap();
        let loaded = load_table_data(path, "main", "users").unwrap();

        assert_eq!(loaded.columns, data.columns);
        assert_eq!(loaded.rows.len(), 2);
    }
}
