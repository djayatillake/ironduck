//! Index management

use super::CatalogId;
use ironduck_common::LogicalType;

pub type IndexId = CatalogId;

/// Type of index
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// B-tree index (default, good for range queries)
    BTree,
    /// Hash index (good for equality lookups)
    Hash,
}

/// An index on a table
#[derive(Debug, Clone)]
pub struct Index {
    /// Unique identifier
    pub id: IndexId,
    /// Index name
    pub name: String,
    /// Table this index is on
    pub table_name: String,
    /// Column names in the index
    pub columns: Vec<String>,
    /// Column types (for comparison)
    pub column_types: Vec<LogicalType>,
    /// Type of index
    pub index_type: IndexType,
    /// Whether this is a unique index
    pub unique: bool,
}

impl Index {
    pub fn new(
        id: IndexId,
        name: String,
        table_name: String,
        columns: Vec<String>,
        column_types: Vec<LogicalType>,
        index_type: IndexType,
        unique: bool,
    ) -> Self {
        Index {
            id,
            name,
            table_name,
            columns,
            column_types,
            index_type,
            unique,
        }
    }

    /// Check if this index covers the given columns (for index selection)
    pub fn covers_columns(&self, columns: &[String]) -> bool {
        // Index covers if its columns are a prefix of or equal to the requested columns
        if columns.len() > self.columns.len() {
            return false;
        }
        columns.iter().zip(&self.columns).all(|(a, b)| a.eq_ignore_ascii_case(b))
    }

    /// Check if this index is on a single column
    pub fn is_single_column(&self) -> bool {
        self.columns.len() == 1
    }
}
