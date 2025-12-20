//! Table management

use super::{CatalogId, Column, ColumnId};

pub type TableId = CatalogId;

/// A table in the catalog
#[derive(Debug, Clone)]
pub struct Table {
    /// Unique identifier
    pub id: TableId,
    /// Table name
    pub name: String,
    /// Columns in this table
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(id: TableId, name: String, columns: Vec<Column>) -> Self {
        Table { id, name, columns }
    }

    /// Get a column by name
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get a column by index
    pub fn get_column_by_id(&self, id: ColumnId) -> Option<&Column> {
        self.columns.get(id as usize)
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get column names
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}
