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

    /// Get a column's index by name
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
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

    /// Add a new column to the table
    pub fn add_column(&mut self, column: Column) {
        self.columns.push(column);
    }

    /// Drop a column by name, returns true if found and removed
    pub fn drop_column(&mut self, name: &str) -> bool {
        if let Some(idx) = self.columns.iter().position(|c| c.name == name) {
            self.columns.remove(idx);
            // Re-number column IDs
            for (i, col) in self.columns.iter_mut().enumerate() {
                col.id = i as u32;
            }
            true
        } else {
            false
        }
    }

    /// Rename a column, returns true if found and renamed
    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> bool {
        if let Some(col) = self.columns.iter_mut().find(|c| c.name == old_name) {
            col.name = new_name.to_string();
            true
        } else {
            false
        }
    }

    /// Change a column's type
    pub fn alter_column_type(&mut self, name: &str, new_type: ironduck_common::LogicalType) -> bool {
        if let Some(col) = self.columns.iter_mut().find(|c| c.name == name) {
            col.logical_type = new_type;
            true
        } else {
            false
        }
    }
}
