//! In-memory table storage

use ironduck_common::{LogicalType, Value};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Storage for all tables in memory
pub struct TableStorage {
    /// Table name -> table data
    tables: RwLock<HashMap<String, Arc<TableData>>>,
}

impl TableStorage {
    pub fn new() -> Self {
        TableStorage {
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create table data
    pub fn get_or_create(&self, schema: &str, table: &str, columns: &[LogicalType]) -> Arc<TableData> {
        let key = format!("{}.{}", schema, table);

        {
            let tables = self.tables.read();
            if let Some(data) = tables.get(&key) {
                return data.clone();
            }
        }

        // Create new table data
        let mut tables = self.tables.write();
        let data = Arc::new(TableData::new(columns.to_vec()));
        tables.insert(key, data.clone());
        data
    }

    /// Get table data if it exists
    pub fn get(&self, schema: &str, table: &str) -> Option<Arc<TableData>> {
        let key = format!("{}.{}", schema, table);
        self.tables.read().get(&key).cloned()
    }

    /// Drop a table
    pub fn drop_table(&self, schema: &str, table: &str) -> bool {
        let key = format!("{}.{}", schema, table);
        self.tables.write().remove(&key).is_some()
    }
}

impl Default for TableStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// Data for a single table
pub struct TableData {
    /// Column types
    pub column_types: Vec<LogicalType>,
    /// Rows of data
    rows: RwLock<Vec<Vec<Value>>>,
}

impl TableData {
    pub fn new(column_types: Vec<LogicalType>) -> Self {
        TableData {
            column_types,
            rows: RwLock::new(Vec::new()),
        }
    }

    /// Insert a row
    pub fn insert(&self, row: Vec<Value>) {
        self.rows.write().push(row);
    }

    /// Insert multiple rows
    pub fn insert_batch(&self, rows: Vec<Vec<Value>>) {
        self.rows.write().extend(rows);
    }

    /// Get all rows
    pub fn scan(&self) -> Vec<Vec<Value>> {
        self.rows.read().clone()
    }

    /// Get row count
    pub fn row_count(&self) -> usize {
        self.rows.read().len()
    }

    /// Clear all data
    pub fn truncate(&self) {
        self.rows.write().clear();
    }

    /// Delete rows matching a predicate
    pub fn delete<F>(&self, predicate: F) -> usize
    where
        F: Fn(&[Value]) -> bool,
    {
        let mut rows = self.rows.write();
        let before = rows.len();
        rows.retain(|row| !predicate(row));
        before - rows.len()
    }

    /// Update rows matching a predicate
    pub fn update<F, U>(&self, predicate: F, updater: U) -> usize
    where
        F: Fn(&[Value]) -> bool,
        U: Fn(&[Value]) -> Vec<Value>,
    {
        let mut rows = self.rows.write();
        let mut count = 0;
        for row in rows.iter_mut() {
            if predicate(row) {
                *row = updater(row);
                count += 1;
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_storage() {
        let storage = TableStorage::new();
        let columns = vec![LogicalType::Integer, LogicalType::Varchar];

        let table = storage.get_or_create("main", "test", &columns);
        table.insert(vec![Value::Integer(1), Value::Varchar("a".to_string())]);
        table.insert(vec![Value::Integer(2), Value::Varchar("b".to_string())]);

        let rows = table.scan();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_table_delete() {
        let storage = TableStorage::new();
        let columns = vec![LogicalType::Integer];

        let table = storage.get_or_create("main", "test", &columns);
        table.insert(vec![Value::Integer(1)]);
        table.insert(vec![Value::Integer(2)]);
        table.insert(vec![Value::Integer(3)]);

        let deleted = table.delete(|row| row[0] == Value::Integer(2));
        assert_eq!(deleted, 1);
        assert_eq!(table.row_count(), 2);
    }
}
