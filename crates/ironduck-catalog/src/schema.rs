//! Schema management

use super::{CatalogId, Table, TableId};
use hashbrown::HashMap;
use ironduck_common::{Error, Result};
use parking_lot::RwLock;
use std::sync::Arc;

pub type SchemaId = CatalogId;

/// A schema contains tables, views, and other database objects
pub struct Schema {
    /// Unique identifier
    pub id: SchemaId,
    /// Schema name
    pub name: String,
    /// Tables in this schema
    tables: RwLock<HashMap<String, Arc<Table>>>,
}

impl Schema {
    pub fn new(id: SchemaId, name: String) -> Self {
        Schema {
            id,
            name,
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Add a table to this schema
    pub fn add_table(&self, table: Table) -> Result<()> {
        let mut tables = self.tables.write();
        if tables.contains_key(&table.name) {
            return Err(Error::TableAlreadyExists(table.name.clone()));
        }
        tables.insert(table.name.clone(), Arc::new(table));
        Ok(())
    }

    /// Get a table by name
    pub fn get_table(&self, name: &str) -> Option<Arc<Table>> {
        self.tables.read().get(name).cloned()
    }

    /// List all table names in this schema
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    /// Remove a table by name
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut tables = self.tables.write();
        if tables.remove(name).is_none() {
            return Err(Error::TableNotFound(name.to_string()));
        }
        Ok(())
    }
}
