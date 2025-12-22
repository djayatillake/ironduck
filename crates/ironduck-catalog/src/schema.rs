//! Schema management

use super::{CatalogId, Table, TableId, View};
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
    /// Views in this schema
    views: RwLock<HashMap<String, Arc<View>>>,
}

impl Schema {
    pub fn new(id: SchemaId, name: String) -> Self {
        Schema {
            id,
            name,
            tables: RwLock::new(HashMap::new()),
            views: RwLock::new(HashMap::new()),
        }
    }

    /// Add a table to this schema (name is normalized to lowercase)
    pub fn add_table(&self, mut table: Table) -> Result<()> {
        let mut tables = self.tables.write();
        // Normalize table name to lowercase for case-insensitive lookup
        table.name = table.name.to_lowercase();
        if tables.contains_key(&table.name) {
            return Err(Error::TableAlreadyExists(table.name.clone()));
        }
        tables.insert(table.name.clone(), Arc::new(table));
        Ok(())
    }

    /// Get a table by name (case-insensitive)
    pub fn get_table(&self, name: &str) -> Option<Arc<Table>> {
        let name_lower = name.to_lowercase();
        self.tables.read().get(&name_lower).cloned()
    }

    /// List all table names in this schema
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    /// Remove a table by name (case-insensitive)
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut tables = self.tables.write();
        let name_lower = name.to_lowercase();
        if tables.remove(&name_lower).is_none() {
            return Err(Error::TableNotFound(name.to_string()));
        }
        Ok(())
    }

    /// Add a view to this schema (name is normalized to lowercase)
    pub fn add_view(&self, mut view: View) -> Result<()> {
        let mut views = self.views.write();
        view.name = view.name.to_lowercase();
        if views.contains_key(&view.name) {
            return Err(Error::ViewAlreadyExists(view.name.clone()));
        }
        views.insert(view.name.clone(), Arc::new(view));
        Ok(())
    }

    /// Add or replace a view (for CREATE OR REPLACE VIEW)
    pub fn add_or_replace_view(&self, mut view: View) {
        let mut views = self.views.write();
        view.name = view.name.to_lowercase();
        views.insert(view.name.clone(), Arc::new(view));
    }

    /// Get a view by name (case-insensitive)
    pub fn get_view(&self, name: &str) -> Option<Arc<View>> {
        let name_lower = name.to_lowercase();
        self.views.read().get(&name_lower).cloned()
    }

    /// Remove a view by name (case-insensitive)
    pub fn drop_view(&self, name: &str) -> Result<()> {
        let mut views = self.views.write();
        let name_lower = name.to_lowercase();
        if views.remove(&name_lower).is_none() {
            return Err(Error::ViewNotFound(name.to_string()));
        }
        Ok(())
    }
}
