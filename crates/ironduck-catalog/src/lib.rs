//! IronDuck Catalog - Schema and table management
//!
//! The catalog manages database objects: schemas, tables, columns, functions.

use hashbrown::HashMap;
use ironduck_common::{Error, LogicalType, Result, Value};
use parking_lot::RwLock;
use std::sync::Arc;

mod column;
mod function;
mod schema;
mod sequence;
mod table;
mod view;

pub use column::{Column, ColumnId};
pub use function::{Function, FunctionId};
pub use schema::{Schema, SchemaId};
pub use sequence::{Sequence, SequenceId};
pub use table::{Table, TableId};
pub use view::{View, ViewId};

/// Unique identifier for catalog entries
pub type CatalogId = u64;

/// The main catalog that holds all database objects
pub struct Catalog {
    /// All schemas in the catalog
    schemas: RwLock<HashMap<String, Arc<Schema>>>,
    /// Next available ID for new objects
    next_id: std::sync::atomic::AtomicU64,
}

impl Catalog {
    /// Create a new empty catalog with the default schema
    pub fn new() -> Self {
        let catalog = Catalog {
            schemas: RwLock::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
        };

        // Create default "main" schema
        let main_schema = Schema::new(0, "main".to_string());
        catalog
            .schemas
            .write()
            .insert("main".to_string(), Arc::new(main_schema));

        catalog
    }

    /// Get the next unique ID
    fn next_id(&self) -> CatalogId {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Get a schema by name (case-insensitive)
    pub fn get_schema(&self, name: &str) -> Option<Arc<Schema>> {
        let name_lower = name.to_lowercase();
        self.schemas.read().get(&name_lower).cloned()
    }

    /// Get the default schema
    pub fn default_schema(&self) -> Arc<Schema> {
        self.get_schema("main").expect("main schema must exist")
    }

    /// Create a new schema (name is normalized to lowercase)
    pub fn create_schema(&self, name: &str) -> Result<Arc<Schema>> {
        let mut schemas = self.schemas.write();
        let name_lower = name.to_lowercase();
        if schemas.contains_key(&name_lower) {
            return Err(Error::SchemaAlreadyExists(name.to_string()));
        }

        let schema = Arc::new(Schema::new(self.next_id(), name_lower.clone()));
        schemas.insert(name_lower, schema.clone());
        Ok(schema)
    }

    /// Create a table in the given schema
    pub fn create_table(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: Vec<(String, LogicalType)>,
    ) -> Result<TableId> {
        let defaults: Vec<Option<Value>> = vec![None; columns.len()];
        self.create_table_with_defaults(schema_name, table_name, columns, defaults)
    }

    /// Create a table in the given schema with default values
    pub fn create_table_with_defaults(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: Vec<(String, LogicalType)>,
        default_values: Vec<Option<Value>>,
    ) -> Result<TableId> {
        let schema = self
            .get_schema(schema_name)
            .ok_or_else(|| Error::SchemaNotFound(schema_name.to_string()))?;

        let table_id = self.next_id();
        let columns: Vec<Column> = columns
            .into_iter()
            .enumerate()
            .map(|(idx, (name, logical_type))| Column {
                id: idx as ColumnId,
                name,
                logical_type,
                nullable: true,
                default_value: default_values.get(idx).cloned().flatten(),
            })
            .collect();

        let table = Table::new(table_id, table_name.to_string(), columns);
        schema.add_table(table)?;

        Ok(table_id)
    }

    /// Get a table by schema and table name
    pub fn get_table(&self, schema_name: &str, table_name: &str) -> Option<Arc<Table>> {
        let schema = self.get_schema(schema_name)?;
        schema.get_table(table_name)
    }

    /// Create a view in the given schema
    pub fn create_view(
        &self,
        schema_name: &str,
        view_name: &str,
        sql: String,
        column_names: Vec<String>,
        or_replace: bool,
    ) -> Result<ViewId> {
        let schema = self
            .get_schema(schema_name)
            .ok_or_else(|| Error::SchemaNotFound(schema_name.to_string()))?;

        let view_id = self.next_id();
        let view = View::new(view_id, view_name.to_string(), sql, column_names);

        if or_replace {
            schema.add_or_replace_view(view);
        } else {
            schema.add_view(view)?;
        }

        Ok(view_id)
    }

    /// Get a view by schema and view name
    pub fn get_view(&self, schema_name: &str, view_name: &str) -> Option<Arc<View>> {
        let schema = self.get_schema(schema_name)?;
        schema.get_view(view_name)
    }

    /// Create a sequence in the given schema
    pub fn create_sequence(
        &self,
        schema_name: &str,
        sequence_name: &str,
        start: i64,
        increment: i64,
        min_value: i64,
        max_value: i64,
        cycle: bool,
    ) -> Result<SequenceId> {
        let schema = self
            .get_schema(schema_name)
            .ok_or_else(|| Error::SchemaNotFound(schema_name.to_string()))?;

        let sequence_id = self.next_id();
        let sequence = Sequence::new(
            sequence_id,
            sequence_name.to_string(),
            start,
            increment,
            min_value,
            max_value,
            cycle,
        );
        schema.add_sequence(sequence)?;

        Ok(sequence_id)
    }

    /// Get a sequence by schema and sequence name
    pub fn get_sequence(&self, schema_name: &str, sequence_name: &str) -> Option<Arc<Sequence>> {
        let schema = self.get_schema(schema_name)?;
        schema.get_sequence(sequence_name)
    }

    /// List all schema names
    pub fn list_schemas(&self) -> Vec<String> {
        self.schemas.read().keys().cloned().collect()
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_new() {
        let catalog = Catalog::new();
        assert!(catalog.get_schema("main").is_some());
    }

    #[test]
    fn test_create_schema() {
        let catalog = Catalog::new();
        let result = catalog.create_schema("test");
        assert!(result.is_ok());
        assert!(catalog.get_schema("test").is_some());
    }

    #[test]
    fn test_create_table() {
        let catalog = Catalog::new();
        let columns = vec![
            ("id".to_string(), LogicalType::Integer),
            ("name".to_string(), LogicalType::Varchar),
        ];
        let result = catalog.create_table("main", "users", columns);
        assert!(result.is_ok());
        assert!(catalog.get_table("main", "users").is_some());
    }
}
