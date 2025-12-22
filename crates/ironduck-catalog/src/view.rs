//! View definition

use super::CatalogId;

/// Unique identifier for a view
pub type ViewId = CatalogId;

/// A view definition
#[derive(Debug, Clone)]
pub struct View {
    /// Unique identifier
    pub id: ViewId,
    /// View name
    pub name: String,
    /// The SQL query that defines this view
    pub sql: String,
    /// Column names (derived from the query)
    pub column_names: Vec<String>,
}

impl View {
    pub fn new(id: ViewId, name: String, sql: String, column_names: Vec<String>) -> Self {
        View {
            id,
            name,
            sql,
            column_names,
        }
    }
}
