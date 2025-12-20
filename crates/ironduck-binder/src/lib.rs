//! IronDuck Binder - Semantic analysis and schema binding
//!
//! The binder takes parsed SQL and resolves:
//! - Table and column references
//! - Type inference and checking
//! - Function resolution

mod bound_expression;
mod bound_statement;
mod expression_binder;
mod statement_binder;

pub use bound_expression::*;
pub use bound_statement::*;

use ironduck_catalog::Catalog;
use ironduck_common::{Error, LogicalType, Result};
use sqlparser::ast as sql;
use std::sync::Arc;

/// The binder context holds state during binding
pub struct Binder {
    catalog: Arc<Catalog>,
    /// Current schema for unqualified table references
    current_schema: String,
}

impl Binder {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Binder {
            catalog,
            current_schema: "main".to_string(),
        }
    }

    /// Bind a parsed statement
    pub fn bind(&self, statement: &sql::Statement) -> Result<BoundStatement> {
        statement_binder::bind_statement(self, statement)
    }

    /// Get the catalog
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Get the current schema name
    pub fn current_schema(&self) -> &str {
        &self.current_schema
    }
}
