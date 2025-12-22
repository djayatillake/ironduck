//! IronDuck - A pure Rust analytical database, DuckDB compatible
//!
//! IronDuck is a complete rewrite of DuckDB in Rust, providing:
//! - Memory safety by design (no leaks, no segfaults)
//! - Full SQL compatibility with DuckDB
//! - Vectorized query execution
//! - Columnar storage
//! - ACID transactions
//!
//! # Example
//!
//! ```rust
//! use ironduck::Database;
//!
//! let db = Database::new();
//!
//! // Execute a simple query
//! let result = db.execute("SELECT 1 + 1 AS answer").unwrap();
//! assert_eq!(result.row_count(), 1);
//! ```

pub use ironduck_catalog as catalog;
pub use ironduck_common as common;
pub use ironduck_execution as execution;
pub use ironduck_optimizer as optimizer;
pub use ironduck_parser as parser;
pub use ironduck_planner as planner;
pub use ironduck_storage as storage;
pub use ironduck_transaction as transaction;

use ironduck_binder::Binder;
use ironduck_catalog::Catalog;
use ironduck_common::{Error, Result, Value};
use ironduck_execution::Executor;
use std::sync::Arc;

/// The main database instance
pub struct Database {
    catalog: Arc<Catalog>,
    executor: Executor,
}

impl Database {
    /// Create a new in-memory database
    pub fn new() -> Self {
        let catalog = Arc::new(Catalog::new());
        let executor = Executor::new(catalog.clone());

        Database { catalog, executor }
    }

    /// Execute a SQL query
    pub fn execute(&self, sql: &str) -> Result<QueryResult> {
        // Parse
        let statements = parser::parse_sql(sql)?;

        if statements.is_empty() {
            return Ok(QueryResult::empty());
        }

        // For now, just execute the first statement
        let statement = &statements[0];

        // Bind
        let binder = Binder::new(self.catalog.clone());
        let bound = binder.bind(statement)?;

        // Plan
        let plan = planner::create_logical_plan(&bound)?;

        // Optimize
        let plan = optimizer::optimize(plan)?;

        // Execute
        let exec_result = self.executor.execute(&plan)?;

        Ok(QueryResult {
            columns: exec_result.columns,
            rows: exec_result.rows,
        })
    }

    /// Execute multiple SQL statements
    pub fn execute_batch(&self, sql: &str) -> Result<Vec<QueryResult>> {
        let statements = parser::parse_sql(sql)?;
        let mut results = Vec::new();

        for statement in &statements {
            let binder = Binder::new(self.catalog.clone());
            let bound = binder.bind(statement)?;
            let plan = planner::create_logical_plan(&bound)?;
            let plan = optimizer::optimize(plan)?;
            let exec_result = self.executor.execute(&plan)?;

            results.push(QueryResult {
                columns: exec_result.columns,
                rows: exec_result.rows,
            });
        }

        Ok(results)
    }

    /// Get the catalog
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Run a query and return a single value
    pub fn query_value(&self, sql: &str) -> Result<Value> {
        let result = self.execute(sql)?;
        result
            .rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next())
            .ok_or_else(|| Error::Internal("No result".to_string()))
    }

    /// Run a query and return all values from the first column
    pub fn query_column(&self, sql: &str) -> Result<Vec<Value>> {
        let result = self.execute(sql)?;
        Ok(result
            .rows
            .into_iter()
            .filter_map(|row| row.into_iter().next())
            .collect())
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a query execution
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Rows of values
    pub rows: Vec<Vec<Value>>,
}

impl QueryResult {
    /// Create an empty result
    pub fn empty() -> Self {
        QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get a single value (first row, first column)
    pub fn scalar(&self) -> Option<&Value> {
        self.rows.first().and_then(|row| row.first())
    }

    /// Format result as a table string
    pub fn to_table_string(&self) -> String {
        if self.columns.is_empty() {
            return String::new();
        }

        let mut output = String::new();

        // Calculate column widths
        let mut widths: Vec<usize> = self.columns.iter().map(|c| c.len()).collect();
        for row in &self.rows {
            for (i, val) in row.iter().enumerate() {
                if i < widths.len() {
                    widths[i] = widths[i].max(val.to_string().len());
                }
            }
        }

        // Header
        let header: Vec<String> = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{:width$}", c, width = widths[i]))
            .collect();
        output.push_str(&header.join(" | "));
        output.push('\n');

        // Separator
        let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
        output.push_str(&sep.join("-+-"));
        output.push('\n');

        // Rows
        for row in &self.rows {
            let formatted: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let width = widths.get(i).copied().unwrap_or(10);
                    format!("{:width$}", v.to_string(), width = width)
                })
                .collect();
            output.push_str(&formatted.join(" | "));
            output.push('\n');
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_new() {
        let db = Database::new();
        assert!(db.catalog().get_schema("main").is_some());
    }

    #[test]
    fn test_select_constant() {
        let db = Database::new();
        let result = db.execute("SELECT 1").unwrap();
        assert_eq!(result.row_count(), 1);
        assert_eq!(result.scalar(), Some(&Value::Integer(1)));
    }

    #[test]
    fn test_select_expression() {
        let db = Database::new();
        let result = db.execute("SELECT 1 + 2").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Integer(3)));
    }

    #[test]
    fn test_select_multiple_columns() {
        let db = Database::new();
        let result = db.execute("SELECT 1, 2, 3").unwrap();
        assert_eq!(result.column_count(), 3);
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn test_select_with_alias() {
        let db = Database::new();
        let result = db.execute("SELECT 42 AS answer").unwrap();
        assert_eq!(result.columns, vec!["answer"]);
    }

    #[test]
    fn test_select_string() {
        let db = Database::new();
        let result = db.execute("SELECT 'hello'").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Varchar("hello".to_string())));
    }

    #[test]
    fn test_arithmetic() {
        let db = Database::new();

        let result = db.execute("SELECT 10 - 3").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Integer(7)));

        let result = db.execute("SELECT 6 * 7").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Integer(42)));

        let result = db.execute("SELECT 10 / 3").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Integer(3)));
    }

    #[test]
    fn test_comparison() {
        let db = Database::new();

        let result = db.execute("SELECT 1 < 2").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Boolean(true)));

        let result = db.execute("SELECT 1 = 1").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Boolean(true)));

        let result = db.execute("SELECT 1 > 2").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Boolean(false)));
    }

    #[test]
    fn test_null_handling() {
        let db = Database::new();

        let result = db.execute("SELECT NULL").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Null));

        let result = db.execute("SELECT NULL IS NULL").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Boolean(true)));

        let result = db.execute("SELECT 1 IS NOT NULL").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Boolean(true)));
    }

    #[test]
    fn test_coalesce() {
        let db = Database::new();
        let result = db.execute("SELECT COALESCE(NULL, 42)").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Integer(42)));
    }

    #[test]
    fn test_greatest_least() {
        let db = Database::new();
        let result = db.execute("SELECT GREATEST(3, 1, 5)").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Integer(5)));

        let result = db.execute("SELECT LEAST(3, 1, 5)").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Integer(1)));

        let result = db.execute("SELECT GREATEST(NULL, 10, 5)").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Integer(10)));
    }

    #[test]
    fn test_case_expression() {
        let db = Database::new();
        let result = db.execute("SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Varchar("yes".to_string())));
    }

    #[test]
    fn test_create_table() {
        let db = Database::new();
        let result = db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)").unwrap();
        assert!(db.catalog().get_table("main", "test").is_some());
    }

    #[test]
    fn test_string_functions() {
        let db = Database::new();

        let result = db.execute("SELECT LOWER('HELLO')").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Varchar("hello".to_string())));

        let result = db.execute("SELECT UPPER('hello')").unwrap();
        assert_eq!(result.scalar(), Some(&Value::Varchar("HELLO".to_string())));

        let result = db.execute("SELECT LENGTH('hello')").unwrap();
        assert_eq!(result.scalar(), Some(&Value::BigInt(5)));
    }
}
