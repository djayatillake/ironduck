//! IronDuck Parser - SQL parsing with DuckDB dialect support
//!
//! This crate wraps sqlparser-rs and extends it with DuckDB-specific syntax.

use ironduck_common::{Error, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub mod ast;

/// Parse a SQL string into a list of statements
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    // Use GenericDialect for now - we'll create a DuckDB dialect later
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|e| Error::Parse(e.to_string()))
}

/// Parse a single SQL statement
pub fn parse_statement(sql: &str) -> Result<Statement> {
    let statements = parse_sql(sql)?;
    if statements.len() != 1 {
        return Err(Error::Parse(format!(
            "Expected 1 statement, got {}",
            statements.len()
        )));
    }
    Ok(statements.into_iter().next().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let result = parse_sql("SELECT 1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_parse_select_from() {
        let result = parse_sql("SELECT * FROM users WHERE id = 1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_multiple_statements() {
        let result = parse_sql("SELECT 1; SELECT 2");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_parse_error() {
        let result = parse_sql("SELECT * FORM users");
        assert!(result.is_err());
    }
}
