//! AST extensions for DuckDB-specific syntax
//!
//! This module will contain DuckDB-specific AST nodes that aren't
//! present in sqlparser-rs.

// Re-export sqlparser AST for convenience
pub use sqlparser::ast::*;

// TODO: Add DuckDB-specific AST nodes:
// - COPY ... FROM/TO with DuckDB options
// - PIVOT/UNPIVOT
// - QUALIFY clause
// - EXCLUDE/REPLACE in SELECT
// - Lambda expressions
// - STRUCT and LIST literals
