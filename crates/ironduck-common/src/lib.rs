//! IronDuck Common - Core types and utilities shared across all crates
//!
//! This crate provides the foundational types used throughout IronDuck:
//! - `LogicalType`: The type system matching DuckDB semantics
//! - `Value`: Runtime value representation
//! - `Error`: Unified error types

pub mod error;
pub mod types;
pub mod value;

pub use error::{Error, Result};
pub use types::LogicalType;
pub use value::Value;
