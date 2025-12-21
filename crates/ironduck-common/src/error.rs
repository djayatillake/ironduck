//! Error types for IronDuck

use thiserror::Error;

/// The main error type for IronDuck operations
#[derive(Error, Debug)]
pub enum Error {
    // Parser errors
    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Syntax error at position {position}: {message}")]
    Syntax { position: usize, message: String },

    // Catalog errors
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("Schema already exists: {0}")]
    SchemaAlreadyExists(String),

    // Binder errors
    #[error("Ambiguous column reference: {0}")]
    AmbiguousColumn(String),

    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("Invalid type cast from {from} to {to}")]
    InvalidCast { from: String, to: String },

    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),

    // Execution errors
    #[error("Division by zero")]
    DivisionByZero,

    #[error("Overflow in {operation}")]
    Overflow { operation: String },

    #[error("Out of memory")]
    OutOfMemory,

    #[error("Execution error: {0}")]
    Execution(String),

    // Storage errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Buffer pool full")]
    BufferPoolFull,

    #[error("Page not found: {0}")]
    PageNotFound(u64),

    #[error("Corrupted page: {0}")]
    CorruptedPage(u64),

    // Transaction errors
    #[error("Transaction aborted: {0}")]
    TransactionAborted(String),

    #[error("Write conflict on row {0}")]
    WriteConflict(u64),

    #[error("Serialization failure")]
    SerializationFailure,

    // General errors
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type alias using IronDuck's Error
pub type Result<T> = std::result::Result<T, Error>;
