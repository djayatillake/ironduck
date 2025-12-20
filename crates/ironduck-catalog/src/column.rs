//! Column definition

use ironduck_common::{LogicalType, Value};

pub type ColumnId = u32;

/// A column in a table
#[derive(Debug, Clone)]
pub struct Column {
    /// Column index within the table
    pub id: ColumnId,
    /// Column name
    pub name: String,
    /// Column type
    pub logical_type: LogicalType,
    /// Whether the column allows NULL values
    pub nullable: bool,
    /// Default value for the column (if any)
    pub default_value: Option<Value>,
}

impl Column {
    pub fn new(id: ColumnId, name: String, logical_type: LogicalType) -> Self {
        Column {
            id,
            name,
            logical_type,
            nullable: true,
            default_value: None,
        }
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn with_default(mut self, default: Value) -> Self {
        self.default_value = Some(default);
        self
    }
}
