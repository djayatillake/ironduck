//! Function registry

use super::CatalogId;
use ironduck_common::LogicalType;

pub type FunctionId = CatalogId;

/// Type of function
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionType {
    /// Scalar function (one output per input row)
    Scalar,
    /// Aggregate function (one output for group of rows)
    Aggregate,
    /// Window function (operates over a window frame)
    Window,
    /// Table function (returns a table)
    Table,
}

/// A function in the catalog
#[derive(Debug, Clone)]
pub struct Function {
    /// Unique identifier
    pub id: FunctionId,
    /// Function name
    pub name: String,
    /// Function type
    pub function_type: FunctionType,
    /// Parameter types
    pub parameters: Vec<LogicalType>,
    /// Return type
    pub return_type: LogicalType,
    /// Whether the function is variadic
    pub variadic: bool,
}

impl Function {
    pub fn new_scalar(
        id: FunctionId,
        name: String,
        parameters: Vec<LogicalType>,
        return_type: LogicalType,
    ) -> Self {
        Function {
            id,
            name,
            function_type: FunctionType::Scalar,
            parameters,
            return_type,
            variadic: false,
        }
    }

    pub fn new_aggregate(
        id: FunctionId,
        name: String,
        parameters: Vec<LogicalType>,
        return_type: LogicalType,
    ) -> Self {
        Function {
            id,
            name,
            function_type: FunctionType::Aggregate,
            parameters,
            return_type,
            variadic: false,
        }
    }
}
