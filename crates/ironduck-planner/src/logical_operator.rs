//! Logical operators for query plans

use ironduck_common::{LogicalType, Value};

/// Index into the list of tables in a query
pub type TableIndex = usize;

/// A logical operator in the query plan
#[derive(Debug, Clone)]
pub enum LogicalOperator {
    /// Scan a table
    Scan {
        schema: String,
        table: String,
        column_names: Vec<String>,
        output_types: Vec<LogicalType>,
    },

    /// Dummy scan that produces a single empty row (for SELECT without FROM)
    DummyScan,

    /// Filter rows
    Filter {
        input: Box<LogicalOperator>,
        predicate: Expression,
    },

    /// Project columns
    Project {
        input: Box<LogicalOperator>,
        expressions: Vec<Expression>,
        output_names: Vec<String>,
        output_types: Vec<LogicalType>,
    },

    /// Aggregate
    Aggregate {
        input: Box<LogicalOperator>,
        group_by: Vec<Expression>,
        aggregates: Vec<AggregateExpression>,
    },

    /// Join two relations
    Join {
        left: Box<LogicalOperator>,
        right: Box<LogicalOperator>,
        join_type: JoinType,
        condition: Option<Expression>,
    },

    /// Sort
    Sort {
        input: Box<LogicalOperator>,
        order_by: Vec<OrderByExpression>,
    },

    /// Limit
    Limit {
        input: Box<LogicalOperator>,
        limit: Option<u64>,
        offset: Option<u64>,
    },

    /// Distinct (all columns or DISTINCT ON specific expressions)
    Distinct {
        input: Box<LogicalOperator>,
        /// For DISTINCT ON, the expressions to deduplicate by
        on_exprs: Option<Vec<Expression>>,
    },

    /// Return constant values
    Values {
        values: Vec<Vec<Expression>>,
        output_types: Vec<LogicalType>,
    },

    /// CREATE TABLE
    CreateTable {
        schema: String,
        name: String,
        columns: Vec<(String, LogicalType)>,
        if_not_exists: bool,
        /// For CREATE TABLE ... AS SELECT ...
        source: Option<Box<LogicalOperator>>,
    },

    /// CREATE SCHEMA
    CreateSchema {
        name: String,
        if_not_exists: bool,
    },

    /// CREATE VIEW
    CreateView {
        schema: String,
        name: String,
        sql: String,
        column_names: Vec<String>,
        or_replace: bool,
    },

    /// INSERT
    Insert {
        schema: String,
        table: String,
        columns: Vec<String>,
        /// VALUES clause - direct values to insert
        values: Vec<Vec<Expression>>,
        /// Source query - for INSERT ... SELECT ...
        source: Option<Box<LogicalOperator>>,
    },

    /// DELETE
    Delete {
        schema: String,
        table: String,
        /// WHERE predicate for filtering rows to delete
        predicate: Option<Expression>,
    },

    /// UPDATE
    Update {
        schema: String,
        table: String,
        /// Column assignments (column_name, new_value)
        assignments: Vec<(String, Expression)>,
        /// WHERE predicate for filtering rows to update
        predicate: Option<Expression>,
    },

    /// DROP
    Drop {
        object_type: String,
        schema: Option<String>,
        name: String,
        if_exists: bool,
    },

    /// Set operation (UNION, INTERSECT, EXCEPT)
    SetOperation {
        left: Box<LogicalOperator>,
        right: Box<LogicalOperator>,
        op: SetOperationType,
        /// If true, don't eliminate duplicates (e.g., UNION ALL)
        all: bool,
    },

    /// Window function operator
    Window {
        input: Box<LogicalOperator>,
        /// Window function expressions
        window_exprs: Vec<WindowExpression>,
        /// Column names for output (input columns + window columns)
        output_names: Vec<String>,
        /// Types for output
        output_types: Vec<LogicalType>,
    },

    /// EXPLAIN - returns the query plan as text
    Explain {
        input: Box<LogicalOperator>,
    },

    /// No-op - for PRAGMA, SET, and other configuration statements
    NoOp,

    /// Table-valued function (e.g., range(), generate_series())
    TableFunction {
        function: TableFunctionKind,
        column_name: String,
        output_type: LogicalType,
    },
}

/// Types of table-valued functions
#[derive(Debug, Clone)]
pub enum TableFunctionKind {
    /// range(start, stop, step) - generates a sequence of integers
    Range {
        start: Expression,
        stop: Expression,
        step: Expression,
    },
}

/// Type of set operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperationType {
    Union,
    Intersect,
    Except,
}

impl LogicalOperator {
    /// Get the output types of this operator
    pub fn output_types(&self) -> Vec<LogicalType> {
        match self {
            LogicalOperator::Scan { output_types, .. } => output_types.clone(),
            LogicalOperator::DummyScan => vec![],
            LogicalOperator::Filter { input, .. } => input.output_types(),
            LogicalOperator::Project { output_types, .. } => output_types.clone(),
            LogicalOperator::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                let mut types = Vec::new();
                // Group by columns keep their types
                for _ in group_by {
                    types.push(LogicalType::Unknown); // TODO: Proper type
                }
                // Aggregate outputs
                for agg in aggregates {
                    types.push(agg.output_type());
                }
                types
            }
            LogicalOperator::Join { left, right, .. } => {
                let mut types = left.output_types();
                types.extend(right.output_types());
                types
            }
            LogicalOperator::Sort { input, .. } => input.output_types(),
            LogicalOperator::Limit { input, .. } => input.output_types(),
            LogicalOperator::Distinct { input, .. } => input.output_types(),
            LogicalOperator::Values { output_types, .. } => output_types.clone(),
            LogicalOperator::CreateTable { .. } => vec![LogicalType::Varchar],
            LogicalOperator::CreateSchema { .. } => vec![LogicalType::Varchar],
            LogicalOperator::CreateView { .. } => vec![LogicalType::Varchar],
            LogicalOperator::Insert { .. } => vec![LogicalType::BigInt],
            LogicalOperator::Delete { .. } => vec![LogicalType::BigInt],
            LogicalOperator::Update { .. } => vec![LogicalType::BigInt],
            LogicalOperator::Drop { .. } => vec![LogicalType::Varchar],
            LogicalOperator::SetOperation { left, .. } => left.output_types(),
            LogicalOperator::Window { output_types, .. } => output_types.clone(),
            LogicalOperator::Explain { .. } => vec![LogicalType::Varchar],
            LogicalOperator::NoOp => vec![LogicalType::Varchar],
            LogicalOperator::TableFunction { output_type, .. } => vec![output_type.clone()],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Semi,
    Anti,
}

/// An expression in the logical plan
#[derive(Debug, Clone)]
pub enum Expression {
    /// Column reference
    ColumnRef {
        table_index: TableIndex,
        column_index: usize,
        name: String,
    },
    /// Constant value
    Constant(Value),
    /// Binary operation
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// Unary operation
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    /// Function call
    Function {
        name: String,
        args: Vec<Expression>,
    },
    /// CASE expression
    Case {
        operand: Option<Box<Expression>>,
        conditions: Vec<Expression>,
        results: Vec<Expression>,
        else_result: Option<Box<Expression>>,
    },
    /// Cast expression
    Cast {
        expr: Box<Expression>,
        target_type: LogicalType,
    },
    /// IS NULL
    IsNull(Box<Expression>),
    /// IS NOT NULL
    IsNotNull(Box<Expression>),
    /// IN list
    InList {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    },
    /// IN subquery: expr IN (SELECT ...)
    InSubquery {
        expr: Box<Expression>,
        subquery: Box<LogicalOperator>,
        negated: bool,
    },
    /// EXISTS subquery
    Exists {
        subquery: Box<LogicalOperator>,
        negated: bool,
    },
    /// Scalar subquery
    Subquery(Box<LogicalOperator>),
    /// Row ID pseudo-column
    RowId {
        table_index: TableIndex,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,

    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    // Logical
    And,
    Or,

    // String
    Concat,
    Like,
    ILike,

    // Bitwise
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    ShiftLeft,
    ShiftRight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Negate,
    Not,
    BitwiseNot,
}

#[derive(Debug, Clone)]
pub struct AggregateExpression {
    pub function: AggregateFunction,
    pub args: Vec<Expression>,
    pub distinct: bool,
    pub filter: Option<Expression>,
}

impl AggregateExpression {
    pub fn output_type(&self) -> LogicalType {
        match self.function {
            AggregateFunction::Count => LogicalType::BigInt,
            AggregateFunction::Sum => LogicalType::BigInt, // TODO: Depends on input
            AggregateFunction::Avg => LogicalType::Double,
            AggregateFunction::Min | AggregateFunction::Max => LogicalType::Unknown, // Same as input
            AggregateFunction::First | AggregateFunction::Last => LogicalType::Unknown,
            AggregateFunction::StringAgg => LogicalType::Varchar,
            AggregateFunction::ArrayAgg => LogicalType::Unknown,
            AggregateFunction::StdDev | AggregateFunction::StdDevPop => LogicalType::Double,
            AggregateFunction::Variance | AggregateFunction::VariancePop => LogicalType::Double,
            AggregateFunction::BoolAnd | AggregateFunction::BoolOr => LogicalType::Boolean,
            AggregateFunction::BitAnd | AggregateFunction::BitOr | AggregateFunction::BitXor => LogicalType::BigInt,
            AggregateFunction::Product => LogicalType::Double,
            AggregateFunction::Median | AggregateFunction::PercentileCont => LogicalType::Double,
            AggregateFunction::PercentileDisc => LogicalType::Unknown, // Same as input
            AggregateFunction::Mode => LogicalType::Unknown, // Same as input
            AggregateFunction::CovarPop | AggregateFunction::CovarSamp | AggregateFunction::Corr => LogicalType::Double,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    StringAgg,
    ArrayAgg,
    StdDev,      // Sample standard deviation
    StdDevPop,   // Population standard deviation
    Variance,    // Sample variance
    VariancePop, // Population variance
    BoolAnd,     // Logical AND of all values
    BoolOr,      // Logical OR of all values
    BitAnd,        // Bitwise AND of all values
    BitOr,         // Bitwise OR of all values
    BitXor,        // Bitwise XOR of all values
    Product,       // Product of all values
    Median,        // Median (50th percentile)
    PercentileCont, // Continuous percentile
    PercentileDisc, // Discrete percentile
    Mode,          // Most frequent value
    CovarPop,      // Population covariance
    CovarSamp,     // Sample covariance
    Corr,          // Correlation coefficient
}

#[derive(Debug, Clone)]
pub struct OrderByExpression {
    pub expr: Expression,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// Window function expression
#[derive(Debug, Clone)]
pub struct WindowExpression {
    /// The window function (ROW_NUMBER, RANK, etc.)
    pub function: WindowFunction,
    /// Arguments to the function
    pub args: Vec<Expression>,
    /// PARTITION BY expressions
    pub partition_by: Vec<Expression>,
    /// ORDER BY expressions
    pub order_by: Vec<OrderByExpression>,
    /// Output type
    pub output_type: LogicalType,
}

/// Window function types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunction {
    // Ranking functions
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile,
    // Value functions
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
    // Aggregate functions as window functions
    Sum,
    Count,
    Avg,
    Min,
    Max,
}
