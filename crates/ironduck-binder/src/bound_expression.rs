//! Bound expressions with resolved types

use ironduck_common::{LogicalType, Value};

/// A bound expression with type information
#[derive(Debug, Clone)]
pub struct BoundExpression {
    pub expr: BoundExpressionKind,
    pub return_type: LogicalType,
    pub alias: Option<String>,
}

impl BoundExpression {
    pub fn new(expr: BoundExpressionKind, return_type: LogicalType) -> Self {
        BoundExpression {
            expr,
            return_type,
            alias: None,
        }
    }

    pub fn with_alias(mut self, alias: String) -> Self {
        self.alias = Some(alias);
        self
    }

    /// Get the display name for this expression
    pub fn name(&self) -> String {
        if let Some(alias) = &self.alias {
            return alias.clone();
        }
        match &self.expr {
            BoundExpressionKind::Constant(v) => v.to_string(),
            BoundExpressionKind::ColumnRef { name, .. } => name.clone(),
            BoundExpressionKind::BinaryOp { op, .. } => format!("{:?}", op),
            BoundExpressionKind::UnaryOp { op, .. } => format!("{:?}", op),
            BoundExpressionKind::Function { name, .. } => name.clone(),
            BoundExpressionKind::Cast { .. } => "CAST".to_string(),
            BoundExpressionKind::Case { .. } => "CASE".to_string(),
            BoundExpressionKind::IsNull { .. } => "IS NULL".to_string(),
            BoundExpressionKind::IsNotNull { .. } => "IS NOT NULL".to_string(),
            BoundExpressionKind::Between { .. } => "BETWEEN".to_string(),
            BoundExpressionKind::InList { .. } => "IN".to_string(),
            BoundExpressionKind::InSubquery { .. } => "IN".to_string(),
            BoundExpressionKind::Exists { .. } => "EXISTS".to_string(),
            BoundExpressionKind::ScalarSubquery(_) => "SUBQUERY".to_string(),
            BoundExpressionKind::Star => "*".to_string(),
            BoundExpressionKind::WindowFunction { name, .. } => name.clone(),
            BoundExpressionKind::RowId { .. } => "rowid".to_string(),
        }
    }
}

/// The kind of bound expression
#[derive(Debug, Clone)]
pub enum BoundExpressionKind {
    /// Constant value
    Constant(Value),

    /// Column reference
    ColumnRef {
        table_idx: usize,
        column_idx: usize,
        name: String,
    },

    /// Binary operation
    BinaryOp {
        left: Box<BoundExpression>,
        op: BoundBinaryOperator,
        right: Box<BoundExpression>,
    },

    /// Unary operation
    UnaryOp {
        op: BoundUnaryOperator,
        expr: Box<BoundExpression>,
    },

    /// Function call
    Function {
        name: String,
        args: Vec<BoundExpression>,
        is_aggregate: bool,
        /// DISTINCT modifier for aggregates (e.g., COUNT(DISTINCT x))
        distinct: bool,
    },

    /// Type cast
    Cast {
        expr: Box<BoundExpression>,
        target_type: LogicalType,
    },

    /// CASE expression
    Case {
        operand: Option<Box<BoundExpression>>,
        when_clauses: Vec<(BoundExpression, BoundExpression)>,
        else_result: Option<Box<BoundExpression>>,
    },

    /// IS NULL
    IsNull(Box<BoundExpression>),

    /// IS NOT NULL
    IsNotNull(Box<BoundExpression>),

    /// BETWEEN
    Between {
        expr: Box<BoundExpression>,
        low: Box<BoundExpression>,
        high: Box<BoundExpression>,
        negated: bool,
    },

    /// IN list
    InList {
        expr: Box<BoundExpression>,
        list: Vec<BoundExpression>,
        negated: bool,
    },

    /// IN subquery: expr IN (SELECT ...)
    InSubquery {
        expr: Box<BoundExpression>,
        subquery: Box<super::BoundSelect>,
        negated: bool,
    },

    /// EXISTS subquery: EXISTS (SELECT ...)
    Exists {
        subquery: Box<super::BoundSelect>,
        negated: bool,
    },

    /// Scalar subquery: (SELECT x FROM ... LIMIT 1)
    ScalarSubquery(Box<super::BoundSelect>),

    /// Star (SELECT *)
    Star,

    /// Window function
    WindowFunction {
        /// The function name (ROW_NUMBER, RANK, DENSE_RANK, etc.)
        name: String,
        /// Function arguments (for SUM, LAG, LEAD, etc.)
        args: Vec<BoundExpression>,
        /// PARTITION BY expressions
        partition_by: Vec<BoundExpression>,
        /// ORDER BY expressions (expression, ascending, nulls_first)
        order_by: Vec<(BoundExpression, bool, bool)>,
    },

    /// Row ID pseudo-column
    RowId {
        table_idx: usize,
    },
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundBinaryOperator {
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
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundUnaryOperator {
    Negate,
    Not,
    IsNull,
    IsNotNull,
}

impl BoundBinaryOperator {
    /// Get the result type of this operator given operand types
    pub fn result_type(&self, left: &LogicalType, right: &LogicalType) -> LogicalType {
        match self {
            // Comparison always returns boolean
            BoundBinaryOperator::Equal
            | BoundBinaryOperator::NotEqual
            | BoundBinaryOperator::LessThan
            | BoundBinaryOperator::LessThanOrEqual
            | BoundBinaryOperator::GreaterThan
            | BoundBinaryOperator::GreaterThanOrEqual
            | BoundBinaryOperator::Like
            | BoundBinaryOperator::ILike => LogicalType::Boolean,

            // Logical operators return boolean
            BoundBinaryOperator::And | BoundBinaryOperator::Or => LogicalType::Boolean,

            // String concat returns varchar
            BoundBinaryOperator::Concat => LogicalType::Varchar,

            // Arithmetic returns the common supertype
            BoundBinaryOperator::Add
            | BoundBinaryOperator::Subtract
            | BoundBinaryOperator::Multiply
            | BoundBinaryOperator::Divide
            | BoundBinaryOperator::Modulo => {
                left.common_supertype(right).unwrap_or(LogicalType::Double)
            }
        }
    }
}
