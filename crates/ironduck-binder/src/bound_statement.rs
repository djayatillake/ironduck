//! Bound statements

use super::BoundExpression;
use ironduck_common::LogicalType;

/// A bound statement ready for planning
#[derive(Debug, Clone)]
pub enum BoundStatement {
    Select(BoundSelect),
    SetOperation(BoundSetOperation),
    Insert(BoundInsert),
    Delete(BoundDelete),
    Update(BoundUpdate),
    CreateTable(BoundCreateTable),
    CreateSchema(BoundCreateSchema),
    Drop(BoundDrop),
    Explain(Box<BoundStatement>),
    /// No-op statement (PRAGMA, SET, etc.)
    NoOp,
}

/// Bound set operation (UNION, INTERSECT, EXCEPT)
#[derive(Debug, Clone)]
pub struct BoundSetOperation {
    /// Left query
    pub left: Box<BoundSelect>,
    /// Right query
    pub right: Box<BoundSelect>,
    /// Type of set operation
    pub set_op: SetOperationType,
    /// Whether to eliminate duplicates (true for UNION, false for UNION ALL)
    pub all: bool,
    /// ORDER BY (applied to final result)
    pub order_by: Vec<BoundOrderBy>,
    /// LIMIT (applied to final result)
    pub limit: Option<u64>,
    /// OFFSET (applied to final result)
    pub offset: Option<u64>,
}

/// Type of set operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperationType {
    Union,
    Intersect,
    Except,
}

/// Bound SELECT statement
#[derive(Debug, Clone)]
pub struct BoundSelect {
    /// SELECT expressions
    pub select_list: Vec<BoundExpression>,
    /// FROM clause (table references)
    pub from: Vec<BoundTableRef>,
    /// WHERE clause
    pub where_clause: Option<BoundExpression>,
    /// GROUP BY expressions
    pub group_by: Vec<BoundExpression>,
    /// HAVING clause
    pub having: Option<BoundExpression>,
    /// ORDER BY expressions
    pub order_by: Vec<BoundOrderBy>,
    /// LIMIT
    pub limit: Option<u64>,
    /// OFFSET
    pub offset: Option<u64>,
    /// DISTINCT or DISTINCT ON
    pub distinct: DistinctKind,
}

/// DISTINCT mode
#[derive(Debug, Clone)]
pub enum DistinctKind {
    /// No DISTINCT
    None,
    /// DISTINCT (all columns)
    All,
    /// DISTINCT ON (specific columns)
    On(Vec<BoundExpression>),
}

impl BoundSelect {
    pub fn new(select_list: Vec<BoundExpression>) -> Self {
        BoundSelect {
            select_list,
            from: Vec::new(),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            distinct: DistinctKind::None,
        }
    }

    /// Get output column types
    pub fn output_types(&self) -> Vec<LogicalType> {
        self.select_list.iter().map(|e| e.return_type.clone()).collect()
    }

    /// Get output column names
    pub fn output_names(&self) -> Vec<String> {
        self.select_list.iter().map(|e| e.name()).collect()
    }
}

/// Bound table reference
#[derive(Debug, Clone)]
pub enum BoundTableRef {
    /// Base table
    BaseTable {
        schema: String,
        name: String,
        alias: Option<String>,
        column_names: Vec<String>,
        column_types: Vec<LogicalType>,
    },
    /// Subquery
    Subquery {
        subquery: Box<BoundSelect>,
        alias: String,
    },
    /// Join
    Join {
        left: Box<BoundTableRef>,
        right: Box<BoundTableRef>,
        join_type: BoundJoinType,
        condition: Option<BoundExpression>,
    },
    /// Empty (for SELECT without FROM)
    Empty,
}

/// Join types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundJoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// ORDER BY expression
#[derive(Debug, Clone)]
pub struct BoundOrderBy {
    pub expr: BoundExpression,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// Bound INSERT statement
#[derive(Debug, Clone)]
pub struct BoundInsert {
    pub schema: String,
    pub table: String,
    pub columns: Vec<String>,
    /// VALUES clause - used for INSERT ... VALUES (...)
    pub values: Vec<Vec<BoundExpression>>,
    /// Source query - used for INSERT ... SELECT ...
    pub source_query: Option<Box<BoundSelect>>,
}

/// Bound DELETE statement
#[derive(Debug, Clone)]
pub struct BoundDelete {
    pub schema: String,
    pub table: String,
    /// WHERE clause (optional)
    pub where_clause: Option<BoundExpression>,
}

/// Bound UPDATE statement
#[derive(Debug, Clone)]
pub struct BoundUpdate {
    pub schema: String,
    pub table: String,
    /// Column assignments (column_name, value_expression)
    pub assignments: Vec<(String, BoundExpression)>,
    /// WHERE clause (optional)
    pub where_clause: Option<BoundExpression>,
}

/// Bound CREATE TABLE statement
#[derive(Debug, Clone)]
pub struct BoundCreateTable {
    pub schema: String,
    pub name: String,
    pub columns: Vec<BoundColumnDef>,
    pub if_not_exists: bool,
}

/// Column definition
#[derive(Debug, Clone)]
pub struct BoundColumnDef {
    pub name: String,
    pub data_type: LogicalType,
    pub nullable: bool,
    pub default: Option<BoundExpression>,
}

/// Bound CREATE SCHEMA statement
#[derive(Debug, Clone)]
pub struct BoundCreateSchema {
    pub name: String,
    pub if_not_exists: bool,
}

/// Bound DROP statement
#[derive(Debug, Clone)]
pub struct BoundDrop {
    pub object_type: DropObjectType,
    pub schema: Option<String>,
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropObjectType {
    Table,
    Schema,
    View,
}

/// Bound Common Table Expression (CTE)
#[derive(Debug, Clone)]
pub struct BoundCTE {
    /// Name of the CTE
    pub name: String,
    /// Column aliases (optional)
    pub column_aliases: Vec<String>,
    /// The bound query for this CTE
    pub query: BoundSelect,
}
