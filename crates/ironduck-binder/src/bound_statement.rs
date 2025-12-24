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
    CreateView(BoundCreateView),
    CreateSequence(BoundCreateSequence),
    Drop(BoundDrop),
    AlterTable(BoundAlterTable),
    Explain(Box<BoundStatement>),
    /// No-op statement (PRAGMA, SET, etc.)
    NoOp,
}

/// Operand of a set operation (can be a select or another set operation)
#[derive(Debug, Clone)]
pub enum SetOperand {
    /// A simple SELECT query
    Select(BoundSelect),
    /// A nested set operation
    SetOperation(Box<BoundSetOperation>),
}

impl SetOperand {
    /// Get output column types
    pub fn output_types(&self) -> Vec<LogicalType> {
        match self {
            SetOperand::Select(sel) => sel.output_types(),
            SetOperand::SetOperation(op) => op.left.output_types(),
        }
    }

    /// Get output column names
    pub fn output_names(&self) -> Vec<String> {
        match self {
            SetOperand::Select(sel) => sel.output_names(),
            SetOperand::SetOperation(op) => op.left.output_names(),
        }
    }
}

/// Bound set operation (UNION, INTERSECT, EXCEPT)
#[derive(Debug, Clone)]
pub struct BoundSetOperation {
    /// Left operand (query or nested set operation)
    pub left: Box<SetOperand>,
    /// Right operand (query or nested set operation)
    pub right: Box<SetOperand>,
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
    /// GROUP BY expressions (simple group by)
    pub group_by: Vec<BoundExpression>,
    /// GROUPING SETS (for ROLLUP, CUBE, GROUPING SETS)
    /// Each inner Vec is one grouping set combination
    pub grouping_sets: Vec<Vec<BoundExpression>>,
    /// HAVING clause
    pub having: Option<BoundExpression>,
    /// QUALIFY clause (filters after window functions are evaluated)
    pub qualify: Option<BoundExpression>,
    /// ORDER BY expressions
    pub order_by: Vec<BoundOrderBy>,
    /// LIMIT
    pub limit: Option<u64>,
    /// OFFSET
    pub offset: Option<u64>,
    /// DISTINCT or DISTINCT ON
    pub distinct: DistinctKind,
    /// CTEs (Common Table Expressions) used in this query
    /// Includes recursive CTEs with their base and recursive cases
    pub ctes: Vec<BoundCTE>,
    /// VALUES rows (for VALUES clause)
    pub values_rows: Vec<Vec<BoundExpression>>,
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
            grouping_sets: Vec::new(),
            having: None,
            qualify: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            distinct: DistinctKind::None,
            ctes: Vec::new(),
            values_rows: Vec::new(),
        }
    }

    /// Create a new BoundSelect for VALUES clause
    pub fn new_values(values_rows: Vec<Vec<BoundExpression>>) -> Self {
        // Use the first row as the select_list for column types
        let select_list = values_rows.first().cloned().unwrap_or_default();
        BoundSelect {
            select_list,
            from: Vec::new(),
            where_clause: None,
            group_by: Vec::new(),
            grouping_sets: Vec::new(),
            having: None,
            qualify: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            distinct: DistinctKind::None,
            ctes: Vec::new(),
            values_rows,
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
        /// Whether this is a lateral subquery (can reference preceding tables)
        is_lateral: bool,
    },
    /// Set operation subquery (e.g., (SELECT ... UNION SELECT ...) AS alias)
    SetOperationSubquery {
        set_operation: Box<BoundSetOperation>,
        alias: String,
        /// Column names from the set operation
        column_names: Vec<String>,
        /// Column types from the set operation
        column_types: Vec<LogicalType>,
    },
    /// Join
    Join {
        left: Box<BoundTableRef>,
        right: Box<BoundTableRef>,
        join_type: BoundJoinType,
        condition: Option<BoundExpression>,
        /// Column names from USING clause that are deduplicated (excluded from right side in wildcards)
        using_columns: Vec<String>,
    },
    /// Table-valued function (e.g., range(), generate_series())
    TableFunction {
        function: TableFunctionType,
        alias: Option<String>,
        column_alias: Option<String>,
    },
    /// Recursive CTE reference (self-reference within a recursive CTE)
    RecursiveCTERef {
        /// Name of the CTE being referenced
        cte_name: String,
        /// Alias for this reference
        alias: String,
        /// Column names from the CTE
        column_names: Vec<String>,
        /// Column types from the CTE
        column_types: Vec<LogicalType>,
    },
    /// PIVOT operation - transforms rows to columns
    Pivot {
        /// The source table/subquery
        source: Box<BoundTableRef>,
        /// Aggregate function (e.g., SUM, COUNT)
        aggregate_function: String,
        /// Argument to the aggregate function
        aggregate_arg: BoundExpression,
        /// Column to pivot on (the FOR column)
        value_column: String,
        /// Values to create columns for (the IN values)
        pivot_values: Vec<String>,
        /// Alias for the result
        alias: Option<String>,
    },
    /// UNPIVOT operation - transforms columns to rows
    Unpivot {
        /// The source table/subquery
        source: Box<BoundTableRef>,
        /// Name for the value column
        value_column: String,
        /// Name for the name column
        name_column: String,
        /// Columns to unpivot
        unpivot_columns: Vec<String>,
        /// Alias for the result
        alias: Option<String>,
    },
    /// Empty (for SELECT without FROM)
    Empty,
}

/// Types of table-valued functions
#[derive(Debug, Clone)]
pub enum TableFunctionType {
    /// range(start, stop, step) or range(stop) - generates a sequence of integers
    Range {
        start: BoundExpression,
        stop: BoundExpression,
        step: BoundExpression,
    },
    /// unnest(array) - expands an array into rows
    Unnest {
        array_expr: BoundExpression,
    },
    /// generate_subscripts(array, dim) - generates subscripts for an array dimension
    GenerateSubscripts {
        array_expr: BoundExpression,
        dim: i32,
    },
}

/// Join types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundJoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    /// Semi join - returns rows from left that have a match in right
    Semi,
    /// Anti join - returns rows from left that have no match in right
    Anti,
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
    /// For CREATE TABLE ... AS SELECT ...
    pub source_query: Option<Box<BoundSelect>>,
}

/// Column definition
#[derive(Debug, Clone)]
pub struct BoundColumnDef {
    pub name: String,
    pub data_type: LogicalType,
    pub nullable: bool,
    pub default: Option<BoundExpression>,
    /// Whether this column is a primary key
    pub is_primary_key: bool,
    /// Whether this column has a UNIQUE constraint
    pub is_unique: bool,
    /// CHECK constraint expression (if any)
    pub check: Option<BoundExpression>,
}

/// Bound CREATE SCHEMA statement
#[derive(Debug, Clone)]
pub struct BoundCreateSchema {
    pub name: String,
    pub if_not_exists: bool,
}

/// Bound CREATE VIEW statement
#[derive(Debug, Clone)]
pub struct BoundCreateView {
    pub schema: String,
    pub name: String,
    /// The SQL query that defines the view
    pub sql: String,
    /// Column names derived from the query
    pub column_names: Vec<String>,
    /// Whether to replace if exists
    pub or_replace: bool,
}

/// Bound CREATE SEQUENCE statement
#[derive(Debug, Clone)]
pub struct BoundCreateSequence {
    pub schema: String,
    pub name: String,
    pub start: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cycle: bool,
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

/// Bound ALTER TABLE statement
#[derive(Debug, Clone)]
pub struct BoundAlterTable {
    pub schema: String,
    pub table_name: String,
    pub operation: AlterTableOperation,
}

/// ALTER TABLE operations
#[derive(Debug, Clone)]
pub enum AlterTableOperation {
    /// ADD COLUMN
    AddColumn {
        column: BoundColumnDef,
    },
    /// DROP COLUMN
    DropColumn {
        column_name: String,
        if_exists: bool,
    },
    /// RENAME COLUMN
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    /// RENAME TABLE
    RenameTable {
        new_name: String,
    },
    /// ALTER COLUMN TYPE
    AlterColumnType {
        column_name: String,
        new_type: LogicalType,
    },
    /// SET/DROP DEFAULT
    SetColumnDefault {
        column_name: String,
        default: Option<BoundExpression>,
    },
    /// SET/DROP NOT NULL
    SetColumnNotNull {
        column_name: String,
        not_null: bool,
    },
}

/// Bound Common Table Expression (CTE)
#[derive(Debug, Clone)]
pub struct BoundCTE {
    /// Name of the CTE
    pub name: String,
    /// Column aliases (optional)
    pub column_aliases: Vec<String>,
    /// The bound query for this CTE (base case for recursive CTEs)
    pub query: BoundSelect,
    /// Whether this is a recursive CTE
    pub is_recursive: bool,
    /// Recursive case query (only set for recursive CTEs)
    pub recursive_query: Option<BoundSelect>,
    /// Whether to use UNION ALL (true) or UNION (false) for recursive CTEs
    pub union_all: bool,
}

/// Bound Recursive CTE with separate base and recursive parts
#[derive(Debug, Clone)]
pub struct BoundRecursiveCTE {
    /// Name of the CTE
    pub name: String,
    /// Column aliases (optional)
    pub column_aliases: Vec<String>,
    /// Base case query (non-recursive part)
    pub base_case: BoundSelect,
    /// Recursive case query (references the CTE)
    pub recursive_case: BoundSelect,
    /// Whether to use UNION ALL (true) or UNION (false) semantics
    pub union_all: bool,
}
