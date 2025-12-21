//! Expression binding

use super::{BoundBinaryOperator, BoundExpression, BoundExpressionKind, BoundTableRef, BoundUnaryOperator, Binder};
use ironduck_common::{Error, LogicalType, Result, Value};
use sqlparser::ast as sql;

/// Context for binding expressions
pub struct ExpressionBinderContext<'a> {
    /// Available tables for column resolution
    pub tables: &'a [BoundTableRef],
}

impl<'a> ExpressionBinderContext<'a> {
    pub fn empty() -> Self {
        ExpressionBinderContext { tables: &[] }
    }

    pub fn new(tables: &'a [BoundTableRef]) -> Self {
        ExpressionBinderContext { tables }
    }
}

/// Bind an expression
pub fn bind_expression(
    _binder: &Binder,
    expr: &sql::Expr,
    ctx: &ExpressionBinderContext,
) -> Result<BoundExpression> {
    match expr {
        // Literals
        sql::Expr::Value(value) => bind_value(value),

        // Identifiers (column references)
        sql::Expr::Identifier(ident) => bind_column_ref(&[ident.clone()], ctx),

        sql::Expr::CompoundIdentifier(idents) => bind_column_ref(idents, ctx),

        // Binary operations
        sql::Expr::BinaryOp { left, op, right } => {
            let left = bind_expression(_binder, left, ctx)?;
            let right = bind_expression(_binder, right, ctx)?;
            let bound_op = bind_binary_op(op)?;
            let return_type = bound_op.result_type(&left.return_type, &right.return_type);

            Ok(BoundExpression::new(
                BoundExpressionKind::BinaryOp {
                    left: Box::new(left),
                    op: bound_op,
                    right: Box::new(right),
                },
                return_type,
            ))
        }

        // Unary operations
        sql::Expr::UnaryOp { op, expr } => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            let (bound_op, return_type) = match op {
                sql::UnaryOperator::Minus => {
                    (BoundUnaryOperator::Negate, bound_expr.return_type.clone())
                }
                sql::UnaryOperator::Plus => {
                    return Ok(bound_expr);
                }
                sql::UnaryOperator::Not => (BoundUnaryOperator::Not, LogicalType::Boolean),
                _ => return Err(Error::NotImplemented(format!("Unary operator {:?}", op))),
            };

            Ok(BoundExpression::new(
                BoundExpressionKind::UnaryOp {
                    op: bound_op,
                    expr: Box::new(bound_expr),
                },
                return_type,
            ))
        }

        // Nested expressions
        sql::Expr::Nested(inner) => bind_expression(_binder, inner, ctx),

        // Function calls
        sql::Expr::Function(func) => bind_function(_binder, func, ctx),

        // IS NULL / IS NOT NULL
        sql::Expr::IsNull(expr) => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            Ok(BoundExpression::new(
                BoundExpressionKind::IsNull(Box::new(bound_expr)),
                LogicalType::Boolean,
            ))
        }

        sql::Expr::IsNotNull(expr) => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            Ok(BoundExpression::new(
                BoundExpressionKind::IsNotNull(Box::new(bound_expr)),
                LogicalType::Boolean,
            ))
        }

        // BETWEEN
        sql::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            let bound_low = bind_expression(_binder, low, ctx)?;
            let bound_high = bind_expression(_binder, high, ctx)?;

            Ok(BoundExpression::new(
                BoundExpressionKind::Between {
                    expr: Box::new(bound_expr),
                    low: Box::new(bound_low),
                    high: Box::new(bound_high),
                    negated: *negated,
                },
                LogicalType::Boolean,
            ))
        }

        // IN list
        sql::Expr::InList { expr, list, negated } => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            let bound_list: Result<Vec<_>> = list
                .iter()
                .map(|e| bind_expression(_binder, e, ctx))
                .collect();

            Ok(BoundExpression::new(
                BoundExpressionKind::InList {
                    expr: Box::new(bound_expr),
                    list: bound_list?,
                    negated: *negated,
                },
                LogicalType::Boolean,
            ))
        }

        // CASE expression
        sql::Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let bound_operand = operand
                .as_ref()
                .map(|e| bind_expression(_binder, e, ctx))
                .transpose()?;

            let mut when_clauses = Vec::new();
            for (cond, result) in conditions.iter().zip(results.iter()) {
                let bound_cond = bind_expression(_binder, cond, ctx)?;
                let bound_result = bind_expression(_binder, result, ctx)?;
                when_clauses.push((bound_cond, bound_result));
            }

            let bound_else = else_result
                .as_ref()
                .map(|e| bind_expression(_binder, e, ctx))
                .transpose()?;

            // Determine return type from first result
            let return_type = when_clauses
                .first()
                .map(|(_, r)| r.return_type.clone())
                .unwrap_or(LogicalType::Null);

            Ok(BoundExpression::new(
                BoundExpressionKind::Case {
                    operand: bound_operand.map(Box::new),
                    when_clauses,
                    else_result: bound_else.map(Box::new),
                },
                return_type,
            ))
        }

        // Cast
        sql::Expr::Cast { expr, data_type, .. } => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            let target_type = bind_data_type(data_type)?;

            Ok(BoundExpression::new(
                BoundExpressionKind::Cast {
                    expr: Box::new(bound_expr),
                    target_type: target_type.clone(),
                },
                target_type,
            ))
        }

        // Wildcard
        sql::Expr::Wildcard(_) => Ok(BoundExpression::new(
            BoundExpressionKind::Star,
            LogicalType::Unknown,
        )),

        // IN subquery: expr IN (SELECT ...)
        sql::Expr::InSubquery { expr, subquery, negated } => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            let bound_subquery = super::statement_binder::bind_subquery(_binder, subquery)?;

            Ok(BoundExpression::new(
                BoundExpressionKind::InSubquery {
                    expr: Box::new(bound_expr),
                    subquery: Box::new(bound_subquery),
                    negated: *negated,
                },
                LogicalType::Boolean,
            ))
        }

        // EXISTS subquery
        sql::Expr::Exists { subquery, negated } => {
            let bound_subquery = super::statement_binder::bind_subquery(_binder, subquery)?;

            Ok(BoundExpression::new(
                BoundExpressionKind::Exists {
                    subquery: Box::new(bound_subquery),
                    negated: *negated,
                },
                LogicalType::Boolean,
            ))
        }

        // Scalar subquery
        sql::Expr::Subquery(subquery) => {
            let bound_subquery = super::statement_binder::bind_subquery(_binder, subquery)?;

            // Return type is the type of the first column
            let return_type = bound_subquery
                .select_list
                .first()
                .map(|e| e.return_type.clone())
                .unwrap_or(LogicalType::Null);

            Ok(BoundExpression::new(
                BoundExpressionKind::ScalarSubquery(Box::new(bound_subquery)),
                return_type,
            ))
        }

        // LIKE pattern matching
        sql::Expr::Like { negated, expr, pattern, .. } => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            let bound_pattern = bind_expression(_binder, pattern, ctx)?;

            let like_expr = BoundExpression::new(
                BoundExpressionKind::BinaryOp {
                    left: Box::new(bound_expr),
                    op: BoundBinaryOperator::Like,
                    right: Box::new(bound_pattern),
                },
                LogicalType::Boolean,
            );

            if *negated {
                Ok(BoundExpression::new(
                    BoundExpressionKind::UnaryOp {
                        op: BoundUnaryOperator::Not,
                        expr: Box::new(like_expr),
                    },
                    LogicalType::Boolean,
                ))
            } else {
                Ok(like_expr)
            }
        }

        // ILIKE pattern matching (case-insensitive)
        sql::Expr::ILike { negated, expr, pattern, .. } => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            let bound_pattern = bind_expression(_binder, pattern, ctx)?;

            let ilike_expr = BoundExpression::new(
                BoundExpressionKind::BinaryOp {
                    left: Box::new(bound_expr),
                    op: BoundBinaryOperator::ILike,
                    right: Box::new(bound_pattern),
                },
                LogicalType::Boolean,
            );

            if *negated {
                Ok(BoundExpression::new(
                    BoundExpressionKind::UnaryOp {
                        op: BoundUnaryOperator::Not,
                        expr: Box::new(ilike_expr),
                    },
                    LogicalType::Boolean,
                ))
            } else {
                Ok(ilike_expr)
            }
        }

        // SUBSTRING(string, start, length) or SUBSTRING(string FROM start FOR length)
        sql::Expr::Substring { expr, substring_from, substring_for, .. } => {
            let mut args = vec![bind_expression(_binder, expr, ctx)?];
            if let Some(from_expr) = substring_from {
                args.push(bind_expression(_binder, from_expr, ctx)?);
            }
            if let Some(for_expr) = substring_for {
                args.push(bind_expression(_binder, for_expr, ctx)?);
            }
            Ok(BoundExpression::new(
                BoundExpressionKind::Function {
                    name: "SUBSTRING".to_string(),
                    args,
                    is_aggregate: false,
                    distinct: false,
                },
                LogicalType::Varchar,
            ))
        }

        // TRIM([BOTH|LEADING|TRAILING] [characters] FROM string)
        sql::Expr::Trim { expr, trim_where, trim_what, .. } => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            let func_name = match trim_where {
                Some(sql::TrimWhereField::Leading) => "LTRIM",
                Some(sql::TrimWhereField::Trailing) => "RTRIM",
                Some(sql::TrimWhereField::Both) | None => "TRIM",
            };
            let mut args = vec![bound_expr];
            if let Some(what) = trim_what {
                args.push(bind_expression(_binder, what, ctx)?);
            }
            Ok(BoundExpression::new(
                BoundExpressionKind::Function {
                    name: func_name.to_string(),
                    args,
                    is_aggregate: false,
                    distinct: false,
                },
                LogicalType::Varchar,
            ))
        }

        // POSITION(substring IN string)
        sql::Expr::Position { expr, r#in } => {
            let substr = bind_expression(_binder, expr, ctx)?;
            let string = bind_expression(_binder, r#in, ctx)?;
            // Executor expects (haystack, needle) = (string, substring)
            Ok(BoundExpression::new(
                BoundExpressionKind::Function {
                    name: "POSITION".to_string(),
                    args: vec![string, substr],
                    is_aggregate: false,
                    distinct: false,
                },
                LogicalType::Integer,
            ))
        }

        // CEIL(expr)
        sql::Expr::Ceil { expr, .. } => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            Ok(BoundExpression::new(
                BoundExpressionKind::Function {
                    name: "CEIL".to_string(),
                    args: vec![bound_expr],
                    is_aggregate: false,
                    distinct: false,
                },
                LogicalType::Double,
            ))
        }

        // FLOOR(expr)
        sql::Expr::Floor { expr, .. } => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            Ok(BoundExpression::new(
                BoundExpressionKind::Function {
                    name: "FLOOR".to_string(),
                    args: vec![bound_expr],
                    is_aggregate: false,
                    distinct: false,
                },
                LogicalType::Double,
            ))
        }

        // EXTRACT(field FROM source)
        sql::Expr::Extract { field, expr, .. } => {
            let field_str = format!("{:?}", field).to_uppercase();
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            Ok(BoundExpression::new(
                BoundExpressionKind::Function {
                    name: "EXTRACT".to_string(),
                    args: vec![
                        BoundExpression::new(
                            BoundExpressionKind::Constant(Value::Varchar(field_str)),
                            LogicalType::Varchar,
                        ),
                        bound_expr,
                    ],
                    is_aggregate: false,
                    distinct: false,
                },
                LogicalType::BigInt,
            ))
        }

        // TypedString: TIMESTAMP '2024-01-01 00:00:00', DATE '2024-01-01', etc.
        sql::Expr::TypedString { data_type, value } => {
            use chrono::{NaiveDateTime, NaiveDate};
            match data_type {
                sql::DataType::Timestamp(_, _) => {
                    // Parse timestamp string
                    let dt = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
                        .map_err(|e| Error::Parse(format!("Invalid timestamp: {}", e)))?;
                    Ok(BoundExpression::new(
                        BoundExpressionKind::Constant(Value::Timestamp(dt)),
                        LogicalType::Timestamp,
                    ))
                }
                sql::DataType::Date => {
                    // Parse date string
                    let d = NaiveDate::parse_from_str(value, "%Y-%m-%d")
                        .map_err(|e| Error::Parse(format!("Invalid date: {}", e)))?;
                    Ok(BoundExpression::new(
                        BoundExpressionKind::Constant(Value::Date(d)),
                        LogicalType::Date,
                    ))
                }
                _ => Err(Error::NotImplemented(format!("TypedString for {:?}", data_type))),
            }
        }

        _ => Err(Error::NotImplemented(format!(
            "Expression type: {:?}",
            expr
        ))),
    }
}

/// Bind a literal value
fn bind_value(value: &sql::Value) -> Result<BoundExpression> {
    let (val, typ) = match value {
        sql::Value::Number(n, _) => {
            // Try to parse as integer first, then float
            if let Ok(i) = n.parse::<i64>() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    (Value::Integer(i as i32), LogicalType::Integer)
                } else {
                    (Value::BigInt(i), LogicalType::BigInt)
                }
            } else if let Ok(f) = n.parse::<f64>() {
                (Value::Double(f), LogicalType::Double)
            } else {
                return Err(Error::Parse(format!("Invalid number: {}", n)));
            }
        }
        sql::Value::SingleQuotedString(s) | sql::Value::DoubleQuotedString(s) => {
            (Value::Varchar(s.clone()), LogicalType::Varchar)
        }
        sql::Value::Boolean(b) => (Value::Boolean(*b), LogicalType::Boolean),
        sql::Value::Null => (Value::Null, LogicalType::Null),
        _ => {
            return Err(Error::NotImplemented(format!(
                "Value type: {:?}",
                value
            )))
        }
    };

    Ok(BoundExpression::new(BoundExpressionKind::Constant(val), typ))
}

/// Bind a column reference
fn bind_column_ref(idents: &[sql::Ident], ctx: &ExpressionBinderContext) -> Result<BoundExpression> {
    let column_name = idents.last().map(|i| i.value.clone()).unwrap_or_default();

    // Check if there's a table qualifier (e.g., "e.id" has qualifier "e")
    let table_qualifier = if idents.len() >= 2 {
        Some(idents[idents.len() - 2].value.clone())
    } else {
        None
    };

    // If no tables, this is an error (unless it's a constant expression context)
    if ctx.tables.is_empty() {
        return Err(Error::ColumnNotFound(column_name));
    }

    // Collect all base tables from the context (flatten joins)
    let base_tables = collect_base_tables(ctx.tables);

    // Track column offset for multi-table joins
    let mut col_offset = 0;

    // Search for the column in available tables
    for (table_idx, (name, alias, column_names, column_types)) in base_tables.iter().enumerate() {
        // Check if this table matches the qualifier (if any)
        let table_matches = match &table_qualifier {
            Some(qualifier) => {
                // Match against alias first, then table name
                alias.as_ref().map(|a| a.eq_ignore_ascii_case(qualifier)).unwrap_or(false)
                    || name.eq_ignore_ascii_case(qualifier)
            }
            None => true, // No qualifier means search all tables
        };

        if table_matches {
            if let Some(col_idx) = column_names.iter().position(|n| n.eq_ignore_ascii_case(&column_name)) {
                return Ok(BoundExpression::new(
                    BoundExpressionKind::ColumnRef {
                        table_idx,
                        column_idx: col_offset + col_idx,
                        name: column_name,
                    },
                    column_types[col_idx].clone(),
                ));
            }
        }

        col_offset += column_names.len();
    }

    Err(Error::ColumnNotFound(column_name))
}

/// Collect all base tables from table references, flattening joins
fn collect_base_tables(tables: &[BoundTableRef]) -> Vec<(String, Option<String>, Vec<String>, Vec<LogicalType>)> {
    let mut result = Vec::new();
    for table in tables {
        collect_base_tables_rec(table, &mut result);
    }
    result
}

fn collect_base_tables_rec(table_ref: &BoundTableRef, result: &mut Vec<(String, Option<String>, Vec<String>, Vec<LogicalType>)>) {
    match table_ref {
        BoundTableRef::BaseTable { name, alias, column_names, column_types, .. } => {
            result.push((name.clone(), alias.clone(), column_names.clone(), column_types.clone()));
        }
        BoundTableRef::Join { left, right, .. } => {
            collect_base_tables_rec(left, result);
            collect_base_tables_rec(right, result);
        }
        BoundTableRef::Subquery { subquery, alias } => {
            // Extract column names from the subquery's select list
            let column_names: Vec<String> = subquery.select_list.iter().map(|e| e.name()).collect();
            let column_types: Vec<LogicalType> = subquery.select_list.iter().map(|e| e.return_type.clone()).collect();
            result.push((alias.clone(), Some(alias.clone()), column_names, column_types));
        }
        BoundTableRef::Empty => {}
    }
}

/// Bind a binary operator
fn bind_binary_op(op: &sql::BinaryOperator) -> Result<BoundBinaryOperator> {
    match op {
        sql::BinaryOperator::Plus => Ok(BoundBinaryOperator::Add),
        sql::BinaryOperator::Minus => Ok(BoundBinaryOperator::Subtract),
        sql::BinaryOperator::Multiply => Ok(BoundBinaryOperator::Multiply),
        sql::BinaryOperator::Divide => Ok(BoundBinaryOperator::Divide),
        sql::BinaryOperator::Modulo => Ok(BoundBinaryOperator::Modulo),
        sql::BinaryOperator::Eq => Ok(BoundBinaryOperator::Equal),
        sql::BinaryOperator::NotEq => Ok(BoundBinaryOperator::NotEqual),
        sql::BinaryOperator::Lt => Ok(BoundBinaryOperator::LessThan),
        sql::BinaryOperator::LtEq => Ok(BoundBinaryOperator::LessThanOrEqual),
        sql::BinaryOperator::Gt => Ok(BoundBinaryOperator::GreaterThan),
        sql::BinaryOperator::GtEq => Ok(BoundBinaryOperator::GreaterThanOrEqual),
        sql::BinaryOperator::And => Ok(BoundBinaryOperator::And),
        sql::BinaryOperator::Or => Ok(BoundBinaryOperator::Or),
        sql::BinaryOperator::StringConcat => Ok(BoundBinaryOperator::Concat),
        _ => Err(Error::NotImplemented(format!("Binary operator {:?}", op))),
    }
}

/// Bind a function call
fn bind_function(
    binder: &Binder,
    func: &sql::Function,
    ctx: &ExpressionBinderContext,
) -> Result<BoundExpression> {
    let name = func.name.to_string().to_uppercase();

    // Bind arguments - FunctionArguments may be List or None
    let args: Result<Vec<_>> = match &func.args {
        sql::FunctionArguments::List(arg_list) => arg_list
            .args
            .iter()
            .filter_map(|arg| match arg {
                sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(e)) => {
                    Some(bind_expression(binder, e, ctx))
                }
                sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Wildcard) => {
                    Some(Ok(BoundExpression::new(
                        BoundExpressionKind::Star,
                        LogicalType::Unknown,
                    )))
                }
                _ => None,
            })
            .collect(),
        sql::FunctionArguments::None => Ok(vec![]),
        sql::FunctionArguments::Subquery(_) => {
            return Err(Error::NotImplemented("Subquery in function".to_string()));
        }
    };
    let args = args?;

    // Check if this is a window function (has OVER clause)
    if let Some(over) = &func.over {
        return bind_window_function(binder, &name, args, over, ctx);
    }

    // Check for DISTINCT modifier in aggregate functions
    let distinct = match &func.args {
        sql::FunctionArguments::List(arg_list) => arg_list.duplicate_treatment == Some(sql::DuplicateTreatment::Distinct),
        _ => false,
    };

    // Determine if aggregate and return type
    let (is_aggregate, return_type) = match name.as_str() {
        // Aggregate functions
        "COUNT" => (true, LogicalType::BigInt),
        "SUM" => {
            let arg_type = args.first().map(|a| &a.return_type).unwrap_or(&LogicalType::BigInt);
            let ret = if arg_type.is_floating_point() {
                LogicalType::Double
            } else {
                LogicalType::BigInt
            };
            (true, ret)
        }
        "AVG" => (true, LogicalType::Double),
        "MIN" | "MAX" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Null);
            (true, arg_type)
        }
        "FIRST" | "LAST" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Null);
            (true, arg_type)
        }
        // String aggregate
        "STRING_AGG" | "GROUP_CONCAT" | "LISTAGG" => (true, LogicalType::Varchar),
        // Array aggregate
        "ARRAY_AGG" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Null);
            (true, LogicalType::List(Box::new(arg_type)))
        }
        // Statistical aggregates
        "STDDEV" | "STDDEV_SAMP" | "STDDEV_POP" => (true, LogicalType::Double),
        "VARIANCE" | "VAR_SAMP" | "VAR_POP" => (true, LogicalType::Double),
        // Boolean aggregates
        "BOOL_AND" | "EVERY" => (true, LogicalType::Boolean),
        "BOOL_OR" | "ANY" => (true, LogicalType::Boolean),
        // Bitwise aggregates
        "BIT_AND" | "BIT_OR" | "BIT_XOR" => (true, LogicalType::BigInt),
        // Product aggregate
        "PRODUCT" => (true, LogicalType::Double),
        // Percentile/median aggregates
        "MEDIAN" => (true, LogicalType::Double),
        "PERCENTILE_CONT" | "PERCENTILE" => (true, LogicalType::Double),
        "PERCENTILE_DISC" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Double);
            (true, arg_type)
        }
        "MODE" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Unknown);
            (true, arg_type)
        }
        // Covariance and correlation
        "COVAR_POP" | "COVAR_SAMP" | "CORR" => (true, LogicalType::Double),

        // Scalar functions
        "LOWER" | "UPPER" | "TRIM" | "LTRIM" | "RTRIM" => (false, LogicalType::Varchar),
        "LENGTH" | "CHAR_LENGTH" => (false, LogicalType::BigInt),
        "ASCII" => (false, LogicalType::Integer),
        "CHAR" | "CHR" => (false, LogicalType::Varchar),
        "ABS" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Double);
            (false, arg_type)
        }
        "COALESCE" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Null);
            (false, arg_type)
        }
        "NULLIF" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Null);
            (false, arg_type)
        }
        "IFNULL" | "NVL" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Null);
            (false, arg_type)
        }
        "GREATEST" | "LEAST" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Null);
            (false, arg_type)
        }
        "NOW" | "CURRENT_TIMESTAMP" => (false, LogicalType::Timestamp),
        "CURRENT_DATE" => (false, LogicalType::Date),
        "GCD" | "LCM" | "FACTORIAL" => (false, LogicalType::BigInt),

        _ => {
            // Unknown function - assume scalar returning null for now
            (false, LogicalType::Null)
        }
    };

    Ok(BoundExpression::new(
        BoundExpressionKind::Function {
            name,
            args,
            is_aggregate,
            distinct,
        },
        return_type,
    ))
}

/// Bind a SQL data type to LogicalType
pub fn bind_data_type(data_type: &sql::DataType) -> Result<LogicalType> {
    match data_type {
        sql::DataType::Boolean => Ok(LogicalType::Boolean),
        sql::DataType::TinyInt(_) => Ok(LogicalType::TinyInt),
        sql::DataType::SmallInt(_) => Ok(LogicalType::SmallInt),
        sql::DataType::Int(_) | sql::DataType::Integer(_) => Ok(LogicalType::Integer),
        sql::DataType::BigInt(_) => Ok(LogicalType::BigInt),
        sql::DataType::Float(_) | sql::DataType::Real => Ok(LogicalType::Float),
        sql::DataType::Double | sql::DataType::DoublePrecision => Ok(LogicalType::Double),
        sql::DataType::Decimal(info) | sql::DataType::Numeric(info) => {
            let (width, scale) = match info {
                sql::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as u8),
                sql::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                sql::ExactNumberInfo::None => (18, 3),
            };
            Ok(LogicalType::Decimal { width, scale })
        }
        sql::DataType::Varchar(_) | sql::DataType::Text | sql::DataType::String(_) => {
            Ok(LogicalType::Varchar)
        }
        sql::DataType::Blob(_) | sql::DataType::Bytea => Ok(LogicalType::Blob),
        sql::DataType::Date => Ok(LogicalType::Date),
        sql::DataType::Time(_, _) => Ok(LogicalType::Time),
        sql::DataType::Timestamp(_, _) => Ok(LogicalType::Timestamp),
        sql::DataType::Uuid => Ok(LogicalType::Uuid),
        sql::DataType::Array(inner) => {
            match inner {
                sql::ArrayElemTypeDef::AngleBracket(inner_type) |
                sql::ArrayElemTypeDef::SquareBracket(inner_type, _) |
                sql::ArrayElemTypeDef::Parenthesis(inner_type) => {
                    let element_type = bind_data_type(inner_type)?;
                    Ok(LogicalType::List(Box::new(element_type)))
                }
                sql::ArrayElemTypeDef::None => {
                    Err(Error::Parse("Array type requires element type".to_string()))
                }
            }
        }
        _ => Err(Error::NotImplemented(format!("Data type: {:?}", data_type))),
    }
}

/// Bind a window function
fn bind_window_function(
    binder: &Binder,
    name: &str,
    args: Vec<BoundExpression>,
    over: &sql::WindowType,
    ctx: &ExpressionBinderContext,
) -> Result<BoundExpression> {
    // Extract window specification
    let (partition_by, order_by) = match over {
        sql::WindowType::WindowSpec(spec) => {
            // Bind PARTITION BY
            let partition_by: Result<Vec<_>> = spec
                .partition_by
                .iter()
                .map(|e| bind_expression(binder, e, ctx))
                .collect();
            let partition_by = partition_by?;

            // Bind ORDER BY
            let order_by: Result<Vec<_>> = spec
                .order_by
                .iter()
                .map(|o| {
                    let expr = bind_expression(binder, &o.expr, ctx)?;
                    let ascending = o.asc.unwrap_or(true);
                    let nulls_first = o.nulls_first.unwrap_or(false);
                    Ok((expr, ascending, nulls_first))
                })
                .collect();
            let order_by = order_by?;

            (partition_by, order_by)
        }
        sql::WindowType::NamedWindow(_) => {
            return Err(Error::NotImplemented("Named window references".to_string()));
        }
    };

    // Determine return type based on window function
    let return_type = match name {
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" => LogicalType::BigInt,
        "PERCENT_RANK" | "CUME_DIST" => LogicalType::Double,
        "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {
            args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Null)
        }
        // Aggregate functions used as window functions
        "SUM" => {
            let arg_type = args.first().map(|a| &a.return_type).unwrap_or(&LogicalType::BigInt);
            if arg_type.is_floating_point() {
                LogicalType::Double
            } else {
                LogicalType::BigInt
            }
        }
        "COUNT" => LogicalType::BigInt,
        "AVG" => LogicalType::Double,
        "MIN" | "MAX" => args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Null),
        _ => LogicalType::Null,
    };

    Ok(BoundExpression::new(
        BoundExpressionKind::WindowFunction {
            name: name.to_string(),
            args,
            partition_by,
            order_by,
        },
        return_type,
    ))
}
