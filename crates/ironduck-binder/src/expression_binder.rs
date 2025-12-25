//! Expression binding

use super::{BoundBinaryOperator, BoundExpression, BoundExpressionKind, BoundTableRef, BoundUnaryOperator, Binder};
use ironduck_common::{Error, LogicalType, Result, Value};
use sqlparser::ast as sql;

/// Context for binding expressions
pub struct ExpressionBinderContext<'a> {
    /// Available tables for column resolution
    pub tables: &'a [BoundTableRef],
    /// Named window definitions from WINDOW clause
    pub named_windows: &'a [sql::NamedWindowDefinition],
    /// Outer query tables for correlated subqueries
    pub outer_tables: &'a [BoundTableRef],
}

impl<'a> ExpressionBinderContext<'a> {
    pub fn empty() -> Self {
        ExpressionBinderContext { tables: &[], named_windows: &[], outer_tables: &[] }
    }

    pub fn new(tables: &'a [BoundTableRef]) -> Self {
        ExpressionBinderContext { tables, named_windows: &[], outer_tables: &[] }
    }

    pub fn with_named_windows(tables: &'a [BoundTableRef], named_windows: &'a [sql::NamedWindowDefinition]) -> Self {
        ExpressionBinderContext { tables, named_windows, outer_tables: &[] }
    }

    pub fn with_outer_tables(tables: &'a [BoundTableRef], outer_tables: &'a [BoundTableRef]) -> Self {
        ExpressionBinderContext { tables, named_windows: &[], outer_tables }
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

        // Cast (CAST and TRY_CAST)
        sql::Expr::Cast { expr, data_type, kind, .. } => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            let target_type = bind_data_type(data_type)?;

            let kind = match kind {
                sql::CastKind::TryCast => BoundExpressionKind::TryCast {
                    expr: Box::new(bound_expr),
                    target_type: target_type.clone(),
                },
                _ => BoundExpressionKind::Cast {
                    expr: Box::new(bound_expr),
                    target_type: target_type.clone(),
                },
            };

            Ok(BoundExpression::new(kind, target_type))
        }

        // Wildcard
        sql::Expr::Wildcard(_) => Ok(BoundExpression::new(
            BoundExpressionKind::Star,
            LogicalType::Unknown,
        )),

        // IN subquery: expr IN (SELECT ...)
        sql::Expr::InSubquery { expr, subquery, negated } => {
            let bound_expr = bind_expression(_binder, expr, ctx)?;
            // Pass outer tables for correlated subquery support
            let bound_subquery = if !ctx.tables.is_empty() {
                super::statement_binder::bind_subquery_with_outer(_binder, subquery, ctx.tables)?
            } else {
                super::statement_binder::bind_subquery(_binder, subquery)?
            };

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
            // Pass outer tables for correlated subquery support
            let bound_subquery = if !ctx.tables.is_empty() {
                super::statement_binder::bind_subquery_with_outer(_binder, subquery, ctx.tables)?
            } else {
                super::statement_binder::bind_subquery(_binder, subquery)?
            };

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
            // Pass outer tables for correlated subquery support
            let bound_subquery = if !ctx.tables.is_empty() {
                super::statement_binder::bind_subquery_with_outer(_binder, subquery, ctx.tables)?
            } else {
                super::statement_binder::bind_subquery(_binder, subquery)?
            };

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
                    order_by: vec![],
                    filter: None,
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
                    order_by: vec![],
                    filter: None,
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
                    order_by: vec![],
                    filter: None,
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
                    order_by: vec![],
                    filter: None,
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
                    order_by: vec![],
                    filter: None,
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
                    order_by: vec![],
                    filter: None,
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

        // Interval expressions: INTERVAL '1 day', INTERVAL '1' DAY, etc.
        sql::Expr::Interval(interval_expr) => {
            use ironduck_common::value::Interval as IronInterval;

            // Get the interval value string
            let value_str = match &*interval_expr.value {
                sql::Expr::Value(sql::Value::SingleQuotedString(s)) => s.clone(),
                sql::Expr::Value(sql::Value::Number(n, _)) => n.clone(),
                _ => return Err(Error::NotImplemented(format!("Interval expression type: {:?}", interval_expr.value))),
            };

            // Parse the interval based on leading_field or the string content
            let interval = if let Some(field) = &interval_expr.leading_field {
                // INTERVAL '1' DAY form
                let num: i64 = value_str.parse()
                    .map_err(|_| Error::Parse(format!("Invalid interval number: {}", value_str)))?;

                match field {
                    sql::DateTimeField::Year => IronInterval { months: num as i32 * 12, days: 0, micros: 0 },
                    sql::DateTimeField::Month => IronInterval { months: num as i32, days: 0, micros: 0 },
                    sql::DateTimeField::Day => IronInterval { months: 0, days: num as i32, micros: 0 },
                    sql::DateTimeField::Hour => IronInterval { months: 0, days: 0, micros: num * 60 * 60 * 1_000_000 },
                    sql::DateTimeField::Minute => IronInterval { months: 0, days: 0, micros: num * 60 * 1_000_000 },
                    sql::DateTimeField::Second => IronInterval { months: 0, days: 0, micros: num * 1_000_000 },
                    _ => return Err(Error::NotImplemented(format!("Interval field: {:?}", field))),
                }
            } else {
                // INTERVAL '1 day' form - parse the string
                parse_interval_string(&value_str)?
            };

            Ok(BoundExpression::new(
                BoundExpressionKind::Constant(Value::Interval(interval)),
                LogicalType::Interval,
            ))
        }

        // Array literal: [1, 2, 3] or ARRAY[1, 2, 3]
        sql::Expr::Array(arr) => {
            let mut elements = Vec::new();
            let mut element_type = LogicalType::Null;

            for elem in &arr.elem {
                let bound = bind_expression(_binder, elem, ctx)?;
                if element_type == LogicalType::Null {
                    element_type = bound.return_type.clone();
                }
                elements.push(bound);
            }

            // Create a list type
            let list_type = LogicalType::List(Box::new(element_type));

            // Convert to a constant List value if all elements are constants
            let all_constants = elements.iter().all(|e| matches!(e.expr, BoundExpressionKind::Constant(_)));
            if all_constants {
                let values: Vec<Value> = elements
                    .iter()
                    .filter_map(|e| {
                        if let BoundExpressionKind::Constant(v) = &e.expr {
                            Some(v.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok(BoundExpression::new(
                    BoundExpressionKind::Constant(Value::List(values)),
                    list_type,
                ))
            } else {
                // For non-constant arrays, we need an array expression type
                // For now, return an error as this is less common
                Err(Error::NotImplemented("Non-constant array literals".to_string()))
            }
        }

        // Array/Struct subscript: arr[index] or struct['field']
        sql::Expr::Subscript { expr, subscript } => {
            let base_expr = bind_expression(_binder, expr, ctx)?;
            match subscript.as_ref() {
                sql::Subscript::Index { index } => {
                    let index_expr = bind_expression(_binder, index, ctx)?;

                    // Check if this is a struct access or array access
                    let is_struct = matches!(&base_expr.return_type, LogicalType::Struct(_));
                    let is_string_key = matches!(&index_expr.return_type, LogicalType::Varchar);

                    if is_struct || is_string_key {
                        // Struct field access
                        Ok(BoundExpression::new(
                            BoundExpressionKind::Function {
                                name: "STRUCT_EXTRACT".to_string(),
                                args: vec![base_expr, index_expr],
                                is_aggregate: false,
                                distinct: false,
                                order_by: vec![],
                                filter: None,
                            },
                            LogicalType::Unknown,
                        ))
                    } else {
                        // Array element access
                        let element_type = match &base_expr.return_type {
                            LogicalType::List(inner) => inner.as_ref().clone(),
                            _ => LogicalType::Unknown,
                        };
                        Ok(BoundExpression::new(
                            BoundExpressionKind::Function {
                                name: "ARRAY_EXTRACT".to_string(),
                                args: vec![base_expr, index_expr],
                                is_aggregate: false,
                                distinct: false,
                                order_by: vec![],
                                filter: None,
                            },
                            element_type,
                        ))
                    }
                }
                sql::Subscript::Slice { lower_bound, upper_bound, stride } => {
                    let lower = lower_bound.as_ref()
                        .map(|e| bind_expression(_binder, e, ctx))
                        .transpose()?;
                    let upper = upper_bound.as_ref()
                        .map(|e| bind_expression(_binder, e, ctx))
                        .transpose()?;
                    let _stride = stride.as_ref()
                        .map(|e| bind_expression(_binder, e, ctx))
                        .transpose()?;

                    let mut args = vec![base_expr.clone()];
                    if let Some(l) = lower {
                        args.push(l);
                    } else {
                        args.push(BoundExpression::new(
                            BoundExpressionKind::Constant(Value::BigInt(1)),
                            LogicalType::BigInt,
                        ));
                    }
                    if let Some(u) = upper {
                        args.push(u);
                    }

                    Ok(BoundExpression::new(
                        BoundExpressionKind::Function {
                            name: "ARRAY_SLICE".to_string(),
                            args,
                            is_aggregate: false,
                            distinct: false,
                            order_by: vec![],
                            filter: None,
                        },
                        base_expr.return_type,
                    ))
                }
            }
        }

        // Dictionary/Struct literal: {'a': 1, 'b': 2}
        sql::Expr::Dictionary(fields) => {
            let mut keys = Vec::new();
            let mut values = Vec::new();
            for field in fields {
                keys.push(field.key.value.clone());
                let bound_value = bind_expression(_binder, &field.value, ctx)?;
                values.push(bound_value);
            }

            // Convert to constant Struct if all values are constants
            let all_constants = values.iter().all(|e| matches!(e.expr, BoundExpressionKind::Constant(_)));
            if all_constants {
                let struct_values: Vec<(String, Value)> = keys
                    .into_iter()
                    .zip(values.iter())
                    .filter_map(|(k, v)| {
                        if let BoundExpressionKind::Constant(val) = &v.expr {
                            Some((k, val.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok(BoundExpression::new(
                    BoundExpressionKind::Constant(Value::Struct(struct_values)),
                    LogicalType::Struct(vec![]),
                ))
            } else {
                Err(Error::NotImplemented("Non-constant struct literals".to_string()))
            }
        }

        // MapAccess: map['key'] or struct['field']
        sql::Expr::MapAccess { column, keys } => {
            let map_expr = bind_expression(_binder, column, ctx)?;
            if let Some(key) = keys.first() {
                let key_expr = match &key.key {
                    sql::Expr::Value(sql::Value::SingleQuotedString(s)) => {
                        BoundExpression::new(
                            BoundExpressionKind::Constant(Value::Varchar(s.clone())),
                            LogicalType::Varchar,
                        )
                    }
                    sql::Expr::Value(sql::Value::Number(n, _)) => {
                        let idx: i64 = n.parse().unwrap_or(0);
                        BoundExpression::new(
                            BoundExpressionKind::Constant(Value::BigInt(idx)),
                            LogicalType::BigInt,
                        )
                    }
                    other => bind_expression(_binder, other, ctx)?,
                };

                Ok(BoundExpression::new(
                    BoundExpressionKind::Function {
                        name: "STRUCT_EXTRACT".to_string(),
                        args: vec![map_expr, key_expr],
                        is_aggregate: false,
                        distinct: false,
                        order_by: vec![],
                        filter: None,
                    },
                    LogicalType::Unknown,
                ))
            } else {
                Err(Error::InvalidArguments("Map access requires a key".to_string()))
            }
        }

        _ => Err(Error::NotImplemented(format!(
            "Expression type: {:?}",
            expr
        ))),
    }
}

/// Parse an interval string like "1 day", "2 hours 30 minutes", etc.
fn parse_interval_string(s: &str) -> Result<ironduck_common::value::Interval> {
    use ironduck_common::value::Interval;

    let s = s.trim().to_lowercase();
    let mut months = 0i32;
    let mut days = 0i32;
    let mut micros = 0i64;

    // Handle PostgreSQL @ prefix
    let s = s.strip_prefix('@').unwrap_or(&s).trim();

    // Split by spaces and parse pairs
    let parts: Vec<&str> = s.split_whitespace().collect();
    let mut i = 0;

    while i < parts.len() {
        // Try to get a number
        if let Ok(num) = parts[i].parse::<i64>() {
            // Get the unit (next part or combined like "1day")
            if i + 1 < parts.len() {
                let unit = parts[i + 1].trim_end_matches('s'); // Remove plural 's'
                match unit {
                    "year" => months += (num * 12) as i32,
                    "month" | "mon" => months += num as i32,
                    "week" => days += (num * 7) as i32,
                    "day" => days += num as i32,
                    "hour" => micros += num * 60 * 60 * 1_000_000,
                    "minute" | "min" => micros += num * 60 * 1_000_000,
                    "second" | "sec" => micros += num * 1_000_000,
                    "millisecond" | "ms" => micros += num * 1_000,
                    "microsecond" | "us" => micros += num,
                    _ => {}
                }
                i += 2;
            } else {
                // Just a number, assume days
                days += num as i32;
                i += 1;
            }
        } else {
            // Try parsing "1day" combined form
            let part = parts[i];
            let num_part = part.trim_end_matches(|c: char| c.is_alphabetic());
            if let Ok(num) = num_part.parse::<i64>() {
                let unit_part = &part[num_part.len()..];
                let unit = unit_part.trim_end_matches('s'); // Remove plural 's'
                match unit {
                    "year" => months += (num * 12) as i32,
                    "month" | "mon" => months += num as i32,
                    "week" => days += (num * 7) as i32,
                    "day" => days += num as i32,
                    "hour" => micros += num * 60 * 60 * 1_000_000,
                    "minute" | "min" => micros += num * 60 * 1_000_000,
                    "second" | "sec" => micros += num * 1_000_000,
                    _ => {}
                }
            }
            i += 1;
        }
    }

    Ok(Interval { months, days, micros })
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

    // If no tables and no outer tables, this is an error
    if ctx.tables.is_empty() && ctx.outer_tables.is_empty() {
        return Err(Error::ColumnNotFound(column_name));
    }

    // First, try to find the column in the inner tables
    if !ctx.tables.is_empty() {
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

        // Check for rowid pseudo-column
        if column_name.eq_ignore_ascii_case("rowid") {
            // Find the matching table for rowid
            for (table_idx, (name, alias, _column_names, _column_types)) in base_tables.iter().enumerate() {
                let table_matches = match &table_qualifier {
                    Some(qualifier) => {
                        alias.as_ref().map(|a| a.eq_ignore_ascii_case(qualifier)).unwrap_or(false)
                            || name.eq_ignore_ascii_case(qualifier)
                    }
                    None => true,
                };

                if table_matches {
                    return Ok(BoundExpression::new(
                        BoundExpressionKind::RowId { table_idx },
                        LogicalType::BigInt,
                    ));
                }
            }
        }
    }

    // If not found in inner tables, try outer tables (for correlated subqueries)
    if !ctx.outer_tables.is_empty() {
        let outer_base_tables = collect_base_tables(ctx.outer_tables);

        // Track column offset for multi-table joins
        let mut col_offset = 0;

        for (_table_idx, (name, alias, column_names, column_types)) in outer_base_tables.iter().enumerate() {
            let table_matches = match &table_qualifier {
                Some(qualifier) => {
                    alias.as_ref().map(|a| a.eq_ignore_ascii_case(qualifier)).unwrap_or(false)
                        || name.eq_ignore_ascii_case(qualifier)
                }
                None => true,
            };

            if table_matches {
                if let Some(col_idx) = column_names.iter().position(|n| n.eq_ignore_ascii_case(&column_name)) {
                    return Ok(BoundExpression::new(
                        BoundExpressionKind::OuterColumnRef {
                            depth: 0, // Immediate parent scope
                            column_idx: col_offset + col_idx,
                            name: column_name,
                        },
                        column_types[col_idx].clone(),
                    ));
                }
            }

            col_offset += column_names.len();
        }
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
        BoundTableRef::Subquery { subquery, alias, .. } => {
            // Extract column names from the subquery's select list
            let column_names: Vec<String> = subquery.select_list.iter().map(|e| e.name()).collect();
            let column_types: Vec<LogicalType> = subquery.select_list.iter().map(|e| e.return_type.clone()).collect();
            result.push((alias.clone(), Some(alias.clone()), column_names, column_types));
        }
        BoundTableRef::TableFunction { alias, column_alias, .. } => {
            // Table function produces a single column
            let col_name = column_alias.clone().unwrap_or_else(|| "range".to_string());
            let table_name = alias.clone().unwrap_or_else(|| "range".to_string());
            result.push((table_name.clone(), alias.clone(), vec![col_name], vec![LogicalType::BigInt]));
        }
        BoundTableRef::RecursiveCTERef { cte_name, alias, column_names, column_types } => {
            result.push((cte_name.clone(), Some(alias.clone()), column_names.clone(), column_types.clone()));
        }
        BoundTableRef::SetOperationSubquery { alias, column_names, column_types, .. } => {
            result.push((alias.clone(), Some(alias.clone()), column_names.clone(), column_types.clone()));
        }
        BoundTableRef::Pivot { source, .. } => {
            collect_base_tables_rec(source, result);
        }
        BoundTableRef::Unpivot { source, .. } => {
            collect_base_tables_rec(source, result);
        }
        BoundTableRef::FileTableFunction { path, alias, column_names, column_types, .. } => {
            let table_name = alias.clone().unwrap_or_else(|| path.clone());
            result.push((table_name.clone(), alias.clone(), column_names.clone(), column_types.clone()));
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
        sql::BinaryOperator::BitwiseAnd => Ok(BoundBinaryOperator::BitwiseAnd),
        sql::BinaryOperator::BitwiseOr => Ok(BoundBinaryOperator::BitwiseOr),
        sql::BinaryOperator::BitwiseXor => Ok(BoundBinaryOperator::BitwiseXor),
        sql::BinaryOperator::Xor => Ok(BoundBinaryOperator::BitwiseXor),
        sql::BinaryOperator::PGBitwiseXor => Ok(BoundBinaryOperator::BitwiseXor),
        sql::BinaryOperator::PGBitwiseShiftLeft => Ok(BoundBinaryOperator::ShiftLeft),
        sql::BinaryOperator::PGBitwiseShiftRight => Ok(BoundBinaryOperator::ShiftRight),
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

    // Extract ORDER BY clause for ordered aggregates (e.g., SUM(x ORDER BY y))
    let mut order_by: Vec<(BoundExpression, bool, bool)> = match &func.args {
        sql::FunctionArguments::List(arg_list) => {
            let mut order_by_exprs = Vec::new();
            for clause in &arg_list.clauses {
                if let sql::FunctionArgumentClause::OrderBy(order_by_list) = clause {
                    for order_expr in order_by_list {
                        let bound_expr = bind_expression(binder, &order_expr.expr, ctx)?;
                        let ascending = order_expr.asc.unwrap_or(true);
                        let nulls_first = order_expr.nulls_first.unwrap_or(!ascending);
                        order_by_exprs.push((bound_expr, ascending, nulls_first));
                    }
                }
            }
            order_by_exprs
        }
        _ => vec![],
    };

    // Handle WITHIN GROUP (ORDER BY ...) syntax for ordered-set aggregates
    // e.g., PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)
    if !func.within_group.is_empty() {
        for order_expr in &func.within_group {
            let bound_expr = bind_expression(binder, &order_expr.expr, ctx)?;
            let ascending = order_expr.asc.unwrap_or(true);
            let nulls_first = order_expr.nulls_first.unwrap_or(!ascending);
            order_by.push((bound_expr, ascending, nulls_first));
        }
    }

    // Extract FILTER clause for aggregates (e.g., SUM(x) FILTER (WHERE y > 0))
    let filter: Option<Box<BoundExpression>> = match &func.filter {
        Some(filter_expr) => Some(Box::new(bind_expression(binder, filter_expr, ctx)?)),
        None => None,
    };

    // Validate argument count for functions that require specific numbers
    match name.as_str() {
        // Single-argument aggregate functions
        "AVG" | "SUM" | "MIN" | "MAX" | "FIRST" | "LAST" | "STDDEV" | "STDDEV_SAMP"
        | "STDDEV_POP" | "VARIANCE" | "VAR_SAMP" | "VAR_POP" | "BIT_AND" | "BIT_OR"
        | "BIT_XOR" | "PRODUCT" | "MEDIAN" | "MODE" | "ARRAY_AGG" | "LIST_AGG" | "LIST" | "BOOL_AND"
        | "BOOL_OR" | "EVERY" | "ANY" => {
            // These functions need exactly 1 argument (excluding COUNT which can be 0 with *)
            if args.is_empty() {
                return Err(Error::InvalidArguments(format!(
                    "{}() requires an argument", name
                )));
            }
            if args.len() > 1 {
                return Err(Error::InvalidArguments(format!(
                    "{}() takes only 1 argument, got {}", name, args.len()
                )));
            }
        }
        // Two-argument functions
        "COVAR_POP" | "COVAR_SAMP" | "CORR" => {
            if args.len() != 2 {
                return Err(Error::InvalidArguments(format!(
                    "{}() requires exactly 2 arguments, got {}", name, args.len()
                )));
            }
        }
        _ => {}
    }

    // Determine if aggregate and return type
    let (is_aggregate, return_type) = match name.as_str() {
        // Aggregate functions
        "COUNT" => (true, LogicalType::BigInt),
        "SUM" => {
            let arg_type = args.first().map(|a| &a.return_type).unwrap_or(&LogicalType::BigInt);
            let ret = if arg_type.is_floating_point() {
                LogicalType::Double
            } else {
                LogicalType::HugeInt  // Use HugeInt to prevent overflow
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
        "ARRAY_AGG" | "LIST_AGG" | "LIST" => {
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
        // ArgMax/ArgMin - returns value of first arg when second arg is max/min
        "ARG_MAX" | "ARGMAX" | "MAX_BY" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Unknown);
            (true, arg_type)
        }
        "ARG_MIN" | "ARGMIN" | "MIN_BY" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Unknown);
            (true, arg_type)
        }
        // Statistical aggregates
        "ENTROPY" => (true, LogicalType::Double),
        "KURTOSIS" | "KURTOSIS_POP" => (true, LogicalType::Double),
        "SKEWNESS" | "SKEW" => (true, LogicalType::Double),
        // Approximate count distinct
        "APPROX_COUNT_DISTINCT" | "APPROX_DISTINCT" => (true, LogicalType::BigInt),
        // Histogram
        "HISTOGRAM" => (true, LogicalType::Unknown), // Returns MAP
        // Regression functions
        "REGR_SLOPE" => (true, LogicalType::Double),
        "REGR_INTERCEPT" => (true, LogicalType::Double),
        "REGR_COUNT" => (true, LogicalType::BigInt),
        "REGR_R2" => (true, LogicalType::Double),
        "REGR_AVGX" => (true, LogicalType::Double),
        "REGR_AVGY" => (true, LogicalType::Double),
        "REGR_SXX" => (true, LogicalType::Double),
        "REGR_SYY" => (true, LogicalType::Double),
        "REGR_SXY" => (true, LogicalType::Double),
        // Additional aggregate functions
        "ANY_VALUE" | "ARBITRARY" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Unknown);
            (true, arg_type)
        }
        "FSUM" | "KAHAN_SUM" => (true, LogicalType::Double),
        "QUANTILE" | "APPROX_QUANTILE" => (true, LogicalType::Double),
        "COUNT_IF" | "COUNTIF" => (true, LogicalType::BigInt),
        "SUM_IF" | "SUMIF" => (true, LogicalType::BigInt),
        "AVG_IF" | "AVGIF" => (true, LogicalType::Double),
        "MIN_IF" | "MINIF" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Unknown);
            (true, arg_type)
        }
        "MAX_IF" | "MAXIF" => {
            let arg_type = args.first().map(|a| a.return_type.clone()).unwrap_or(LogicalType::Unknown);
            (true, arg_type)
        }

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
            order_by,
            filter,
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
        // TimeTz (time with timezone) - must come before generic Time
        sql::DataType::Time(_, sql::TimezoneInfo::Tz)
        | sql::DataType::Time(_, sql::TimezoneInfo::WithTimeZone) => Ok(LogicalType::TimeTz),
        sql::DataType::Time(_, _) => Ok(LogicalType::Time),
        // TimestampTz (timestamp with timezone) - must come before generic Timestamp
        sql::DataType::Timestamp(_, sql::TimezoneInfo::Tz)
        | sql::DataType::Timestamp(_, sql::TimezoneInfo::WithTimeZone) => {
            Ok(LogicalType::TimestampTz)
        }
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
        sql::DataType::Interval => Ok(LogicalType::Interval),
        // Handle DuckDB custom types that aren't in sqlparser
        sql::DataType::Custom(name, _) => {
            let type_name = name.to_string().to_uppercase();
            match type_name.as_str() {
                "HUGEINT" => Ok(LogicalType::HugeInt),
                "UHUGEINT" => Ok(LogicalType::UHugeInt),
                "UTINYINT" => Ok(LogicalType::UTinyInt),
                "USMALLINT" => Ok(LogicalType::USmallInt),
                "UINTEGER" => Ok(LogicalType::UInteger),
                "UBIGINT" => Ok(LogicalType::UBigInt),
                "TIMESTAMPTZ" => Ok(LogicalType::TimestampTz),
                "TIMETZ" => Ok(LogicalType::TimeTz),
                _ => Err(Error::NotImplemented(format!("Data type: {}", type_name))),
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
    let (partition_by, order_by, frame) = match over {
        sql::WindowType::WindowSpec(spec) => {
            bind_window_spec(binder, spec, ctx)?
        }
        sql::WindowType::NamedWindow(name) => {
            // Look up the named window definition
            let window_name = name.value.to_uppercase();
            let named_def = ctx.named_windows.iter().find(|def| {
                def.0.value.to_uppercase() == window_name
            }).ok_or_else(|| Error::InvalidArguments(format!("Unknown window: {}", name.value)))?;

            // Resolve the window spec from the definition
            match &named_def.1 {
                sql::NamedWindowExpr::WindowSpec(spec) => {
                    bind_window_spec(binder, spec, ctx)?
                }
                sql::NamedWindowExpr::NamedWindow(ref_name) => {
                    // Recursive reference to another named window
                    let ref_window_name = ref_name.value.to_uppercase();
                    let ref_def = ctx.named_windows.iter().find(|def| {
                        def.0.value.to_uppercase() == ref_window_name
                    }).ok_or_else(|| Error::InvalidArguments(format!("Unknown window: {}", ref_name.value)))?;

                    match &ref_def.1 {
                        sql::NamedWindowExpr::WindowSpec(spec) => {
                            bind_window_spec(binder, spec, ctx)?
                        }
                        _ => return Err(Error::NotImplemented("Nested named window references".to_string())),
                    }
                }
            }
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
            frame,
        },
        return_type,
    ))
}

/// Bind a window specification (shared between inline and named windows)
fn bind_window_spec(
    binder: &Binder,
    spec: &sql::WindowSpec,
    ctx: &ExpressionBinderContext,
) -> Result<(Vec<BoundExpression>, Vec<(BoundExpression, bool, bool)>, Option<super::WindowFrame>)> {
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

    // Bind window frame if present
    let frame = if let Some(window_frame) = &spec.window_frame {
        Some(bind_window_frame(binder, window_frame, ctx)?)
    } else {
        None
    };

    Ok((partition_by, order_by, frame))
}

/// Bind a window frame specification
fn bind_window_frame(
    binder: &Binder,
    frame: &sql::WindowFrame,
    ctx: &ExpressionBinderContext,
) -> Result<super::WindowFrame> {
    use super::{WindowFrame, WindowFrameBound, WindowFrameType};

    let frame_type = match frame.units {
        sql::WindowFrameUnits::Rows => WindowFrameType::Rows,
        sql::WindowFrameUnits::Range => WindowFrameType::Range,
        sql::WindowFrameUnits::Groups => WindowFrameType::Groups,
    };

    let start = bind_window_frame_bound(binder, &frame.start_bound, ctx)?;

    let end = match &frame.end_bound {
        Some(end) => bind_window_frame_bound(binder, end, ctx)?,
        None => WindowFrameBound::CurrentRow,
    };

    Ok(WindowFrame {
        frame_type,
        start,
        end,
    })
}

/// Bind a window frame bound
fn bind_window_frame_bound(
    binder: &Binder,
    bound: &sql::WindowFrameBound,
    ctx: &ExpressionBinderContext,
) -> Result<super::WindowFrameBound> {
    use super::WindowFrameBound;

    match bound {
        sql::WindowFrameBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        sql::WindowFrameBound::Preceding(None) => Ok(WindowFrameBound::UnboundedPreceding),
        sql::WindowFrameBound::Preceding(Some(expr)) => {
            let bound_expr = bind_expression(binder, expr, ctx)?;
            Ok(WindowFrameBound::Preceding(Box::new(bound_expr)))
        }
        sql::WindowFrameBound::Following(None) => Ok(WindowFrameBound::UnboundedFollowing),
        sql::WindowFrameBound::Following(Some(expr)) => {
            let bound_expr = bind_expression(binder, expr, ctx)?;
            Ok(WindowFrameBound::Following(Box::new(bound_expr)))
        }
    }
}
