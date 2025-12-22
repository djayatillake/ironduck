//! Plan builder - converts bound statements to logical plans

use super::{LogicalOperator, LogicalPlan, SetOperationType};
use ironduck_binder::{
    BoundDelete, BoundExpression, BoundExpressionKind, BoundSelect, BoundSetOperation,
    BoundStatement, BoundTableRef, BoundUpdate, DistinctKind,
};
use ironduck_common::{Error, LogicalType, Result, Value};

/// Build a logical plan from a bound statement
pub fn build_plan(statement: &BoundStatement) -> Result<LogicalPlan> {
    match statement {
        BoundStatement::Select(select) => build_select_plan(select),
        BoundStatement::CreateTable(create) => {
            // Build source plan if we have a SELECT source (CTAS)
            let source = if let Some(source_query) = &create.source_query {
                let source_plan = build_select_plan(source_query)?;
                Some(Box::new(source_plan.root))
            } else {
                None
            };

            Ok(LogicalPlan::new(
                LogicalOperator::CreateTable {
                    schema: create.schema.clone(),
                    name: create.name.clone(),
                    columns: create
                        .columns
                        .iter()
                        .map(|c| (c.name.clone(), c.data_type.clone()))
                        .collect(),
                    if_not_exists: create.if_not_exists,
                    source,
                },
                vec!["Success".to_string()],
            ))
        }
        BoundStatement::Insert(insert) => {
            let values: Vec<Vec<super::Expression>> = insert
                .values
                .iter()
                .map(|row| row.iter().map(convert_expression).collect())
                .collect();

            // Build source plan if we have a SELECT source
            let source = if let Some(source_query) = &insert.source_query {
                let source_plan = build_select_plan(source_query)?;
                Some(Box::new(source_plan.root))
            } else {
                None
            };

            Ok(LogicalPlan::new(
                LogicalOperator::Insert {
                    schema: insert.schema.clone(),
                    table: insert.table.clone(),
                    columns: insert.columns.clone(),
                    values,
                    source,
                },
                vec!["Count".to_string()],
            ))
        }
        BoundStatement::CreateSchema(create) => Ok(LogicalPlan::new(
            LogicalOperator::CreateSchema {
                name: create.name.clone(),
                if_not_exists: create.if_not_exists,
            },
            vec!["Success".to_string()],
        )),
        BoundStatement::CreateView(view) => Ok(LogicalPlan::new(
            LogicalOperator::CreateView {
                schema: view.schema.clone(),
                name: view.name.clone(),
                sql: view.sql.clone(),
                column_names: view.column_names.clone(),
                or_replace: view.or_replace,
            },
            vec!["Success".to_string()],
        )),
        BoundStatement::CreateSequence(seq) => Ok(LogicalPlan::new(
            LogicalOperator::CreateSequence {
                schema: seq.schema.clone(),
                name: seq.name.clone(),
                start: seq.start,
                increment: seq.increment,
                min_value: seq.min_value,
                max_value: seq.max_value,
                cycle: seq.cycle,
                if_not_exists: seq.if_not_exists,
            },
            vec!["Success".to_string()],
        )),
        BoundStatement::Drop(drop) => Ok(LogicalPlan::new(
            LogicalOperator::Drop {
                object_type: format!("{:?}", drop.object_type),
                schema: drop.schema.clone(),
                name: drop.name.clone(),
                if_exists: drop.if_exists,
            },
            vec!["Success".to_string()],
        )),
        BoundStatement::Delete(delete) => {
            let predicate = delete.where_clause.as_ref().map(convert_expression);

            Ok(LogicalPlan::new(
                LogicalOperator::Delete {
                    schema: delete.schema.clone(),
                    table: delete.table.clone(),
                    predicate,
                },
                vec!["Count".to_string()],
            ))
        }
        BoundStatement::Update(update) => {
            let assignments: Vec<_> = update.assignments.iter()
                .map(|(col, expr)| (col.clone(), convert_expression(expr)))
                .collect();
            let predicate = update.where_clause.as_ref().map(convert_expression);

            Ok(LogicalPlan::new(
                LogicalOperator::Update {
                    schema: update.schema.clone(),
                    table: update.table.clone(),
                    assignments,
                    predicate,
                },
                vec!["Count".to_string()],
            ))
        }
        BoundStatement::SetOperation(set_op) => build_set_operation_plan(set_op),
        BoundStatement::Explain(inner) => {
            let inner_plan = build_plan(inner)?;
            Ok(LogicalPlan::new(
                LogicalOperator::Explain {
                    input: Box::new(inner_plan.root),
                },
                vec!["plan".to_string()],
            ))
        }
        BoundStatement::NoOp => {
            // No-op statements (PRAGMA, SET, etc.) produce an empty result
            Ok(LogicalPlan::new(
                LogicalOperator::NoOp,
                vec!["Success".to_string()],
            ))
        }
    }
}

/// Build a plan for a set operation (UNION, INTERSECT, EXCEPT)
fn build_set_operation_plan(set_op: &BoundSetOperation) -> Result<LogicalPlan> {
    let left_plan = build_select_plan(&set_op.left)?;
    let right_plan = build_select_plan(&set_op.right)?;

    let op = match set_op.set_op {
        ironduck_binder::SetOperationType::Union => SetOperationType::Union,
        ironduck_binder::SetOperationType::Intersect => SetOperationType::Intersect,
        ironduck_binder::SetOperationType::Except => SetOperationType::Except,
    };

    let mut plan = LogicalOperator::SetOperation {
        left: Box::new(left_plan.root),
        right: Box::new(right_plan.root),
        op,
        all: set_op.all,
    };

    // Apply ORDER BY
    if !set_op.order_by.is_empty() {
        let order_by: Vec<_> = set_op
            .order_by
            .iter()
            .map(|o| super::OrderByExpression {
                expr: convert_expression(&o.expr),
                ascending: o.ascending,
                nulls_first: o.nulls_first,
            })
            .collect();

        plan = LogicalOperator::Sort {
            input: Box::new(plan),
            order_by,
        };
    }

    // Apply LIMIT/OFFSET
    if set_op.limit.is_some() || set_op.offset.is_some() {
        plan = LogicalOperator::Limit {
            input: Box::new(plan),
            limit: set_op.limit,
            offset: set_op.offset,
        };
    }

    // Use output names from left side
    Ok(LogicalPlan::new(plan, left_plan.output_names))
}

/// Build a plan for SELECT
fn build_select_plan(select: &BoundSelect) -> Result<LogicalPlan> {
    let output_names = select.output_names();

    // Start with the source (FROM clause)
    let mut plan = build_from_plan(&select.from)?;

    // Add WHERE filter
    if let Some(where_clause) = &select.where_clause {
        plan = LogicalOperator::Filter {
            input: Box::new(plan),
            predicate: convert_expression(where_clause),
        };
    }

    // Check if we have aggregates
    let has_aggregates = select.select_list.iter().any(has_aggregate);
    let has_group_by = !select.group_by.is_empty();
    let has_having_aggregates = select.having.as_ref().map_or(false, has_aggregate);

    if has_aggregates || has_group_by || has_having_aggregates {
        // Build aggregate plan
        let group_by: Vec<_> = select.group_by.iter().map(convert_expression).collect();
        let num_group_by = group_by.len();

        // Collect aggregates from SELECT list
        let mut aggregates: Vec<_> = select
            .select_list
            .iter()
            .filter_map(|expr| extract_aggregate(expr))
            .collect();

        // Also collect aggregates from HAVING clause
        let having_agg_start = aggregates.len();
        if let Some(having) = &select.having {
            collect_aggregates_from_expr(having, &mut aggregates);
        }

        plan = LogicalOperator::Aggregate {
            input: Box::new(plan),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
        };

        // Apply HAVING filter BEFORE projection (so we can reference aggregate outputs)
        if let Some(having) = &select.having {
            let having_predicate = convert_having_expression(
                having,
                &group_by,
                num_group_by,
                &select.select_list,
                having_agg_start,
            );
            plan = LogicalOperator::Filter {
                input: Box::new(plan),
                predicate: having_predicate,
            };
        }

        // For aggregate queries, build projection with column references to aggregate output
        // The aggregate output is: [group_by_cols..., aggregate_cols...]
        let mut expressions = Vec::new();
        let mut agg_idx = 0;

        for (idx, bound_expr) in select.select_list.iter().enumerate() {
            if is_aggregate_expr(bound_expr) {
                // This is an aggregate - reference its position in the aggregate output
                expressions.push(super::Expression::ColumnRef {
                    table_index: 0,
                    column_index: num_group_by + agg_idx,
                    name: output_names.get(idx).cloned().unwrap_or_default(),
                });
                agg_idx += 1;
            } else if has_group_by {
                // This should be a group by column - find its position
                let converted = convert_expression(bound_expr);
                // Find matching group by position
                if let Some(pos) = group_by.iter().position(|g| expressions_equal(g, &converted)) {
                    expressions.push(super::Expression::ColumnRef {
                        table_index: 0,
                        column_index: pos,
                        name: output_names.get(idx).cloned().unwrap_or_default(),
                    });
                } else {
                    expressions.push(converted);
                }
            } else {
                expressions.push(convert_expression(bound_expr));
            }
        }

        let output_types: Vec<_> = select.select_list.iter().map(|e| e.return_type.clone()).collect();

        plan = LogicalOperator::Project {
            input: Box::new(plan),
            expressions,
            output_names: output_names.clone(),
            output_types,
        };
    } else {
        // Check for window functions
        let has_windows = select.select_list.iter().any(has_window_function);

        if has_windows {
            // Extract window expressions
            let window_exprs: Vec<_> = select
                .select_list
                .iter()
                .filter_map(extract_window_expression)
                .collect();

            // Get input types for the Window operator
            let input_types = plan.output_types();
            let input_cols = input_types.len();

            // Build output types and names for Window operator
            let mut window_output_types = input_types.clone();
            let mut window_output_names: Vec<String> = (0..input_cols).map(|i| format!("col{}", i)).collect();

            for (idx, win_expr) in window_exprs.iter().enumerate() {
                window_output_types.push(win_expr.output_type.clone());
                window_output_names.push(format!("window_{}", idx));
            }

            // Add Window operator
            plan = LogicalOperator::Window {
                input: Box::new(plan),
                window_exprs,
                output_names: window_output_names,
                output_types: window_output_types,
            };

            // Build projection that references window output
            let mut expressions = Vec::new();
            let mut window_idx = 0;

            for bound_expr in &select.select_list {
                if has_window_function(bound_expr) {
                    // Reference the window function output column
                    expressions.push(super::Expression::ColumnRef {
                        table_index: 0,
                        column_index: input_cols + window_idx,
                        name: bound_expr.name(),
                    });
                    window_idx += 1;
                } else {
                    expressions.push(convert_expression(bound_expr));
                }
            }

            let output_types: Vec<_> = select.select_list.iter().map(|e| e.return_type.clone()).collect();

            plan = LogicalOperator::Project {
                input: Box::new(plan),
                expressions,
                output_names: output_names.clone(),
                output_types,
            };
        } else {
            // For non-aggregate, non-window case: apply Sort BEFORE Project
            // This ensures ORDER BY references original columns, not projected values
            if !select.order_by.is_empty() {
                let order_by: Vec<_> = select
                    .order_by
                    .iter()
                    .map(|o| super::OrderByExpression {
                        expr: convert_expression(&o.expr),
                        ascending: o.ascending,
                        nulls_first: o.nulls_first,
                    })
                    .collect();

                plan = LogicalOperator::Sort {
                    input: Box::new(plan),
                    order_by,
                };
            }

            // Add projection (non-aggregate, non-window case)
            let expressions: Vec<_> = select.select_list.iter().map(convert_expression).collect();
            let output_types: Vec<_> = select.select_list.iter().map(|e| e.return_type.clone()).collect();

            plan = LogicalOperator::Project {
                input: Box::new(plan),
                expressions,
                output_names: output_names.clone(),
                output_types,
            };
        }
    }

    // Add ORDER BY (for aggregate/window cases - Sort is already added above for non-aggregate)
    if has_aggregates || select.select_list.iter().any(has_window_function) {
        if !select.order_by.is_empty() {
            let order_by: Vec<_> = select
                .order_by
                .iter()
                .map(|o| {
                    // For aggregate/window queries, we need to remap ORDER BY expressions
                    // to reference the correct column in the projected output
                    let order_expr = convert_expression(&o.expr);

                    // Try to find matching column in select list
                    let remapped_expr = find_matching_output_column(&order_expr, &select.select_list, &output_names)
                        .unwrap_or(order_expr);

                    super::OrderByExpression {
                        expr: remapped_expr,
                        ascending: o.ascending,
                        nulls_first: o.nulls_first,
                    }
                })
                .collect();

            plan = LogicalOperator::Sort {
                input: Box::new(plan),
                order_by,
            };
        }
    }

    // Add LIMIT/OFFSET
    if select.limit.is_some() || select.offset.is_some() {
        plan = LogicalOperator::Limit {
            input: Box::new(plan),
            limit: select.limit,
            offset: select.offset,
        };
    }

    // Add DISTINCT or DISTINCT ON
    match &select.distinct {
        DistinctKind::None => {}
        DistinctKind::All => {
            plan = LogicalOperator::Distinct {
                input: Box::new(plan),
                on_exprs: None,
            };
        }
        DistinctKind::On(exprs) => {
            let on_exprs: Vec<_> = exprs.iter().map(convert_expression).collect();
            plan = LogicalOperator::Distinct {
                input: Box::new(plan),
                on_exprs: Some(on_exprs),
            };
        }
    }

    Ok(LogicalPlan::new(plan, output_names))
}

/// Build plan for FROM clause
fn build_from_plan(from: &[BoundTableRef]) -> Result<LogicalOperator> {
    if from.is_empty() || matches!(from.first(), Some(BoundTableRef::Empty)) {
        // No FROM clause - return a dummy scan that produces one row
        return Ok(LogicalOperator::DummyScan);
    }

    let mut result: Option<LogicalOperator> = None;

    for table_ref in from {
        let table_plan = build_table_ref_plan(table_ref)?;

        result = Some(match result {
            None => table_plan,
            Some(left) => LogicalOperator::Join {
                left: Box::new(left),
                right: Box::new(table_plan),
                join_type: super::JoinType::Cross,
                condition: None,
            },
        });
    }

    Ok(result.unwrap_or(LogicalOperator::DummyScan))
}

/// Build plan for a table reference
fn build_table_ref_plan(table_ref: &BoundTableRef) -> Result<LogicalOperator> {
    match table_ref {
        BoundTableRef::BaseTable {
            schema,
            name,
            column_names,
            column_types,
            ..
        } => Ok(LogicalOperator::Scan {
            schema: schema.clone(),
            table: name.clone(),
            column_names: column_names.clone(),
            output_types: column_types.clone(),
        }),

        BoundTableRef::Subquery { subquery, .. } => {
            let plan = build_select_plan(subquery)?;
            Ok(plan.root)
        }

        BoundTableRef::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            let left_plan = build_table_ref_plan(left)?;
            let right_plan = build_table_ref_plan(right)?;

            let logical_join_type = match join_type {
                ironduck_binder::BoundJoinType::Inner => super::JoinType::Inner,
                ironduck_binder::BoundJoinType::Left => super::JoinType::Left,
                ironduck_binder::BoundJoinType::Right => super::JoinType::Right,
                ironduck_binder::BoundJoinType::Full => super::JoinType::Full,
                ironduck_binder::BoundJoinType::Cross => super::JoinType::Cross,
            };

            Ok(LogicalOperator::Join {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                join_type: logical_join_type,
                condition: condition.as_ref().map(convert_expression),
            })
        }

        BoundTableRef::Empty => Ok(LogicalOperator::DummyScan),

        BoundTableRef::TableFunction { function, column_alias, .. } => {
            match function {
                ironduck_binder::TableFunctionType::Range { start, stop, step } => {
                    let column_name = column_alias.clone().unwrap_or_else(|| "range".to_string());
                    Ok(LogicalOperator::TableFunction {
                        function: super::TableFunctionKind::Range {
                            start: convert_expression(start),
                            stop: convert_expression(stop),
                            step: convert_expression(step),
                        },
                        column_name,
                        output_type: ironduck_common::LogicalType::BigInt,
                    })
                }
            }
        }
    }
}

/// Convert a bound expression to a logical expression
fn convert_expression(expr: &BoundExpression) -> super::Expression {
    match &expr.expr {
        BoundExpressionKind::Constant(value) => super::Expression::Constant(value.clone()),

        BoundExpressionKind::ColumnRef {
            table_idx,
            column_idx,
            name,
        } => super::Expression::ColumnRef {
            table_index: *table_idx,
            column_index: *column_idx,
            name: name.clone(),
        },

        BoundExpressionKind::BinaryOp { left, op, right } => {
            let logical_op = match op {
                ironduck_binder::BoundBinaryOperator::Add => super::BinaryOperator::Add,
                ironduck_binder::BoundBinaryOperator::Subtract => super::BinaryOperator::Subtract,
                ironduck_binder::BoundBinaryOperator::Multiply => super::BinaryOperator::Multiply,
                ironduck_binder::BoundBinaryOperator::Divide => super::BinaryOperator::Divide,
                ironduck_binder::BoundBinaryOperator::Modulo => super::BinaryOperator::Modulo,
                ironduck_binder::BoundBinaryOperator::Equal => super::BinaryOperator::Equal,
                ironduck_binder::BoundBinaryOperator::NotEqual => super::BinaryOperator::NotEqual,
                ironduck_binder::BoundBinaryOperator::LessThan => super::BinaryOperator::LessThan,
                ironduck_binder::BoundBinaryOperator::LessThanOrEqual => {
                    super::BinaryOperator::LessThanOrEqual
                }
                ironduck_binder::BoundBinaryOperator::GreaterThan => {
                    super::BinaryOperator::GreaterThan
                }
                ironduck_binder::BoundBinaryOperator::GreaterThanOrEqual => {
                    super::BinaryOperator::GreaterThanOrEqual
                }
                ironduck_binder::BoundBinaryOperator::And => super::BinaryOperator::And,
                ironduck_binder::BoundBinaryOperator::Or => super::BinaryOperator::Or,
                ironduck_binder::BoundBinaryOperator::Concat => super::BinaryOperator::Concat,
                ironduck_binder::BoundBinaryOperator::Like => super::BinaryOperator::Like,
                ironduck_binder::BoundBinaryOperator::ILike => super::BinaryOperator::ILike,
            };

            super::Expression::BinaryOp {
                left: Box::new(convert_expression(left)),
                op: logical_op,
                right: Box::new(convert_expression(right)),
            }
        }

        BoundExpressionKind::UnaryOp { op, expr } => {
            let logical_op = match op {
                ironduck_binder::BoundUnaryOperator::Negate => super::UnaryOperator::Negate,
                ironduck_binder::BoundUnaryOperator::Not => super::UnaryOperator::Not,
                ironduck_binder::BoundUnaryOperator::IsNull => super::UnaryOperator::Not, // Handled separately
                ironduck_binder::BoundUnaryOperator::IsNotNull => super::UnaryOperator::Not,
            };

            super::Expression::UnaryOp {
                op: logical_op,
                expr: Box::new(convert_expression(expr)),
            }
        }

        BoundExpressionKind::Function {
            name,
            args,
            is_aggregate: _,
            distinct: _, // distinct is handled in extract_aggregate
        } => {
            // Return function expression - aggregates are handled specially in Aggregate operator
            super::Expression::Function {
                name: name.clone(),
                args: args.iter().map(convert_expression).collect(),
            }
        }

        BoundExpressionKind::Cast { expr, target_type } => super::Expression::Cast {
            expr: Box::new(convert_expression(expr)),
            target_type: target_type.clone(),
        },

        BoundExpressionKind::IsNull(expr) => super::Expression::IsNull(Box::new(convert_expression(expr))),

        BoundExpressionKind::IsNotNull(expr) => {
            super::Expression::IsNotNull(Box::new(convert_expression(expr)))
        }

        BoundExpressionKind::Case {
            operand,
            when_clauses,
            else_result,
        } => super::Expression::Case {
            operand: operand.as_ref().map(|e| Box::new(convert_expression(e))),
            conditions: when_clauses.iter().map(|(c, _)| convert_expression(c)).collect(),
            results: when_clauses.iter().map(|(_, r)| convert_expression(r)).collect(),
            else_result: else_result.as_ref().map(|e| Box::new(convert_expression(e))),
        },

        BoundExpressionKind::Between {
            expr,
            low,
            high,
            negated,
        } => {
            // Convert BETWEEN to >= AND <=
            let ge = super::Expression::BinaryOp {
                left: Box::new(convert_expression(expr)),
                op: super::BinaryOperator::GreaterThanOrEqual,
                right: Box::new(convert_expression(low)),
            };
            let le = super::Expression::BinaryOp {
                left: Box::new(convert_expression(expr)),
                op: super::BinaryOperator::LessThanOrEqual,
                right: Box::new(convert_expression(high)),
            };
            let result = super::Expression::BinaryOp {
                left: Box::new(ge),
                op: super::BinaryOperator::And,
                right: Box::new(le),
            };

            if *negated {
                super::Expression::UnaryOp {
                    op: super::UnaryOperator::Not,
                    expr: Box::new(result),
                }
            } else {
                result
            }
        }

        BoundExpressionKind::InList { expr, list, negated } => {
            // Convert IN to OR chain of equalities
            if list.is_empty() {
                return super::Expression::Constant(Value::Boolean(*negated));
            }

            let mut result = super::Expression::BinaryOp {
                left: Box::new(convert_expression(expr)),
                op: super::BinaryOperator::Equal,
                right: Box::new(convert_expression(&list[0])),
            };

            for item in &list[1..] {
                let eq = super::Expression::BinaryOp {
                    left: Box::new(convert_expression(expr)),
                    op: super::BinaryOperator::Equal,
                    right: Box::new(convert_expression(item)),
                };
                result = super::Expression::BinaryOp {
                    left: Box::new(result),
                    op: super::BinaryOperator::Or,
                    right: Box::new(eq),
                };
            }

            if *negated {
                super::Expression::UnaryOp {
                    op: super::UnaryOperator::Not,
                    expr: Box::new(result),
                }
            } else {
                result
            }
        }

        BoundExpressionKind::InSubquery { expr, subquery, negated } => {
            let subquery_plan = build_select_plan(subquery)
                .map(|p| p.root)
                .unwrap_or(super::LogicalOperator::DummyScan);

            super::Expression::InSubquery {
                expr: Box::new(convert_expression(expr)),
                subquery: Box::new(subquery_plan),
                negated: *negated,
            }
        }

        BoundExpressionKind::Exists { subquery, negated } => {
            let subquery_plan = build_select_plan(subquery)
                .map(|p| p.root)
                .unwrap_or(super::LogicalOperator::DummyScan);

            super::Expression::Exists {
                subquery: Box::new(subquery_plan),
                negated: *negated,
            }
        }

        BoundExpressionKind::ScalarSubquery(subquery) => {
            let subquery_plan = build_select_plan(subquery)
                .map(|p| p.root)
                .unwrap_or(super::LogicalOperator::DummyScan);

            super::Expression::Subquery(Box::new(subquery_plan))
        }

        BoundExpressionKind::Star => super::Expression::Constant(Value::Null), // Should be expanded earlier

        BoundExpressionKind::WindowFunction { .. } => {
            // Window functions are handled specially by the Window operator
            // Return a placeholder that will be replaced
            super::Expression::Constant(Value::Null)
        }

        BoundExpressionKind::RowId { table_idx } => super::Expression::RowId {
            table_index: *table_idx,
        },
    }
}

/// Check if an expression contains aggregates
fn has_aggregate(expr: &BoundExpression) -> bool {
    match &expr.expr {
        BoundExpressionKind::Function { is_aggregate, .. } => *is_aggregate,
        BoundExpressionKind::BinaryOp { left, right, .. } => {
            has_aggregate(left) || has_aggregate(right)
        }
        BoundExpressionKind::UnaryOp { expr, .. } => has_aggregate(expr),
        BoundExpressionKind::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand.as_ref().map_or(false, |e| has_aggregate(e))
                || when_clauses.iter().any(|(c, r)| has_aggregate(c) || has_aggregate(r))
                || else_result.as_ref().map_or(false, |e| has_aggregate(e))
        }
        _ => false,
    }
}

/// Check if an expression is directly an aggregate function (not nested)
fn is_aggregate_expr(expr: &BoundExpression) -> bool {
    matches!(&expr.expr, BoundExpressionKind::Function { is_aggregate: true, .. })
}

/// Simple expression equality check for group by matching
fn expressions_equal(a: &super::Expression, b: &super::Expression) -> bool {
    match (a, b) {
        (
            super::Expression::ColumnRef { table_index: t1, column_index: c1, .. },
            super::Expression::ColumnRef { table_index: t2, column_index: c2, .. },
        ) => t1 == t2 && c1 == c2,
        (super::Expression::Constant(v1), super::Expression::Constant(v2)) => {
            format!("{:?}", v1) == format!("{:?}", v2)
        }
        _ => false,
    }
}

/// Extract aggregate expression
fn extract_aggregate(expr: &BoundExpression) -> Option<super::AggregateExpression> {
    match &expr.expr {
        BoundExpressionKind::Function {
            name,
            args,
            is_aggregate,
            distinct,
        } if *is_aggregate => {
            let func = match name.as_str() {
                "COUNT" => super::AggregateFunction::Count,
                "SUM" => super::AggregateFunction::Sum,
                "AVG" => super::AggregateFunction::Avg,
                "MIN" => super::AggregateFunction::Min,
                "MAX" => super::AggregateFunction::Max,
                "FIRST" => super::AggregateFunction::First,
                "LAST" => super::AggregateFunction::Last,
                "STRING_AGG" | "GROUP_CONCAT" | "LISTAGG" => super::AggregateFunction::StringAgg,
                "ARRAY_AGG" => super::AggregateFunction::ArrayAgg,
                "STDDEV" | "STDDEV_SAMP" => super::AggregateFunction::StdDev,
                "STDDEV_POP" => super::AggregateFunction::StdDevPop,
                "VARIANCE" | "VAR_SAMP" => super::AggregateFunction::Variance,
                "VAR_POP" => super::AggregateFunction::VariancePop,
                "BOOL_AND" | "EVERY" => super::AggregateFunction::BoolAnd,
                "BOOL_OR" | "ANY" => super::AggregateFunction::BoolOr,
                "BIT_AND" => super::AggregateFunction::BitAnd,
                "BIT_OR" => super::AggregateFunction::BitOr,
                "BIT_XOR" => super::AggregateFunction::BitXor,
                "PRODUCT" => super::AggregateFunction::Product,
                "MEDIAN" => super::AggregateFunction::Median,
                "PERCENTILE_CONT" | "PERCENTILE" => super::AggregateFunction::PercentileCont,
                "PERCENTILE_DISC" => super::AggregateFunction::PercentileDisc,
                "MODE" => super::AggregateFunction::Mode,
                "COVAR_POP" => super::AggregateFunction::CovarPop,
                "COVAR_SAMP" => super::AggregateFunction::CovarSamp,
                "CORR" => super::AggregateFunction::Corr,
                _ => return None,
            };

            // For COUNT(*), filter out the Star expression so args is empty
            // This allows the executor to treat it as a row count
            let converted_args: Vec<_> = if func == super::AggregateFunction::Count {
                args.iter()
                    .filter(|a| !matches!(a.expr, BoundExpressionKind::Star))
                    .map(convert_expression)
                    .collect()
            } else {
                args.iter().map(convert_expression).collect()
            };

            Some(super::AggregateExpression {
                function: func,
                args: converted_args,
                distinct: *distinct,
                filter: None,
            })
        }
        _ => None,
    }
}

/// Check if an expression contains window functions
fn has_window_function(expr: &BoundExpression) -> bool {
    match &expr.expr {
        BoundExpressionKind::WindowFunction { .. } => true,
        BoundExpressionKind::BinaryOp { left, right, .. } => {
            has_window_function(left) || has_window_function(right)
        }
        BoundExpressionKind::UnaryOp { expr, .. } => has_window_function(expr),
        BoundExpressionKind::Case { operand, when_clauses, else_result } => {
            operand.as_ref().map_or(false, |e| has_window_function(e))
                || when_clauses.iter().any(|(c, r)| has_window_function(c) || has_window_function(r))
                || else_result.as_ref().map_or(false, |e| has_window_function(e))
        }
        _ => false,
    }
}

/// Extract window expression from bound expression
fn extract_window_expression(expr: &BoundExpression) -> Option<super::WindowExpression> {
    match &expr.expr {
        BoundExpressionKind::WindowFunction {
            name,
            args,
            partition_by,
            order_by,
        } => {
            let func = match name.as_str() {
                "ROW_NUMBER" => super::WindowFunction::RowNumber,
                "RANK" => super::WindowFunction::Rank,
                "DENSE_RANK" => super::WindowFunction::DenseRank,
                "PERCENT_RANK" => super::WindowFunction::PercentRank,
                "CUME_DIST" => super::WindowFunction::CumeDist,
                "NTILE" => super::WindowFunction::Ntile,
                "LAG" => super::WindowFunction::Lag,
                "LEAD" => super::WindowFunction::Lead,
                "FIRST_VALUE" => super::WindowFunction::FirstValue,
                "LAST_VALUE" => super::WindowFunction::LastValue,
                "NTH_VALUE" => super::WindowFunction::NthValue,
                "SUM" => super::WindowFunction::Sum,
                "COUNT" => super::WindowFunction::Count,
                "AVG" => super::WindowFunction::Avg,
                "MIN" => super::WindowFunction::Min,
                "MAX" => super::WindowFunction::Max,
                _ => return None,
            };

            Some(super::WindowExpression {
                function: func,
                args: args.iter().map(convert_expression).collect(),
                partition_by: partition_by.iter().map(convert_expression).collect(),
                order_by: order_by
                    .iter()
                    .map(|(e, asc, nulls_first)| super::OrderByExpression {
                        expr: convert_expression(e),
                        ascending: *asc,
                        nulls_first: *nulls_first,
                    })
                    .collect(),
                output_type: expr.return_type.clone(),
            })
        }
        _ => None,
    }
}

/// Convert bound binary operator to logical operator
fn convert_binary_op(op: &ironduck_binder::BoundBinaryOperator) -> super::BinaryOperator {
    match op {
        ironduck_binder::BoundBinaryOperator::Add => super::BinaryOperator::Add,
        ironduck_binder::BoundBinaryOperator::Subtract => super::BinaryOperator::Subtract,
        ironduck_binder::BoundBinaryOperator::Multiply => super::BinaryOperator::Multiply,
        ironduck_binder::BoundBinaryOperator::Divide => super::BinaryOperator::Divide,
        ironduck_binder::BoundBinaryOperator::Modulo => super::BinaryOperator::Modulo,
        ironduck_binder::BoundBinaryOperator::Equal => super::BinaryOperator::Equal,
        ironduck_binder::BoundBinaryOperator::NotEqual => super::BinaryOperator::NotEqual,
        ironduck_binder::BoundBinaryOperator::LessThan => super::BinaryOperator::LessThan,
        ironduck_binder::BoundBinaryOperator::LessThanOrEqual => super::BinaryOperator::LessThanOrEqual,
        ironduck_binder::BoundBinaryOperator::GreaterThan => super::BinaryOperator::GreaterThan,
        ironduck_binder::BoundBinaryOperator::GreaterThanOrEqual => super::BinaryOperator::GreaterThanOrEqual,
        ironduck_binder::BoundBinaryOperator::And => super::BinaryOperator::And,
        ironduck_binder::BoundBinaryOperator::Or => super::BinaryOperator::Or,
        ironduck_binder::BoundBinaryOperator::Concat => super::BinaryOperator::Concat,
        ironduck_binder::BoundBinaryOperator::Like => super::BinaryOperator::Like,
        ironduck_binder::BoundBinaryOperator::ILike => super::BinaryOperator::ILike,
    }
}

/// Convert bound unary operator to logical operator
fn convert_unary_op(op: &ironduck_binder::BoundUnaryOperator) -> super::UnaryOperator {
    match op {
        ironduck_binder::BoundUnaryOperator::Negate => super::UnaryOperator::Negate,
        ironduck_binder::BoundUnaryOperator::Not => super::UnaryOperator::Not,
        ironduck_binder::BoundUnaryOperator::IsNull => super::UnaryOperator::Not, // Handled separately
        ironduck_binder::BoundUnaryOperator::IsNotNull => super::UnaryOperator::Not,
    }
}

/// Collect aggregates from an expression (used for HAVING clause)
fn collect_aggregates_from_expr(expr: &BoundExpression, aggregates: &mut Vec<super::AggregateExpression>) {
    match &expr.expr {
        BoundExpressionKind::Function { is_aggregate: true, .. } => {
            if let Some(agg) = extract_aggregate(expr) {
                // Only add if not already present
                let agg_key = format!("{:?}", agg);
                let exists = aggregates.iter().any(|a| format!("{:?}", a) == agg_key);
                if !exists {
                    aggregates.push(agg);
                }
            }
        }
        BoundExpressionKind::BinaryOp { left, right, .. } => {
            collect_aggregates_from_expr(left, aggregates);
            collect_aggregates_from_expr(right, aggregates);
        }
        BoundExpressionKind::UnaryOp { expr: inner, .. } => {
            collect_aggregates_from_expr(inner, aggregates);
        }
        BoundExpressionKind::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_aggregates_from_expr(op, aggregates);
            }
            for (cond, result) in when_clauses {
                collect_aggregates_from_expr(cond, aggregates);
                collect_aggregates_from_expr(result, aggregates);
            }
            if let Some(els) = else_result {
                collect_aggregates_from_expr(els, aggregates);
            }
        }
        _ => {}
    }
}

/// Convert HAVING expression with proper aggregate column references
fn convert_having_expression(
    expr: &BoundExpression,
    group_by: &[super::Expression],
    num_group_by: usize,
    select_list: &[BoundExpression],
    having_agg_start: usize,
) -> super::Expression {
    // Track which aggregate we're on in HAVING
    let mut having_agg_idx = having_agg_start;

    fn convert_inner(
        expr: &BoundExpression,
        group_by: &[super::Expression],
        num_group_by: usize,
        select_list: &[BoundExpression],
        having_agg_idx: &mut usize,
    ) -> super::Expression {
        match &expr.expr {
            BoundExpressionKind::Function { is_aggregate: true, name, args, .. } => {
                // Try to find matching aggregate in SELECT list first
                let mut select_agg_idx = 0;
                for sel_expr in select_list {
                    if let BoundExpressionKind::Function {
                        is_aggregate: true,
                        name: sel_name,
                        args: sel_args,
                        ..
                    } = &sel_expr.expr
                    {
                        if name == sel_name && args.len() == sel_args.len() {
                            // Check if args match (simple comparison)
                            let args_match = args.iter().zip(sel_args.iter()).all(|(a, b)| {
                                format!("{:?}", a.expr) == format!("{:?}", b.expr)
                            });
                            if args_match {
                                // Reference the SELECT list aggregate
                                return super::Expression::ColumnRef {
                                    table_index: 0,
                                    column_index: num_group_by + select_agg_idx,
                                    name: name.clone(),
                                };
                            }
                        }
                        select_agg_idx += 1;
                    }
                }

                // Not found in SELECT - reference the HAVING-specific aggregate
                let idx = *having_agg_idx;
                *having_agg_idx += 1;
                super::Expression::ColumnRef {
                    table_index: 0,
                    column_index: num_group_by + idx,
                    name: name.clone(),
                }
            }
            BoundExpressionKind::ColumnRef { name, .. } => {
                // Try to find in group by
                let converted = convert_expression(expr);
                if let Some(pos) = group_by.iter().position(|g| expressions_equal(g, &converted)) {
                    super::Expression::ColumnRef {
                        table_index: 0,
                        column_index: pos,
                        name: name.clone(),
                    }
                } else {
                    converted
                }
            }
            BoundExpressionKind::BinaryOp { left, op, right } => {
                let logical_op = convert_binary_op(op);
                super::Expression::BinaryOp {
                    left: Box::new(convert_inner(left, group_by, num_group_by, select_list, having_agg_idx)),
                    op: logical_op,
                    right: Box::new(convert_inner(right, group_by, num_group_by, select_list, having_agg_idx)),
                }
            }
            BoundExpressionKind::UnaryOp { op, expr: inner } => {
                let logical_op = convert_unary_op(op);
                super::Expression::UnaryOp {
                    op: logical_op,
                    expr: Box::new(convert_inner(inner, group_by, num_group_by, select_list, having_agg_idx)),
                }
            }
            _ => convert_expression(expr),
        }
    }

    convert_inner(expr, group_by, num_group_by, select_list, &mut having_agg_idx)
}

/// Find a matching output column for an ORDER BY expression
/// Returns a ColumnRef pointing to the correct output column index if found
fn find_matching_output_column(
    order_expr: &super::Expression,
    select_list: &[BoundExpression],
    output_names: &[String],
) -> Option<super::Expression> {
    // Extract the column name from the ORDER BY expression if it's a column reference
    let order_col_name = match order_expr {
        super::Expression::ColumnRef { name, .. } => Some(name.as_str()),
        _ => None,
    };

    // Try to find a matching column in the select list by comparing converted expressions
    for (idx, bound_expr) in select_list.iter().enumerate() {
        let select_expr = convert_expression(bound_expr);

        // Check if expressions are equal
        if expressions_equal(order_expr, &select_expr) {
            return Some(super::Expression::ColumnRef {
                table_index: 0,
                column_index: idx,
                name: output_names.get(idx).cloned().unwrap_or_default(),
            });
        }

        // Also check by name for column references
        if let Some(order_name) = order_col_name {
            // Check if this select list item has the same name
            if let super::Expression::ColumnRef { name: select_name, .. } = &select_expr {
                if order_name.eq_ignore_ascii_case(select_name) {
                    return Some(super::Expression::ColumnRef {
                        table_index: 0,
                        column_index: idx,
                        name: output_names.get(idx).cloned().unwrap_or_default(),
                    });
                }
            }

            // Check if output name matches
            if let Some(out_name) = output_names.get(idx) {
                if order_name.eq_ignore_ascii_case(out_name) {
                    return Some(super::Expression::ColumnRef {
                        table_index: 0,
                        column_index: idx,
                        name: out_name.clone(),
                    });
                }
            }
        }
    }

    None
}
