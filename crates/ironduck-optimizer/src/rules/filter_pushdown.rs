//! Filter pushdown optimization rule
//!
//! Pushes filter predicates closer to the data source to reduce
//! the amount of data that flows through the query plan.

use crate::OptimizationRule;
use ironduck_planner::{BinaryOperator, Expression, JoinType, LogicalOperator, LogicalPlan};
use std::collections::HashSet;

/// Push filters closer to the data source
pub struct FilterPushdown;

impl OptimizationRule for FilterPushdown {
    fn name(&self) -> &str {
        "filter_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let new_root = pushdown_filters(&plan.root);
        if operator_changed(&plan.root, &new_root) {
            Some(LogicalPlan {
                root: new_root,
                output_names: plan.output_names.clone(),
            })
        } else {
            None
        }
    }
}

/// Check if an operator was modified
fn operator_changed(old: &LogicalOperator, new: &LogicalOperator) -> bool {
    format!("{:?}", old) != format!("{:?}", new)
}

/// Recursively push filters down through the operator tree
fn pushdown_filters(op: &LogicalOperator) -> LogicalOperator {
    match op {
        // Filter over another operator - try to push it down
        LogicalOperator::Filter { input, predicate } => {
            // First, recursively process the input
            let processed_input = pushdown_filters(input);

            // Then try to push the filter down
            push_filter_through(&processed_input, predicate)
        }

        // Recursively process other operators
        LogicalOperator::Project {
            input,
            expressions,
            output_names,
            output_types,
        } => LogicalOperator::Project {
            input: Box::new(pushdown_filters(input)),
            expressions: expressions.clone(),
            output_names: output_names.clone(),
            output_types: output_types.clone(),
        },

        LogicalOperator::Aggregate {
            input,
            group_by,
            aggregates,
        } => LogicalOperator::Aggregate {
            input: Box::new(pushdown_filters(input)),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
        },

        LogicalOperator::Sort { input, order_by } => LogicalOperator::Sort {
            input: Box::new(pushdown_filters(input)),
            order_by: order_by.clone(),
        },

        LogicalOperator::Limit {
            input,
            limit,
            offset,
        } => LogicalOperator::Limit {
            input: Box::new(pushdown_filters(input)),
            limit: *limit,
            offset: *offset,
        },

        LogicalOperator::Distinct { input, on_exprs } => LogicalOperator::Distinct {
            input: Box::new(pushdown_filters(input)),
            on_exprs: on_exprs.clone(),
        },

        LogicalOperator::Join {
            left,
            right,
            join_type,
            condition,
        } => LogicalOperator::Join {
            left: Box::new(pushdown_filters(left)),
            right: Box::new(pushdown_filters(right)),
            join_type: *join_type,
            condition: condition.clone(),
        },

        LogicalOperator::SetOperation {
            left,
            right,
            op,
            all,
        } => LogicalOperator::SetOperation {
            left: Box::new(pushdown_filters(left)),
            right: Box::new(pushdown_filters(right)),
            op: *op,
            all: *all,
        },

        LogicalOperator::Window {
            input,
            window_exprs,
            output_names,
            output_types,
        } => LogicalOperator::Window {
            input: Box::new(pushdown_filters(input)),
            window_exprs: window_exprs.clone(),
            output_names: output_names.clone(),
            output_types: output_types.clone(),
        },

        LogicalOperator::RecursiveCTE {
            name,
            base_case,
            recursive_case,
            output_names,
            output_types,
            union_all,
        } => LogicalOperator::RecursiveCTE {
            name: name.clone(),
            base_case: Box::new(pushdown_filters(base_case)),
            recursive_case: Box::new(pushdown_filters(recursive_case)),
            output_names: output_names.clone(),
            output_types: output_types.clone(),
            union_all: *union_all,
        },

        LogicalOperator::Explain { input } => LogicalOperator::Explain {
            input: Box::new(pushdown_filters(input)),
        },

        LogicalOperator::CreateTable {
            schema,
            name,
            columns,
            default_values,
            if_not_exists,
            source,
        } => LogicalOperator::CreateTable {
            schema: schema.clone(),
            name: name.clone(),
            columns: columns.clone(),
            default_values: default_values.clone(),
            if_not_exists: *if_not_exists,
            source: source.as_ref().map(|s| Box::new(pushdown_filters(s))),
        },

        LogicalOperator::Insert {
            schema,
            table,
            columns,
            values,
            source,
        } => LogicalOperator::Insert {
            schema: schema.clone(),
            table: table.clone(),
            columns: columns.clone(),
            values: values.clone(),
            source: source.as_ref().map(|s| Box::new(pushdown_filters(s))),
        },

        // Pass through operators that don't have child operators
        _ => op.clone(),
    }
}

/// Try to push a filter through an operator
fn push_filter_through(input: &LogicalOperator, predicate: &Expression) -> LogicalOperator {
    match input {
        // Push filter past Sort - always safe and reduces sort cost
        LogicalOperator::Sort { input: sort_input, order_by } => {
            LogicalOperator::Sort {
                input: Box::new(push_filter_through(sort_input, predicate)),
                order_by: order_by.clone(),
            }
        }

        // Push filter past Distinct - always safe
        LogicalOperator::Distinct { input: distinct_input, on_exprs } => {
            LogicalOperator::Distinct {
                input: Box::new(push_filter_through(distinct_input, predicate)),
                on_exprs: on_exprs.clone(),
            }
        }

        // Merge consecutive filters
        LogicalOperator::Filter { input: inner_input, predicate: inner_pred } => {
            // Combine predicates with AND
            let combined = Expression::BinaryOp {
                left: Box::new(inner_pred.clone()),
                op: BinaryOperator::And,
                right: Box::new(predicate.clone()),
            };
            LogicalOperator::Filter {
                input: inner_input.clone(),
                predicate: combined,
            }
        }

        // Push filter through Project if possible
        LogicalOperator::Project {
            input: proj_input,
            expressions,
            output_names,
            output_types,
        } => {
            // Try to rewrite the predicate in terms of the project's input columns
            if let Some(rewritten) = rewrite_predicate_through_project(predicate, expressions, output_names) {
                // We can push the filter below the project
                LogicalOperator::Project {
                    input: Box::new(push_filter_through(proj_input, &rewritten)),
                    expressions: expressions.clone(),
                    output_names: output_names.clone(),
                    output_types: output_types.clone(),
                }
            } else {
                // Can't push through, keep filter above project
                LogicalOperator::Filter {
                    input: Box::new(input.clone()),
                    predicate: predicate.clone(),
                }
            }
        }

        // Push filter into Join
        LogicalOperator::Join {
            left,
            right,
            join_type,
            condition,
        } => {
            let left_columns = count_columns(left);

            // Decompose the predicate into conjuncts
            let conjuncts = split_conjunction(predicate);

            let mut left_filters: Vec<Expression> = Vec::new();
            let mut right_filters: Vec<Expression> = Vec::new();
            let mut remaining_filters: Vec<Expression> = Vec::new();

            for conj in conjuncts {
                let refs = collect_column_refs(&conj);
                let uses_left = refs.iter().any(|(table_idx, _)| *table_idx == 0);
                let uses_right = refs.iter().any(|(table_idx, _)| *table_idx == 1 || refs.iter().any(|(_, col)| *col >= left_columns));

                match (*join_type, uses_left, uses_right) {
                    // For INNER JOIN, we can push filters to either side
                    (JoinType::Inner, true, false) => left_filters.push(conj),
                    (JoinType::Inner, false, true) => right_filters.push(shift_column_refs(&conj, left_columns)),

                    // For LEFT JOIN, we can push filters on the left side
                    (JoinType::Left, true, false) => left_filters.push(conj),

                    // For RIGHT JOIN, we can push filters on the right side
                    (JoinType::Right, false, true) => right_filters.push(shift_column_refs(&conj, left_columns)),

                    // Everything else stays above the join
                    _ => remaining_filters.push(conj),
                }
            }

            // Build new left input with pushed filters
            let new_left = if left_filters.is_empty() {
                left.as_ref().clone()
            } else {
                let filter_pred = combine_conjuncts(&left_filters);
                LogicalOperator::Filter {
                    input: left.clone(),
                    predicate: filter_pred,
                }
            };

            // Build new right input with pushed filters
            let new_right = if right_filters.is_empty() {
                right.as_ref().clone()
            } else {
                let filter_pred = combine_conjuncts(&right_filters);
                LogicalOperator::Filter {
                    input: right.clone(),
                    predicate: filter_pred,
                }
            };

            // Build the join
            let new_join = LogicalOperator::Join {
                left: Box::new(new_left),
                right: Box::new(new_right),
                join_type: *join_type,
                condition: condition.clone(),
            };

            // Apply remaining filters above the join
            if remaining_filters.is_empty() {
                new_join
            } else {
                LogicalOperator::Filter {
                    input: Box::new(new_join),
                    predicate: combine_conjuncts(&remaining_filters),
                }
            }
        }

        // Can't push through these operators
        _ => LogicalOperator::Filter {
            input: Box::new(input.clone()),
            predicate: predicate.clone(),
        },
    }
}

/// Count the number of columns produced by an operator
fn count_columns(op: &LogicalOperator) -> usize {
    op.output_types().len()
}

/// Split a predicate into its AND conjuncts
fn split_conjunction(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            let mut result = split_conjunction(left);
            result.extend(split_conjunction(right));
            result
        }
        _ => vec![expr.clone()],
    }
}

/// Combine expressions with AND
fn combine_conjuncts(exprs: &[Expression]) -> Expression {
    if exprs.len() == 1 {
        return exprs[0].clone();
    }

    let mut result = exprs[0].clone();
    for expr in &exprs[1..] {
        result = Expression::BinaryOp {
            left: Box::new(result),
            op: BinaryOperator::And,
            right: Box::new(expr.clone()),
        };
    }
    result
}

/// Collect all column references in an expression as (table_index, column_index)
fn collect_column_refs(expr: &Expression) -> HashSet<(usize, usize)> {
    let mut refs = HashSet::new();
    collect_column_refs_inner(expr, &mut refs);
    refs
}

fn collect_column_refs_inner(expr: &Expression, refs: &mut HashSet<(usize, usize)>) {
    match expr {
        Expression::ColumnRef { table_index, column_index, .. } => {
            refs.insert((*table_index, *column_index));
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_column_refs_inner(left, refs);
            collect_column_refs_inner(right, refs);
        }
        Expression::UnaryOp { expr, .. } => {
            collect_column_refs_inner(expr, refs);
        }
        Expression::Function { args, .. } => {
            for arg in args {
                collect_column_refs_inner(arg, refs);
            }
        }
        Expression::Case { operand, conditions, results, else_result } => {
            if let Some(op) = operand {
                collect_column_refs_inner(op, refs);
            }
            for c in conditions {
                collect_column_refs_inner(c, refs);
            }
            for r in results {
                collect_column_refs_inner(r, refs);
            }
            if let Some(e) = else_result {
                collect_column_refs_inner(e, refs);
            }
        }
        Expression::Cast { expr, .. } => {
            collect_column_refs_inner(expr, refs);
        }
        Expression::IsNull(e) | Expression::IsNotNull(e) => {
            collect_column_refs_inner(e, refs);
        }
        Expression::InList { expr, list, .. } => {
            collect_column_refs_inner(expr, refs);
            for l in list {
                collect_column_refs_inner(l, refs);
            }
        }
        _ => {}
    }
}

/// Shift column references by subtracting offset (for moving to right side of join)
fn shift_column_refs(expr: &Expression, offset: usize) -> Expression {
    match expr {
        Expression::ColumnRef { table_index, column_index, name } => {
            if *column_index >= offset {
                Expression::ColumnRef {
                    table_index: *table_index,
                    column_index: column_index - offset,
                    name: name.clone(),
                }
            } else {
                expr.clone()
            }
        }
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(shift_column_refs(left, offset)),
            op: *op,
            right: Box::new(shift_column_refs(right, offset)),
        },
        Expression::UnaryOp { op, expr } => Expression::UnaryOp {
            op: *op,
            expr: Box::new(shift_column_refs(expr, offset)),
        },
        Expression::Function { name, args } => Expression::Function {
            name: name.clone(),
            args: args.iter().map(|a| shift_column_refs(a, offset)).collect(),
        },
        Expression::Cast { expr, target_type } => Expression::Cast {
            expr: Box::new(shift_column_refs(expr, offset)),
            target_type: target_type.clone(),
        },
        Expression::IsNull(e) => Expression::IsNull(Box::new(shift_column_refs(e, offset))),
        Expression::IsNotNull(e) => Expression::IsNotNull(Box::new(shift_column_refs(e, offset))),
        _ => expr.clone(),
    }
}

/// Try to rewrite a predicate through a project operation
/// Returns None if the predicate can't be pushed through
fn rewrite_predicate_through_project(
    predicate: &Expression,
    project_exprs: &[Expression],
    output_names: &[String],
) -> Option<Expression> {
    // Build a map from output column names/indices to input expressions
    let column_map: Vec<(&String, &Expression)> = output_names.iter().zip(project_exprs.iter()).collect();

    rewrite_expr_through_project(predicate, &column_map)
}

fn rewrite_expr_through_project(
    expr: &Expression,
    column_map: &[(&String, &Expression)],
) -> Option<Expression> {
    match expr {
        Expression::ColumnRef { column_index, name, .. } => {
            // Try to find the expression for this column
            if *column_index < column_map.len() {
                let (_, proj_expr) = column_map[*column_index];
                // Only push through if the projected expression is simple
                // (a column reference or constant)
                match proj_expr {
                    Expression::ColumnRef { .. } | Expression::Constant(_) => {
                        Some(proj_expr.clone())
                    }
                    _ => None, // Complex expressions - don't push through
                }
            } else {
                None
            }
        }
        Expression::Constant(v) => Some(Expression::Constant(v.clone())),
        Expression::BinaryOp { left, op, right } => {
            let new_left = rewrite_expr_through_project(left, column_map)?;
            let new_right = rewrite_expr_through_project(right, column_map)?;
            Some(Expression::BinaryOp {
                left: Box::new(new_left),
                op: *op,
                right: Box::new(new_right),
            })
        }
        Expression::UnaryOp { op, expr } => {
            let new_expr = rewrite_expr_through_project(expr, column_map)?;
            Some(Expression::UnaryOp {
                op: *op,
                expr: Box::new(new_expr),
            })
        }
        Expression::IsNull(e) => {
            Some(Expression::IsNull(Box::new(rewrite_expr_through_project(e, column_map)?)))
        }
        Expression::IsNotNull(e) => {
            Some(Expression::IsNotNull(Box::new(rewrite_expr_through_project(e, column_map)?)))
        }
        Expression::Function { name, args } => {
            let new_args: Option<Vec<_>> = args
                .iter()
                .map(|a| rewrite_expr_through_project(a, column_map))
                .collect();
            new_args.map(|args| Expression::Function {
                name: name.clone(),
                args,
            })
        }
        Expression::InList { expr, list, negated } => {
            let new_expr = rewrite_expr_through_project(expr, column_map)?;
            let new_list: Option<Vec<_>> = list
                .iter()
                .map(|l| rewrite_expr_through_project(l, column_map))
                .collect();
            new_list.map(|list| Expression::InList {
                expr: Box::new(new_expr),
                list,
                negated: *negated,
            })
        }
        _ => None, // Don't push through for complex expressions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironduck_common::{LogicalType, Value};

    #[test]
    fn test_split_conjunction() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Constant(Value::Boolean(true))),
            op: BinaryOperator::And,
            right: Box::new(Expression::Constant(Value::Boolean(false))),
        };
        let conjuncts = split_conjunction(&expr);
        assert_eq!(conjuncts.len(), 2);
    }

    #[test]
    fn test_push_filter_past_sort() {
        let scan = LogicalOperator::Scan {
            schema: "main".to_string(),
            table: "test".to_string(),
            column_names: vec!["a".to_string()],
            output_types: vec![LogicalType::Integer],
        };

        let sort = LogicalOperator::Sort {
            input: Box::new(scan),
            order_by: vec![],
        };

        let filter = LogicalOperator::Filter {
            input: Box::new(sort),
            predicate: Expression::Constant(Value::Boolean(true)),
        };

        let result = pushdown_filters(&filter);

        // The filter should now be below the sort
        match result {
            LogicalOperator::Sort { input, .. } => {
                match input.as_ref() {
                    LogicalOperator::Filter { .. } => {} // Correct
                    _ => panic!("Expected filter below sort"),
                }
            }
            _ => panic!("Expected sort at top"),
        }
    }
}
