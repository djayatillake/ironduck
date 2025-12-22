//! Projection pushdown optimization rule
//!
//! Pushes column projections closer to the data source to reduce the amount
//! of data that flows through the query plan. This eliminates columns that
//! aren't needed early, reducing memory usage and processing time.

use crate::OptimizationRule;
use ironduck_common::LogicalType;
use ironduck_planner::{Expression, LogicalOperator, LogicalPlan};
use std::collections::HashSet;

/// Push projections closer to the data source
pub struct ProjectionPushdown;

impl OptimizationRule for ProjectionPushdown {
    fn name(&self) -> &str {
        "projection_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        // Collect all column indices that are needed (starting with all output columns)
        let output_count = plan.output_names.len();
        let needed: HashSet<usize> = (0..output_count).collect();

        let new_root = pushdown_projections(&plan.root, &needed);
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

/// Recursively push projections down through the operator tree
fn pushdown_projections(op: &LogicalOperator, needed: &HashSet<usize>) -> LogicalOperator {
    match op {
        // For Scan, we can filter down to only the needed columns
        LogicalOperator::Scan {
            schema,
            table,
            column_names,
            output_types,
        } => {
            // Determine which columns to keep
            let mut needed_indices: Vec<usize> = needed.iter().cloned().collect();
            needed_indices.sort();

            // If we need all columns, no change needed
            if needed_indices.len() == column_names.len()
                && needed_indices == (0..column_names.len()).collect::<Vec<_>>()
            {
                return op.clone();
            }

            // If needed indices reference columns beyond what we have, just return original
            if needed_indices.iter().any(|&i| i >= column_names.len()) {
                return op.clone();
            }

            // Create a filtered scan
            let new_names: Vec<String> = needed_indices
                .iter()
                .map(|&i| column_names[i].clone())
                .collect();
            let new_types: Vec<LogicalType> = needed_indices
                .iter()
                .map(|&i| output_types[i].clone())
                .collect();

            LogicalOperator::Scan {
                schema: schema.clone(),
                table: table.clone(),
                column_names: new_names,
                output_types: new_types,
            }
        }

        // For Project, determine what columns are needed from input
        LogicalOperator::Project {
            input,
            expressions,
            output_names,
            output_types,
        } => {
            // Collect columns needed from the input for the output expressions we need
            let mut input_needed = HashSet::new();
            for &idx in needed {
                if idx < expressions.len() {
                    collect_column_refs(&expressions[idx], &mut input_needed);
                }
            }

            LogicalOperator::Project {
                input: Box::new(pushdown_projections(input, &input_needed)),
                expressions: expressions.clone(),
                output_names: output_names.clone(),
                output_types: output_types.clone(),
            }
        }

        // For Filter, we need all columns referenced in the predicate
        LogicalOperator::Filter { input, predicate } => {
            let mut input_needed = needed.clone();
            collect_column_refs(predicate, &mut input_needed);

            LogicalOperator::Filter {
                input: Box::new(pushdown_projections(input, &input_needed)),
                predicate: predicate.clone(),
            }
        }

        // For Sort, we need columns referenced in ORDER BY
        LogicalOperator::Sort { input, order_by } => {
            let mut input_needed = needed.clone();
            for ob in order_by {
                collect_column_refs(&ob.expr, &mut input_needed);
            }

            LogicalOperator::Sort {
                input: Box::new(pushdown_projections(input, &input_needed)),
                order_by: order_by.clone(),
            }
        }

        // For Limit, we just pass through the needed columns
        LogicalOperator::Limit {
            input,
            limit,
            offset,
        } => LogicalOperator::Limit {
            input: Box::new(pushdown_projections(input, needed)),
            limit: *limit,
            offset: *offset,
        },

        // For Distinct, we need all distinct columns plus output columns
        LogicalOperator::Distinct { input, on_exprs } => {
            let mut input_needed = needed.clone();
            if let Some(exprs) = on_exprs {
                for e in exprs {
                    collect_column_refs(e, &mut input_needed);
                }
            }

            LogicalOperator::Distinct {
                input: Box::new(pushdown_projections(input, &input_needed)),
                on_exprs: on_exprs.clone(),
            }
        }

        // For Aggregate, we need group by columns and aggregate input columns
        LogicalOperator::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let mut input_needed = HashSet::new();
            for gb in group_by {
                collect_column_refs(gb, &mut input_needed);
            }
            for agg in aggregates {
                for arg in &agg.args {
                    collect_column_refs(arg, &mut input_needed);
                }
                if let Some(filter) = &agg.filter {
                    collect_column_refs(filter, &mut input_needed);
                }
                for (ob_expr, _, _) in &agg.order_by {
                    collect_column_refs(ob_expr, &mut input_needed);
                }
            }

            LogicalOperator::Aggregate {
                input: Box::new(pushdown_projections(input, &input_needed)),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
            }
        }

        // For Join, split columns between left and right
        LogicalOperator::Join {
            left,
            right,
            join_type,
            condition,
        } => {
            let left_count = left.output_types().len();

            let mut left_needed = HashSet::new();
            let mut right_needed = HashSet::new();

            for &idx in needed {
                if idx < left_count {
                    left_needed.insert(idx);
                } else {
                    right_needed.insert(idx - left_count);
                }
            }

            // Also add columns needed by the join condition
            if let Some(cond) = condition {
                let mut cond_cols = HashSet::new();
                collect_column_refs(cond, &mut cond_cols);
                for idx in cond_cols {
                    if idx < left_count {
                        left_needed.insert(idx);
                    } else {
                        right_needed.insert(idx - left_count);
                    }
                }
            }

            LogicalOperator::Join {
                left: Box::new(pushdown_projections(left, &left_needed)),
                right: Box::new(pushdown_projections(right, &right_needed)),
                join_type: *join_type,
                condition: condition.clone(),
            }
        }

        // For SetOperation, pass through all columns
        LogicalOperator::SetOperation {
            left,
            right,
            op,
            all,
        } => {
            // Set operations need all columns
            let left_count = left.output_types().len();
            let left_needed: HashSet<usize> = (0..left_count).collect();
            let right_count = right.output_types().len();
            let right_needed: HashSet<usize> = (0..right_count).collect();

            LogicalOperator::SetOperation {
                left: Box::new(pushdown_projections(left, &left_needed)),
                right: Box::new(pushdown_projections(right, &right_needed)),
                op: *op,
                all: *all,
            }
        }

        // For Window, we need window function inputs
        LogicalOperator::Window {
            input,
            window_exprs,
            output_names,
            output_types,
        } => {
            let input_count = input.output_types().len();
            let mut input_needed: HashSet<usize> = (0..input_count).collect();

            for we in window_exprs {
                for arg in &we.args {
                    collect_column_refs(arg, &mut input_needed);
                }
                for pb in &we.partition_by {
                    collect_column_refs(pb, &mut input_needed);
                }
                for ob in &we.order_by {
                    collect_column_refs(&ob.expr, &mut input_needed);
                }
            }

            LogicalOperator::Window {
                input: Box::new(pushdown_projections(input, &input_needed)),
                window_exprs: window_exprs.clone(),
                output_names: output_names.clone(),
                output_types: output_types.clone(),
            }
        }

        // For RecursiveCTE, need all columns
        LogicalOperator::RecursiveCTE {
            name,
            base_case,
            recursive_case,
            output_names,
            output_types,
            union_all,
        } => {
            let col_count = output_types.len();
            let all_cols: HashSet<usize> = (0..col_count).collect();

            LogicalOperator::RecursiveCTE {
                name: name.clone(),
                base_case: Box::new(pushdown_projections(base_case, &all_cols)),
                recursive_case: Box::new(pushdown_projections(recursive_case, &all_cols)),
                output_names: output_names.clone(),
                output_types: output_types.clone(),
                union_all: *union_all,
            }
        }

        LogicalOperator::Explain { input } => {
            let input_count = input.output_types().len();
            let all_cols: HashSet<usize> = (0..input_count).collect();
            LogicalOperator::Explain {
                input: Box::new(pushdown_projections(input, &all_cols)),
            }
        }

        LogicalOperator::CreateTable {
            schema,
            name,
            columns,
            if_not_exists,
            source,
        } => LogicalOperator::CreateTable {
            schema: schema.clone(),
            name: name.clone(),
            columns: columns.clone(),
            if_not_exists: *if_not_exists,
            source: source.as_ref().map(|s| {
                let col_count = s.output_types().len();
                let all_cols: HashSet<usize> = (0..col_count).collect();
                Box::new(pushdown_projections(s, &all_cols))
            }),
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
            source: source.as_ref().map(|s| {
                let col_count = s.output_types().len();
                let all_cols: HashSet<usize> = (0..col_count).collect();
                Box::new(pushdown_projections(s, &all_cols))
            }),
        },

        // Pass through operators that don't have child operators
        _ => op.clone(),
    }
}

/// Collect all column indices referenced by an expression
fn collect_column_refs(expr: &Expression, refs: &mut HashSet<usize>) {
    match expr {
        Expression::ColumnRef { column_index, .. } => {
            refs.insert(*column_index);
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_column_refs(left, refs);
            collect_column_refs(right, refs);
        }
        Expression::UnaryOp { expr, .. } => {
            collect_column_refs(expr, refs);
        }
        Expression::Function { args, .. } => {
            for arg in args {
                collect_column_refs(arg, refs);
            }
        }
        Expression::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                collect_column_refs(op, refs);
            }
            for c in conditions {
                collect_column_refs(c, refs);
            }
            for r in results {
                collect_column_refs(r, refs);
            }
            if let Some(e) = else_result {
                collect_column_refs(e, refs);
            }
        }
        Expression::Cast { expr, .. } => {
            collect_column_refs(expr, refs);
        }
        Expression::IsNull(e) | Expression::IsNotNull(e) => {
            collect_column_refs(e, refs);
        }
        Expression::InList { expr, list, .. } => {
            collect_column_refs(expr, refs);
            for l in list {
                collect_column_refs(l, refs);
            }
        }
        Expression::InSubquery { expr, .. } => {
            collect_column_refs(expr, refs);
        }
        Expression::Subquery(_) | Expression::Exists { .. } => {
            // Subqueries have their own scope - don't collect
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironduck_common::Value;

    #[test]
    fn test_collect_column_refs() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table_index: 0,
                column_index: 0,
                name: "a".to_string(),
            }),
            op: ironduck_planner::BinaryOperator::Add,
            right: Box::new(Expression::ColumnRef {
                table_index: 0,
                column_index: 2,
                name: "c".to_string(),
            }),
        };

        let mut refs = HashSet::new();
        collect_column_refs(&expr, &mut refs);

        assert!(refs.contains(&0));
        assert!(refs.contains(&2));
        assert!(!refs.contains(&1));
    }

    #[test]
    fn test_projection_on_scan() {
        let scan = LogicalOperator::Scan {
            schema: "main".to_string(),
            table: "test".to_string(),
            column_names: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            output_types: vec![
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::Integer,
            ],
        };

        // Only need columns 0 and 2
        let mut needed = HashSet::new();
        needed.insert(0);
        needed.insert(2);

        let result = pushdown_projections(&scan, &needed);

        match result {
            LogicalOperator::Scan { column_names, .. } => {
                assert_eq!(column_names.len(), 2);
                assert_eq!(column_names[0], "a");
                assert_eq!(column_names[1], "c");
            }
            _ => panic!("Expected Scan"),
        }
    }
}
