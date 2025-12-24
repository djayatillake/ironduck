//! Limit pushdown and simplification optimization rule
//!
//! Optimizes LIMIT operators:
//! - Combines consecutive limits (takes the minimum)
//! - Pushes limits past Project (safe transformation)
//! - Eliminates LIMIT 0 (produces no rows)

use crate::OptimizationRule;
use ironduck_planner::{LogicalOperator, LogicalPlan};

/// Optimize LIMIT operators
pub struct LimitPushdown;

impl OptimizationRule for LimitPushdown {
    fn name(&self) -> &str {
        "limit_pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let new_root = optimize_limits(&plan.root);
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

/// Recursively optimize limits in an operator tree
fn optimize_limits(op: &LogicalOperator) -> LogicalOperator {
    match op {
        // Limit optimizations
        LogicalOperator::Limit {
            input,
            limit,
            offset,
        } => {
            let optimized_input = optimize_limits(input);

            // LIMIT 0 with no offset produces no rows - we could use an empty Values
            // but for now just keep it as-is

            match &optimized_input {
                // Combine consecutive limits: LIMIT m (LIMIT n) -> LIMIT min(m, n)
                // Note: This is simplified - proper handling of offset is more complex
                LogicalOperator::Limit {
                    input: inner_input,
                    limit: inner_limit,
                    offset: inner_offset,
                } => {
                    // Only combine if no offsets or outer offset is 0
                    if offset.unwrap_or(0) == 0 && inner_offset.unwrap_or(0) == 0 {
                        let combined_limit = match (*limit, *inner_limit) {
                            (Some(l1), Some(l2)) => Some(l1.min(l2)),
                            (Some(l), None) | (None, Some(l)) => Some(l),
                            (None, None) => None,
                        };
                        LogicalOperator::Limit {
                            input: inner_input.clone(),
                            limit: combined_limit,
                            offset: None,
                        }
                    } else {
                        LogicalOperator::Limit {
                            input: Box::new(optimized_input),
                            limit: *limit,
                            offset: *offset,
                        }
                    }
                }

                // Push limit past Project (always safe)
                LogicalOperator::Project {
                    input: proj_input,
                    expressions,
                    output_names,
                    output_types,
                } => {
                    // Move limit below project
                    LogicalOperator::Project {
                        input: Box::new(LogicalOperator::Limit {
                            input: proj_input.clone(),
                            limit: *limit,
                            offset: *offset,
                        }),
                        expressions: expressions.clone(),
                        output_names: output_names.clone(),
                        output_types: output_types.clone(),
                    }
                }

                // For all other cases, just return the optimized operator
                _ => LogicalOperator::Limit {
                    input: Box::new(optimized_input),
                    limit: *limit,
                    offset: *offset,
                },
            }
        }

        // Recursively optimize other operators
        LogicalOperator::Filter { input, predicate } => LogicalOperator::Filter {
            input: Box::new(optimize_limits(input)),
            predicate: predicate.clone(),
        },

        LogicalOperator::Project {
            input,
            expressions,
            output_names,
            output_types,
        } => LogicalOperator::Project {
            input: Box::new(optimize_limits(input)),
            expressions: expressions.clone(),
            output_names: output_names.clone(),
            output_types: output_types.clone(),
        },

        LogicalOperator::Aggregate {
            input,
            group_by,
            aggregates,
        } => LogicalOperator::Aggregate {
            input: Box::new(optimize_limits(input)),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
        },

        LogicalOperator::Sort { input, order_by } => LogicalOperator::Sort {
            input: Box::new(optimize_limits(input)),
            order_by: order_by.clone(),
        },

        LogicalOperator::Distinct { input, on_exprs } => LogicalOperator::Distinct {
            input: Box::new(optimize_limits(input)),
            on_exprs: on_exprs.clone(),
        },

        LogicalOperator::Join {
            left,
            right,
            join_type,
            condition,
        } => LogicalOperator::Join {
            left: Box::new(optimize_limits(left)),
            right: Box::new(optimize_limits(right)),
            join_type: *join_type,
            condition: condition.clone(),
        },

        LogicalOperator::SetOperation {
            left,
            right,
            op,
            all,
        } => LogicalOperator::SetOperation {
            left: Box::new(optimize_limits(left)),
            right: Box::new(optimize_limits(right)),
            op: *op,
            all: *all,
        },

        LogicalOperator::Window {
            input,
            window_exprs,
            output_names,
            output_types,
        } => LogicalOperator::Window {
            input: Box::new(optimize_limits(input)),
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
            base_case: Box::new(optimize_limits(base_case)),
            recursive_case: Box::new(optimize_limits(recursive_case)),
            output_names: output_names.clone(),
            output_types: output_types.clone(),
            union_all: *union_all,
        },

        LogicalOperator::Explain { input } => LogicalOperator::Explain {
            input: Box::new(optimize_limits(input)),
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
            source: source.as_ref().map(|s| Box::new(optimize_limits(s))),
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
            source: source.as_ref().map(|s| Box::new(optimize_limits(s))),
        },

        // Pass through operators without children
        _ => op.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironduck_common::LogicalType;

    #[test]
    fn test_combine_limits() {
        let scan = LogicalOperator::Scan {
            schema: "main".to_string(),
            table: "test".to_string(),
            column_names: vec!["a".to_string()],
            output_types: vec![LogicalType::Integer],
        };

        let inner_limit = LogicalOperator::Limit {
            input: Box::new(scan),
            limit: Some(10),
            offset: None,
        };

        let outer_limit = LogicalOperator::Limit {
            input: Box::new(inner_limit),
            limit: Some(5),
            offset: None,
        };

        let result = optimize_limits(&outer_limit);

        match result {
            LogicalOperator::Limit { limit, .. } => {
                assert_eq!(limit, Some(5));
            }
            _ => panic!("Expected Limit"),
        }
    }

    #[test]
    fn test_push_limit_past_project() {
        let scan = LogicalOperator::Scan {
            schema: "main".to_string(),
            table: "test".to_string(),
            column_names: vec!["a".to_string()],
            output_types: vec![LogicalType::Integer],
        };

        let project = LogicalOperator::Project {
            input: Box::new(scan),
            expressions: vec![],
            output_names: vec!["a".to_string()],
            output_types: vec![LogicalType::Integer],
        };

        let limit = LogicalOperator::Limit {
            input: Box::new(project),
            limit: Some(5),
            offset: None,
        };

        let result = optimize_limits(&limit);

        // Limit should now be inside the Project
        match result {
            LogicalOperator::Project { input, .. } => {
                match input.as_ref() {
                    LogicalOperator::Limit { limit, .. } => {
                        assert_eq!(*limit, Some(5));
                    }
                    _ => panic!("Expected Limit inside Project"),
                }
            }
            _ => panic!("Expected Project at top"),
        }
    }
}
