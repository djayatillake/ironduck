//! Predicate simplification optimization rule
//!
//! Simplifies boolean expressions to reduce complexity:
//! - TRUE AND x -> x
//! - FALSE AND x -> FALSE
//! - TRUE OR x -> TRUE
//! - FALSE OR x -> x
//! - NOT TRUE -> FALSE
//! - NOT FALSE -> TRUE
//! - x = x -> TRUE (for non-null values)
//! - 1 = 1 -> TRUE (constant comparison)

use crate::OptimizationRule;
use ironduck_common::Value;
use ironduck_planner::{BinaryOperator, Expression, LogicalOperator, LogicalPlan, UnaryOperator};

/// Simplify boolean predicates
pub struct PredicateSimplification;

impl OptimizationRule for PredicateSimplification {
    fn name(&self) -> &str {
        "predicate_simplification"
    }

    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let new_root = simplify_operator(&plan.root);
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

/// Recursively simplify predicates in an operator tree
fn simplify_operator(op: &LogicalOperator) -> LogicalOperator {
    match op {
        LogicalOperator::Filter { input, predicate } => {
            let simplified = simplify_expression(predicate);

            // If predicate simplifies to TRUE, eliminate the filter
            if let Expression::Constant(Value::Boolean(true)) = &simplified {
                return simplify_operator(input);
            }

            // If predicate simplifies to FALSE, this filter produces no rows
            // Keep the filter but with simplified predicate
            LogicalOperator::Filter {
                input: Box::new(simplify_operator(input)),
                predicate: simplified,
            }
        }

        LogicalOperator::Project {
            input,
            expressions,
            output_names,
            output_types,
        } => LogicalOperator::Project {
            input: Box::new(simplify_operator(input)),
            expressions: expressions.iter().map(simplify_expression).collect(),
            output_names: output_names.clone(),
            output_types: output_types.clone(),
        },

        LogicalOperator::Aggregate {
            input,
            group_by,
            aggregates,
        } => LogicalOperator::Aggregate {
            input: Box::new(simplify_operator(input)),
            group_by: group_by.iter().map(simplify_expression).collect(),
            aggregates: aggregates.clone(),
        },

        LogicalOperator::Sort { input, order_by } => LogicalOperator::Sort {
            input: Box::new(simplify_operator(input)),
            order_by: order_by
                .iter()
                .map(|o| ironduck_planner::OrderByExpression {
                    expr: simplify_expression(&o.expr),
                    ascending: o.ascending,
                    nulls_first: o.nulls_first,
                })
                .collect(),
        },

        LogicalOperator::Limit {
            input,
            limit,
            offset,
        } => LogicalOperator::Limit {
            input: Box::new(simplify_operator(input)),
            limit: *limit,
            offset: *offset,
        },

        LogicalOperator::Distinct { input, on_exprs } => LogicalOperator::Distinct {
            input: Box::new(simplify_operator(input)),
            on_exprs: on_exprs.as_ref().map(|es| es.iter().map(simplify_expression).collect()),
        },

        LogicalOperator::Join {
            left,
            right,
            join_type,
            condition,
            is_lateral,
        } => {
            let simplified_cond = condition.as_ref().map(simplify_expression);

            // If join condition simplifies to TRUE, it's a cross join
            // If it simplifies to FALSE, the join produces no rows

            LogicalOperator::Join {
                left: Box::new(simplify_operator(left)),
                right: Box::new(simplify_operator(right)),
                join_type: *join_type,
                condition: simplified_cond,
                is_lateral: *is_lateral,
            }
        }

        LogicalOperator::SetOperation {
            left,
            right,
            op,
            all,
        } => LogicalOperator::SetOperation {
            left: Box::new(simplify_operator(left)),
            right: Box::new(simplify_operator(right)),
            op: *op,
            all: *all,
        },

        LogicalOperator::Window {
            input,
            window_exprs,
            output_names,
            output_types,
        } => LogicalOperator::Window {
            input: Box::new(simplify_operator(input)),
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
            base_case: Box::new(simplify_operator(base_case)),
            recursive_case: Box::new(simplify_operator(recursive_case)),
            output_names: output_names.clone(),
            output_types: output_types.clone(),
            union_all: *union_all,
        },

        LogicalOperator::Explain { input } => LogicalOperator::Explain {
            input: Box::new(simplify_operator(input)),
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
            source: source.as_ref().map(|s| Box::new(simplify_operator(s))),
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
            source: source.as_ref().map(|s| Box::new(simplify_operator(s))),
        },

        // Pass through operators that don't have expressions
        _ => op.clone(),
    }
}

/// Simplify an expression
fn simplify_expression(expr: &Expression) -> Expression {
    match expr {
        // Binary operations with boolean logic
        Expression::BinaryOp { left, op, right } => {
            let simp_left = simplify_expression(left);
            let simp_right = simplify_expression(right);

            match op {
                // AND simplifications
                BinaryOperator::And => {
                    // TRUE AND x -> x
                    if let Expression::Constant(Value::Boolean(true)) = &simp_left {
                        return simp_right;
                    }
                    // x AND TRUE -> x
                    if let Expression::Constant(Value::Boolean(true)) = &simp_right {
                        return simp_left;
                    }
                    // FALSE AND x -> FALSE
                    if let Expression::Constant(Value::Boolean(false)) = &simp_left {
                        return Expression::Constant(Value::Boolean(false));
                    }
                    // x AND FALSE -> FALSE
                    if let Expression::Constant(Value::Boolean(false)) = &simp_right {
                        return Expression::Constant(Value::Boolean(false));
                    }
                }

                // OR simplifications
                BinaryOperator::Or => {
                    // TRUE OR x -> TRUE
                    if let Expression::Constant(Value::Boolean(true)) = &simp_left {
                        return Expression::Constant(Value::Boolean(true));
                    }
                    // x OR TRUE -> TRUE
                    if let Expression::Constant(Value::Boolean(true)) = &simp_right {
                        return Expression::Constant(Value::Boolean(true));
                    }
                    // FALSE OR x -> x
                    if let Expression::Constant(Value::Boolean(false)) = &simp_left {
                        return simp_right;
                    }
                    // x OR FALSE -> x
                    if let Expression::Constant(Value::Boolean(false)) = &simp_right {
                        return simp_left;
                    }
                }

                // Equality with identical constants
                BinaryOperator::Equal => {
                    if let (Expression::Constant(l), Expression::Constant(r)) = (&simp_left, &simp_right) {
                        // Handle NULL comparisons
                        if matches!(l, Value::Null) || matches!(r, Value::Null) {
                            return Expression::Constant(Value::Null);
                        }
                        return Expression::Constant(Value::Boolean(l == r));
                    }
                }

                BinaryOperator::NotEqual => {
                    if let (Expression::Constant(l), Expression::Constant(r)) = (&simp_left, &simp_right) {
                        if matches!(l, Value::Null) || matches!(r, Value::Null) {
                            return Expression::Constant(Value::Null);
                        }
                        return Expression::Constant(Value::Boolean(l != r));
                    }
                }

                _ => {}
            }

            Expression::BinaryOp {
                left: Box::new(simp_left),
                op: *op,
                right: Box::new(simp_right),
            }
        }

        // NOT simplifications
        Expression::UnaryOp { op: UnaryOperator::Not, expr } => {
            let simp = simplify_expression(expr);

            // NOT TRUE -> FALSE
            if let Expression::Constant(Value::Boolean(true)) = &simp {
                return Expression::Constant(Value::Boolean(false));
            }
            // NOT FALSE -> TRUE
            if let Expression::Constant(Value::Boolean(false)) = &simp {
                return Expression::Constant(Value::Boolean(true));
            }
            // NOT NOT x -> x
            if let Expression::UnaryOp { op: UnaryOperator::Not, expr: inner } = &simp {
                return simplify_expression(inner);
            }

            Expression::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(simp),
            }
        }

        Expression::UnaryOp { op, expr } => Expression::UnaryOp {
            op: *op,
            expr: Box::new(simplify_expression(expr)),
        },

        // IS NULL simplifications
        Expression::IsNull(inner) => {
            let simp = simplify_expression(inner);
            if let Expression::Constant(v) = &simp {
                return Expression::Constant(Value::Boolean(matches!(v, Value::Null)));
            }
            Expression::IsNull(Box::new(simp))
        }

        // IS NOT NULL simplifications
        Expression::IsNotNull(inner) => {
            let simp = simplify_expression(inner);
            if let Expression::Constant(v) = &simp {
                return Expression::Constant(Value::Boolean(!matches!(v, Value::Null)));
            }
            Expression::IsNotNull(Box::new(simp))
        }

        // Function calls - simplify arguments
        Expression::Function { name, args } => Expression::Function {
            name: name.clone(),
            args: args.iter().map(simplify_expression).collect(),
        },

        // CASE expressions
        Expression::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let simp_conditions: Vec<_> = conditions.iter().map(simplify_expression).collect();
            let simp_results: Vec<_> = results.iter().map(simplify_expression).collect();

            // If first condition is TRUE, return first result
            if !simp_conditions.is_empty() {
                if let Expression::Constant(Value::Boolean(true)) = &simp_conditions[0] {
                    return simp_results[0].clone();
                }
            }

            Expression::Case {
                operand: operand.as_ref().map(|e| Box::new(simplify_expression(e))),
                conditions: simp_conditions,
                results: simp_results,
                else_result: else_result.as_ref().map(|e| Box::new(simplify_expression(e))),
            }
        }

        // Cast expressions
        Expression::Cast { expr, target_type } => Expression::Cast {
            expr: Box::new(simplify_expression(expr)),
            target_type: target_type.clone(),
        },

        // IN list
        Expression::InList { expr, list, negated } => {
            let simp_list: Vec<_> = list.iter().map(simplify_expression).collect();

            // Empty list
            if simp_list.is_empty() {
                return Expression::Constant(Value::Boolean(*negated));
            }

            Expression::InList {
                expr: Box::new(simplify_expression(expr)),
                list: simp_list,
                negated: *negated,
            }
        }

        // Pass through
        _ => expr.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_true_and_x() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Constant(Value::Boolean(true))),
            op: BinaryOperator::And,
            right: Box::new(Expression::ColumnRef {
                table_index: 0,
                column_index: 0,
                name: "x".to_string(),
            }),
        };

        let result = simplify_expression(&expr);
        match result {
            Expression::ColumnRef { name, .. } => assert_eq!(name, "x"),
            _ => panic!("Expected ColumnRef"),
        }
    }

    #[test]
    fn test_false_or_x() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Constant(Value::Boolean(false))),
            op: BinaryOperator::Or,
            right: Box::new(Expression::ColumnRef {
                table_index: 0,
                column_index: 0,
                name: "x".to_string(),
            }),
        };

        let result = simplify_expression(&expr);
        match result {
            Expression::ColumnRef { name, .. } => assert_eq!(name, "x"),
            _ => panic!("Expected ColumnRef"),
        }
    }

    #[test]
    fn test_not_true() {
        let expr = Expression::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expression::Constant(Value::Boolean(true))),
        };

        let result = simplify_expression(&expr);
        assert!(matches!(result, Expression::Constant(Value::Boolean(false))));
    }

    #[test]
    fn test_is_null_constant() {
        let expr = Expression::IsNull(Box::new(Expression::Constant(Value::Null)));
        let result = simplify_expression(&expr);
        assert!(matches!(result, Expression::Constant(Value::Boolean(true))));

        let expr2 = Expression::IsNull(Box::new(Expression::Constant(Value::Integer(5))));
        let result2 = simplify_expression(&expr2);
        assert!(matches!(result2, Expression::Constant(Value::Boolean(false))));
    }

    #[test]
    fn test_equal_constants() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Constant(Value::Integer(5))),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Constant(Value::Integer(5))),
        };

        let result = simplify_expression(&expr);
        assert!(matches!(result, Expression::Constant(Value::Boolean(true))));
    }
}
