//! Constant folding optimization rule
//!
//! Evaluates constant expressions at compile time to reduce runtime computation.
//! For example, `1 + 2` becomes `3`.

use crate::OptimizationRule;
use ironduck_common::Value;
use ironduck_planner::{BinaryOperator, Expression, LogicalOperator, LogicalPlan};

/// Evaluate constant expressions at compile time
pub struct ConstantFolding;

impl OptimizationRule for ConstantFolding {
    fn name(&self) -> &str {
        "constant_folding"
    }

    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let new_root = fold_operator(&plan.root);
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
    // Simple check - compare debug output
    format!("{:?}", old) != format!("{:?}", new)
}

/// Recursively fold constants in an operator tree
fn fold_operator(op: &LogicalOperator) -> LogicalOperator {
    match op {
        LogicalOperator::Project {
            input,
            expressions,
            output_names,
            output_types,
        } => LogicalOperator::Project {
            input: Box::new(fold_operator(input)),
            expressions: expressions.iter().map(fold_expression).collect(),
            output_names: output_names.clone(),
            output_types: output_types.clone(),
        },

        LogicalOperator::Filter { input, predicate } => LogicalOperator::Filter {
            input: Box::new(fold_operator(input)),
            predicate: fold_expression(predicate),
        },

        LogicalOperator::Aggregate {
            input,
            group_by,
            aggregates,
        } => LogicalOperator::Aggregate {
            input: Box::new(fold_operator(input)),
            group_by: group_by.iter().map(fold_expression).collect(),
            aggregates: aggregates.clone(), // Aggregates have their own folding
        },

        LogicalOperator::Sort { input, order_by } => LogicalOperator::Sort {
            input: Box::new(fold_operator(input)),
            order_by: order_by
                .iter()
                .map(|o| ironduck_planner::OrderByExpression {
                    expr: fold_expression(&o.expr),
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
            input: Box::new(fold_operator(input)),
            limit: *limit,
            offset: *offset,
        },

        LogicalOperator::Join {
            left,
            right,
            join_type,
            condition,
        } => LogicalOperator::Join {
            left: Box::new(fold_operator(left)),
            right: Box::new(fold_operator(right)),
            join_type: *join_type,
            condition: condition.as_ref().map(fold_expression),
        },

        LogicalOperator::SetOperation {
            left,
            right,
            op,
            all,
        } => LogicalOperator::SetOperation {
            left: Box::new(fold_operator(left)),
            right: Box::new(fold_operator(right)),
            op: *op,
            all: *all,
        },

        // Pass through operators that don't have expressions to fold
        _ => op.clone(),
    }
}

/// Fold constants in an expression
fn fold_expression(expr: &Expression) -> Expression {
    match expr {
        // Binary operations with constant operands can be evaluated
        Expression::BinaryOp { left, op, right } => {
            let folded_left = fold_expression(left);
            let folded_right = fold_expression(right);

            // Try to evaluate if both sides are constants
            if let (Expression::Constant(lval), Expression::Constant(rval)) =
                (&folded_left, &folded_right)
            {
                if let Some(result) = evaluate_binary_op(lval, *op, rval) {
                    return Expression::Constant(result);
                }
            }

            Expression::BinaryOp {
                left: Box::new(folded_left),
                op: *op,
                right: Box::new(folded_right),
            }
        }

        // Unary operations
        Expression::UnaryOp { op, expr: inner } => {
            let folded = fold_expression(inner);
            if let Expression::Constant(val) = &folded {
                if let Some(result) = evaluate_unary_op(*op, val) {
                    return Expression::Constant(result);
                }
            }
            Expression::UnaryOp {
                op: *op,
                expr: Box::new(folded),
            }
        }

        // Function calls - fold arguments
        Expression::Function { name, args } => Expression::Function {
            name: name.clone(),
            args: args.iter().map(fold_expression).collect(),
        },

        // CASE expressions
        Expression::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Expression::Case {
            operand: operand.as_ref().map(|e| Box::new(fold_expression(e))),
            conditions: conditions.iter().map(fold_expression).collect(),
            results: results.iter().map(fold_expression).collect(),
            else_result: else_result.as_ref().map(|e| Box::new(fold_expression(e))),
        },

        // Cast expressions
        Expression::Cast { expr: inner, target_type } => Expression::Cast {
            expr: Box::new(fold_expression(inner)),
            target_type: target_type.clone(),
        },

        // IS NULL / IS NOT NULL
        Expression::IsNull(inner) => Expression::IsNull(Box::new(fold_expression(inner))),
        Expression::IsNotNull(inner) => Expression::IsNotNull(Box::new(fold_expression(inner))),

        // IN list
        Expression::InList { expr: inner, list, negated } => Expression::InList {
            expr: Box::new(fold_expression(inner)),
            list: list.iter().map(fold_expression).collect(),
            negated: *negated,
        },

        // Pass through constants and column refs unchanged
        _ => expr.clone(),
    }
}

/// Evaluate a binary operation on constant values
fn evaluate_binary_op(left: &Value, op: BinaryOperator, right: &Value) -> Option<Value> {
    match op {
        // Arithmetic operations
        BinaryOperator::Add => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a + b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::BigInt(a + b)),
            (Value::Double(a), Value::Double(b)) => Some(Value::Double(a + b)),
            (Value::Integer(a), Value::BigInt(b)) => Some(Value::BigInt(*a as i64 + b)),
            (Value::BigInt(a), Value::Integer(b)) => Some(Value::BigInt(a + *b as i64)),
            _ => None,
        },
        BinaryOperator::Subtract => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a - b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::BigInt(a - b)),
            (Value::Double(a), Value::Double(b)) => Some(Value::Double(a - b)),
            _ => None,
        },
        BinaryOperator::Multiply => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a * b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::BigInt(a * b)),
            (Value::Double(a), Value::Double(b)) => Some(Value::Double(a * b)),
            _ => None,
        },
        BinaryOperator::Divide => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) if *b != 0 => Some(Value::Integer(a / b)),
            (Value::BigInt(a), Value::BigInt(b)) if *b != 0 => Some(Value::BigInt(a / b)),
            (Value::Double(a), Value::Double(b)) if *b != 0.0 => Some(Value::Double(a / b)),
            _ => None,
        },
        BinaryOperator::Modulo => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) if *b != 0 => Some(Value::Integer(a % b)),
            (Value::BigInt(a), Value::BigInt(b)) if *b != 0 => Some(Value::BigInt(a % b)),
            _ => None,
        },

        // Comparison operations
        BinaryOperator::Equal => Some(Value::Boolean(left == right)),
        BinaryOperator::NotEqual => Some(Value::Boolean(left != right)),
        BinaryOperator::LessThan => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Boolean(a < b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::Boolean(a < b)),
            (Value::Double(a), Value::Double(b)) => Some(Value::Boolean(a < b)),
            (Value::Varchar(a), Value::Varchar(b)) => Some(Value::Boolean(a < b)),
            _ => None,
        },
        BinaryOperator::LessThanOrEqual => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Boolean(a <= b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::Boolean(a <= b)),
            (Value::Double(a), Value::Double(b)) => Some(Value::Boolean(a <= b)),
            (Value::Varchar(a), Value::Varchar(b)) => Some(Value::Boolean(a <= b)),
            _ => None,
        },
        BinaryOperator::GreaterThan => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Boolean(a > b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::Boolean(a > b)),
            (Value::Double(a), Value::Double(b)) => Some(Value::Boolean(a > b)),
            (Value::Varchar(a), Value::Varchar(b)) => Some(Value::Boolean(a > b)),
            _ => None,
        },
        BinaryOperator::GreaterThanOrEqual => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Boolean(a >= b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::Boolean(a >= b)),
            (Value::Double(a), Value::Double(b)) => Some(Value::Boolean(a >= b)),
            (Value::Varchar(a), Value::Varchar(b)) => Some(Value::Boolean(a >= b)),
            _ => None,
        },

        // Logical operations
        BinaryOperator::And => match (left, right) {
            (Value::Boolean(a), Value::Boolean(b)) => Some(Value::Boolean(*a && *b)),
            _ => None,
        },
        BinaryOperator::Or => match (left, right) {
            (Value::Boolean(a), Value::Boolean(b)) => Some(Value::Boolean(*a || *b)),
            _ => None,
        },

        // String operations
        BinaryOperator::Concat => match (left, right) {
            (Value::Varchar(a), Value::Varchar(b)) => {
                Some(Value::Varchar(format!("{}{}", a, b)))
            }
            _ => None,
        },

        // Bitwise operations
        BinaryOperator::BitwiseAnd => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a & b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::BigInt(a & b)),
            _ => None,
        },
        BinaryOperator::BitwiseOr => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a | b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::BigInt(a | b)),
            _ => None,
        },
        BinaryOperator::BitwiseXor => match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a ^ b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::BigInt(a ^ b)),
            _ => None,
        },

        _ => None,
    }
}

/// Evaluate a unary operation on a constant value
fn evaluate_unary_op(op: ironduck_planner::UnaryOperator, val: &Value) -> Option<Value> {
    match op {
        ironduck_planner::UnaryOperator::Negate => match val {
            Value::Integer(n) => Some(Value::Integer(-n)),
            Value::BigInt(n) => Some(Value::BigInt(-n)),
            Value::Double(n) => Some(Value::Double(-n)),
            _ => None,
        },
        ironduck_planner::UnaryOperator::Not => match val {
            Value::Boolean(b) => Some(Value::Boolean(!b)),
            _ => None,
        },
        ironduck_planner::UnaryOperator::BitwiseNot => match val {
            Value::Integer(n) => Some(Value::Integer(!n)),
            Value::BigInt(n) => Some(Value::BigInt(!n)),
            _ => None,
        },
        _ => None,
    }
}
