//! Vectorized expression evaluation
//!
//! Evaluates expressions on entire DataChunks at once for better performance.

use crate::chunk::DataChunk;
use crate::vector::{Vector, VectorData, ValueVector};
use ironduck_common::{Error, LogicalType, Result, Value};
use ironduck_planner::Expression;

/// Evaluate an expression on a DataChunk, returning a result Vector
pub fn evaluate_vectorized(expr: &Expression, chunk: &DataChunk) -> Result<Vector> {
    match expr {
        Expression::Constant(value) => {
            // Create a constant vector
            Ok(Vector::new_constant(value.clone()))
        }

        Expression::ColumnRef { column_index, .. } => {
            // Return a reference to the column (clone for now)
            if *column_index < chunk.column_count() {
                Ok(chunk.column(*column_index).clone())
            } else {
                Err(Error::Internal(format!(
                    "Column index {} out of bounds (have {})",
                    column_index,
                    chunk.column_count()
                )))
            }
        }

        Expression::BinaryOp { left, op, right } => {
            let left_vec = evaluate_vectorized(left, chunk)?;
            let right_vec = evaluate_vectorized(right, chunk)?;
            evaluate_binary_op(&left_vec, op, &right_vec, chunk.row_count())
        }

        Expression::UnaryOp { op, expr } => {
            let vec = evaluate_vectorized(expr, chunk)?;
            evaluate_unary_op(op, &vec, chunk.row_count())
        }

        Expression::Function { name, args } => {
            let arg_vecs: Result<Vec<_>> = args
                .iter()
                .map(|a| evaluate_vectorized(a, chunk))
                .collect();
            evaluate_function(name, &arg_vecs?, chunk.row_count())
        }

        Expression::IsNull(expr) => {
            let vec = evaluate_vectorized(expr, chunk)?;
            let mut result = Vector::new_flat(LogicalType::Boolean, chunk.row_count());
            result.data = VectorData::Values(ValueVector {
                values: (0..chunk.row_count())
                    .map(|i| Value::Boolean(!vec.validity.is_valid(i) || vec.get_value(i).is_null()))
                    .collect(),
            });
            Ok(result)
        }

        Expression::IsNotNull(expr) => {
            let vec = evaluate_vectorized(expr, chunk)?;
            let mut result = Vector::new_flat(LogicalType::Boolean, chunk.row_count());
            result.data = VectorData::Values(ValueVector {
                values: (0..chunk.row_count())
                    .map(|i| Value::Boolean(vec.validity.is_valid(i) && !vec.get_value(i).is_null()))
                    .collect(),
            });
            Ok(result)
        }

        Expression::Cast { expr, target_type } => {
            let vec = evaluate_vectorized(expr, chunk)?;
            cast_vector(&vec, target_type, chunk.row_count())
        }

        Expression::Case {
            operand,
            conditions,
            results,
            else_result,
        } => evaluate_case(operand, conditions, results, else_result, chunk),

        Expression::Subquery(_) => Err(Error::NotImplemented("Vectorized subquery".to_string())),
        Expression::InList { .. } => Err(Error::NotImplemented("Vectorized IN list".to_string())),
        Expression::InSubquery { .. } => Err(Error::NotImplemented("Vectorized IN subquery".to_string())),
        Expression::Exists { .. } => Err(Error::NotImplemented("Vectorized EXISTS".to_string())),
    }
}

/// Evaluate a binary operation on two vectors
fn evaluate_binary_op(
    left: &Vector,
    op: &ironduck_planner::BinaryOperator,
    right: &Vector,
    count: usize,
) -> Result<Vector> {
    use ironduck_planner::BinaryOperator::*;

    let mut values = Vec::with_capacity(count);

    for i in 0..count {
        let l = left.get_value(i);
        let r = right.get_value(i);

        let result = match op {
            Add => numeric_op(&l, &r, |a, b| a + b, |a, b| a + b),
            Subtract => numeric_op(&l, &r, |a, b| a - b, |a, b| a - b),
            Multiply => numeric_op(&l, &r, |a, b| a * b, |a, b| a * b),
            Divide => {
                if matches!(&r, Value::Integer(0) | Value::BigInt(0)) {
                    Value::Null // Division by zero
                } else {
                    numeric_op(&l, &r, |a, b| a / b, |a, b| a / b)
                }
            }
            Modulo => numeric_op(&l, &r, |a, b| a % b, |a, b| a % b),
            Equal => Value::Boolean(l == r),
            NotEqual => Value::Boolean(l != r),
            LessThan => compare_values(&l, &r, |ord| ord == std::cmp::Ordering::Less),
            LessThanOrEqual => {
                compare_values(&l, &r, |ord| matches!(ord, std::cmp::Ordering::Less | std::cmp::Ordering::Equal))
            }
            GreaterThan => compare_values(&l, &r, |ord| ord == std::cmp::Ordering::Greater),
            GreaterThanOrEqual => {
                compare_values(&l, &r, |ord| matches!(ord, std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
            }
            And => match (&l, &r) {
                (Value::Boolean(a), Value::Boolean(b)) => Value::Boolean(*a && *b),
                _ => Value::Null,
            },
            Or => match (&l, &r) {
                (Value::Boolean(a), Value::Boolean(b)) => Value::Boolean(*a || *b),
                _ => Value::Null,
            },
            Concat => {
                let ls = value_to_string(&l);
                let rs = value_to_string(&r);
                Value::Varchar(format!("{}{}", ls, rs))
            }
            Like => {
                let text = value_to_string(&l);
                let pattern = value_to_string(&r);
                Value::Boolean(like_match(&text, &pattern, false))
            }
            ILike => {
                let text = value_to_string(&l);
                let pattern = value_to_string(&r);
                Value::Boolean(like_match(&text, &pattern, true))
            }
            BitwiseAnd => {
                let l = l.as_i64().unwrap_or(0);
                let r = r.as_i64().unwrap_or(0);
                Value::BigInt(l & r)
            }
            BitwiseOr => {
                let l = l.as_i64().unwrap_or(0);
                let r = r.as_i64().unwrap_or(0);
                Value::BigInt(l | r)
            }
            BitwiseXor => {
                let l = l.as_i64().unwrap_or(0);
                let r = r.as_i64().unwrap_or(0);
                Value::BigInt(l ^ r)
            }
            ShiftLeft => {
                let l = l.as_i64().unwrap_or(0);
                let r = r.as_i64().unwrap_or(0);
                Value::BigInt(l << r)
            }
            ShiftRight => {
                let l = l.as_i64().unwrap_or(0);
                let r = r.as_i64().unwrap_or(0);
                Value::BigInt(l >> r)
            }
        };
        values.push(result);
    }

    let return_type = infer_binary_type(op, &left.logical_type, &right.logical_type);
    Ok(Vector::from_values(&values, return_type))
}

/// Evaluate a unary operation
fn evaluate_unary_op(
    op: &ironduck_planner::UnaryOperator,
    vec: &Vector,
    count: usize,
) -> Result<Vector> {
    use ironduck_planner::UnaryOperator::*;

    let mut values = Vec::with_capacity(count);

    for i in 0..count {
        let v = vec.get_value(i);
        let result = match op {
            Negate => match v {
                Value::Integer(i) => Value::Integer(-i),
                Value::BigInt(i) => Value::BigInt(-i),
                Value::Float(f) => Value::Float(-f),
                Value::Double(f) => Value::Double(-f),
                _ => Value::Null,
            },
            Not => match v {
                Value::Boolean(b) => Value::Boolean(!b),
                _ => Value::Null,
            },
            BitwiseNot => match v {
                Value::Integer(i) => Value::Integer(!i),
                Value::BigInt(i) => Value::BigInt(!i),
                _ => Value::Null,
            },
        };
        values.push(result);
    }

    Ok(Vector::from_values(&values, vec.logical_type.clone()))
}

/// Evaluate a function on vectors
fn evaluate_function(name: &str, args: &[Vector], count: usize) -> Result<Vector> {
    let mut values = Vec::with_capacity(count);

    for i in 0..count {
        let arg_values: Vec<_> = args.iter().map(|a| a.get_value(i)).collect();
        let result = evaluate_scalar_function(name, &arg_values)?;
        values.push(result);
    }

    let return_type = infer_function_type(name, args);
    Ok(Vector::from_values(&values, return_type))
}

/// Evaluate a scalar function on a single row
fn evaluate_scalar_function(name: &str, args: &[Value]) -> Result<Value> {
    match name.to_uppercase().as_str() {
        "LOWER" => {
            let s = args.first().map(value_to_string).unwrap_or_default();
            Ok(Value::Varchar(s.to_lowercase()))
        }
        "UPPER" => {
            let s = args.first().map(value_to_string).unwrap_or_default();
            Ok(Value::Varchar(s.to_uppercase()))
        }
        "LENGTH" | "CHAR_LENGTH" => {
            let s = args.first().map(value_to_string).unwrap_or_default();
            Ok(Value::BigInt(s.len() as i64))
        }
        "COALESCE" => {
            for arg in args {
                if !arg.is_null() {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        "NULLIF" => {
            if args.len() >= 2 && args[0] == args[1] {
                Ok(Value::Null)
            } else {
                Ok(args.first().cloned().unwrap_or(Value::Null))
            }
        }
        "ABS" => match args.first() {
            Some(Value::Integer(i)) => Ok(Value::Integer(i.abs())),
            Some(Value::BigInt(i)) => Ok(Value::BigInt(i.abs())),
            Some(Value::Float(f)) => Ok(Value::Float(f.abs())),
            Some(Value::Double(f)) => Ok(Value::Double(f.abs())),
            _ => Ok(Value::Null),
        },
        "TRIM" => {
            let s = args.first().map(value_to_string).unwrap_or_default();
            Ok(Value::Varchar(s.trim().to_string()))
        }
        "LTRIM" => {
            let s = args.first().map(value_to_string).unwrap_or_default();
            Ok(Value::Varchar(s.trim_start().to_string()))
        }
        "RTRIM" => {
            let s = args.first().map(value_to_string).unwrap_or_default();
            Ok(Value::Varchar(s.trim_end().to_string()))
        }
        "CONCAT" => {
            let result: String = args.iter().map(value_to_string).collect();
            Ok(Value::Varchar(result))
        }
        "SUBSTRING" | "SUBSTR" => {
            let s = args.first().map(value_to_string).unwrap_or_default();
            let start = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1) as usize;
            let len = args.get(2).and_then(|v| v.as_i64()).map(|l| l as usize);

            let start_idx = start.saturating_sub(1); // SQL is 1-indexed
            let result = if let Some(l) = len {
                s.chars().skip(start_idx).take(l).collect()
            } else {
                s.chars().skip(start_idx).collect()
            };
            Ok(Value::Varchar(result))
        }
        _ => Ok(Value::Null),
    }
}

/// Evaluate CASE expression
fn evaluate_case(
    operand: &Option<Box<Expression>>,
    conditions: &[Expression],
    results: &[Expression],
    else_result: &Option<Box<Expression>>,
    chunk: &DataChunk,
) -> Result<Vector> {
    let operand_vec = operand
        .as_ref()
        .map(|e| evaluate_vectorized(e, chunk))
        .transpose()?;

    let mut values = Vec::with_capacity(chunk.row_count());

    for i in 0..chunk.row_count() {
        let mut found = false;

        for (cond, result) in conditions.iter().zip(results.iter()) {
            let cond_vec = evaluate_vectorized(cond, chunk)?;
            let cond_val = cond_vec.get_value(i);

            let matches = if let Some(ref op_vec) = operand_vec {
                let op_val = op_vec.get_value(i);
                op_val == cond_val
            } else {
                matches!(cond_val, Value::Boolean(true))
            };

            if matches {
                let result_vec = evaluate_vectorized(result, chunk)?;
                values.push(result_vec.get_value(i));
                found = true;
                break;
            }
        }

        if !found {
            if let Some(else_expr) = else_result {
                let else_vec = evaluate_vectorized(else_expr, chunk)?;
                values.push(else_vec.get_value(i));
            } else {
                values.push(Value::Null);
            }
        }
    }

    let return_type = results
        .first()
        .map(|r| infer_expression_type(r))
        .unwrap_or(LogicalType::Null);

    Ok(Vector::from_values(&values, return_type))
}

/// Cast a vector to a target type
fn cast_vector(vec: &Vector, target_type: &LogicalType, count: usize) -> Result<Vector> {
    let mut values = Vec::with_capacity(count);

    for i in 0..count {
        let v = vec.get_value(i);
        let casted = cast_value(v, target_type);
        values.push(casted);
    }

    Ok(Vector::from_values(&values, target_type.clone()))
}

/// Cast a single value
fn cast_value(value: Value, target_type: &LogicalType) -> Value {
    if value.is_null() {
        return Value::Null;
    }

    match target_type {
        LogicalType::Integer => match value {
            Value::Integer(i) => Value::Integer(i),
            Value::BigInt(i) => Value::Integer(i as i32),
            Value::Float(f) => Value::Integer(f as i32),
            Value::Double(f) => Value::Integer(f as i32),
            Value::Varchar(s) => s.parse().map(Value::Integer).unwrap_or(Value::Null),
            Value::Boolean(b) => Value::Integer(if b { 1 } else { 0 }),
            _ => Value::Null,
        },
        LogicalType::BigInt => match value {
            Value::Integer(i) => Value::BigInt(i as i64),
            Value::BigInt(i) => Value::BigInt(i),
            Value::Float(f) => Value::BigInt(f as i64),
            Value::Double(f) => Value::BigInt(f as i64),
            Value::Varchar(s) => s.parse().map(Value::BigInt).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        LogicalType::Double => match value {
            Value::Integer(i) => Value::Double(i as f64),
            Value::BigInt(i) => Value::Double(i as f64),
            Value::Float(f) => Value::Double(f as f64),
            Value::Double(f) => Value::Double(f),
            Value::Varchar(s) => s.parse().map(Value::Double).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        LogicalType::Varchar => Value::Varchar(value.to_string()),
        LogicalType::Boolean => match value {
            Value::Boolean(b) => Value::Boolean(b),
            Value::Integer(i) => Value::Boolean(i != 0),
            Value::Varchar(s) => {
                let lower = s.to_lowercase();
                Value::Boolean(lower == "true" || lower == "1" || lower == "yes")
            }
            _ => Value::Null,
        },
        _ => value,
    }
}

// Helper functions

fn numeric_op<F, G>(l: &Value, r: &Value, int_op: F, float_op: G) -> Value
where
    F: Fn(i64, i64) -> i64,
    G: Fn(f64, f64) -> f64,
{
    match (l, r) {
        (Value::Integer(a), Value::Integer(b)) => Value::Integer(int_op(*a as i64, *b as i64) as i32),
        (Value::BigInt(a), Value::BigInt(b)) => Value::BigInt(int_op(*a, *b)),
        (Value::Integer(a), Value::BigInt(b)) | (Value::BigInt(b), Value::Integer(a)) => {
            Value::BigInt(int_op(*a as i64, *b))
        }
        (Value::Float(a), Value::Float(b)) => Value::Float(float_op(*a as f64, *b as f64) as f32),
        (Value::Double(a), Value::Double(b)) => Value::Double(float_op(*a, *b)),
        (Value::Integer(a), Value::Double(b)) | (Value::Double(b), Value::Integer(a)) => {
            Value::Double(float_op(*a as f64, *b))
        }
        (Value::BigInt(a), Value::Double(b)) | (Value::Double(b), Value::BigInt(a)) => {
            Value::Double(float_op(*a as f64, *b))
        }
        _ => Value::Null,
    }
}

fn compare_values<F>(l: &Value, r: &Value, f: F) -> Value
where
    F: Fn(std::cmp::Ordering) -> bool,
{
    if l.is_null() || r.is_null() {
        return Value::Null;
    }
    match l.partial_cmp(r) {
        Some(ord) => Value::Boolean(f(ord)),
        None => Value::Null,
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Varchar(s) => s.clone(),
        _ => v.to_string(),
    }
}

fn infer_binary_type(op: &ironduck_planner::BinaryOperator, left: &LogicalType, right: &LogicalType) -> LogicalType {
    use ironduck_planner::BinaryOperator::*;
    match op {
        Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual | And | Or | Like | ILike => {
            LogicalType::Boolean
        }
        Concat => LogicalType::Varchar,
        _ => {
            // Numeric operations - return wider type
            if left.is_floating_point() || right.is_floating_point() {
                LogicalType::Double
            } else if matches!(left, LogicalType::BigInt) || matches!(right, LogicalType::BigInt) {
                LogicalType::BigInt
            } else {
                LogicalType::Integer
            }
        }
    }
}

fn infer_function_type(name: &str, args: &[Vector]) -> LogicalType {
    match name.to_uppercase().as_str() {
        "LOWER" | "UPPER" | "TRIM" | "LTRIM" | "RTRIM" | "CONCAT" | "SUBSTRING" | "SUBSTR" => {
            LogicalType::Varchar
        }
        "LENGTH" | "CHAR_LENGTH" => LogicalType::BigInt,
        "ABS" => args.first().map(|v| v.logical_type.clone()).unwrap_or(LogicalType::Double),
        "COALESCE" | "NULLIF" => args.first().map(|v| v.logical_type.clone()).unwrap_or(LogicalType::Null),
        _ => LogicalType::Null,
    }
}

fn infer_expression_type(expr: &Expression) -> LogicalType {
    match expr {
        Expression::Constant(v) => v.logical_type(),
        _ => LogicalType::Unknown,
    }
}

/// SQL LIKE pattern matching
fn like_match(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    let text = if case_insensitive { text.to_lowercase() } else { text.to_string() };
    let pattern = if case_insensitive { pattern.to_lowercase() } else { pattern.to_string() };

    // Convert SQL LIKE pattern to regex
    let regex_pattern = pattern
        .replace("\\%", "\x00") // Escape sequences
        .replace("\\_", "\x01")
        .replace("%", ".*")     // % matches any sequence
        .replace("_", ".")      // _ matches single char
        .replace('\x00', "%")   // Restore escaped %
        .replace('\x01', "_");  // Restore escaped _

    regex::Regex::new(&format!("^{}$", regex_pattern))
        .map(|re: regex::Regex| re.is_match(&text))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vectorized_constant() {
        let chunk = DataChunk::empty();
        let expr = Expression::Constant(Value::Integer(42));
        let result = evaluate_vectorized(&expr, &chunk).unwrap();
        assert!(result.is_constant());
        assert_eq!(result.get_constant(), Some(&Value::Integer(42)));
    }

    #[test]
    fn test_vectorized_add() {
        let types = vec![LogicalType::Integer, LogicalType::Integer];
        let rows = vec![
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(10), Value::Integer(20)],
        ];
        let chunk = DataChunk::from_rows(&rows, &types);

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table_index: 0,
                column_index: 0,
                name: "a".to_string(),
            }),
            op: ironduck_planner::BinaryOperator::Add,
            right: Box::new(Expression::ColumnRef {
                table_index: 0,
                column_index: 1,
                name: "b".to_string(),
            }),
        };

        let result = evaluate_vectorized(&expr, &chunk).unwrap();
        assert_eq!(result.get_value(0), Value::Integer(3));
        assert_eq!(result.get_value(1), Value::Integer(30));
    }

    #[test]
    fn test_vectorized_function() {
        let types = vec![LogicalType::Varchar];
        let rows = vec![
            vec![Value::Varchar("Hello".to_string())],
            vec![Value::Varchar("World".to_string())],
        ];
        let chunk = DataChunk::from_rows(&rows, &types);

        let expr = Expression::Function {
            name: "UPPER".to_string(),
            args: vec![Expression::ColumnRef {
                table_index: 0,
                column_index: 0,
                name: "s".to_string(),
            }],
        };

        let result = evaluate_vectorized(&expr, &chunk).unwrap();
        assert_eq!(result.get_value(0), Value::Varchar("HELLO".to_string()));
        assert_eq!(result.get_value(1), Value::Varchar("WORLD".to_string()));
    }

    #[test]
    fn test_like_match() {
        assert!(like_match("hello", "hello", false));
        assert!(like_match("hello", "h%", false));
        assert!(like_match("hello", "%llo", false));
        assert!(like_match("hello", "h_llo", false));
        assert!(like_match("hello", "%e%", false));
        assert!(!like_match("hello", "world", false));
        assert!(like_match("HELLO", "hello", true));  // case insensitive
    }
}
