//! Expression evaluation

use ironduck_common::{Error, LogicalType, Result, Value};
use ironduck_planner::{BinaryOperator, Expression, UnaryOperator};
use std::cmp::Ordering;

/// Evaluate an expression against a row of values
pub fn evaluate(expr: &Expression, row: &[Value]) -> Result<Value> {
    match expr {
        Expression::Constant(value) => Ok(value.clone()),

        Expression::ColumnRef { column_index, .. } => {
            row.get(*column_index)
                .cloned()
                .ok_or_else(|| Error::Internal(format!("Column index {} out of bounds", column_index)))
        }

        Expression::BinaryOp { left, op, right } => {
            let left_val = evaluate(left, row)?;
            let right_val = evaluate(right, row)?;
            evaluate_binary_op(&left_val, *op, &right_val)
        }

        Expression::UnaryOp { op, expr } => {
            let val = evaluate(expr, row)?;
            evaluate_unary_op(*op, &val)
        }

        Expression::Function { name, args } => {
            let arg_values: Result<Vec<_>> = args.iter().map(|a| evaluate(a, row)).collect();
            evaluate_function(name, &arg_values?)
        }

        Expression::Cast { expr, target_type } => {
            let val = evaluate(expr, row)?;
            cast_value(&val, target_type)
        }

        Expression::IsNull(expr) => {
            let val = evaluate(expr, row)?;
            Ok(Value::Boolean(val.is_null()))
        }

        Expression::IsNotNull(expr) => {
            let val = evaluate(expr, row)?;
            Ok(Value::Boolean(!val.is_null()))
        }

        Expression::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let operand_val = operand.as_ref().map(|e| evaluate(e, row)).transpose()?;

            for (cond, result) in conditions.iter().zip(results.iter()) {
                let cond_val = evaluate(cond, row)?;

                let matches = match &operand_val {
                    Some(op_val) => {
                        // CASE operand WHEN cond THEN result
                        match evaluate_binary_op(op_val, BinaryOperator::Equal, &cond_val)? {
                            Value::Boolean(true) => true,
                            _ => false,
                        }
                    }
                    None => {
                        // CASE WHEN cond THEN result
                        matches!(cond_val, Value::Boolean(true))
                    }
                };

                if matches {
                    return evaluate(result, row);
                }
            }

            // No condition matched, return ELSE or NULL
            match else_result {
                Some(else_expr) => evaluate(else_expr, row),
                None => Ok(Value::Null),
            }
        }

        Expression::InList { expr, list, negated } => {
            let val = evaluate(expr, row)?;
            if val.is_null() {
                return Ok(Value::Null);
            }

            let mut found = false;
            for item in list {
                let item_val = evaluate(item, row)?;
                if !item_val.is_null() {
                    if let Value::Boolean(true) = evaluate_binary_op(&val, BinaryOperator::Equal, &item_val)? {
                        found = true;
                        break;
                    }
                }
            }

            let result = if *negated { !found } else { found };
            Ok(Value::Boolean(result))
        }

        Expression::InSubquery { .. } | Expression::Exists { .. } | Expression::Subquery(_) => {
            // Subqueries need executor access - handled separately in executor
            Err(Error::NotImplemented("Subquery in simple expression evaluation".to_string()))
        }
    }
}

/// Evaluate a binary operation
fn evaluate_binary_op(left: &Value, op: BinaryOperator, right: &Value) -> Result<Value> {
    // Handle NULL propagation for most operators
    if left.is_null() || right.is_null() {
        match op {
            // AND/OR have special NULL handling
            BinaryOperator::And => {
                if matches!(left, Value::Boolean(false)) || matches!(right, Value::Boolean(false)) {
                    return Ok(Value::Boolean(false));
                }
                return Ok(Value::Null);
            }
            BinaryOperator::Or => {
                if matches!(left, Value::Boolean(true)) || matches!(right, Value::Boolean(true)) {
                    return Ok(Value::Boolean(true));
                }
                return Ok(Value::Null);
            }
            _ => return Ok(Value::Null),
        }
    }

    match op {
        // Arithmetic operations
        BinaryOperator::Add => arithmetic_op(left, right, |a, b| a + b, |a, b| a + b),
        BinaryOperator::Subtract => arithmetic_op(left, right, |a, b| a - b, |a, b| a - b),
        BinaryOperator::Multiply => arithmetic_op(left, right, |a, b| a * b, |a, b| a * b),
        BinaryOperator::Divide => {
            // Division by zero returns NULL in SQL (DuckDB behavior)
            match right {
                Value::Integer(0) | Value::BigInt(0) => return Ok(Value::Null),
                Value::Double(f) if *f == 0.0 => return Ok(Value::Null),
                Value::Float(f) if *f == 0.0 => return Ok(Value::Null),
                _ => {}
            }
            arithmetic_op(left, right, |a, b| a / b, |a, b| a / b)
        }
        BinaryOperator::Modulo => {
            // Modulo by zero returns NULL in SQL (DuckDB behavior)
            match right {
                Value::Integer(0) | Value::BigInt(0) => return Ok(Value::Null),
                Value::Double(f) if *f == 0.0 => return Ok(Value::Null),
                Value::Float(f) if *f == 0.0 => return Ok(Value::Null),
                _ => {}
            }
            arithmetic_op(left, right, |a, b| a % b, |a, b| a % b)
        }

        // Comparison operations
        BinaryOperator::Equal => comparison_op(left, right, |o| o == std::cmp::Ordering::Equal),
        BinaryOperator::NotEqual => comparison_op(left, right, |o| o != std::cmp::Ordering::Equal),
        BinaryOperator::LessThan => comparison_op(left, right, |o| o == std::cmp::Ordering::Less),
        BinaryOperator::LessThanOrEqual => {
            comparison_op(left, right, |o| o != std::cmp::Ordering::Greater)
        }
        BinaryOperator::GreaterThan => {
            comparison_op(left, right, |o| o == std::cmp::Ordering::Greater)
        }
        BinaryOperator::GreaterThanOrEqual => {
            comparison_op(left, right, |o| o != std::cmp::Ordering::Less)
        }

        // Logical operations
        BinaryOperator::And => {
            let l = left.as_bool().ok_or_else(|| Error::TypeMismatch {
                expected: "BOOLEAN".to_string(),
                got: format!("{:?}", left),
            })?;
            let r = right.as_bool().ok_or_else(|| Error::TypeMismatch {
                expected: "BOOLEAN".to_string(),
                got: format!("{:?}", right),
            })?;
            Ok(Value::Boolean(l && r))
        }
        BinaryOperator::Or => {
            let l = left.as_bool().ok_or_else(|| Error::TypeMismatch {
                expected: "BOOLEAN".to_string(),
                got: format!("{:?}", left),
            })?;
            let r = right.as_bool().ok_or_else(|| Error::TypeMismatch {
                expected: "BOOLEAN".to_string(),
                got: format!("{:?}", right),
            })?;
            Ok(Value::Boolean(l || r))
        }

        // String operations
        BinaryOperator::Concat => {
            let l = value_to_string(left);
            let r = value_to_string(right);
            Ok(Value::Varchar(format!("{}{}", l, r)))
        }
        BinaryOperator::Like => {
            let text = left.as_str().unwrap_or("");
            let pattern = right.as_str().unwrap_or("");
            Ok(Value::Boolean(like_match(text, pattern)))
        }
        BinaryOperator::ILike => {
            let text = left.as_str().unwrap_or("").to_lowercase();
            let pattern = right.as_str().unwrap_or("").to_lowercase();
            Ok(Value::Boolean(like_match(&text, &pattern)))
        }

        // Bitwise operations
        BinaryOperator::BitwiseAnd => {
            let l = left.as_i64().ok_or_else(|| Error::TypeMismatch {
                expected: "integer".to_string(),
                got: format!("{:?}", left),
            })?;
            let r = right.as_i64().ok_or_else(|| Error::TypeMismatch {
                expected: "integer".to_string(),
                got: format!("{:?}", right),
            })?;
            Ok(Value::BigInt(l & r))
        }
        BinaryOperator::BitwiseOr => {
            let l = left.as_i64().ok_or_else(|| Error::TypeMismatch {
                expected: "integer".to_string(),
                got: format!("{:?}", left),
            })?;
            let r = right.as_i64().ok_or_else(|| Error::TypeMismatch {
                expected: "integer".to_string(),
                got: format!("{:?}", right),
            })?;
            Ok(Value::BigInt(l | r))
        }
        BinaryOperator::BitwiseXor => {
            let l = left.as_i64().ok_or_else(|| Error::TypeMismatch {
                expected: "integer".to_string(),
                got: format!("{:?}", left),
            })?;
            let r = right.as_i64().ok_or_else(|| Error::TypeMismatch {
                expected: "integer".to_string(),
                got: format!("{:?}", right),
            })?;
            Ok(Value::BigInt(l ^ r))
        }
        BinaryOperator::ShiftLeft => {
            let l = left.as_i64().ok_or_else(|| Error::TypeMismatch {
                expected: "integer".to_string(),
                got: format!("{:?}", left),
            })?;
            let r = right.as_i64().ok_or_else(|| Error::TypeMismatch {
                expected: "integer".to_string(),
                got: format!("{:?}", right),
            })?;
            Ok(Value::BigInt(l << r))
        }
        BinaryOperator::ShiftRight => {
            let l = left.as_i64().ok_or_else(|| Error::TypeMismatch {
                expected: "integer".to_string(),
                got: format!("{:?}", left),
            })?;
            let r = right.as_i64().ok_or_else(|| Error::TypeMismatch {
                expected: "integer".to_string(),
                got: format!("{:?}", right),
            })?;
            Ok(Value::BigInt(l >> r))
        }
    }
}

/// Evaluate a unary operation
fn evaluate_unary_op(op: UnaryOperator, val: &Value) -> Result<Value> {
    if val.is_null() {
        return Ok(Value::Null);
    }

    match op {
        UnaryOperator::Negate => match val {
            Value::Integer(i) => Ok(Value::Integer(-i)),
            Value::BigInt(i) => Ok(Value::BigInt(-i)),
            Value::Float(f) => Ok(Value::Float(-f)),
            Value::Double(f) => Ok(Value::Double(-f)),
            _ => Err(Error::TypeMismatch {
                expected: "numeric".to_string(),
                got: format!("{:?}", val),
            }),
        },
        UnaryOperator::Not => match val {
            Value::Boolean(b) => Ok(Value::Boolean(!b)),
            _ => Err(Error::TypeMismatch {
                expected: "BOOLEAN".to_string(),
                got: format!("{:?}", val),
            }),
        },
        UnaryOperator::BitwiseNot => match val {
            Value::Integer(i) => Ok(Value::Integer(!i)),
            Value::BigInt(i) => Ok(Value::BigInt(!i)),
            _ => Err(Error::TypeMismatch {
                expected: "integer".to_string(),
                got: format!("{:?}", val),
            }),
        },
    }
}

/// Evaluate a function call
fn evaluate_function(name: &str, args: &[Value]) -> Result<Value> {
    match name.to_uppercase().as_str() {
        // String functions
        "LOWER" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Varchar(s.to_lowercase()))
        }
        "UPPER" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Varchar(s.to_uppercase()))
        }
        "LENGTH" | "CHAR_LENGTH" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::BigInt(s.chars().count() as i64))
        }
        "TRIM" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Varchar(s.trim().to_string()))
        }
        "LTRIM" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Varchar(s.trim_start().to_string()))
        }
        "RTRIM" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Varchar(s.trim_end().to_string()))
        }
        "CONCAT" => {
            let result: String = args.iter().map(value_to_string).collect();
            Ok(Value::Varchar(result))
        }
        "CONCAT_WS" => {
            // First arg is the separator, rest are values to join
            if args.is_empty() {
                return Ok(Value::Null);
            }
            let separator = value_to_string(&args[0]);
            // Filter out NULL values and join with separator
            let parts: Vec<String> = args[1..]
                .iter()
                .filter(|v| !v.is_null())
                .map(value_to_string)
                .collect();
            Ok(Value::Varchar(parts.join(&separator)))
        }
        "SUBSTRING" | "SUBSTR" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let start = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1) as usize;
            let len = args.get(2).and_then(|v| v.as_i64());

            let start_idx = start.saturating_sub(1); // SQL is 1-indexed
            let chars: Vec<char> = s.chars().collect();

            let result: String = match len {
                Some(l) => chars.iter().skip(start_idx).take(l as usize).collect(),
                None => chars.iter().skip(start_idx).collect(),
            };
            Ok(Value::Varchar(result))
        }
        "REPLACE" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let from = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let to = args.get(2).and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Varchar(s.replace(from, to)))
        }
        "LEFT" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let n = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            Ok(Value::Varchar(s.chars().take(n).collect()))
        }
        "RIGHT" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let n = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            Ok(Value::Varchar(chars.into_iter().skip(start).collect()))
        }
        "ASCII" => {
            // Return the ASCII code of the first character
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            if s.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(Value::Integer(s.chars().next().unwrap() as i32))
            }
        }
        "CHAR" | "CHR" => {
            // Return the character for an ASCII code
            match args.first() {
                Some(Value::Null) => Ok(Value::Null),
                Some(v) => {
                    let code = v.as_i64().unwrap_or(0) as u32;
                    match char::from_u32(code) {
                        Some(c) => Ok(Value::Varchar(c.to_string())),
                        None => Ok(Value::Null),
                    }
                }
                None => Ok(Value::Null),
            }
        }
        "REVERSE" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Varchar(s.chars().rev().collect()))
        }
        "TRANSLATE" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let from_chars = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let to_chars = args.get(2).and_then(|v| v.as_str()).unwrap_or("");
            let from: Vec<char> = from_chars.chars().collect();
            let to: Vec<char> = to_chars.chars().collect();
            let result: String = s.chars().filter_map(|c| {
                if let Some(pos) = from.iter().position(|&fc| fc == c) {
                    to.get(pos).copied()
                } else {
                    Some(c)
                }
            }).collect();
            Ok(Value::Varchar(result))
        }
        "REPEAT" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let n = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            Ok(Value::Varchar(s.repeat(n)))
        }
        "LPAD" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let len = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            let pad = args.get(2).and_then(|v| v.as_str()).unwrap_or(" ");
            if s.len() >= len {
                Ok(Value::Varchar(s.to_string()))
            } else {
                let pad_len = len - s.len();
                let padding: String = pad.chars().cycle().take(pad_len).collect();
                Ok(Value::Varchar(format!("{}{}", padding, s)))
            }
        }
        "RPAD" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let len = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            let pad = args.get(2).and_then(|v| v.as_str()).unwrap_or(" ");
            if s.len() >= len {
                Ok(Value::Varchar(s.to_string()))
            } else {
                let pad_len = len - s.len();
                let padding: String = pad.chars().cycle().take(pad_len).collect();
                Ok(Value::Varchar(format!("{}{}", s, padding)))
            }
        }
        "INSTR" | "POSITION" | "STRPOS" => {
            let haystack = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let needle = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            match haystack.find(needle) {
                Some(pos) => Ok(Value::BigInt((pos + 1) as i64)), // 1-indexed
                None => Ok(Value::BigInt(0)),
            }
        }
        "SPLIT_PART" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let delimiter = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let part = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1) as usize;
            let parts: Vec<&str> = s.split(delimiter).collect();
            Ok(Value::Varchar(parts.get(part.saturating_sub(1)).unwrap_or(&"").to_string()))
        }
        "INITCAP" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let result: String = s.split_whitespace()
                .map(|word| {
                    let mut chars: Vec<char> = word.chars().collect();
                    if !chars.is_empty() {
                        chars[0] = chars[0].to_uppercase().next().unwrap_or(chars[0]);
                        for c in &mut chars[1..] {
                            *c = c.to_lowercase().next().unwrap_or(*c);
                        }
                    }
                    chars.into_iter().collect::<String>()
                })
                .collect::<Vec<_>>()
                .join(" ");
            Ok(Value::Varchar(result))
        }
        "STARTS_WITH" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let prefix = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Boolean(s.starts_with(prefix)))
        }
        "ENDS_WITH" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let suffix = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Boolean(s.ends_with(suffix)))
        }
        "CONTAINS" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let needle = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Boolean(s.contains(needle)))
        }

        // Regular expression functions
        "REGEXP_MATCHES" | "REGEXP_LIKE" | "REGEXP" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let pattern = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            match regex::Regex::new(pattern) {
                Ok(re) => Ok(Value::Boolean(re.is_match(s))),
                Err(_) => Ok(Value::Boolean(false)),
            }
        }
        "REGEXP_REPLACE" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let pattern = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let replacement = args.get(2).and_then(|v| v.as_str()).unwrap_or("");
            match regex::Regex::new(pattern) {
                Ok(re) => Ok(Value::Varchar(re.replace_all(s, replacement).to_string())),
                Err(_) => Ok(Value::Varchar(s.to_string())),
            }
        }
        "REGEXP_EXTRACT" | "REGEXP_SUBSTR" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let pattern = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let group_idx = args.get(2).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            match regex::Regex::new(pattern) {
                Ok(re) => {
                    if let Some(caps) = re.captures(s) {
                        if let Some(m) = caps.get(group_idx) {
                            return Ok(Value::Varchar(m.as_str().to_string()));
                        }
                    }
                    Ok(Value::Null)
                }
                Err(_) => Ok(Value::Null),
            }
        }
        "REGEXP_SPLIT_TO_ARRAY" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let pattern = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            match regex::Regex::new(pattern) {
                Ok(re) => {
                    let parts: Vec<Value> = re.split(s)
                        .map(|p| Value::Varchar(p.to_string()))
                        .collect();
                    Ok(Value::List(parts))
                }
                Err(_) => Ok(Value::List(vec![Value::Varchar(s.to_string())])),
            }
        }
        "REGEXP_COUNT" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let pattern = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            match regex::Regex::new(pattern) {
                Ok(re) => Ok(Value::BigInt(re.find_iter(s).count() as i64)),
                Err(_) => Ok(Value::BigInt(0)),
            }
        }

        // Array/List functions
        "LIST_VALUE" | "ARRAY" | "LIST" => {
            // Create a list from the arguments
            Ok(Value::List(args.to_vec()))
        }
        "LIST_EXTRACT" | "ARRAY_EXTRACT" | "LIST_ELEMENT" => {
            // Get element at index (1-indexed)
            match args.first() {
                Some(Value::List(list)) => {
                    let idx = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1) as usize;
                    if idx == 0 || idx > list.len() {
                        Ok(Value::Null)
                    } else {
                        Ok(list[idx - 1].clone())
                    }
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_LENGTH" | "ARRAY_LENGTH" | "LEN" => {
            match args.first() {
                Some(Value::List(list)) => Ok(Value::BigInt(list.len() as i64)),
                Some(Value::Varchar(s)) => Ok(Value::BigInt(s.chars().count() as i64)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_CONCAT" | "ARRAY_CONCAT" => {
            // Concatenate two lists
            let list1 = match args.first() {
                Some(Value::List(l)) => l.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => vec![],
            };
            let list2 = match args.get(1) {
                Some(Value::List(l)) => l.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => vec![],
            };
            let mut result = list1;
            result.extend(list2);
            Ok(Value::List(result))
        }
        "LIST_CONTAINS" | "ARRAY_CONTAINS" | "ARRAY_HAS" => {
            // Check if list contains element
            match args.first() {
                Some(Value::List(list)) => {
                    let element = args.get(1).unwrap_or(&Value::Null);
                    Ok(Value::Boolean(list.contains(element)))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Boolean(false)),
            }
        }
        "LIST_POSITION" | "ARRAY_POSITION" | "ARRAY_INDEXOF" => {
            // Find position of element in list (1-indexed, 0 if not found)
            match args.first() {
                Some(Value::List(list)) => {
                    let element = args.get(1).unwrap_or(&Value::Null);
                    match list.iter().position(|v| v == element) {
                        Some(pos) => Ok(Value::BigInt((pos + 1) as i64)),
                        None => Ok(Value::BigInt(0)),
                    }
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::BigInt(0)),
            }
        }
        "LIST_APPEND" | "ARRAY_APPEND" | "ARRAY_PUSH_BACK" => {
            // Append element to end of list
            match args.first() {
                Some(Value::List(list)) => {
                    let element = args.get(1).cloned().unwrap_or(Value::Null);
                    let mut result = list.clone();
                    result.push(element);
                    Ok(Value::List(result))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_PREPEND" | "ARRAY_PREPEND" | "ARRAY_PUSH_FRONT" => {
            // Prepend element to start of list
            let element = args.first().cloned().unwrap_or(Value::Null);
            match args.get(1) {
                Some(Value::List(list)) => {
                    let mut result = vec![element];
                    result.extend(list.clone());
                    Ok(Value::List(result))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_REVERSE" | "ARRAY_REVERSE" => {
            match args.first() {
                Some(Value::List(list)) => {
                    let mut result = list.clone();
                    result.reverse();
                    Ok(Value::List(result))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_SLICE" | "ARRAY_SLICE" => {
            // Slice list from start to end (1-indexed, end exclusive)
            match args.first() {
                Some(Value::List(list)) => {
                    let start = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1) as usize;
                    let end = args.get(2).and_then(|v| v.as_i64()).map(|e| e as usize);

                    let start_idx = start.saturating_sub(1);
                    let end_idx = end.unwrap_or(list.len()).min(list.len());

                    if start_idx >= list.len() {
                        Ok(Value::List(vec![]))
                    } else {
                        Ok(Value::List(list[start_idx..end_idx].to_vec()))
                    }
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::List(vec![])),
            }
        }
        "ARRAY_REMOVE" | "LIST_REMOVE" => {
            // Remove all occurrences of element from array
            match args.first() {
                Some(Value::List(list)) => {
                    let remove = args.get(1).unwrap_or(&Value::Null);
                    let result: Vec<Value> = list.iter()
                        .filter(|v| *v != remove)
                        .cloned()
                        .collect();
                    Ok(Value::List(result))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::List(vec![])),
            }
        }
        "LIST_DISTINCT" | "ARRAY_DISTINCT" | "LIST_UNIQUE" => {
            // Get unique elements (preserving order)
            match args.first() {
                Some(Value::List(list)) => {
                    let mut seen = Vec::new();
                    for item in list {
                        if !seen.contains(item) {
                            seen.push(item.clone());
                        }
                    }
                    Ok(Value::List(seen))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::List(vec![])),
            }
        }
        "LIST_SORT" | "ARRAY_SORT" => {
            match args.first() {
                Some(Value::List(list)) => {
                    let mut result = list.clone();
                    result.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                    Ok(Value::List(result))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::List(vec![])),
            }
        }
        "LIST_SUM" | "ARRAY_SUM" => {
            match args.first() {
                Some(Value::List(list)) => {
                    let sum: f64 = list.iter()
                        .filter_map(|v| v.as_f64())
                        .sum();
                    Ok(Value::Double(sum))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_AVG" | "ARRAY_AVG" => {
            match args.first() {
                Some(Value::List(list)) => {
                    let values: Vec<f64> = list.iter()
                        .filter_map(|v| v.as_f64())
                        .collect();
                    if values.is_empty() {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Double(values.iter().sum::<f64>() / values.len() as f64))
                    }
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_MIN" | "ARRAY_MIN" => {
            match args.first() {
                Some(Value::List(list)) => {
                    Ok(list.iter()
                        .filter(|v| !v.is_null())
                        .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                        .cloned()
                        .unwrap_or(Value::Null))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_MAX" | "ARRAY_MAX" => {
            match args.first() {
                Some(Value::List(list)) => {
                    Ok(list.iter()
                        .filter(|v| !v.is_null())
                        .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                        .cloned()
                        .unwrap_or(Value::Null))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "FLATTEN" => {
            // Flatten nested lists one level
            match args.first() {
                Some(Value::List(list)) => {
                    let mut result = Vec::new();
                    for item in list {
                        match item {
                            Value::List(inner) => result.extend(inner.clone()),
                            other => result.push(other.clone()),
                        }
                    }
                    Ok(Value::List(result))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "GENERATE_SERIES" | "RANGE" => {
            // Generate a series of integers
            let start = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let end = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
            let step = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1);

            if step == 0 {
                return Err(Error::Internal("Step cannot be zero".to_string()));
            }

            let mut result = Vec::new();
            if step > 0 {
                let mut i = start;
                while i <= end {
                    result.push(Value::BigInt(i));
                    i += step;
                }
            } else {
                let mut i = start;
                while i >= end {
                    result.push(Value::BigInt(i));
                    i += step;
                }
            }
            Ok(Value::List(result))
        }
        "STRING_TO_ARRAY" | "SPLIT" => {
            // Split string into array by delimiter
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let delimiter = args.get(1).and_then(|v| v.as_str()).unwrap_or(",");
            let parts: Vec<Value> = s.split(delimiter)
                .map(|p| Value::Varchar(p.to_string()))
                .collect();
            Ok(Value::List(parts))
        }
        "ARRAY_TO_STRING" | "LIST_TO_STRING" | "ARRAY_JOIN" => {
            // Join array elements into string with delimiter
            match args.first() {
                Some(Value::List(list)) => {
                    let delimiter = args.get(1).and_then(|v| v.as_str()).unwrap_or(",");
                    let parts: Vec<String> = list.iter()
                        .filter(|v| !v.is_null())
                        .map(|v| value_to_string(v))
                        .collect();
                    Ok(Value::Varchar(parts.join(delimiter)))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_FILTER" => {
            // Filter out NULL values from list
            match args.first() {
                Some(Value::List(list)) => {
                    let result: Vec<Value> = list.iter()
                        .filter(|v| !v.is_null())
                        .cloned()
                        .collect();
                    Ok(Value::List(result))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }

        // Numeric functions
        "ABS" => {
            let val = args.first().unwrap_or(&Value::Null);
            match val {
                Value::Integer(i) => Ok(Value::Integer(i.abs())),
                Value::BigInt(i) => Ok(Value::BigInt(i.abs())),
                Value::Float(f) => Ok(Value::Float(f.abs())),
                Value::Double(f) => Ok(Value::Double(f.abs())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    got: format!("{:?}", val),
                }),
            }
        }
        "CEIL" | "CEILING" => {
            let val = args.first().unwrap_or(&Value::Null);
            match val {
                Value::Float(f) => Ok(Value::Float(f.ceil())),
                Value::Double(f) => Ok(Value::Double(f.ceil())),
                Value::Integer(_) | Value::BigInt(_) => Ok(val.clone()),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    got: format!("{:?}", val),
                }),
            }
        }
        "FLOOR" => {
            let val = args.first().unwrap_or(&Value::Null);
            match val {
                Value::Float(f) => Ok(Value::Float(f.floor())),
                Value::Double(f) => Ok(Value::Double(f.floor())),
                Value::Integer(_) | Value::BigInt(_) => Ok(val.clone()),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    got: format!("{:?}", val),
                }),
            }
        }
        "ROUND" => {
            let val = args.first().unwrap_or(&Value::Null);
            let decimals = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
            let factor = 10f64.powi(decimals as i32);

            match val {
                Value::Float(f) => Ok(Value::Float((f * factor as f32).round() / factor as f32)),
                Value::Double(f) => Ok(Value::Double((f * factor).round() / factor)),
                Value::Integer(_) | Value::BigInt(_) => Ok(val.clone()),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    got: format!("{:?}", val),
                }),
            }
        }
        "TRUNC" | "TRUNCATE" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let decimals = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
            if decimals == 0 {
                Ok(Value::Double(val.trunc()))
            } else {
                let factor = 10f64.powi(decimals as i32);
                Ok(Value::Double((val * factor).trunc() / factor))
            }
        }
        "POWER" | "POW" => {
            let base = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let exp = args.get(1).and_then(|v| v.as_f64()).unwrap_or(1.0);
            Ok(Value::Double(base.powf(exp)))
        }
        "SQRT" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            if val < 0.0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Double(val.sqrt()))
            }
        }
        "CBRT" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(val.cbrt()))
        }
        "LOG" | "LN" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(1.0);
            if val <= 0.0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Double(val.ln()))
            }
        }
        "LOG10" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(1.0);
            if val <= 0.0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Double(val.log10()))
            }
        }
        "LOG2" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(1.0);
            if val <= 0.0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Double(val.log2()))
            }
        }
        "EXP" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(val.exp()))
        }
        "SIGN" => {
            let val = args.first().unwrap_or(&Value::Null);
            match val {
                Value::Integer(i) => Ok(Value::Integer(i.signum())),
                Value::BigInt(i) => Ok(Value::BigInt(i.signum())),
                Value::Float(f) if *f > 0.0 => Ok(Value::Integer(1)),
                Value::Float(f) if *f < 0.0 => Ok(Value::Integer(-1)),
                Value::Float(_) => Ok(Value::Integer(0)),
                Value::Double(f) if *f > 0.0 => Ok(Value::Integer(1)),
                Value::Double(f) if *f < 0.0 => Ok(Value::Integer(-1)),
                Value::Double(_) => Ok(Value::Integer(0)),
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "MOD" => {
            let a = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let b = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1);
            if b == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::BigInt(a % b))
            }
        }
        "PI" => Ok(Value::Double(std::f64::consts::PI)),
        "RANDOM" | "RAND" => Ok(Value::Double(rand::random())),

        // Trigonometric functions
        "SIN" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(val.sin()))
        }
        "COS" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(val.cos()))
        }
        "TAN" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(val.tan()))
        }
        "ASIN" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            if val < -1.0 || val > 1.0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Double(val.asin()))
            }
        }
        "ACOS" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            if val < -1.0 || val > 1.0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Double(val.acos()))
            }
        }
        "ATAN" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(val.atan()))
        }
        "ATAN2" => {
            let y = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let x = args.get(1).and_then(|v| v.as_f64()).unwrap_or(1.0);
            Ok(Value::Double(y.atan2(x)))
        }
        "SINH" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(val.sinh()))
        }
        "COSH" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(val.cosh()))
        }
        "TANH" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(val.tanh()))
        }
        "DEGREES" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(val.to_degrees()))
        }
        "RADIANS" => {
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(val.to_radians()))
        }
        "GCD" => {
            let a = args.first().and_then(|v| v.as_i64()).unwrap_or(0).abs();
            let b = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0).abs();
            fn gcd(mut a: i64, mut b: i64) -> i64 {
                while b != 0 {
                    let t = b;
                    b = a % b;
                    a = t;
                }
                a
            }
            Ok(Value::BigInt(gcd(a, b)))
        }
        "LCM" => {
            let a = args.first().and_then(|v| v.as_i64()).unwrap_or(0).abs();
            let b = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0).abs();
            if a == 0 || b == 0 {
                Ok(Value::BigInt(0))
            } else {
                fn gcd(mut a: i64, mut b: i64) -> i64 {
                    while b != 0 {
                        let t = b;
                        b = a % b;
                        a = t;
                    }
                    a
                }
                Ok(Value::BigInt((a / gcd(a, b)) * b))
            }
        }
        "FACTORIAL" => {
            let n = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            if n < 0 {
                Ok(Value::Null)
            } else if n > 20 {
                // Factorial of 21+ overflows i64
                Ok(Value::Null)
            } else {
                let result: i64 = (1..=n).product();
                Ok(Value::BigInt(result))
            }
        }

        // UUID functions
        "GEN_RANDOM_UUID" | "UUID" => {
            Ok(Value::Uuid(uuid::Uuid::new_v4()))
        }

        // Hash functions
        "HASH" | "MD5" => {
            use std::hash::{Hash, Hasher};
            use std::collections::hash_map::DefaultHasher;
            let s = value_to_string(args.first().unwrap_or(&Value::Null));
            let mut hasher = DefaultHasher::new();
            s.hash(&mut hasher);
            Ok(Value::BigInt(hasher.finish() as i64))
        }

        // Bit manipulation functions
        "BIT_COUNT" => {
            let val = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(Value::Integer(val.count_ones() as i32))
        }
        "BIT_LENGTH" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Integer((s.len() * 8) as i32))
        }
        "OCTET_LENGTH" | "BYTE_LENGTH" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Integer(s.len() as i32))
        }

        // Format function
        "FORMAT" | "PRINTF" => {
            // Simple format - just return the first string with replacements
            let template = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let mut arg_idx = 1;

            // Process format specifiers
            let mut i = 0;
            let chars: Vec<char> = template.chars().collect();
            let mut new_result = String::new();
            while i < chars.len() {
                if chars[i] == '%' && i + 1 < chars.len() {
                    // Check for format specifier
                    let mut j = i + 1;
                    let mut width = 0;
                    let mut zero_pad = false;

                    // Check for zero padding
                    if j < chars.len() && chars[j] == '0' {
                        zero_pad = true;
                        j += 1;
                    }

                    // Parse width
                    while j < chars.len() && chars[j].is_ascii_digit() {
                        width = width * 10 + (chars[j] as usize - '0' as usize);
                        j += 1;
                    }

                    if j < chars.len() {
                        let arg = args.get(arg_idx).cloned().unwrap_or(Value::Null);
                        match chars[j] {
                            's' => {
                                new_result.push_str(&value_to_string(&arg));
                                arg_idx += 1;
                                i = j + 1;
                                continue;
                            }
                            'd' | 'i' => {
                                let num = arg.as_i64().unwrap_or(0);
                                let formatted = if zero_pad && width > 0 {
                                    format!("{:0>width$}", num, width = width)
                                } else if width > 0 {
                                    format!("{:>width$}", num, width = width)
                                } else {
                                    format!("{}", num)
                                };
                                new_result.push_str(&formatted);
                                arg_idx += 1;
                                i = j + 1;
                                continue;
                            }
                            'f' => {
                                let num = arg.as_f64().unwrap_or(0.0);
                                new_result.push_str(&format!("{}", num));
                                arg_idx += 1;
                                i = j + 1;
                                continue;
                            }
                            '%' => {
                                new_result.push('%');
                                i = j + 1;
                                continue;
                            }
                            _ => {}
                        }
                    }
                }
                new_result.push(chars[i]);
                i += 1;
            }
            Ok(Value::Varchar(new_result))
        }

        // String padding with zeros
        "LPAD_ZERO" | "ZFILL" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let width = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            if s.len() >= width {
                Ok(Value::Varchar(s.to_string()))
            } else {
                Ok(Value::Varchar(format!("{:0>width$}", s, width = width)))
            }
        }

        // Encode/Decode
        "HEX" | "TO_HEX" => {
            match args.first() {
                Some(Value::Integer(i)) => Ok(Value::Varchar(format!("{:x}", i))),
                Some(Value::BigInt(i)) => Ok(Value::Varchar(format!("{:x}", i))),
                Some(Value::Varchar(s)) => {
                    let hex: String = s.bytes().map(|b| format!("{:02x}", b)).collect();
                    Ok(Value::Varchar(hex))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "BIN" | "TO_BIN" => {
            match args.first() {
                Some(Value::Integer(i)) => Ok(Value::Varchar(format!("{:b}", i))),
                Some(Value::BigInt(i)) => Ok(Value::Varchar(format!("{:b}", i))),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "OCT" | "TO_OCT" => {
            match args.first() {
                Some(Value::Integer(i)) => Ok(Value::Varchar(format!("{:o}", i))),
                Some(Value::BigInt(i)) => Ok(Value::Varchar(format!("{:o}", i))),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "UNHEX" | "FROM_HEX" => {
            match args.first() {
                Some(Value::Varchar(s)) => {
                    let mut bytes = Vec::new();
                    let mut valid = true;
                    let mut i = 0;
                    while i + 1 < s.len() && valid {
                        match u8::from_str_radix(&s[i..i + 2], 16) {
                            Ok(b) => bytes.push(b),
                            Err(_) => valid = false,
                        }
                        i += 2;
                    }
                    if valid {
                        Ok(Value::Varchar(String::from_utf8_lossy(&bytes).to_string()))
                    } else {
                        Ok(Value::Null)
                    }
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "BASE64" | "TO_BASE64" => {
            // Simple base64 encoding
            const BASE64_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let bytes = s.as_bytes();
            let mut result = String::new();
            let mut i = 0;
            while i < bytes.len() {
                let b0 = bytes[i] as usize;
                let b1 = if i + 1 < bytes.len() { bytes[i + 1] as usize } else { 0 };
                let b2 = if i + 2 < bytes.len() { bytes[i + 2] as usize } else { 0 };

                result.push(BASE64_CHARS[(b0 >> 2) & 0x3F] as char);
                result.push(BASE64_CHARS[((b0 << 4) | (b1 >> 4)) & 0x3F] as char);
                if i + 1 < bytes.len() {
                    result.push(BASE64_CHARS[((b1 << 2) | (b2 >> 6)) & 0x3F] as char);
                } else {
                    result.push('=');
                }
                if i + 2 < bytes.len() {
                    result.push(BASE64_CHARS[b2 & 0x3F] as char);
                } else {
                    result.push('=');
                }
                i += 3;
            }
            Ok(Value::Varchar(result))
        }
        "FROM_BASE64" | "DECODE_BASE64" => {
            const BASE64_DECODE: [i8; 256] = {
                let mut table = [-1i8; 256];
                let mut i = 0u8;
                while i < 26 {
                    table[(b'A' + i) as usize] = i as i8;
                    table[(b'a' + i) as usize] = (i + 26) as i8;
                    i += 1;
                }
                let mut i = 0u8;
                while i < 10 {
                    table[(b'0' + i) as usize] = (i + 52) as i8;
                    i += 1;
                }
                table[b'+' as usize] = 62;
                table[b'/' as usize] = 63;
                table
            };

            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let bytes: Vec<u8> = s.bytes().filter(|&b| b != b'=').collect();
            let mut result = Vec::new();
            let mut i = 0;
            while i + 3 < bytes.len() {
                let b0 = BASE64_DECODE[bytes[i] as usize];
                let b1 = BASE64_DECODE[bytes[i + 1] as usize];
                let b2 = BASE64_DECODE[bytes[i + 2] as usize];
                let b3 = BASE64_DECODE[bytes[i + 3] as usize];
                if b0 < 0 || b1 < 0 || b2 < 0 || b3 < 0 {
                    return Ok(Value::Null);
                }
                result.push(((b0 << 2) | (b1 >> 4)) as u8);
                result.push(((b1 << 4) | (b2 >> 2)) as u8);
                result.push(((b2 << 6) | b3) as u8);
                i += 4;
            }
            // Handle remaining bytes
            if i < bytes.len() {
                let remaining = bytes.len() - i;
                if remaining >= 2 {
                    let b0 = BASE64_DECODE[bytes[i] as usize];
                    let b1 = BASE64_DECODE[bytes[i + 1] as usize];
                    if b0 >= 0 && b1 >= 0 {
                        result.push(((b0 << 2) | (b1 >> 4)) as u8);
                    }
                    if remaining >= 3 {
                        let b2 = BASE64_DECODE[bytes[i + 2] as usize];
                        if b2 >= 0 {
                            result.push(((b1 << 4) | (b2 >> 2)) as u8);
                        }
                    }
                }
            }
            Ok(Value::Varchar(String::from_utf8_lossy(&result).to_string()))
        }
        "FORMAT_BYTES" | "PG_SIZE_PRETTY" => {
            let bytes = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let abs_bytes = bytes.abs() as f64;
            let sign = if bytes < 0 { "-" } else { "" };
            let result = if abs_bytes >= 1024.0 * 1024.0 * 1024.0 * 1024.0 {
                format!("{}{}TB", sign, (abs_bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0)) as i64)
            } else if abs_bytes >= 1024.0 * 1024.0 * 1024.0 {
                format!("{}{}GB", sign, (abs_bytes / (1024.0 * 1024.0 * 1024.0)) as i64)
            } else if abs_bytes >= 1024.0 * 1024.0 {
                format!("{}{}MB", sign, (abs_bytes / (1024.0 * 1024.0)) as i64)
            } else if abs_bytes >= 1024.0 {
                format!("{}{}KB", sign, (abs_bytes / 1024.0) as i64)
            } else {
                format!("{}{} bytes", sign, bytes.abs())
            };
            Ok(Value::Varchar(result))
        }
        "LEVENSHTEIN" | "EDITDIST" | "EDIT_DISTANCE" => {
            // Levenshtein distance (edit distance) between two strings
            let s1 = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let s2 = args.get(1).and_then(|v| v.as_str()).unwrap_or("");

            let m = s1.chars().count();
            let n = s2.chars().count();

            if m == 0 { return Ok(Value::Integer(n as i32)); }
            if n == 0 { return Ok(Value::Integer(m as i32)); }

            let s1_chars: Vec<char> = s1.chars().collect();
            let s2_chars: Vec<char> = s2.chars().collect();

            let mut prev: Vec<usize> = (0..=n).collect();
            let mut curr = vec![0; n + 1];

            for i in 1..=m {
                curr[0] = i;
                for j in 1..=n {
                    let cost = if s1_chars[i - 1] == s2_chars[j - 1] { 0 } else { 1 };
                    curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
                }
                std::mem::swap(&mut prev, &mut curr);
            }
            Ok(Value::Integer(prev[n] as i32))
        }
        "SOUNDEX" => {
            // Soundex phonetic algorithm
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            if s.is_empty() {
                return Ok(Value::Varchar("".to_string()));
            }
            let chars: Vec<char> = s.to_uppercase().chars().filter(|c| c.is_ascii_alphabetic()).collect();
            if chars.is_empty() {
                return Ok(Value::Varchar("".to_string()));
            }
            let first = chars[0];
            let code = |c: char| -> Option<char> {
                match c {
                    'B' | 'F' | 'P' | 'V' => Some('1'),
                    'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => Some('2'),
                    'D' | 'T' => Some('3'),
                    'L' => Some('4'),
                    'M' | 'N' => Some('5'),
                    'R' => Some('6'),
                    _ => None,
                }
            };
            let mut result = String::new();
            result.push(first);
            let mut prev_code = code(first);
            for &c in &chars[1..] {
                if result.len() >= 4 { break; }
                let curr_code = code(c);
                if curr_code.is_some() && curr_code != prev_code {
                    result.push(curr_code.unwrap());
                }
                prev_code = curr_code;
            }
            while result.len() < 4 {
                result.push('0');
            }
            Ok(Value::Varchar(result))
        }
        "HAMMING" | "HAMMING_DISTANCE" => {
            // Hamming distance - number of positions where characters differ
            let s1 = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let s2 = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            if s1.len() != s2.len() {
                // Hamming distance is only defined for equal length strings
                return Ok(Value::Null);
            }
            let dist = s1.chars().zip(s2.chars())
                .filter(|(a, b)| a != b)
                .count();
            Ok(Value::Integer(dist as i32))
        }
        "JACCARD" | "JACCARD_SIMILARITY" => {
            // Jaccard similarity - intersection / union of character sets
            let s1 = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let s2 = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            use std::collections::HashSet;
            let set1: HashSet<char> = s1.chars().collect();
            let set2: HashSet<char> = s2.chars().collect();
            if set1.is_empty() && set2.is_empty() {
                return Ok(Value::Double(1.0));
            }
            let intersection = set1.intersection(&set2).count();
            let union = set1.union(&set2).count();
            Ok(Value::Double(intersection as f64 / union as f64))
        }
        "JARO_WINKLER" | "JARO" => {
            // Jaro-Winkler similarity (0 to 1)
            let s1 = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let s2 = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            if s1.is_empty() && s2.is_empty() {
                return Ok(Value::Double(1.0));
            }
            if s1.is_empty() || s2.is_empty() {
                return Ok(Value::Double(0.0));
            }
            let s1_chars: Vec<char> = s1.chars().collect();
            let s2_chars: Vec<char> = s2.chars().collect();
            let match_distance = (s1_chars.len().max(s2_chars.len()) / 2).saturating_sub(1);

            let mut s1_matches = vec![false; s1_chars.len()];
            let mut s2_matches = vec![false; s2_chars.len()];
            let mut matches = 0;
            let mut transpositions = 0;

            for i in 0..s1_chars.len() {
                let start = i.saturating_sub(match_distance);
                let end = (i + match_distance + 1).min(s2_chars.len());
                for j in start..end {
                    if s2_matches[j] || s1_chars[i] != s2_chars[j] { continue; }
                    s1_matches[i] = true;
                    s2_matches[j] = true;
                    matches += 1;
                    break;
                }
            }

            if matches == 0 { return Ok(Value::Double(0.0)); }

            let mut k = 0;
            for i in 0..s1_chars.len() {
                if !s1_matches[i] { continue; }
                while !s2_matches[k] { k += 1; }
                if s1_chars[i] != s2_chars[k] { transpositions += 1; }
                k += 1;
            }

            let jaro = (matches as f64 / s1_chars.len() as f64
                + matches as f64 / s2_chars.len() as f64
                + (matches - transpositions / 2) as f64 / matches as f64) / 3.0;
            Ok(Value::Double(jaro))
        }

        // Conditional functions
        "GREATEST" => {
            let mut max: Option<Value> = None;
            for arg in args {
                if arg.is_null() {
                    continue;
                }
                max = Some(match max {
                    None => arg.clone(),
                    Some(m) => {
                        if arg.partial_cmp(&m) == Some(std::cmp::Ordering::Greater) {
                            arg.clone()
                        } else {
                            m
                        }
                    }
                });
            }
            Ok(max.unwrap_or(Value::Null))
        }
        "LEAST" => {
            let mut min: Option<Value> = None;
            for arg in args {
                if arg.is_null() {
                    continue;
                }
                min = Some(match min {
                    None => arg.clone(),
                    Some(m) => {
                        if arg.partial_cmp(&m) == Some(std::cmp::Ordering::Less) {
                            arg.clone()
                        } else {
                            m
                        }
                    }
                });
            }
            Ok(min.unwrap_or(Value::Null))
        }
        "IIF" => {
            // IIF(condition, true_value, false_value)
            let cond = args.first().map(|v| matches!(v, Value::Boolean(true))).unwrap_or(false);
            if cond {
                Ok(args.get(1).cloned().unwrap_or(Value::Null))
            } else {
                Ok(args.get(2).cloned().unwrap_or(Value::Null))
            }
        }

        // NULL handling
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
        "IFNULL" | "NVL" => {
            let first = args.first().unwrap_or(&Value::Null);
            if first.is_null() {
                Ok(args.get(1).cloned().unwrap_or(Value::Null))
            } else {
                Ok(first.clone())
            }
        }

        // Type functions
        "TYPEOF" => {
            let val = args.first().unwrap_or(&Value::Null);
            Ok(Value::Varchar(val.logical_type().to_string()))
        }

        // Date/Time functions
        "NOW" | "CURRENT_TIMESTAMP" => {
            use chrono::{Local, NaiveDateTime};
            let now = Local::now().naive_local();
            Ok(Value::Timestamp(now))
        }
        "CURRENT_DATE" => {
            use chrono::Local;
            let today = Local::now().date_naive();
            Ok(Value::Date(today))
        }
        "DATE_PART" | "EXTRACT" => {
            use chrono::{Datelike, Timelike};
            let part = args.first().and_then(|v| v.as_str()).unwrap_or("").to_uppercase();
            let ts = args.get(1).unwrap_or(&Value::Null);

            // Extract datetime components
            let (year, month, day, hour, minute, second, day_of_week, day_of_year) = match ts {
                Value::Timestamp(dt) => (
                    dt.year() as i64,
                    dt.month() as i64,
                    dt.day() as i64,
                    dt.hour() as i64,
                    dt.minute() as i64,
                    dt.second() as i64,
                    dt.weekday().num_days_from_sunday() as i64,
                    dt.ordinal() as i64,
                ),
                Value::TimestampTz(dt) => (
                    dt.year() as i64,
                    dt.month() as i64,
                    dt.day() as i64,
                    dt.hour() as i64,
                    dt.minute() as i64,
                    dt.second() as i64,
                    dt.weekday().num_days_from_sunday() as i64,
                    dt.ordinal() as i64,
                ),
                Value::Date(d) => (
                    d.year() as i64,
                    d.month() as i64,
                    d.day() as i64,
                    0, 0, 0,
                    d.weekday().num_days_from_sunday() as i64,
                    d.ordinal() as i64,
                ),
                Value::Null => return Ok(Value::Null),
                _ => return Err(Error::TypeMismatch {
                    expected: "timestamp or date".to_string(),
                    got: format!("{:?}", ts),
                }),
            };

            let result = match part.as_str() {
                "YEAR" | "YEARS" => year,
                "MONTH" | "MONTHS" => month,
                "DAY" | "DAYS" => day,
                "HOUR" | "HOURS" => hour,
                "MINUTE" | "MINUTES" => minute,
                "SECOND" | "SECONDS" => second,
                "DOW" | "DAYOFWEEK" => day_of_week,
                "DOY" | "DAYOFYEAR" => day_of_year,
                "QUARTER" => ((month - 1) / 3 + 1),
                "WEEK" => {
                    // Get ISO week number
                    match ts {
                        Value::Timestamp(dt) => dt.iso_week().week() as i64,
                        Value::Date(d) => d.iso_week().week() as i64,
                        _ => 0,
                    }
                }
                _ => return Err(Error::NotImplemented(format!("DATE_PART field: {}", part))),
            };
            Ok(Value::BigInt(result))
        }
        "DATE_TRUNC" => {
            use chrono::{Datelike, Timelike, NaiveDateTime, NaiveDate, NaiveTime};
            let part = args.first().and_then(|v| v.as_str()).unwrap_or("").to_uppercase();
            let ts = args.get(1).unwrap_or(&Value::Null);

            let dt = match ts {
                Value::Timestamp(dt) => *dt,
                Value::Null => return Ok(Value::Null),
                _ => return Err(Error::TypeMismatch {
                    expected: "timestamp".to_string(),
                    got: format!("{:?}", ts),
                }),
            };

            let truncated = match part.as_str() {
                "SECOND" => NaiveDateTime::new(
                    dt.date(),
                    NaiveTime::from_hms_opt(dt.hour(), dt.minute(), dt.second()).unwrap(),
                ),
                "MINUTE" => NaiveDateTime::new(
                    dt.date(),
                    NaiveTime::from_hms_opt(dt.hour(), dt.minute(), 0).unwrap(),
                ),
                "HOUR" => NaiveDateTime::new(
                    dt.date(),
                    NaiveTime::from_hms_opt(dt.hour(), 0, 0).unwrap(),
                ),
                "DAY" => NaiveDateTime::new(
                    dt.date(),
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                ),
                _ => return Err(Error::NotImplemented(format!("DATE_TRUNC field: {}", part))),
            };
            Ok(Value::Timestamp(truncated))
        }
        "YEAR" => {
            use chrono::Datelike;
            match args.first() {
                Some(Value::Date(d)) => Ok(Value::BigInt(d.year() as i64)),
                Some(Value::Timestamp(dt)) => Ok(Value::BigInt(dt.year() as i64)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "MONTH" => {
            use chrono::Datelike;
            match args.first() {
                Some(Value::Date(d)) => Ok(Value::BigInt(d.month() as i64)),
                Some(Value::Timestamp(dt)) => Ok(Value::BigInt(dt.month() as i64)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "DAY" | "DAYOFMONTH" => {
            use chrono::Datelike;
            match args.first() {
                Some(Value::Date(d)) => Ok(Value::BigInt(d.day() as i64)),
                Some(Value::Timestamp(dt)) => Ok(Value::BigInt(dt.day() as i64)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "HOUR" => {
            use chrono::Timelike;
            match args.first() {
                Some(Value::Timestamp(dt)) => Ok(Value::BigInt(dt.hour() as i64)),
                Some(Value::Time(t)) => Ok(Value::BigInt(t.hour() as i64)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "MINUTE" => {
            use chrono::Timelike;
            match args.first() {
                Some(Value::Timestamp(dt)) => Ok(Value::BigInt(dt.minute() as i64)),
                Some(Value::Time(t)) => Ok(Value::BigInt(t.minute() as i64)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "SECOND" => {
            use chrono::Timelike;
            match args.first() {
                Some(Value::Timestamp(dt)) => Ok(Value::BigInt(dt.second() as i64)),
                Some(Value::Time(t)) => Ok(Value::BigInt(t.second() as i64)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "DAYOFWEEK" | "DOW" => {
            use chrono::Datelike;
            match args.first() {
                Some(Value::Date(d)) => Ok(Value::BigInt(d.weekday().num_days_from_sunday() as i64)),
                Some(Value::Timestamp(dt)) => Ok(Value::BigInt(dt.weekday().num_days_from_sunday() as i64)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "DAYOFYEAR" | "DOY" => {
            use chrono::Datelike;
            match args.first() {
                Some(Value::Date(d)) => Ok(Value::BigInt(d.ordinal() as i64)),
                Some(Value::Timestamp(dt)) => Ok(Value::BigInt(dt.ordinal() as i64)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "WEEK" | "WEEKOFYEAR" => {
            use chrono::Datelike;
            match args.first() {
                Some(Value::Date(d)) => Ok(Value::BigInt(d.iso_week().week() as i64)),
                Some(Value::Timestamp(dt)) => Ok(Value::BigInt(dt.iso_week().week() as i64)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "QUARTER" => {
            use chrono::Datelike;
            match args.first() {
                Some(Value::Date(d)) => Ok(Value::BigInt(((d.month() - 1) / 3 + 1) as i64)),
                Some(Value::Timestamp(dt)) => Ok(Value::BigInt(((dt.month() - 1) / 3 + 1) as i64)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "DATE_ADD" | "DATEADD" => {
            use chrono::{Duration, Datelike};
            let date = args.first().unwrap_or(&Value::Null);
            let interval = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
            let unit = args.get(2).and_then(|v| v.as_str()).unwrap_or("day").to_uppercase();

            match date {
                Value::Date(d) => {
                    let result = match unit.as_str() {
                        "DAY" | "DAYS" => *d + Duration::days(interval),
                        "WEEK" | "WEEKS" => *d + Duration::weeks(interval),
                        "MONTH" | "MONTHS" => {
                            let new_month = d.month() as i64 + interval;
                            let years_delta = (new_month - 1) / 12;
                            let new_month = ((new_month - 1) % 12 + 1) as u32;
                            let new_year = d.year() + years_delta as i32;
                            chrono::NaiveDate::from_ymd_opt(new_year, new_month, d.day().min(28))
                                .unwrap_or(*d)
                        }
                        "YEAR" | "YEARS" => {
                            chrono::NaiveDate::from_ymd_opt(d.year() + interval as i32, d.month(), d.day())
                                .unwrap_or(*d)
                        }
                        _ => return Err(Error::NotImplemented(format!("DATE_ADD unit: {}", unit))),
                    };
                    Ok(Value::Date(result))
                }
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "date".to_string(),
                    got: format!("{:?}", date),
                }),
            }
        }
        "DATE_SUB" | "DATESUB" => {
            use chrono::{Duration, Datelike};
            let date = args.first().unwrap_or(&Value::Null);
            let interval = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
            let unit = args.get(2).and_then(|v| v.as_str()).unwrap_or("day").to_uppercase();

            match date {
                Value::Date(d) => {
                    let result = match unit.as_str() {
                        "DAY" | "DAYS" => *d - Duration::days(interval),
                        "WEEK" | "WEEKS" => *d - Duration::weeks(interval),
                        "MONTH" | "MONTHS" => {
                            let new_month = d.month() as i64 - interval;
                            let years_delta = if new_month <= 0 { (new_month / 12) - 1 } else { 0 };
                            let new_month = ((new_month - 1).rem_euclid(12) + 1) as u32;
                            let new_year = d.year() + years_delta as i32;
                            chrono::NaiveDate::from_ymd_opt(new_year, new_month, d.day().min(28))
                                .unwrap_or(*d)
                        }
                        "YEAR" | "YEARS" => {
                            chrono::NaiveDate::from_ymd_opt(d.year() - interval as i32, d.month(), d.day())
                                .unwrap_or(*d)
                        }
                        _ => return Err(Error::NotImplemented(format!("DATE_SUB unit: {}", unit))),
                    };
                    Ok(Value::Date(result))
                }
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "date".to_string(),
                    got: format!("{:?}", date),
                }),
            }
        }
        "DATEDIFF" | "DATE_DIFF" => {
            use chrono::Datelike;
            let date1 = args.first().unwrap_or(&Value::Null);
            let date2 = args.get(1).unwrap_or(&Value::Null);

            match (date1, date2) {
                (Value::Date(d1), Value::Date(d2)) => {
                    let days = (*d2 - *d1).num_days();
                    Ok(Value::BigInt(days))
                }
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "date".to_string(),
                    got: format!("{:?}, {:?}", date1, date2),
                }),
            }
        }
        "MAKE_DATE" => {
            let year = args.first().and_then(|v| v.as_i64()).unwrap_or(1970) as i32;
            let month = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1) as u32;
            let day = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1) as u32;

            match chrono::NaiveDate::from_ymd_opt(year, month, day) {
                Some(d) => Ok(Value::Date(d)),
                None => Ok(Value::Null),
            }
        }
        "MAKE_TIMESTAMP" => {
            use chrono::{NaiveDate, NaiveTime, NaiveDateTime};
            let year = args.first().and_then(|v| v.as_i64()).unwrap_or(1970) as i32;
            let month = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1) as u32;
            let day = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1) as u32;
            let hour = args.get(3).and_then(|v| v.as_i64()).unwrap_or(0) as u32;
            let minute = args.get(4).and_then(|v| v.as_i64()).unwrap_or(0) as u32;
            let second = args.get(5).and_then(|v| v.as_i64()).unwrap_or(0) as u32;

            match (NaiveDate::from_ymd_opt(year, month, day), NaiveTime::from_hms_opt(hour, minute, second)) {
                (Some(d), Some(t)) => Ok(Value::Timestamp(NaiveDateTime::new(d, t))),
                _ => Ok(Value::Null),
            }
        }
        "LAST_DAY" => {
            use chrono::Datelike;
            match args.first() {
                Some(Value::Date(d)) => {
                    let next_month = if d.month() == 12 {
                        chrono::NaiveDate::from_ymd_opt(d.year() + 1, 1, 1)
                    } else {
                        chrono::NaiveDate::from_ymd_opt(d.year(), d.month() + 1, 1)
                    };
                    match next_month {
                        Some(nm) => Ok(Value::Date(nm - chrono::Duration::days(1))),
                        None => Ok(Value::Null),
                    }
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "EPOCH" | "EPOCH_MS" => {
            match args.first() {
                Some(Value::Timestamp(dt)) => {
                    let epoch = dt.and_utc().timestamp();
                    Ok(Value::BigInt(epoch))
                }
                Some(Value::Date(d)) => {
                    let epoch = d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
                    Ok(Value::BigInt(epoch))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "TO_DATE" => {
            match args.first() {
                Some(Value::Varchar(s)) => {
                    match chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        Ok(d) => Ok(Value::Date(d)),
                        Err(_) => Ok(Value::Null),
                    }
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "TO_TIMESTAMP" => {
            match args.first() {
                Some(Value::Varchar(s)) => {
                    match chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                        Ok(dt) => Ok(Value::Timestamp(dt)),
                        Err(_) => {
                            // Try ISO format
                            match chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                                Ok(dt) => Ok(Value::Timestamp(dt)),
                                Err(_) => Ok(Value::Null),
                            }
                        }
                    }
                }
                Some(Value::BigInt(epoch)) => {
                    match chrono::DateTime::from_timestamp(*epoch, 0) {
                        Some(dt) => Ok(Value::Timestamp(dt.naive_utc())),
                        None => Ok(Value::Null),
                    }
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "STRFTIME" | "FORMAT_DATE" => {
            // Format date/timestamp using format string
            use chrono::Datelike;
            let format = args.first().and_then(|v| v.as_str()).unwrap_or("%Y-%m-%d");
            let datetime = args.get(1).unwrap_or(&Value::Null);

            match datetime {
                Value::Timestamp(dt) => Ok(Value::Varchar(dt.format(format).to_string())),
                Value::Date(d) => Ok(Value::Varchar(d.format(format).to_string())),
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "AGE" => {
            // Calculate age/difference between two dates/timestamps
            use chrono::Datelike;
            let date1 = args.first().unwrap_or(&Value::Null);
            let date2 = args.get(1);

            let d1 = match date1 {
                Value::Date(d) => Some(*d),
                Value::Timestamp(dt) => Some(dt.date()),
                _ => None,
            };

            let d2 = match date2 {
                Some(Value::Date(d)) => Some(*d),
                Some(Value::Timestamp(dt)) => Some(dt.date()),
                _ => Some(chrono::Local::now().date_naive()),
            };

            match (d1, d2) {
                (Some(from), Some(to)) => {
                    let years = to.year() - from.year();
                    let months = to.month() as i32 - from.month() as i32;
                    let days = to.day() as i32 - from.day() as i32;

                    let total_months = years * 12 + months;
                    let y = total_months / 12;
                    let m = total_months % 12;

                    Ok(Value::Varchar(format!("{} years {} months {} days", y, m.abs(), days.abs())))
                }
                _ => Ok(Value::Null),
            }
        }

        // JSON functions
        "JSON_EXTRACT" | "JSON_EXTRACT_PATH" | "JSON_VALUE" => {
            let json_str = args.first().and_then(|v| v.as_str()).unwrap_or("{}");
            let path = args.get(1).and_then(|v| v.as_str()).unwrap_or("");

            // Simple JSON path extraction (supports $.key.subkey or just key.subkey)
            let path_str = path.trim_start_matches("$.").trim_start_matches('.');

            // Try to parse JSON and extract value by walking the path
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
                let mut current = &json;
                for key in path_str.split('.') {
                    if key.is_empty() { continue; }
                    match current.get(key) {
                        Some(val) => current = val,
                        None => return Ok(Value::Null),
                    }
                }
                return Ok(json_value_to_value(current));
            }
            Ok(Value::Null)
        }
        "JSON_EXTRACT_STRING" => {
            let json_str = args.first().and_then(|v| v.as_str()).unwrap_or("{}");
            let path = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let key = path.trim_start_matches("$.").trim_start_matches('.');

            if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(val) = json.get(key) {
                    if let Some(s) = val.as_str() {
                        return Ok(Value::Varchar(s.to_string()));
                    }
                    return Ok(Value::Varchar(val.to_string()));
                }
            }
            Ok(Value::Null)
        }
        "JSON_ARRAY_LENGTH" => {
            let json_str = args.first().and_then(|v| v.as_str()).unwrap_or("[]");
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(arr) = json.as_array() {
                    return Ok(Value::BigInt(arr.len() as i64));
                }
            }
            Ok(Value::Null)
        }
        "JSON_TYPE" => {
            let json_str = args.first().and_then(|v| v.as_str()).unwrap_or("null");
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
                let type_name = match json {
                    serde_json::Value::Null => "null",
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::String(_) => "string",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::Object(_) => "object",
                };
                return Ok(Value::Varchar(type_name.to_string()));
            }
            Ok(Value::Null)
        }
        "JSON_VALID" => {
            let json_str = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Boolean(serde_json::from_str::<serde_json::Value>(json_str).is_ok()))
        }
        "JSON_KEYS" => {
            let json_str = args.first().and_then(|v| v.as_str()).unwrap_or("{}");
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(obj) = json.as_object() {
                    let keys: Vec<Value> = obj.keys()
                        .map(|k| Value::Varchar(k.clone()))
                        .collect();
                    return Ok(Value::List(keys));
                }
            }
            Ok(Value::Null)
        }
        "TO_JSON" | "JSON" => {
            // Convert value to JSON string
            match args.first() {
                Some(v) => Ok(Value::Varchar(value_to_json_string(v))),
                None => Ok(Value::Varchar("null".to_string())),
            }
        }
        "JSON_ARRAY" => {
            // Create JSON array from arguments
            let json_vals: Vec<String> = args.iter()
                .map(|v| value_to_json_string(v))
                .collect();
            Ok(Value::Varchar(format!("[{}]", json_vals.join(","))))
        }
        "JSON_OBJECT" => {
            // Create JSON object from key-value pairs
            let mut obj = String::from("{");
            let mut first = true;
            for chunk in args.chunks(2) {
                if chunk.len() == 2 {
                    if !first { obj.push(','); }
                    first = false;
                    let key = value_to_string(&chunk[0]);
                    let val = value_to_json_string(&chunk[1]);
                    obj.push_str(&format!("\"{}\":{}", key, val));
                }
            }
            obj.push('}');
            Ok(Value::Varchar(obj))
        }

        _ => Err(Error::NotImplemented(format!("Function: {}", name))),
    }
}

/// Convert a serde_json::Value to an ironduck Value
fn json_value_to_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::BigInt(i)
            } else if let Some(f) = n.as_f64() {
                Value::Double(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::Varchar(s.clone()),
        serde_json::Value::Array(arr) => {
            let values: Vec<Value> = arr.iter().map(json_value_to_value).collect();
            Value::List(values)
        }
        serde_json::Value::Object(obj) => {
            let fields: Vec<(String, Value)> = obj.iter()
                .map(|(k, v)| (k.clone(), json_value_to_value(v)))
                .collect();
            Value::Struct(fields)
        }
    }
}

/// Convert an ironduck Value to a JSON string
fn value_to_json_string(val: &Value) -> String {
    match val {
        Value::Null => "null".to_string(),
        Value::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(f) => f.to_string(),
        Value::Varchar(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
        Value::List(items) => {
            let json_items: Vec<String> = items.iter().map(value_to_json_string).collect();
            format!("[{}]", json_items.join(","))
        }
        Value::Struct(fields) => {
            let json_fields: Vec<String> = fields.iter()
                .map(|(k, v)| format!("\"{}\":{}", k, value_to_json_string(v)))
                .collect();
            format!("{{{}}}", json_fields.join(","))
        }
        _ => format!("\"{}\"", val),
    }
}

/// Cast a value to a target type
fn cast_value(val: &Value, target: &LogicalType) -> Result<Value> {
    if val.is_null() {
        return Ok(Value::Null);
    }

    match target {
        LogicalType::Boolean => {
            let b = match val {
                Value::Boolean(b) => *b,
                Value::Integer(i) => *i != 0,
                Value::BigInt(i) => *i != 0,
                Value::Varchar(s) => s.eq_ignore_ascii_case("true") || s == "1",
                _ => return Err(Error::InvalidCast {
                    from: val.logical_type().to_string(),
                    to: target.to_string(),
                }),
            };
            Ok(Value::Boolean(b))
        }
        LogicalType::Integer => {
            let i = match val {
                Value::Integer(i) => *i,
                Value::BigInt(i) => *i as i32,
                Value::Float(f) => *f as i32,
                Value::Double(f) => *f as i32,
                Value::Boolean(b) => if *b { 1 } else { 0 },
                Value::Varchar(s) => s.parse().map_err(|_| Error::InvalidCast {
                    from: "VARCHAR".to_string(),
                    to: "INTEGER".to_string(),
                })?,
                _ => return Err(Error::InvalidCast {
                    from: val.logical_type().to_string(),
                    to: target.to_string(),
                }),
            };
            Ok(Value::Integer(i))
        }
        LogicalType::BigInt => {
            let i = match val {
                Value::Integer(i) => *i as i64,
                Value::BigInt(i) => *i,
                Value::Float(f) => *f as i64,
                Value::Double(f) => *f as i64,
                Value::Boolean(b) => if *b { 1 } else { 0 },
                Value::Varchar(s) => s.parse().map_err(|_| Error::InvalidCast {
                    from: "VARCHAR".to_string(),
                    to: "BIGINT".to_string(),
                })?,
                _ => return Err(Error::InvalidCast {
                    from: val.logical_type().to_string(),
                    to: target.to_string(),
                }),
            };
            Ok(Value::BigInt(i))
        }
        LogicalType::Double => {
            let f = match val {
                Value::Integer(i) => *i as f64,
                Value::BigInt(i) => *i as f64,
                Value::Float(f) => *f as f64,
                Value::Double(f) => *f,
                Value::Varchar(s) => s.parse().map_err(|_| Error::InvalidCast {
                    from: "VARCHAR".to_string(),
                    to: "DOUBLE".to_string(),
                })?,
                _ => return Err(Error::InvalidCast {
                    from: val.logical_type().to_string(),
                    to: target.to_string(),
                }),
            };
            Ok(Value::Double(f))
        }
        LogicalType::Varchar => {
            Ok(Value::Varchar(value_to_string(val)))
        }
        _ => Err(Error::NotImplemented(format!("Cast to {:?}", target))),
    }
}

/// Perform arithmetic operation
fn arithmetic_op<F, G>(left: &Value, right: &Value, int_op: F, float_op: G) -> Result<Value>
where
    F: Fn(i64, i64) -> i64,
    G: Fn(f64, f64) -> f64,
{
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(int_op(*l as i64, *r as i64) as i32)),
        (Value::Integer(l), Value::BigInt(r)) => Ok(Value::BigInt(int_op(*l as i64, *r))),
        (Value::BigInt(l), Value::Integer(r)) => Ok(Value::BigInt(int_op(*l, *r as i64))),
        (Value::BigInt(l), Value::BigInt(r)) => Ok(Value::BigInt(int_op(*l, *r))),
        (Value::Float(l), Value::Float(r)) => Ok(Value::Float(float_op(*l as f64, *r as f64) as f32)),
        (Value::Double(l), Value::Double(r)) => Ok(Value::Double(float_op(*l, *r))),
        // Mixed int/float
        (Value::Integer(l), Value::Double(r)) => Ok(Value::Double(float_op(*l as f64, *r))),
        (Value::Double(l), Value::Integer(r)) => Ok(Value::Double(float_op(*l, *r as f64))),
        (Value::BigInt(l), Value::Double(r)) => Ok(Value::Double(float_op(*l as f64, *r))),
        (Value::Double(l), Value::BigInt(r)) => Ok(Value::Double(float_op(*l, *r as f64))),
        _ => Err(Error::TypeMismatch {
            expected: "numeric".to_string(),
            got: format!("{:?}, {:?}", left, right),
        }),
    }
}

/// Perform comparison operation
fn comparison_op<F>(left: &Value, right: &Value, compare: F) -> Result<Value>
where
    F: Fn(std::cmp::Ordering) -> bool,
{
    let ordering = left.partial_cmp(right).ok_or_else(|| Error::TypeMismatch {
        expected: "comparable types".to_string(),
        got: format!("{:?}, {:?}", left, right),
    })?;
    Ok(Value::Boolean(compare(ordering)))
}

/// Convert value to string
fn value_to_string(val: &Value) -> String {
    match val {
        Value::Null => "".to_string(),
        Value::Varchar(s) => s.clone(),
        _ => val.to_string(),
    }
}

/// Simple LIKE pattern matching
fn like_match(text: &str, pattern: &str) -> bool {
    let pattern = pattern.replace("%", ".*").replace("_", ".");
    regex::Regex::new(&format!("^{}$", pattern))
        .map(|re: regex::Regex| re.is_match(text))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant() {
        let expr = Expression::Constant(Value::Integer(42));
        let result = evaluate(&expr, &[]).unwrap();
        assert_eq!(result, Value::Integer(42));
    }

    #[test]
    fn test_add() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Constant(Value::Integer(1))),
            op: BinaryOperator::Add,
            right: Box::new(Expression::Constant(Value::Integer(2))),
        };
        let result = evaluate(&expr, &[]).unwrap();
        assert_eq!(result, Value::Integer(3));
    }

    #[test]
    fn test_comparison() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Constant(Value::Integer(1))),
            op: BinaryOperator::LessThan,
            right: Box::new(Expression::Constant(Value::Integer(2))),
        };
        let result = evaluate(&expr, &[]).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_coalesce() {
        let result = evaluate_function("COALESCE", &[Value::Null, Value::Integer(42)]).unwrap();
        assert_eq!(result, Value::Integer(42));
    }
}
