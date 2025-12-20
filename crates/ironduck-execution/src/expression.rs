//! Expression evaluation

use ironduck_common::{Error, LogicalType, Result, Value};
use ironduck_planner::{BinaryOperator, Expression, UnaryOperator};

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
            // Check for division by zero
            match right {
                Value::Integer(0) | Value::BigInt(0) => return Err(Error::DivisionByZero),
                Value::Double(f) if *f == 0.0 => return Err(Error::DivisionByZero),
                Value::Float(f) if *f == 0.0 => return Err(Error::DivisionByZero),
                _ => {}
            }
            arithmetic_op(left, right, |a, b| a / b, |a, b| a / b)
        }
        BinaryOperator::Modulo => arithmetic_op(left, right, |a, b| a % b, |a, b| a % b),

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
        "REVERSE" => {
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            Ok(Value::Varchar(s.chars().rev().collect()))
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

        _ => Err(Error::NotImplemented(format!("Function: {}", name))),
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
