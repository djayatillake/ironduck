//! Expression evaluation

use ironduck_catalog::Catalog;
use ironduck_common::{Error, LogicalType, Result, Value};
use ironduck_planner::{BinaryOperator, Expression, UnaryOperator};
use std::cmp::Ordering;
use std::sync::Arc;

/// Context for expression evaluation
#[derive(Clone, Default)]
pub struct EvalContext {
    /// Row indices for each table (for rowid pseudo-column)
    pub row_indices: Vec<i64>,
    /// Optional catalog reference for functions like NEXTVAL
    catalog: Option<Arc<Catalog>>,
    /// Outer row for correlated subqueries
    pub outer_row: Option<Vec<Value>>,
}

impl std::fmt::Debug for EvalContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvalContext")
            .field("row_indices", &self.row_indices)
            .field("has_catalog", &self.catalog.is_some())
            .field("has_outer_row", &self.outer_row.is_some())
            .finish()
    }
}

impl EvalContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_row_index(row_idx: i64) -> Self {
        Self {
            row_indices: vec![row_idx],
            catalog: None,
            outer_row: None,
        }
    }

    pub fn with_catalog(catalog: Arc<Catalog>) -> Self {
        Self {
            row_indices: Vec::new(),
            catalog: Some(catalog),
            outer_row: None,
        }
    }

    pub fn with_outer_row(mut self, outer_row: Vec<Value>) -> Self {
        self.outer_row = Some(outer_row);
        self
    }

    pub fn catalog(&self) -> Option<&Catalog> {
        self.catalog.as_deref()
    }
}

/// Evaluate an expression against a row of values
pub fn evaluate(expr: &Expression, row: &[Value]) -> Result<Value> {
    evaluate_with_ctx(expr, row, &EvalContext::new())
}

/// Evaluate an expression with context (for rowid support)
pub fn evaluate_with_ctx(expr: &Expression, row: &[Value], ctx: &EvalContext) -> Result<Value> {
    match expr {
        Expression::Constant(value) => Ok(value.clone()),

        Expression::ColumnRef { column_index, .. } => {
            row.get(*column_index)
                .cloned()
                .ok_or_else(|| Error::Internal(format!("Column index {} out of bounds", column_index)))
        }

        Expression::OuterColumnRef { column_index, name, .. } => {
            // Look up value from outer row (for correlated subqueries)
            ctx.outer_row
                .as_ref()
                .and_then(|outer| outer.get(*column_index))
                .cloned()
                .ok_or_else(|| Error::Internal(format!("Outer column {} (index {}) not found in context", name, column_index)))
        }

        Expression::BinaryOp { left, op, right } => {
            let left_val = evaluate_with_ctx(left, row, ctx)?;
            let right_val = evaluate_with_ctx(right, row, ctx)?;
            evaluate_binary_op(&left_val, *op, &right_val)
        }

        Expression::UnaryOp { op, expr } => {
            let val = evaluate_with_ctx(expr, row, ctx)?;
            evaluate_unary_op(*op, &val)
        }

        Expression::Function { name, args } => {
            let name_upper = name.to_uppercase();

            // Handle NEXTVAL/CURRVAL specially - they need catalog access
            if name_upper == "NEXTVAL" || name_upper == "CURRVAL" {
                if args.is_empty() {
                    return Err(Error::InvalidArguments(format!("{} requires a sequence name", name_upper)));
                }
                let seq_name = evaluate_with_ctx(&args[0], row, ctx)?;
                let seq_name_str = match seq_name {
                    Value::Varchar(s) => s,
                    _ => return Err(Error::InvalidArguments("Sequence name must be a string".to_string())),
                };

                // Get catalog from context
                let catalog = ctx.catalog().ok_or_else(|| {
                    Error::NotImplemented(format!("Function: {}", name_upper))
                })?;

                // Look up the sequence (try main schema)
                let sequence = catalog.get_sequence("main", &seq_name_str)
                    .ok_or_else(|| Error::SequenceNotFound(seq_name_str.clone()))?;

                if name_upper == "NEXTVAL" {
                    return Ok(Value::BigInt(sequence.nextval()));
                } else {
                    return Ok(Value::BigInt(sequence.currval()));
                }
            }

            let arg_values: Result<Vec<_>> = args.iter().map(|a| evaluate_with_ctx(a, row, ctx)).collect();
            evaluate_function(name, &arg_values?)
        }

        Expression::Cast { expr, target_type } => {
            let val = evaluate_with_ctx(expr, row, ctx)?;
            cast_value(&val, target_type)
        }

        Expression::TryCast { expr, target_type } => {
            let val = evaluate_with_ctx(expr, row, ctx)?;
            // TRY_CAST returns NULL on error instead of failing
            match cast_value(&val, target_type) {
                Ok(v) => Ok(v),
                Err(_) => Ok(Value::Null),
            }
        }

        Expression::IsNull(expr) => {
            let val = evaluate_with_ctx(expr, row, ctx)?;
            Ok(Value::Boolean(val.is_null()))
        }

        Expression::IsNotNull(expr) => {
            let val = evaluate_with_ctx(expr, row, ctx)?;
            Ok(Value::Boolean(!val.is_null()))
        }

        Expression::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let operand_val = operand.as_ref().map(|e| evaluate_with_ctx(e, row, ctx)).transpose()?;

            for (cond, result) in conditions.iter().zip(results.iter()) {
                let cond_val = evaluate_with_ctx(cond, row, ctx)?;

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
                    return evaluate_with_ctx(result, row, ctx);
                }
            }

            // No condition matched, return ELSE or NULL
            match else_result {
                Some(else_expr) => evaluate_with_ctx(else_expr, row, ctx),
                None => Ok(Value::Null),
            }
        }

        Expression::InList { expr, list, negated } => {
            let val = evaluate_with_ctx(expr, row, ctx)?;
            if val.is_null() {
                return Ok(Value::Null);
            }

            let mut found = false;
            for item in list {
                let item_val = evaluate_with_ctx(item, row, ctx)?;
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

        Expression::RowId { table_index } => {
            // Return the row index for the specified table
            ctx.row_indices
                .get(*table_index)
                .map(|&idx| Value::BigInt(idx))
                .ok_or_else(|| Error::Internal(format!("No row index for table {}", table_index)))
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
        BinaryOperator::Add => {
            // Handle date/timestamp + interval
            match (left, right) {
                (Value::Date(d), Value::Interval(i)) | (Value::Interval(i), Value::Date(d)) => {
                    use chrono::Months;
                    let mut result = *d;
                    if i.months != 0 {
                        result = result.checked_add_months(Months::new(i.months.unsigned_abs()))
                            .unwrap_or(result);
                        if i.months < 0 {
                            result = result.checked_sub_months(Months::new((i.months.abs() * 2) as u32))
                                .unwrap_or(result);
                        }
                    }
                    if i.days != 0 {
                        result = result + chrono::Duration::days(i.days as i64);
                    }
                    Ok(Value::Date(result))
                }
                (Value::Timestamp(ts), Value::Interval(i)) | (Value::Interval(i), Value::Timestamp(ts)) => {
                    use chrono::Months;
                    let mut result = *ts;
                    if i.months != 0 {
                        let new_date = result.date().checked_add_months(Months::new(i.months.unsigned_abs()))
                            .unwrap_or(result.date());
                        result = new_date.and_time(result.time());
                        if i.months < 0 {
                            let new_date = result.date().checked_sub_months(Months::new((i.months.abs() * 2) as u32))
                                .unwrap_or(result.date());
                            result = new_date.and_time(result.time());
                        }
                    }
                    if i.days != 0 {
                        result = result + chrono::Duration::days(i.days as i64);
                    }
                    if i.micros != 0 {
                        result = result + chrono::Duration::microseconds(i.micros);
                    }
                    Ok(Value::Timestamp(result))
                }
                _ => arithmetic_op(left, right, |a, b| a + b, |a, b| a + b),
            }
        }
        BinaryOperator::Subtract => {
            // Handle date/timestamp - interval
            match (left, right) {
                (Value::Date(d), Value::Interval(i)) => {
                    use chrono::Months;
                    let mut result = *d;
                    if i.months != 0 {
                        if i.months > 0 {
                            result = result.checked_sub_months(Months::new(i.months as u32))
                                .unwrap_or(result);
                        } else {
                            result = result.checked_add_months(Months::new(i.months.unsigned_abs()))
                                .unwrap_or(result);
                        }
                    }
                    if i.days != 0 {
                        result = result - chrono::Duration::days(i.days as i64);
                    }
                    Ok(Value::Date(result))
                }
                (Value::Timestamp(ts), Value::Interval(i)) => {
                    use chrono::Months;
                    let mut result = *ts;
                    if i.months != 0 {
                        if i.months > 0 {
                            let new_date = result.date().checked_sub_months(Months::new(i.months as u32))
                                .unwrap_or(result.date());
                            result = new_date.and_time(result.time());
                        } else {
                            let new_date = result.date().checked_add_months(Months::new(i.months.unsigned_abs()))
                                .unwrap_or(result.date());
                            result = new_date.and_time(result.time());
                        }
                    }
                    if i.days != 0 {
                        result = result - chrono::Duration::days(i.days as i64);
                    }
                    if i.micros != 0 {
                        result = result - chrono::Duration::microseconds(i.micros);
                    }
                    Ok(Value::Timestamp(result))
                }
                (Value::Date(d1), Value::Date(d2)) => {
                    // Date - Date = number of days
                    let days = (*d1 - *d2).num_days();
                    Ok(Value::BigInt(days))
                }
                (Value::Timestamp(t1), Value::Timestamp(t2)) => {
                    // Timestamp - Timestamp = interval in microseconds
                    let micros = (*t1 - *t2).num_microseconds().unwrap_or(0);
                    Ok(Value::Interval(ironduck_common::value::Interval::new(0, 0, micros)))
                }
                _ => arithmetic_op(left, right, |a, b| a - b, |a, b| a - b),
            }
        }
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
        // String functions - all return NULL if input is NULL
        "LOWER" | "LCASE" => {
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            Ok(Value::Varchar(s.to_lowercase()))
        }
        "UPPER" | "UCASE" => {
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            Ok(Value::Varchar(s.to_uppercase()))
        }
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            Ok(Value::BigInt(s.chars().count() as i64))
        }
        "TRIM" => {
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            // Check for second argument (characters to trim)
            if let Some(chars_arg) = args.get(1) {
                if matches!(chars_arg, Value::Null) {
                    return Ok(Value::Null);
                }
                let chars_to_trim = chars_arg.as_str().unwrap_or("");
                if chars_to_trim.is_empty() {
                    return Ok(Value::Varchar(s.to_string()));
                }
                // Trim from both ends any characters in chars_to_trim
                let ltrimmed: String = s.chars().skip_while(|c| chars_to_trim.contains(*c)).collect();
                let reversed: String = ltrimmed.chars().rev().skip_while(|c| chars_to_trim.contains(*c)).collect();
                Ok(Value::Varchar(reversed.chars().rev().collect()))
            } else {
                Ok(Value::Varchar(s.trim().to_string()))
            }
        }
        "LTRIM" => {
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            // Check for second argument (characters to trim)
            if let Some(chars_arg) = args.get(1) {
                if matches!(chars_arg, Value::Null) {
                    return Ok(Value::Null);
                }
                let chars_to_trim = chars_arg.as_str().unwrap_or("");
                if chars_to_trim.is_empty() {
                    return Ok(Value::Varchar(s.to_string()));
                }
                // Trim from start any characters in chars_to_trim
                let result: String = s.chars().skip_while(|c| chars_to_trim.contains(*c)).collect();
                Ok(Value::Varchar(result))
            } else {
                Ok(Value::Varchar(s.trim_start().to_string()))
            }
        }
        "RTRIM" => {
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            // Check for second argument (characters to trim)
            if let Some(chars_arg) = args.get(1) {
                if matches!(chars_arg, Value::Null) {
                    return Ok(Value::Null);
                }
                let chars_to_trim = chars_arg.as_str().unwrap_or("");
                if chars_to_trim.is_empty() {
                    return Ok(Value::Varchar(s.to_string()));
                }
                // Trim from end any characters in chars_to_trim
                let reversed: String = s.chars().rev().skip_while(|c| chars_to_trim.contains(*c)).collect();
                Ok(Value::Varchar(reversed.chars().rev().collect()))
            } else {
                Ok(Value::Varchar(s.trim_end().to_string()))
            }
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
            // Return NULL if separator is NULL
            if matches!(args.first(), Some(Value::Null)) {
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
            // Return NULL if string is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let start = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_i64().unwrap_or(1) as usize,
                None => 1,
            };
            let len = match args.get(2) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_i64(),
                None => None,
            };

            let start_idx = start.saturating_sub(1); // SQL is 1-indexed
            let chars: Vec<char> = s.chars().collect();

            let result: String = match len {
                Some(l) => chars.iter().skip(start_idx).take(l as usize).collect(),
                None => chars.iter().skip(start_idx).collect(),
            };
            Ok(Value::Varchar(result))
        }
        "REPLACE" => {
            // Return NULL if any argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let from = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let to = match args.get(2) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            Ok(Value::Varchar(s.replace(from, to)))
        }
        "LEFT" => {
            // Return NULL if any argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let n = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_i64().unwrap_or(0) as usize,
                None => 0,
            };
            Ok(Value::Varchar(s.chars().take(n).collect()))
        }
        "RIGHT" => {
            // Return NULL if any argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let n = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_i64().unwrap_or(0) as usize,
                None => 0,
            };
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            Ok(Value::Varchar(chars.into_iter().skip(start).collect()))
        }
        "ASCII" | "ORD" => {
            // Return NULL if argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
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
                    let code = v.as_i64().unwrap_or(0);
                    // Validate codepoint range
                    if code < 0 {
                        return Err(Error::InvalidArguments(format!(
                            "Invalid codepoint value for chr: {}", code
                        )));
                    }
                    if code > 0x10FFFF {
                        return Err(Error::InvalidArguments(format!(
                            "Invalid codepoint value for chr: {}", code
                        )));
                    }
                    match char::from_u32(code as u32) {
                        Some(c) => Ok(Value::Varchar(c.to_string())),
                        None => Err(Error::InvalidArguments(format!(
                            "Invalid codepoint value for chr: {}", code
                        ))),
                    }
                }
                None => Ok(Value::Null),
            }
        }
        "UNICODE" => {
            // Return Unicode code point of first character
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            if s.is_empty() {
                // DuckDB returns -1 for empty string
                Ok(Value::Integer(-1))
            } else {
                Ok(Value::Integer(s.chars().next().unwrap() as i32))
            }
        }
        "STRIP_ACCENTS" => {
            // Remove accents from characters (simplified version)
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let result: String = s.chars().map(|c| {
                match c {
                    'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' => 'a',
                    'é' | 'è' | 'ê' | 'ë' => 'e',
                    'í' | 'ì' | 'î' | 'ï' => 'i',
                    'ó' | 'ò' | 'ô' | 'ö' | 'õ' => 'o',
                    'ú' | 'ù' | 'û' | 'ü' => 'u',
                    'ñ' => 'n',
                    'ç' => 'c',
                    'Á' | 'À' | 'Â' | 'Ä' | 'Ã' | 'Å' => 'A',
                    'É' | 'È' | 'Ê' | 'Ë' => 'E',
                    'Í' | 'Ì' | 'Î' | 'Ï' => 'I',
                    'Ó' | 'Ò' | 'Ô' | 'Ö' | 'Õ' => 'O',
                    'Ú' | 'Ù' | 'Û' | 'Ü' => 'U',
                    'Ñ' => 'N',
                    'Ç' => 'C',
                    _ => c,
                }
            }).collect();
            Ok(Value::Varchar(result))
        }
        "BAR" => {
            // Create a bar chart string (like DuckDB's bar function)
            // bar(value, min, max, width) -> string of █ characters
            let value = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let min = args.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
            let max = args.get(2).and_then(|v| v.as_f64()).unwrap_or(100.0);
            let width = args.get(3).and_then(|v| v.as_i64()).unwrap_or(80) as usize;

            if max <= min {
                return Ok(Value::Varchar(String::new()));
            }

            let ratio = ((value - min) / (max - min)).clamp(0.0, 1.0);
            let filled = (ratio * width as f64).round() as usize;
            let bar: String = "█".repeat(filled);
            Ok(Value::Varchar(bar))
        }
        "REVERSE" => {
            // Return NULL if argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            Ok(Value::Varchar(s.chars().rev().collect()))
        }
        "TRANSLATE" => {
            // Return NULL if any argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let from_chars = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let to_chars = match args.get(2) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
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
            // Return NULL if string is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            // Return NULL if count is NULL, empty string if negative or zero
            // Validate that second argument is numeric (not a string)
            let n = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(Value::Varchar(_)) => {
                    return Err(ironduck_common::Error::InvalidArguments(
                        "REPEAT count argument must be an integer, not a string".to_string()
                    ));
                }
                Some(v) => v.as_i64().unwrap_or(0),
                None => 0,
            };
            if n <= 0 {
                Ok(Value::Varchar(String::new()))
            } else {
                Ok(Value::Varchar(s.repeat(n as usize)))
            }
        }
        "LPAD" => {
            // Return NULL if any argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let len = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_i64().unwrap_or(0),
                None => 0,
            };
            let pad = match args.get(2) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(" "),
                None => " ",
            };
            // Negative or zero length returns empty string
            if len <= 0 {
                return Ok(Value::Varchar(String::new()));
            }
            // Error on huge lengths to prevent memory exhaustion
            const MAX_PAD_LENGTH: i64 = 10_000_000;
            if len > MAX_PAD_LENGTH {
                return Err(ironduck_common::Error::InvalidArguments(format!(
                    "LPAD length {} exceeds maximum allowed ({})", len, MAX_PAD_LENGTH
                )));
            }
            let len = len as usize;
            let s_chars: Vec<char> = s.chars().collect();
            if s_chars.len() >= len {
                // Truncate to len characters
                Ok(Value::Varchar(s_chars[..len].iter().collect()))
            } else {
                // Empty pad string is an error only when we need to pad
                if pad.is_empty() {
                    return Err(ironduck_common::Error::Execution("LPAD padding string cannot be empty".to_string()));
                }
                let pad_len = len - s_chars.len();
                let padding: String = pad.chars().cycle().take(pad_len).collect();
                Ok(Value::Varchar(format!("{}{}", padding, s)))
            }
        }
        "RPAD" => {
            // Return NULL if any argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let len = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_i64().unwrap_or(0),
                None => 0,
            };
            let pad = match args.get(2) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(" "),
                None => " ",
            };
            // Negative or zero length returns empty string
            if len <= 0 {
                return Ok(Value::Varchar(String::new()));
            }
            // Error on huge lengths to prevent memory exhaustion
            const MAX_PAD_LENGTH: i64 = 10_000_000;
            if len > MAX_PAD_LENGTH {
                return Err(ironduck_common::Error::InvalidArguments(format!(
                    "RPAD length {} exceeds maximum allowed ({})", len, MAX_PAD_LENGTH
                )));
            }
            let len = len as usize;
            let s_chars: Vec<char> = s.chars().collect();
            if s_chars.len() >= len {
                // Truncate to len characters
                Ok(Value::Varchar(s_chars[..len].iter().collect()))
            } else {
                // Empty pad string is an error only when we need to pad
                if pad.is_empty() {
                    return Err(ironduck_common::Error::Execution("RPAD padding string cannot be empty".to_string()));
                }
                let pad_len = len - s_chars.len();
                let padding: String = pad.chars().cycle().take(pad_len).collect();
                Ok(Value::Varchar(format!("{}{}", s, padding)))
            }
        }
        "INSTR" | "POSITION" | "STRPOS" => {
            // Return NULL if either argument is NULL
            let haystack = match args.first() {
                Some(Value::Null) | None => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
            };
            let needle = match args.get(1) {
                Some(Value::Null) | None => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
            };
            // find() returns byte position, we need character position
            match haystack.find(needle) {
                Some(byte_pos) => {
                    // Count characters up to byte_pos
                    let char_pos = haystack[..byte_pos].chars().count();
                    Ok(Value::BigInt((char_pos + 1) as i64)) // 1-indexed
                }
                None => Ok(Value::BigInt(0)),
            }
        }
        "SPLIT_PART" => {
            // Return NULL if any argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let delimiter = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let part = match args.get(2) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_i64().unwrap_or(1),
                None => 1,
            };
            // Index 0 returns empty string
            if part == 0 {
                return Ok(Value::Varchar(String::new()));
            }
            // Empty delimiter: split each character
            let parts: Vec<&str> = if delimiter.is_empty() {
                s.chars().map(|c| {
                    let start = s.find(c).unwrap();
                    &s[start..start + c.len_utf8()]
                }).collect()
            } else {
                s.split(delimiter).collect()
            };
            // Handle negative indices (count from end)
            let idx = if part < 0 {
                let abs_part = (-part) as usize;
                if abs_part > parts.len() {
                    return Ok(Value::Varchar(String::new()));
                }
                parts.len() - abs_part
            } else {
                (part - 1) as usize
            };
            Ok(Value::Varchar(parts.get(idx).unwrap_or(&"").to_string()))
        }
        "STRING_SPLIT" | "STR_SPLIT" | "STRING_TO_ARRAY" => {
            // Return NULL if string is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let delimiter = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
            let parts: Vec<Value> = s.split(delimiter).map(|p| Value::Varchar(p.to_string())).collect();
            Ok(Value::List(parts))
        }
        "INITCAP" => {
            // Return NULL if argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
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
        "STARTS_WITH" | "PREFIX" => {
            // Return NULL if either argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let prefix = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            Ok(Value::Boolean(s.starts_with(prefix)))
        }
        "ENDS_WITH" | "SUFFIX" => {
            // Return NULL if either argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let suffix = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            Ok(Value::Boolean(s.ends_with(suffix)))
        }
        "CONTAINS" => {
            // Return NULL if either argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let needle = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            Ok(Value::Boolean(s.contains(needle)))
        }
        // Regular expression functions
        "REGEXP_MATCHES" | "REGEXP_LIKE" | "REGEXP" => {
            // Return NULL if either argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let pattern = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            match regex::Regex::new(pattern) {
                Ok(re) => Ok(Value::Boolean(re.is_match(s))),
                Err(_) => Ok(Value::Boolean(false)),
            }
        }
        "REGEXP_REPLACE" => {
            // Return NULL if any argument is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let pattern = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let replacement = match args.get(2) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            match regex::Regex::new(pattern) {
                Ok(re) => Ok(Value::Varchar(re.replace_all(s, replacement).to_string())),
                Err(_) => Ok(Value::Varchar(s.to_string())),
            }
        }
        "REGEXP_EXTRACT" | "REGEXP_SUBSTR" => {
            // Return NULL if string or pattern is NULL
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let pattern = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
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
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let pattern = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
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
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            let pattern = match args.get(1) {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
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
        "LIST_HAS_ANY" | "ARRAY_HAS_ANY" => {
            // Check if list1 has any elements in common with list2
            match (args.first(), args.get(1)) {
                (Some(Value::List(list1)), Some(Value::List(list2))) => {
                    let has_any = list1.iter().any(|v| list2.contains(v));
                    Ok(Value::Boolean(has_any))
                }
                (Some(Value::Null), _) | (_, Some(Value::Null)) => Ok(Value::Null),
                _ => Ok(Value::Boolean(false)),
            }
        }
        "LIST_HAS_ALL" | "ARRAY_HAS_ALL" => {
            // Check if list1 contains all elements from list2
            match (args.first(), args.get(1)) {
                (Some(Value::List(list1)), Some(Value::List(list2))) => {
                    let has_all = list2.iter().all(|v| list1.contains(v));
                    Ok(Value::Boolean(has_all))
                }
                (Some(Value::Null), _) | (_, Some(Value::Null)) => Ok(Value::Null),
                _ => Ok(Value::Boolean(false)),
            }
        }
        "ARRAY_EXTRACT" | "LIST_EXTRACT" | "LIST_ELEMENT" => {
            // Get element at index (1-based)
            match (args.first(), args.get(1)) {
                (Some(Value::List(list)), Some(idx)) => {
                    let index = idx.as_i64().unwrap_or(0);
                    if index <= 0 || index as usize > list.len() {
                        Ok(Value::Null)
                    } else {
                        Ok(list[(index - 1) as usize].clone())
                    }
                }
                (Some(Value::Null), _) | (_, Some(Value::Null)) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "ARRAY_SLICE" | "LIST_SLICE" => {
            // Get slice of array: array_slice(arr, start, end) - 1-based, inclusive
            match args.first() {
                Some(Value::List(list)) => {
                    let start = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1) as usize;
                    let end = args.get(2).and_then(|v| v.as_i64()).map(|e| e as usize).unwrap_or(list.len());

                    if start == 0 || start > list.len() {
                        return Ok(Value::List(vec![]));
                    }

                    let start_idx = start.saturating_sub(1);
                    let end_idx = end.min(list.len());

                    if start_idx >= end_idx {
                        Ok(Value::List(vec![]))
                    } else {
                        Ok(Value::List(list[start_idx..end_idx].to_vec()))
                    }
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::List(vec![])),
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
        "LIST_REVERSE_SORT" | "ARRAY_REVERSE_SORT" => {
            // Sort list in descending order
            match args.first() {
                Some(Value::List(list)) => {
                    let mut result = list.clone();
                    result.sort_by(|a, b| b.partial_cmp(a).unwrap_or(Ordering::Equal));
                    Ok(Value::List(result))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::List(vec![])),
            }
        }
        "LIST_COSINE_SIMILARITY" | "ARRAY_COSINE_SIMILARITY" => {
            // Calculate cosine similarity between two numeric lists
            // cosine_similarity(a, b) = dot(a, b) / (||a|| * ||b||)
            match (args.first(), args.get(1)) {
                (Some(Value::List(list1)), Some(Value::List(list2))) => {
                    if list1.len() != list2.len() {
                        return Ok(Value::Null);
                    }

                    let mut dot_product = 0.0f64;
                    let mut norm1 = 0.0f64;
                    let mut norm2 = 0.0f64;

                    for (v1, v2) in list1.iter().zip(list2.iter()) {
                        let f1 = v1.as_f64().unwrap_or(0.0);
                        let f2 = v2.as_f64().unwrap_or(0.0);
                        dot_product += f1 * f2;
                        norm1 += f1 * f1;
                        norm2 += f2 * f2;
                    }

                    let denominator = (norm1.sqrt() * norm2.sqrt());
                    if denominator == 0.0 {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Double(dot_product / denominator))
                    }
                }
                (Some(Value::Null), _) | (_, Some(Value::Null)) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_INNER_PRODUCT" | "ARRAY_INNER_PRODUCT" | "LIST_DOT" | "ARRAY_DOT" => {
            // Calculate inner product (dot product) between two numeric lists
            match (args.first(), args.get(1)) {
                (Some(Value::List(list1)), Some(Value::List(list2))) => {
                    if list1.len() != list2.len() {
                        return Ok(Value::Null);
                    }

                    let mut dot_product = 0.0f64;
                    for (v1, v2) in list1.iter().zip(list2.iter()) {
                        let f1 = v1.as_f64().unwrap_or(0.0);
                        let f2 = v2.as_f64().unwrap_or(0.0);
                        dot_product += f1 * f2;
                    }

                    Ok(Value::Double(dot_product))
                }
                (Some(Value::Null), _) | (_, Some(Value::Null)) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_ZIP" | "ARRAY_ZIP" => {
            // Combine multiple lists into a list of lists (element-wise)
            // LIST_ZIP([1, 2], ['a', 'b']) -> [[1, 'a'], [2, 'b']]
            if args.is_empty() {
                return Ok(Value::List(vec![]));
            }

            let lists: Vec<&Vec<Value>> = args.iter()
                .filter_map(|v| match v {
                    Value::List(l) => Some(l),
                    _ => None,
                })
                .collect();

            if lists.is_empty() {
                return Ok(Value::Null);
            }

            let min_len = lists.iter().map(|l| l.len()).min().unwrap_or(0);
            let mut result = Vec::new();

            for i in 0..min_len {
                let row: Vec<Value> = lists.iter()
                    .map(|l| l[i].clone())
                    .collect();
                result.push(Value::List(row));
            }

            Ok(Value::List(result))
        }
        "LIST_REDUCE" | "ARRAY_REDUCE" => {
            // Reduce list to a single value using a binary operation
            // LIST_REDUCE([1, 2, 3], (a, b) -> a + b) - but since we don't have lambdas,
            // we'll implement common operations: 'sum', 'product', 'min', 'max', 'concat'
            match (args.first(), args.get(1)) {
                (Some(Value::List(list)), Some(Value::Varchar(op))) => {
                    if list.is_empty() {
                        return Ok(Value::Null);
                    }

                    match op.to_uppercase().as_str() {
                        "SUM" | "+" => {
                            let sum: f64 = list.iter()
                                .filter_map(|v| v.as_f64())
                                .sum();
                            Ok(Value::Double(sum))
                        }
                        "PRODUCT" | "*" => {
                            let product: f64 = list.iter()
                                .filter_map(|v| v.as_f64())
                                .product();
                            Ok(Value::Double(product))
                        }
                        "MIN" => {
                            Ok(list.iter()
                                .filter(|v| !v.is_null())
                                .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                                .cloned()
                                .unwrap_or(Value::Null))
                        }
                        "MAX" => {
                            Ok(list.iter()
                                .filter(|v| !v.is_null())
                                .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                                .cloned()
                                .unwrap_or(Value::Null))
                        }
                        "CONCAT" | "||" => {
                            let result: String = list.iter()
                                .map(|v| value_to_string(v))
                                .collect();
                            Ok(Value::Varchar(result))
                        }
                        _ => Ok(Value::Null),
                    }
                }
                (Some(Value::Null), _) => Ok(Value::Null),
                _ => Ok(Value::Null),
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
        "LIST_PRODUCT" | "ARRAY_PRODUCT" => {
            match args.first() {
                Some(Value::List(list)) => {
                    let product = list.iter()
                        .filter(|v| !v.is_null())
                        .try_fold(1.0f64, |acc, v| {
                            v.as_f64().map(|f| acc * f)
                        });
                    match product {
                        Some(p) => Ok(Value::Double(p)),
                        None => Ok(Value::Null),
                    }
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_ANY" | "ARRAY_ANY" | "LIST_BOOL_OR" => {
            // Returns TRUE if any element is TRUE
            match args.first() {
                Some(Value::List(list)) => {
                    let any_true = list.iter().any(|v| {
                        matches!(v, Value::Boolean(true))
                    });
                    Ok(Value::Boolean(any_true))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_ALL" | "ARRAY_ALL" | "LIST_BOOL_AND" => {
            // Returns TRUE if all elements are TRUE
            match args.first() {
                Some(Value::List(list)) => {
                    if list.is_empty() {
                        return Ok(Value::Boolean(true));
                    }
                    let all_true = list.iter().all(|v| {
                        matches!(v, Value::Boolean(true))
                    });
                    Ok(Value::Boolean(all_true))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_COUNT" | "ARRAY_COUNT" => {
            // Count non-null elements in a list
            match args.first() {
                Some(Value::List(list)) => {
                    let count = list.iter().filter(|v| !v.is_null()).count();
                    Ok(Value::BigInt(count as i64))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "LIST_STRING_AGG" | "ARRAY_STRING_AGG" => {
            // Concatenate list elements with separator
            let list = match args.first() {
                Some(Value::List(list)) => list,
                Some(Value::Null) => return Ok(Value::Null),
                _ => return Ok(Value::Null),
            };
            let separator = args.get(1)
                .and_then(|v| v.as_str())
                .unwrap_or(",");

            let result: String = list.iter()
                .filter(|v| !v.is_null())
                .map(|v| value_to_string(v))
                .collect::<Vec<_>>()
                .join(separator);
            Ok(Value::Varchar(result))
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

        // Map functions
        "MAP" | "MAP_FROM_ENTRIES" => {
            // Create a map from key-value pairs
            // MAP([key1, key2, ...], [value1, value2, ...]) or
            // MAP_FROM_ENTRIES([[key1, value1], [key2, value2], ...])
            match args {
                [Value::List(keys), Value::List(values)] => {
                    // MAP(keys, values) form
                    let pairs: Vec<(Value, Value)> = keys.iter()
                        .zip(values.iter())
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    Ok(Value::Map(pairs))
                }
                [Value::List(entries)] => {
                    // MAP_FROM_ENTRIES form: list of [key, value] pairs
                    let pairs: Vec<(Value, Value)> = entries.iter()
                        .filter_map(|e| {
                            if let Value::List(pair) = e {
                                if pair.len() >= 2 {
                                    return Some((pair[0].clone(), pair[1].clone()));
                                }
                            }
                            None
                        })
                        .collect();
                    Ok(Value::Map(pairs))
                }
                _ => Ok(Value::Map(vec![])),
            }
        }
        "MAP_KEYS" => {
            // Extract keys from a map as a list
            match args.first() {
                Some(Value::Map(pairs)) => {
                    let keys: Vec<Value> = pairs.iter().map(|(k, _)| k.clone()).collect();
                    Ok(Value::List(keys))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::List(vec![])),
            }
        }
        "MAP_VALUES" => {
            // Extract values from a map as a list
            match args.first() {
                Some(Value::Map(pairs)) => {
                    let values: Vec<Value> = pairs.iter().map(|(_, v)| v.clone()).collect();
                    Ok(Value::List(values))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::List(vec![])),
            }
        }
        "MAP_ENTRIES" => {
            // Convert map to list of [key, value] pairs
            match args.first() {
                Some(Value::Map(pairs)) => {
                    let entries: Vec<Value> = pairs.iter()
                        .map(|(k, v)| Value::List(vec![k.clone(), v.clone()]))
                        .collect();
                    Ok(Value::List(entries))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::List(vec![])),
            }
        }
        "MAP_CONTAINS" | "MAP_HAS" => {
            // Check if map contains a key
            match (args.first(), args.get(1)) {
                (Some(Value::Map(pairs)), Some(key)) => {
                    let found = pairs.iter().any(|(k, _)| k == key);
                    Ok(Value::Boolean(found))
                }
                (Some(Value::Null), _) | (_, Some(Value::Null)) => Ok(Value::Null),
                _ => Ok(Value::Boolean(false)),
            }
        }
        "MAP_EXTRACT" | "ELEMENT_AT" => {
            // Extract value from map by key
            match (args.first(), args.get(1)) {
                (Some(Value::Map(pairs)), Some(key)) => {
                    let value = pairs.iter()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.clone())
                        .unwrap_or(Value::Null);
                    Ok(value)
                }
                _ => Ok(Value::Null),
            }
        }
        "CARDINALITY" => {
            // Return number of elements in map or list
            match args.first() {
                Some(Value::Map(pairs)) => Ok(Value::BigInt(pairs.len() as i64)),
                Some(Value::List(list)) => Ok(Value::BigInt(list.len() as i64)),
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::BigInt(0)),
            }
        }

        // Struct functions
        "STRUCT" | "ROW" => {
            // Create a struct from values
            // STRUCT(val1, val2, ...) - creates an unnamed struct
            let fields: Vec<(String, Value)> = args.iter().enumerate()
                .map(|(i, v)| (format!("v{}", i + 1), v.clone()))
                .collect();
            Ok(Value::Struct(fields))
        }
        "STRUCT_PACK" => {
            // Create a struct with named fields
            // Called like STRUCT_PACK(a := 1, b := 2) which we receive as alternating name, value
            let mut fields = Vec::new();
            let mut i = 0;
            while i + 1 < args.len() {
                if let Value::Varchar(name) = &args[i] {
                    fields.push((name.clone(), args[i + 1].clone()));
                }
                i += 2;
            }
            Ok(Value::Struct(fields))
        }
        "STRUCT_EXTRACT" => {
            // Extract a field from a struct by name
            match (args.first(), args.get(1)) {
                (Some(Value::Struct(fields)), Some(Value::Varchar(name))) => {
                    let value = fields.iter()
                        .find(|(n, _)| n == name)
                        .map(|(_, v)| v.clone())
                        .unwrap_or(Value::Null);
                    Ok(value)
                }
                _ => Ok(Value::Null),
            }
        }
        "STRUCT_KEYS" => {
            // Get field names from a struct
            match args.first() {
                Some(Value::Struct(fields)) => {
                    let keys: Vec<Value> = fields.iter()
                        .map(|(k, _)| Value::Varchar(k.clone()))
                        .collect();
                    Ok(Value::List(keys))
                }
                _ => Ok(Value::List(vec![])),
            }
        }
        "STRUCT_VALUES" => {
            // Get field values from a struct
            match args.first() {
                Some(Value::Struct(fields)) => {
                    let values: Vec<Value> = fields.iter()
                        .map(|(_, v)| v.clone())
                        .collect();
                    Ok(Value::List(values))
                }
                _ => Ok(Value::List(vec![])),
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
                Value::TinyInt(_) | Value::SmallInt(_) | Value::Integer(_) | Value::BigInt(_) => Ok(val.clone()),
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
                Value::TinyInt(_) | Value::SmallInt(_) | Value::Integer(_) | Value::BigInt(_) => Ok(val.clone()),
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
                Value::TinyInt(_) | Value::SmallInt(_) | Value::Integer(_) | Value::BigInt(_) => Ok(val.clone()),
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
            let val_a = args.first().unwrap_or(&Value::Null);
            let val_b = args.get(1).unwrap_or(&Value::Null);

            // Check for NULL
            if val_a.is_null() || val_b.is_null() {
                return Ok(Value::Null);
            }

            // Use floating-point modulo if either value is a float
            let is_float = matches!(val_a, Value::Float(_) | Value::Double(_) | Value::Decimal { .. })
                        || matches!(val_b, Value::Float(_) | Value::Double(_) | Value::Decimal { .. });

            if is_float {
                let a = val_a.as_f64().unwrap_or(0.0);
                let b = val_b.as_f64().unwrap_or(1.0);
                if b == 0.0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Double(a % b))
                }
            } else {
                let a = val_a.as_i64().unwrap_or(0);
                let b = val_b.as_i64().unwrap_or(1);
                if b == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::BigInt(a % b))
                }
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
            // Return NULL for NULL input
            match args.first() {
                Some(Value::Null) | None => return Ok(Value::Null),
                _ => {}
            }
            let n = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            if n < 0 {
                // DuckDB returns 1 for negative factorials
                Ok(Value::BigInt(1))
            } else if n > 33 {
                // DuckDB errors for factorial >= 34 (overflow in 128-bit integer)
                Err(Error::Execution(format!(
                    "Out of range error: cannot compute factorial of {}", n
                )))
            } else if n > 20 {
                // Use HugeInt for n > 20 (factorial(21) overflows i64)
                let mut result: i128 = 1;
                for i in 2..=n {
                    result = result.saturating_mul(i as i128);
                }
                Ok(Value::HugeInt(result))
            } else {
                let result: i64 = (1..=n).product();
                Ok(Value::BigInt(result))
            }
        }
        "EVEN" => {
            // Round to nearest even number
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let ceil = val.ceil();
            let result = if ceil as i64 % 2 == 0 {
                ceil
            } else if val >= 0.0 {
                ceil + 1.0
            } else {
                ceil - 1.0
            };
            Ok(Value::Double(result))
        }
        "ODD" => {
            // Round to nearest odd number
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let ceil = val.ceil();
            let result = if ceil as i64 % 2 != 0 {
                ceil
            } else if val >= 0.0 {
                ceil + 1.0
            } else {
                ceil - 1.0
            };
            Ok(Value::Double(result))
        }
        "DIV" => {
            // Integer division
            let a = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let b = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1);
            if b == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::BigInt(a / b))
            }
        }
        "ISNAN" => {
            let val = args.first().and_then(|v| v.as_f64());
            match val {
                Some(f) => Ok(Value::Boolean(f.is_nan())),
                None => Ok(Value::Null),
            }
        }
        "ISINF" => {
            let val = args.first().and_then(|v| v.as_f64());
            match val {
                Some(f) => Ok(Value::Boolean(f.is_infinite())),
                None => Ok(Value::Null),
            }
        }
        "ISFINITE" => {
            let val = args.first().and_then(|v| v.as_f64());
            match val {
                Some(f) => Ok(Value::Boolean(f.is_finite())),
                None => Ok(Value::Null),
            }
        }
        "GAMMA" | "LGAMMA" => {
            // Log gamma function (approximation using Stirling's formula)
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            if val <= 0.0 {
                Ok(Value::Null)
            } else {
                // Using Stirling's approximation for log(Gamma(x))
                let result = (val - 0.5) * val.ln() - val + 0.5 * (2.0 * std::f64::consts::PI).ln()
                    + 1.0 / (12.0 * val);
                Ok(Value::Double(result))
            }
        }
        "NEXTAFTER" => {
            // Return the next representable floating-point value after x towards y
            let x = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = args.get(1).and_then(|v| v.as_f64()).unwrap_or(1.0);
            if x == y {
                Ok(Value::Double(y))
            } else if y > x {
                // Get next higher value
                let bits = x.to_bits();
                let next_bits = if x >= 0.0 { bits + 1 } else { bits - 1 };
                Ok(Value::Double(f64::from_bits(next_bits)))
            } else {
                // Get next lower value
                let bits = x.to_bits();
                let next_bits = if x > 0.0 { bits - 1 } else { bits + 1 };
                Ok(Value::Double(f64::from_bits(next_bits)))
            }
        }
        "FMOD" => {
            // Floating-point modulo
            let x = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = args.get(1).and_then(|v| v.as_f64()).unwrap_or(1.0);
            if y == 0.0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Double(x % y))
            }
        }
        "COPYSIGN" => {
            // Return x with the sign of y
            let x = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = args.get(1).and_then(|v| v.as_f64()).unwrap_or(1.0);
            Ok(Value::Double(x.copysign(y)))
        }
        "HYPOT" => {
            // Return sqrt(x*x + y*y) without overflow
            let x = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = args.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(x.hypot(y)))
        }
        "LDEXP" => {
            // Return x * 2^exp
            let x = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let exp = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as i32;
            Ok(Value::Double(x * (2.0_f64).powi(exp)))
        }
        "SIGNBIT" => {
            // Return true if sign bit is set (including -0.0)
            let val = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Boolean(val.is_sign_negative()))
        }
        "FDIM" => {
            // Return positive difference: max(x - y, 0)
            let x = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = args.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double((x - y).max(0.0)))
        }
        "FMA" => {
            // Fused multiply-add: x * y + z
            let x = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = args.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
            let z = args.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(Value::Double(x.mul_add(y, z)))
        }
        "REMAINDER" | "IEEE_REMAINDER" => {
            // IEEE 754 remainder
            let x = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = args.get(1).and_then(|v| v.as_f64()).unwrap_or(1.0);
            if y == 0.0 {
                Ok(Value::Null)
            } else {
                // IEEE remainder: x - n*y where n is the integer nearest x/y
                let n = (x / y).round();
                Ok(Value::Double(x - n * y))
            }
        }

        // UUID functions
        "GEN_RANDOM_UUID" | "UUID" => {
            Ok(Value::Uuid(uuid::Uuid::new_v4()))
        }

        // Hash functions
        "HASH" | "MD5" => {
            // Return NULL for NULL input
            match args.first() {
                Some(Value::Null) | None => return Ok(Value::Null),
                _ => {}
            }
            use std::hash::{Hash, Hasher};
            use std::collections::hash_map::DefaultHasher;
            let s = value_to_string(args.first().unwrap_or(&Value::Null));
            let mut hasher = DefaultHasher::new();
            s.hash(&mut hasher);
            Ok(Value::BigInt(hasher.finish() as i64))
        }
        "SHA256" | "SHA2" => {
            // Return NULL for NULL input
            match args.first() {
                Some(Value::Null) | None => return Ok(Value::Null),
                _ => {}
            }
            // Simple SHA-256 implementation for string hashing
            let s = value_to_string(args.first().unwrap_or(&Value::Null));
            let bytes = s.as_bytes();

            // SHA-256 constants
            const K: [u32; 64] = [
                0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
                0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
                0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
                0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
                0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
                0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
                0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
                0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
            ];

            let mut h: [u32; 8] = [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
            ];

            // Padding
            let bit_len = (bytes.len() as u64) * 8;
            let mut padded = bytes.to_vec();
            padded.push(0x80);
            while (padded.len() % 64) != 56 {
                padded.push(0);
            }
            padded.extend_from_slice(&bit_len.to_be_bytes());

            // Process each 512-bit chunk
            for chunk in padded.chunks(64) {
                let mut w = [0u32; 64];
                for (i, c) in chunk.chunks(4).enumerate() {
                    w[i] = u32::from_be_bytes([c[0], c[1], c[2], c[3]]);
                }
                for i in 16..64 {
                    let s0 = w[i-15].rotate_right(7) ^ w[i-15].rotate_right(18) ^ (w[i-15] >> 3);
                    let s1 = w[i-2].rotate_right(17) ^ w[i-2].rotate_right(19) ^ (w[i-2] >> 10);
                    w[i] = w[i-16].wrapping_add(s0).wrapping_add(w[i-7]).wrapping_add(s1);
                }

                let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
                    (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);

                for i in 0..64 {
                    let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
                    let ch = (e & f) ^ ((!e) & g);
                    let temp1 = hh.wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
                    let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
                    let maj = (a & b) ^ (a & c) ^ (b & c);
                    let temp2 = s0.wrapping_add(maj);

                    hh = g; g = f; f = e;
                    e = d.wrapping_add(temp1);
                    d = c; c = b; b = a;
                    a = temp1.wrapping_add(temp2);
                }

                h[0] = h[0].wrapping_add(a); h[1] = h[1].wrapping_add(b);
                h[2] = h[2].wrapping_add(c); h[3] = h[3].wrapping_add(d);
                h[4] = h[4].wrapping_add(e); h[5] = h[5].wrapping_add(f);
                h[6] = h[6].wrapping_add(g); h[7] = h[7].wrapping_add(hh);
            }

            // Convert to hex string
            let hash = format!(
                "{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}{:08x}",
                h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]
            );
            Ok(Value::Varchar(hash))
        }

        // Bit manipulation functions
        "BIT_COUNT" => {
            let val = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(Value::Integer(val.count_ones() as i32))
        }
        "BIT_LENGTH" => {
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            Ok(Value::Integer((s.len() * 8) as i32))
        }
        "OCTET_LENGTH" | "BYTE_LENGTH" => {
            let s = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or(""),
                None => "",
            };
            Ok(Value::Integer(s.len() as i32))
        }
        "BIT_AND" | "BITAND" => {
            let a = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let b = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(Value::BigInt(a & b))
        }
        "BIT_OR" | "BITOR" => {
            let a = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let b = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(Value::BigInt(a | b))
        }
        "BIT_XOR" | "BITXOR" | "XOR" => {
            let a = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let b = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(Value::BigInt(a ^ b))
        }
        "BIT_NOT" | "BITNOT" => {
            let a = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(Value::BigInt(!a))
        }
        "LEFT_SHIFT" | "LSHIFT" | "SHIFTLEFT" => {
            let a = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let b = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as u32;
            Ok(Value::BigInt(a << b.min(63)))
        }
        "RIGHT_SHIFT" | "RSHIFT" | "SHIFTRIGHT" => {
            let a = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let b = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as u32;
            Ok(Value::BigInt(a >> b.min(63)))
        }
        "GET_BIT" => {
            // Get the bit at position n (0-indexed from right)
            let val = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let pos = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as u32;
            if pos >= 64 {
                Ok(Value::Integer(0))
            } else {
                Ok(Value::Integer(((val >> pos) & 1) as i32))
            }
        }
        "SET_BIT" => {
            // Set the bit at position n to 1 or 0
            let val = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let pos = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as u32;
            let new_val = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
            if pos >= 64 {
                Ok(Value::BigInt(val))
            } else if new_val != 0 {
                Ok(Value::BigInt(val | (1 << pos)))
            } else {
                Ok(Value::BigInt(val & !(1 << pos)))
            }
        }
        "BITSTRING" => {
            // Convert integer to binary string
            let val = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let width = args.get(1).and_then(|v| v.as_i64()).unwrap_or(64) as usize;
            let binary = format!("{:0>width$b}", val.abs(), width = width.min(64));
            Ok(Value::Varchar(binary))
        }
        "BIT_POSITION" => {
            // Find the position of the first set bit (1-indexed from right, 0 if no bit set)
            let val = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            if val == 0 {
                Ok(Value::Integer(0))
            } else {
                // Find position of lowest set bit (1-indexed)
                Ok(Value::Integer((val.trailing_zeros() + 1) as i32))
            }
        }

        // Format function
        "FORMAT" | "PRINTF" | "SPRINTF" => {
            // Return NULL for NULL format string
            match args.first() {
                Some(Value::Null) | None => return Ok(Value::Null),
                _ => {}
            }
            let template = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let mut arg_idx = 1;

            // Process format specifiers
            let mut i = 0;
            let chars: Vec<char> = template.chars().collect();
            let mut new_result = String::new();
            while i < chars.len() {
                if chars[i] == '%' && i + 1 < chars.len() {
                    let mut j = i + 1;

                    // Skip flags: -, +, space, #, 0
                    let mut alt_form = false;
                    let mut zero_pad = false;
                    let mut left_align = false;
                    let mut plus_sign = false;
                    while j < chars.len() && matches!(chars[j], '-' | '+' | ' ' | '#' | '0') {
                        if chars[j] == '#' { alt_form = true; }
                        if chars[j] == '0' { zero_pad = true; }
                        if chars[j] == '-' { left_align = true; }
                        if chars[j] == '+' { plus_sign = true; }
                        j += 1;
                    }

                    // Parse width (could be * for dynamic width)
                    let mut width: usize = 0;
                    if j < chars.len() && chars[j] == '*' {
                        // Dynamic width from argument
                        width = args.get(arg_idx).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
                        arg_idx += 1;
                        j += 1;
                    } else {
                        while j < chars.len() && chars[j].is_ascii_digit() {
                            width = width * 10 + (chars[j] as usize - '0' as usize);
                            j += 1;
                        }
                    }

                    // Parse precision
                    let mut precision: Option<usize> = None;
                    if j < chars.len() && chars[j] == '.' {
                        j += 1;
                        let mut prec = 0;
                        while j < chars.len() && chars[j].is_ascii_digit() {
                            prec = prec * 10 + (chars[j] as usize - '0' as usize);
                            j += 1;
                        }
                        precision = Some(prec);
                    }

                    // Skip size modifiers: hh, h, l, ll
                    while j < chars.len() && matches!(chars[j], 'h' | 'l') {
                        j += 1;
                    }

                    if j < chars.len() {
                        let type_char = chars[j];
                        match type_char {
                            '%' => {
                                new_result.push('%');
                                i = j + 1;
                                continue;
                            }
                            's' => {
                                let arg = args.get(arg_idx).cloned().unwrap_or(Value::Null);
                                let s = match &arg {
                                    Value::Null => "NULL".to_string(),
                                    _ => value_to_string(&arg),
                                };
                                let formatted = if left_align && width > 0 {
                                    format!("{:<width$}", s, width = width)
                                } else if width > 0 {
                                    format!("{:>width$}", s, width = width)
                                } else {
                                    s
                                };
                                new_result.push_str(&formatted);
                                arg_idx += 1;
                                i = j + 1;
                                continue;
                            }
                            'd' | 'i' => {
                                let arg = args.get(arg_idx).cloned().unwrap_or(Value::Null);
                                // Handle boolean explicitly
                                let num = match &arg {
                                    Value::Boolean(true) => 1i64,
                                    Value::Boolean(false) => 0i64,
                                    _ => arg.as_i64().unwrap_or(0),
                                };
                                let formatted = if zero_pad && width > 0 && !left_align {
                                    format!("{:0>width$}", num, width = width)
                                } else if left_align && width > 0 {
                                    format!("{:<width$}", num, width = width)
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
                            'x' => {
                                let arg = args.get(arg_idx).cloned().unwrap_or(Value::Null);
                                let num = arg.as_i64().unwrap_or(0);
                                let formatted = if alt_form {
                                    format!("{:#x}", num)
                                } else {
                                    format!("{:x}", num)
                                };
                                new_result.push_str(&formatted);
                                arg_idx += 1;
                                i = j + 1;
                                continue;
                            }
                            'X' => {
                                let arg = args.get(arg_idx).cloned().unwrap_or(Value::Null);
                                let num = arg.as_i64().unwrap_or(0);
                                let formatted = if alt_form {
                                    format!("{:#X}", num)
                                } else {
                                    format!("{:X}", num)
                                };
                                new_result.push_str(&formatted);
                                arg_idx += 1;
                                i = j + 1;
                                continue;
                            }
                            'o' => {
                                let arg = args.get(arg_idx).cloned().unwrap_or(Value::Null);
                                let num = arg.as_i64().unwrap_or(0);
                                let formatted = if alt_form {
                                    // DuckDB uses 0 prefix, not 0o
                                    format!("0{:o}", num)
                                } else {
                                    format!("{:o}", num)
                                };
                                new_result.push_str(&formatted);
                                arg_idx += 1;
                                i = j + 1;
                                continue;
                            }
                            'c' => {
                                let arg = args.get(arg_idx).cloned().unwrap_or(Value::Null);
                                let ch = arg.as_i64().unwrap_or(0) as u8 as char;
                                new_result.push(ch);
                                arg_idx += 1;
                                i = j + 1;
                                continue;
                            }
                            'f' | 'F' => {
                                let arg = args.get(arg_idx).cloned().unwrap_or(Value::Null);
                                let num = arg.as_f64().unwrap_or(0.0);
                                let prec = precision.unwrap_or(6);
                                let formatted = format!("{:.prec$}", num, prec = prec);
                                new_result.push_str(&formatted);
                                arg_idx += 1;
                                i = j + 1;
                                continue;
                            }
                            'e' => {
                                let arg = args.get(arg_idx).cloned().unwrap_or(Value::Null);
                                let num = arg.as_f64().unwrap_or(0.0);
                                let prec = precision.unwrap_or(6);
                                let formatted = format!("{:.prec$e}", num, prec = prec);
                                // DuckDB uses two-digit exponent with + sign
                                let formatted = if let Some(e_pos) = formatted.find('e') {
                                    let (base, exp) = formatted.split_at(e_pos);
                                    let exp_part = &exp[1..]; // skip 'e'
                                    let (sign, exp_num) = if exp_part.starts_with('-') {
                                        ("-", &exp_part[1..])
                                    } else if exp_part.starts_with('+') {
                                        ("+", &exp_part[1..])
                                    } else {
                                        ("+", exp_part)
                                    };
                                    // Add + prefix for positive numbers if plus_sign flag is set
                                    let prefix = if plus_sign && num >= 0.0 { "+" } else { "" };
                                    format!("{}{}e{}{:02}", prefix, base, sign, exp_num.parse::<i32>().unwrap_or(0))
                                } else {
                                    formatted
                                };
                                new_result.push_str(&formatted);
                                arg_idx += 1;
                                i = j + 1;
                                continue;
                            }
                            'E' => {
                                let arg = args.get(arg_idx).cloned().unwrap_or(Value::Null);
                                let num = arg.as_f64().unwrap_or(0.0);
                                let prec = precision.unwrap_or(6);
                                let formatted = format!("{:.prec$E}", num, prec = prec);
                                // DuckDB uses two-digit exponent with + sign
                                let formatted = if let Some(e_pos) = formatted.find('E') {
                                    let (base, exp) = formatted.split_at(e_pos);
                                    let exp_part = &exp[1..]; // skip 'E'
                                    let (sign, exp_num) = if exp_part.starts_with('-') {
                                        ("-", &exp_part[1..])
                                    } else if exp_part.starts_with('+') {
                                        ("+", &exp_part[1..])
                                    } else {
                                        ("+", exp_part)
                                    };
                                    format!("{}E{}{:02}", base, sign, exp_num.parse::<i32>().unwrap_or(0))
                                } else {
                                    formatted
                                };
                                new_result.push_str(&formatted);
                                arg_idx += 1;
                                i = j + 1;
                                continue;
                            }
                            'g' | 'G' => {
                                let arg = args.get(arg_idx).cloned().unwrap_or(Value::Null);
                                let num = arg.as_f64().unwrap_or(0.0);
                                new_result.push_str(&format!("{}", num));
                                arg_idx += 1;
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
        "NVL2" => {
            // NVL2(expr, val_if_not_null, val_if_null)
            let first = args.first().unwrap_or(&Value::Null);
            if first.is_null() {
                Ok(args.get(2).cloned().unwrap_or(Value::Null))
            } else {
                Ok(args.get(1).cloned().unwrap_or(Value::Null))
            }
        }
        "DECODE" => {
            // DECODE(expr, search1, result1, search2, result2, ..., default)
            // Returns result for first matching search value
            let expr = args.first().unwrap_or(&Value::Null);
            let mut i = 1;
            while i + 1 < args.len() {
                if &args[i] == expr {
                    return Ok(args[i + 1].clone());
                }
                i += 2;
            }
            // Return default if no match (last arg if odd number of remaining args)
            if i < args.len() {
                Ok(args[i].clone())
            } else {
                Ok(Value::Null)
            }
        }
        "CHOOSE" => {
            // CHOOSE(index, val1, val2, val3, ...)
            // Returns val at 1-based index
            let index = args.first().and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            if index >= 1 && index < args.len() {
                Ok(args[index].clone())
            } else {
                Ok(Value::Null)
            }
        }
        "ZEROIFNULL" => {
            let first = args.first().unwrap_or(&Value::Null);
            if first.is_null() {
                Ok(Value::Integer(0))
            } else {
                Ok(first.clone())
            }
        }
        "NULLIFZERO" => {
            match args.first() {
                Some(Value::Integer(0)) | Some(Value::BigInt(0)) => Ok(Value::Null),
                Some(Value::Float(f)) if *f == 0.0 => Ok(Value::Null),
                Some(Value::Double(f)) if *f == 0.0 => Ok(Value::Null),
                Some(v) => Ok(v.clone()),
                None => Ok(Value::Null),
            }
        }
        "IFNOT" => {
            // IFNOT(cond, val) returns val if cond is false
            let cond = args.first().map(|v| matches!(v, Value::Boolean(true))).unwrap_or(false);
            if !cond {
                Ok(args.get(1).cloned().unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }

        // Type functions
        "TYPEOF" => {
            let val = args.first().unwrap_or(&Value::Null);
            Ok(Value::Varchar(val.logical_type().to_string()))
        }

        // System/utility functions
        "VERSION" => {
            Ok(Value::Varchar("IronDuck 0.1.0 (DuckDB compatible)".to_string()))
        }
        "CURRENT_DATABASE" | "DATABASE" => {
            Ok(Value::Varchar("memory".to_string()))
        }
        "CURRENT_SCHEMA" | "SCHEMA" => {
            Ok(Value::Varchar("main".to_string()))
        }
        "CURRENT_USER" | "USER" | "SESSION_USER" => {
            Ok(Value::Varchar("ironduck".to_string()))
        }
        "CURRENT_CATALOG" => {
            Ok(Value::Varchar("memory".to_string()))
        }
        "CURRENT_SETTING" => {
            // Return default settings
            let setting = args.first().and_then(|v| v.as_str()).unwrap_or("");
            match setting.to_lowercase().as_str() {
                "timezone" => Ok(Value::Varchar("UTC".to_string())),
                "memory_limit" => Ok(Value::Varchar("unlimited".to_string())),
                "threads" => Ok(Value::Varchar("1".to_string())),
                _ => Ok(Value::Null),
            }
        }
        "ALIAS" => {
            // Returns the alias of a column (for display purposes)
            args.first().cloned().ok_or_else(|| Error::InvalidArguments("ALIAS requires an argument".to_string()))
        }
        "ERROR" => {
            // Raise an error with the given message
            let msg = args.first().and_then(|v| v.as_str()).unwrap_or("User error");
            Err(Error::NotImplemented(msg.to_string()))
        }
        "CONSTANT_OR_NULL" => {
            // Returns constant if all rows have same value, NULL otherwise
            // For scalar context, just return the first arg
            args.first().cloned().ok_or_else(|| Error::InvalidArguments("CONSTANT_OR_NULL requires an argument".to_string()))
        }
        "STATS" => {
            // Return stats about a column (simplified)
            Ok(Value::Varchar("Statistics not available".to_string()))
        }
        "PG_TYPEOF" => {
            // PostgreSQL compatibility - return type name
            let val = args.first().unwrap_or(&Value::Null);
            let type_name = match val.logical_type() {
                LogicalType::Integer => "integer",
                LogicalType::BigInt => "bigint",
                LogicalType::SmallInt => "smallint",
                LogicalType::TinyInt => "tinyint",
                LogicalType::Float => "real",
                LogicalType::Double => "double precision",
                LogicalType::Varchar => "text",
                LogicalType::Boolean => "boolean",
                LogicalType::Date => "date",
                LogicalType::Timestamp => "timestamp without time zone",
                LogicalType::Time => "time without time zone",
                _ => "unknown",
            };
            Ok(Value::Varchar(type_name.to_string()))
        }
        "PG_COLUMN_SIZE" | "PG_RELATION_SIZE" | "PG_TABLE_SIZE" | "PG_TOTAL_RELATION_SIZE" => {
            // PostgreSQL compatibility - return approximate size in bytes
            match args.first() {
                Some(Value::Varchar(s)) => Ok(Value::BigInt(s.len() as i64)),
                Some(Value::List(l)) => {
                    let size: i64 = l.iter().map(|v| {
                        match v {
                            Value::Varchar(s) => s.len() as i64,
                            Value::BigInt(_) | Value::Integer(_) => 8,
                            Value::Double(_) | Value::Float(_) => 8,
                            Value::Boolean(_) => 1,
                            _ => 8,
                        }
                    }).sum();
                    Ok(Value::BigInt(size))
                }
                Some(v) => {
                    // Estimate size based on type
                    let size = match v {
                        Value::Null => 0,
                        Value::Boolean(_) => 1,
                        Value::TinyInt(_) => 1,
                        Value::SmallInt(_) => 2,
                        Value::Integer(_) => 4,
                        Value::BigInt(_) => 8,
                        Value::Float(_) => 4,
                        Value::Double(_) => 8,
                        Value::Varchar(s) => s.len() as i64,
                        _ => 8,
                    };
                    Ok(Value::BigInt(size))
                }
                None => Ok(Value::BigInt(0)),
            }
        }
        "PG_DATABASE_SIZE" => {
            // Return a placeholder database size
            Ok(Value::BigInt(0))
        }
        "PG_TABLESPACE_SIZE" => {
            // Return a placeholder tablespace size
            Ok(Value::BigInt(0))
        }
        "PG_INDEXES_SIZE" => {
            // Return a placeholder indexes size
            Ok(Value::BigInt(0))
        }
        "PG_GET_EXPR" => {
            // Return expression text - simplified
            Ok(args.first().cloned().unwrap_or(Value::Null))
        }
        "PG_GET_CONSTRAINTDEF" | "PG_GET_INDEXDEF" | "PG_GET_VIEWDEF" | "PG_GET_TRIGGERDEF" => {
            // Return definition text - placeholder
            Ok(Value::Varchar("".to_string()))
        }
        "PG_RELATION_FILEPATH" => {
            // Return file path for a relation - placeholder
            Ok(Value::Varchar("".to_string()))
        }
        "PG_BACKEND_PID" => {
            // Return current process ID
            Ok(Value::Integer(std::process::id() as i32))
        }
        "PG_CURRENT_XACT_ID" | "PG_CURRENT_SNAPSHOT" => {
            // Transaction ID placeholder
            Ok(Value::BigInt(1))
        }
        "PG_IS_IN_RECOVERY" | "PG_IS_WAL_REPLAY_PAUSED" => {
            // Recovery status - always false for IronDuck
            Ok(Value::Boolean(false))
        }
        "PG_POSTMASTER_START_TIME" | "PG_CONF_LOAD_TIME" => {
            // Return current timestamp as placeholder
            use chrono::Local;
            Ok(Value::Timestamp(Local::now().naive_local()))
        }
        "PG_STAT_GET_NUMSCANS" | "PG_STAT_GET_TUPLES_RETURNED" | "PG_STAT_GET_TUPLES_FETCHED" |
        "PG_STAT_GET_TUPLES_INSERTED" | "PG_STAT_GET_TUPLES_UPDATED" | "PG_STAT_GET_TUPLES_DELETED" => {
            // Statistics placeholders
            Ok(Value::BigInt(0))
        }
        "PG_HAS_ROLE" | "PG_HAS_TABLE_PRIVILEGE" | "PG_HAS_COLUMN_PRIVILEGE" |
        "PG_HAS_DATABASE_PRIVILEGE" | "PG_HAS_SCHEMA_PRIVILEGE" | "PG_HAS_TABLESPACE_PRIVILEGE" => {
            // Privilege check - always true for IronDuck
            Ok(Value::Boolean(true))
        }
        "PG_CLIENT_ENCODING" => {
            Ok(Value::Varchar("UTF8".to_string()))
        }
        "PG_ENCODING_TO_CHAR" => {
            Ok(Value::Varchar("UTF8".to_string()))
        }
        "PG_CHAR_TO_ENCODING" => {
            Ok(Value::Integer(6)) // UTF8 encoding
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
            use chrono::{Datelike, Timelike, NaiveDate, NaiveDateTime};
            // Return NULL if part is NULL
            let part = match args.first() {
                Some(Value::Null) => return Ok(Value::Null),
                Some(v) => v.as_str().unwrap_or("").to_uppercase(),
                None => "".to_string(),
            };
            let ts = args.get(1).unwrap_or(&Value::Null);

            // Extract datetime components - also handle VARCHAR by parsing
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
                Value::Varchar(s) => {
                    // Try to parse as date or timestamp
                    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        (
                            d.year() as i64,
                            d.month() as i64,
                            d.day() as i64,
                            0, 0, 0,
                            d.weekday().num_days_from_sunday() as i64,
                            d.ordinal() as i64,
                        )
                    } else if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                        (
                            dt.year() as i64,
                            dt.month() as i64,
                            dt.day() as i64,
                            dt.hour() as i64,
                            dt.minute() as i64,
                            dt.second() as i64,
                            dt.weekday().num_days_from_sunday() as i64,
                            dt.ordinal() as i64,
                        )
                    } else {
                        return Err(Error::TypeMismatch {
                            expected: "timestamp or date".to_string(),
                            got: format!("Varchar(\"{}\")", s),
                        });
                    }
                }
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
                "DECADE" => year / 10,
                "CENTURY" => if year > 0 { (year - 1) / 100 + 1 } else { year / 100 },
                "MILLENNIUM" => if year > 0 { (year - 1) / 1000 + 1 } else { year / 1000 },
                "ISODOW" => {
                    // ISO day of week: Monday=1 through Sunday=7
                    match ts {
                        Value::Timestamp(dt) => dt.weekday().number_from_monday() as i64,
                        Value::Date(d) => d.weekday().number_from_monday() as i64,
                        _ => day_of_week,
                    }
                }
                "YEARWEEK" => {
                    // ISO year * 100 + ISO week
                    match ts {
                        Value::Timestamp(dt) => dt.iso_week().year() as i64 * 100 + dt.iso_week().week() as i64,
                        Value::Date(d) => d.iso_week().year() as i64 * 100 + d.iso_week().week() as i64,
                        _ => 0,
                    }
                }
                "EPOCH" => {
                    // Seconds since 1970-01-01
                    match ts {
                        Value::Timestamp(dt) => dt.and_utc().timestamp(),
                        Value::Date(d) => {
                            use chrono::NaiveTime;
                            let dt = d.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                            dt.and_utc().timestamp()
                        }
                        _ => 0,
                    }
                }
                "MILLISECONDS" | "MILLISECOND" => {
                    // Milliseconds of the current second (0-999) plus full seconds
                    match ts {
                        Value::Timestamp(dt) => (second * 1000) + (dt.nanosecond() / 1_000_000) as i64,
                        _ => second * 1000,
                    }
                }
                "MICROSECONDS" | "MICROSECOND" => {
                    // Microseconds of the current second plus full seconds
                    match ts {
                        Value::Timestamp(dt) => (second * 1_000_000) + (dt.nanosecond() / 1000) as i64,
                        _ => second * 1_000_000,
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

            match ts {
                Value::Timestamp(dt) => {
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
                        "WEEK" => {
                            // Truncate to start of week (Monday)
                            let weekday = dt.weekday().num_days_from_monday();
                            let start_of_week = dt.date() - chrono::Duration::days(weekday as i64);
                            NaiveDateTime::new(start_of_week, NaiveTime::from_hms_opt(0, 0, 0).unwrap())
                        }
                        "MONTH" => NaiveDateTime::new(
                            NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1).unwrap(),
                            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                        ),
                        "QUARTER" => {
                            let quarter_month = ((dt.month() - 1) / 3) * 3 + 1;
                            NaiveDateTime::new(
                                NaiveDate::from_ymd_opt(dt.year(), quarter_month, 1).unwrap(),
                                NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                            )
                        }
                        "YEAR" => NaiveDateTime::new(
                            NaiveDate::from_ymd_opt(dt.year(), 1, 1).unwrap(),
                            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                        ),
                        _ => return Err(Error::NotImplemented(format!("DATE_TRUNC field: {}", part))),
                    };
                    Ok(Value::Timestamp(truncated))
                }
                Value::Date(d) => {
                    let truncated = match part.as_str() {
                        "DAY" => *d,
                        "WEEK" => {
                            // Truncate to start of week (Monday)
                            let weekday = d.weekday().num_days_from_monday();
                            *d - chrono::Duration::days(weekday as i64)
                        }
                        "MONTH" => NaiveDate::from_ymd_opt(d.year(), d.month(), 1).unwrap(),
                        "QUARTER" => {
                            let quarter_month = ((d.month() - 1) / 3) * 3 + 1;
                            NaiveDate::from_ymd_opt(d.year(), quarter_month, 1).unwrap()
                        }
                        "YEAR" => NaiveDate::from_ymd_opt(d.year(), 1, 1).unwrap(),
                        _ => return Err(Error::NotImplemented(format!("DATE_TRUNC field: {}", part))),
                    };
                    Ok(Value::Date(truncated))
                }
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "timestamp or date".to_string(),
                    got: format!("{:?}", ts),
                }),
            }
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
            use chrono::{Duration, Datelike, NaiveTime};
            let date = args.first().unwrap_or(&Value::Null);
            let interval_arg = args.get(1).unwrap_or(&Value::Null);

            // Handle Interval type as second argument
            let (months, days, micros) = match interval_arg {
                Value::Interval(iv) => (iv.months as i64, iv.days as i64, iv.micros),
                Value::Null => return Ok(Value::Null),
                _ => {
                    // Legacy: integer + unit string format
                    let interval = interval_arg.as_i64().unwrap_or(0);
                    let unit = args.get(2).and_then(|v| v.as_str()).unwrap_or("day").to_uppercase();
                    match unit.as_str() {
                        "DAY" | "DAYS" => (0, interval, 0),
                        "MONTH" | "MONTHS" => (interval, 0, 0),
                        "YEAR" | "YEARS" => (interval * 12, 0, 0),
                        "HOUR" | "HOURS" => (0, 0, interval * 3_600_000_000),
                        "MINUTE" | "MINUTES" => (0, 0, interval * 60_000_000),
                        "SECOND" | "SECONDS" => (0, 0, interval * 1_000_000),
                        _ => return Err(Error::NotImplemented(format!("DATE_ADD unit: {}", unit))),
                    }
                }
            };

            match date {
                Value::Date(d) => {
                    // Apply months
                    let mut result = if months != 0 {
                        let new_month = d.month() as i64 + months;
                        let years_delta = if new_month > 0 { (new_month - 1) / 12 } else { (new_month - 12) / 12 };
                        let new_month = ((new_month - 1).rem_euclid(12) + 1) as u32;
                        let new_year = d.year() + years_delta as i32;
                        chrono::NaiveDate::from_ymd_opt(new_year, new_month, d.day().min(28))
                            .unwrap_or(*d)
                    } else {
                        *d
                    };
                    // Apply days
                    result = result + Duration::days(days);
                    // Return timestamp with time component from micros
                    let time_micros = micros.rem_euclid(86_400_000_000);
                    let ts = result.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
                        + Duration::microseconds(time_micros);
                    Ok(Value::Timestamp(ts))
                }
                Value::Timestamp(ts) => {
                    // Apply months
                    let mut result = if months != 0 {
                        let d = ts.date();
                        let new_month = d.month() as i64 + months;
                        let years_delta = if new_month > 0 { (new_month - 1) / 12 } else { (new_month - 12) / 12 };
                        let new_month = ((new_month - 1).rem_euclid(12) + 1) as u32;
                        let new_year = d.year() + years_delta as i32;
                        let new_date = chrono::NaiveDate::from_ymd_opt(new_year, new_month, d.day().min(28))
                            .unwrap_or(d);
                        new_date.and_time(ts.time())
                    } else {
                        *ts
                    };
                    // Apply days and micros
                    result = result + Duration::days(days) + Duration::microseconds(micros);
                    Ok(Value::Timestamp(result))
                }
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "date or timestamp".to_string(),
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
        "MAKE_TIME" => {
            use chrono::NaiveTime;
            let hour = args.first().and_then(|v| v.as_i64()).unwrap_or(0) as u32;
            let minute = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as u32;
            let second = args.get(2).and_then(|v| v.as_i64()).unwrap_or(0) as u32;

            match NaiveTime::from_hms_opt(hour, minute, second) {
                Some(t) => Ok(Value::Time(t)),
                None => Ok(Value::Null),
            }
        }
        "TO_DAYS" => {
            // Convert date to number of days since year 0 (Julian day number style)
            // DuckDB: Returns the number of days since year 0
            use chrono::Datelike;
            match args.first() {
                Some(Value::Date(d)) => {
                    // Days from year 1 to this date
                    // Simplified calculation: days from epoch + epoch offset
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let days_from_epoch = (*d - epoch).num_days();
                    // Days from year 0 to epoch (approximately 719528)
                    let epoch_days = 719528i64;
                    Ok(Value::BigInt(epoch_days + days_from_epoch))
                }
                Some(Value::Timestamp(dt)) => {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let days_from_epoch = (dt.date() - epoch).num_days();
                    let epoch_days = 719528i64;
                    Ok(Value::BigInt(epoch_days + days_from_epoch))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "FROM_DAYS" => {
            // Convert number of days since year 0 to date
            let days = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            // Days from year 0 to epoch
            let epoch_days = 719528i64;
            let days_from_epoch = days - epoch_days;
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            match epoch.checked_add_signed(chrono::Duration::days(days_from_epoch)) {
                Some(d) => Ok(Value::Date(d)),
                None => Ok(Value::Null),
            }
        }
        "TIMEZONE" | "AT_TIMEZONE" => {
            // Convert timestamp to a different timezone
            // TIMEZONE(timezone, timestamp) or timestamp AT TIME ZONE 'timezone'
            // For simplicity, we handle common timezone offsets
            use chrono::{Duration, Timelike};
            let tz_str = args.first().and_then(|v| v.as_str()).unwrap_or("UTC");
            let ts = args.get(1).unwrap_or(&Value::Null);

            // Parse timezone offset (e.g., "+05:00", "-08:00", "UTC", "GMT")
            let offset_hours: i64 = if tz_str.eq_ignore_ascii_case("UTC") || tz_str.eq_ignore_ascii_case("GMT") {
                0
            } else if tz_str.starts_with('+') || tz_str.starts_with('-') {
                // Parse offset like "+05:00" or "-08:00"
                let sign = if tz_str.starts_with('-') { -1 } else { 1 };
                let parts: Vec<&str> = tz_str[1..].split(':').collect();
                let hours = parts.first().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
                let minutes = parts.get(1).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
                sign * (hours * 60 + minutes) / 60
            } else {
                // Common timezone abbreviations
                match tz_str.to_uppercase().as_str() {
                    "EST" => -5, "EDT" => -4, "CST" => -6, "CDT" => -5,
                    "MST" => -7, "MDT" => -6, "PST" => -8, "PDT" => -7,
                    "CET" => 1, "CEST" => 2, "JST" => 9, "IST" => 5,
                    _ => 0,
                }
            };

            match ts {
                Value::Timestamp(dt) => {
                    let adjusted = *dt + Duration::hours(offset_hours);
                    Ok(Value::Timestamp(adjusted))
                }
                Value::Null => Ok(Value::Null),
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
        "DAYNAME" => {
            use chrono::Datelike;
            let weekday = match args.first() {
                Some(Value::Date(d)) => Some(d.weekday()),
                Some(Value::Timestamp(dt)) => Some(dt.weekday()),
                _ => None,
            };
            match weekday {
                Some(chrono::Weekday::Mon) => Ok(Value::Varchar("Monday".to_string())),
                Some(chrono::Weekday::Tue) => Ok(Value::Varchar("Tuesday".to_string())),
                Some(chrono::Weekday::Wed) => Ok(Value::Varchar("Wednesday".to_string())),
                Some(chrono::Weekday::Thu) => Ok(Value::Varchar("Thursday".to_string())),
                Some(chrono::Weekday::Fri) => Ok(Value::Varchar("Friday".to_string())),
                Some(chrono::Weekday::Sat) => Ok(Value::Varchar("Saturday".to_string())),
                Some(chrono::Weekday::Sun) => Ok(Value::Varchar("Sunday".to_string())),
                None => Ok(Value::Null),
            }
        }
        "MONTHNAME" => {
            use chrono::Datelike;
            let month = match args.first() {
                Some(Value::Date(d)) => Some(d.month()),
                Some(Value::Timestamp(dt)) => Some(dt.month()),
                _ => None,
            };
            let name = match month {
                Some(1) => "January",
                Some(2) => "February",
                Some(3) => "March",
                Some(4) => "April",
                Some(5) => "May",
                Some(6) => "June",
                Some(7) => "July",
                Some(8) => "August",
                Some(9) => "September",
                Some(10) => "October",
                Some(11) => "November",
                Some(12) => "December",
                _ => return Ok(Value::Null),
            };
            Ok(Value::Varchar(name.to_string()))
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
        "EPOCH_US" => {
            // Return epoch in microseconds
            match args.first() {
                Some(Value::Timestamp(dt)) => {
                    let epoch_us = dt.and_utc().timestamp_micros();
                    Ok(Value::BigInt(epoch_us))
                }
                Some(Value::Date(d)) => {
                    let epoch_us = d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
                    Ok(Value::BigInt(epoch_us))
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "EPOCH_NS" => {
            // Return epoch in nanoseconds
            match args.first() {
                Some(Value::Timestamp(dt)) => {
                    if let Some(epoch_ns) = dt.and_utc().timestamp_nanos_opt() {
                        Ok(Value::BigInt(epoch_ns))
                    } else {
                        Ok(Value::Null) // Overflow
                    }
                }
                Some(Value::Date(d)) => {
                    if let Some(epoch_ns) = d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_nanos_opt() {
                        Ok(Value::BigInt(epoch_ns))
                    } else {
                        Ok(Value::Null)
                    }
                }
                Some(Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        }
        "TIME_BUCKET" => {
            // TIME_BUCKET(bucket_width, timestamp) - truncate timestamp to bucket
            use chrono::{Duration, Datelike, Timelike, NaiveDateTime};

            let bucket_width = args.first();
            let timestamp = args.get(1);

            match (bucket_width, timestamp) {
                (Some(Value::Interval(interval)), Some(Value::Timestamp(ts))) => {
                    // Calculate bucket width in microseconds
                    let bucket_micros = interval.micros
                        + (interval.days as i64 * 24 * 60 * 60 * 1_000_000)
                        + (interval.months as i64 * 30 * 24 * 60 * 60 * 1_000_000);

                    if bucket_micros <= 0 {
                        return Err(Error::Execution("TIME_BUCKET: bucket width must be positive".to_string()));
                    }

                    // Get timestamp as microseconds since epoch
                    let ts_micros = ts.and_utc().timestamp_micros();

                    // Truncate to bucket
                    let bucket_start = (ts_micros / bucket_micros) * bucket_micros;

                    // Convert back to timestamp
                    let secs = bucket_start / 1_000_000;
                    let nsecs = ((bucket_start % 1_000_000) * 1000) as u32;
                    match chrono::DateTime::from_timestamp(secs, nsecs) {
                        Some(dt) => Ok(Value::Timestamp(dt.naive_utc())),
                        None => Ok(Value::Null),
                    }
                }
                (Some(Value::Interval(interval)), Some(Value::Date(d))) => {
                    // For dates, convert to timestamp at midnight
                    let ts = d.and_hms_opt(0, 0, 0).unwrap();
                    let bucket_micros = interval.micros
                        + (interval.days as i64 * 24 * 60 * 60 * 1_000_000)
                        + (interval.months as i64 * 30 * 24 * 60 * 60 * 1_000_000);

                    if bucket_micros <= 0 {
                        return Err(Error::Execution("TIME_BUCKET: bucket width must be positive".to_string()));
                    }

                    let ts_micros = ts.and_utc().timestamp_micros();
                    let bucket_start = (ts_micros / bucket_micros) * bucket_micros;

                    let secs = bucket_start / 1_000_000;
                    let nsecs = ((bucket_start % 1_000_000) * 1000) as u32;
                    match chrono::DateTime::from_timestamp(secs, nsecs) {
                        Some(dt) => Ok(Value::Timestamp(dt.naive_utc())),
                        None => Ok(Value::Null),
                    }
                }
                (Some(Value::Null), _) | (_, Some(Value::Null)) => Ok(Value::Null),
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
        "JSON_MERGE_PATCH" => {
            // Merge two JSON objects (RFC 7396)
            let json1_str = args.first().and_then(|v| v.as_str()).unwrap_or("{}");
            let json2_str = args.get(1).and_then(|v| v.as_str()).unwrap_or("{}");

            let json1: serde_json::Value = serde_json::from_str(json1_str).unwrap_or(serde_json::Value::Null);
            let json2: serde_json::Value = serde_json::from_str(json2_str).unwrap_or(serde_json::Value::Null);

            fn merge(a: &mut serde_json::Value, b: serde_json::Value) {
                if let (Some(a_obj), Some(b_obj)) = (a.as_object_mut(), b.as_object()) {
                    for (k, v) in b_obj {
                        if v.is_null() {
                            a_obj.remove(k);
                        } else if let Some(existing) = a_obj.get_mut(k) {
                            merge(existing, v.clone());
                        } else {
                            a_obj.insert(k.clone(), v.clone());
                        }
                    }
                } else {
                    *a = b;
                }
            }

            let mut result = json1;
            merge(&mut result, json2);
            Ok(Value::Varchar(result.to_string()))
        }
        "JSON_CONTAINS" => {
            // Check if JSON contains a value at path
            let json_str = args.first().and_then(|v| v.as_str()).unwrap_or("{}");
            let needle = args.get(1).and_then(|v| v.as_str()).unwrap_or("");

            let json_str_lower = json_str.to_lowercase();
            let needle_lower = needle.to_lowercase();
            Ok(Value::Boolean(json_str_lower.contains(&needle_lower)))
        }
        "JSON_QUOTE" => {
            // Quote a string as a JSON string
            let s = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let quoted = serde_json::to_string(s).unwrap_or_else(|_| format!("\"{}\"", s));
            Ok(Value::Varchar(quoted))
        }
        "TRY_JSON_EXTRACT" | "JSON_EXTRACT_PATH_TEXT" => {
            // Like JSON_EXTRACT but returns NULL on error instead of failing
            let json_str = args.first().and_then(|v| v.as_str()).unwrap_or("");
            let path = args.get(1).and_then(|v| v.as_str()).unwrap_or("");

            match serde_json::from_str::<serde_json::Value>(json_str) {
                Ok(mut val) => {
                    for key in path.trim_start_matches('$').trim_start_matches('.').split('.') {
                        if key.is_empty() { continue; }
                        val = match val {
                            serde_json::Value::Object(ref obj) => {
                                obj.get(key).cloned().unwrap_or(serde_json::Value::Null)
                            }
                            serde_json::Value::Array(ref arr) => {
                                if let Ok(idx) = key.parse::<usize>() {
                                    arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                                } else {
                                    serde_json::Value::Null
                                }
                            }
                            _ => serde_json::Value::Null,
                        };
                    }
                    if val.is_null() {
                        Ok(Value::Null)
                    } else if let Some(s) = val.as_str() {
                        Ok(Value::Varchar(s.to_string()))
                    } else {
                        Ok(Value::Varchar(val.to_string()))
                    }
                }
                Err(_) => Ok(Value::Null),
            }
        }
        "JSON_SERIALIZE" => {
            // Serialize value to JSON (alias for TO_JSON)
            match args.first() {
                Some(v) => Ok(Value::Varchar(value_to_json_string(v))),
                None => Ok(Value::Varchar("null".to_string())),
            }
        }
        "JSON_DESERIALIZE" | "JSON_PARSE" | "FROM_JSON" => {
            // Parse JSON string into a value
            let json_str = args.first().and_then(|v| v.as_str()).unwrap_or("null");
            match serde_json::from_str::<serde_json::Value>(json_str) {
                Ok(json) => Ok(json_value_to_value(&json)),
                Err(_) => Ok(Value::Null),
            }
        }
        "JSON_GROUP_ARRAY" | "JSON_AGG" => {
            // This is an aggregate function, but we handle scalar case
            let json_vals: Vec<String> = args.iter()
                .map(|v| value_to_json_string(v))
                .collect();
            Ok(Value::Varchar(format!("[{}]", json_vals.join(","))))
        }
        "JSON_GROUP_OBJECT" => {
            // This is an aggregate function, but we handle scalar case
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

/// Parse a timezone offset string like "0530" or "05:30" to seconds
fn parse_tz_offset(s: &str) -> i32 {
    let s = s.replace(':', "");
    if s.len() >= 4 {
        let hours: i32 = s[..2].parse().unwrap_or(0);
        let mins: i32 = s[2..4].parse().unwrap_or(0);
        hours * 3600 + mins * 60
    } else if s.len() >= 2 {
        let hours: i32 = s[..2].parse().unwrap_or(0);
        hours * 3600
    } else {
        0
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
                Value::Null => return Ok(Value::Null),
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
                Value::Null => return Ok(Value::Null),
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
                Value::Null => return Ok(Value::Null),
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
        LogicalType::SmallInt => {
            let i = match val {
                Value::SmallInt(i) => *i,
                Value::Integer(i) => *i as i16,
                Value::BigInt(i) => *i as i16,
                Value::Float(f) => *f as i16,
                Value::Double(f) => *f as i16,
                Value::Boolean(b) => if *b { 1 } else { 0 },
                Value::Varchar(s) => s.parse().map_err(|_| Error::InvalidCast {
                    from: "VARCHAR".to_string(),
                    to: "SMALLINT".to_string(),
                })?,
                Value::Null => return Ok(Value::Null),
                _ => return Err(Error::InvalidCast {
                    from: val.logical_type().to_string(),
                    to: target.to_string(),
                }),
            };
            Ok(Value::SmallInt(i))
        }
        LogicalType::TinyInt => {
            let i = match val {
                Value::TinyInt(i) => *i,
                Value::SmallInt(i) => *i as i8,
                Value::Integer(i) => *i as i8,
                Value::BigInt(i) => *i as i8,
                Value::Float(f) => *f as i8,
                Value::Double(f) => *f as i8,
                Value::Boolean(b) => if *b { 1 } else { 0 },
                Value::Varchar(s) => s.parse().map_err(|_| Error::InvalidCast {
                    from: "VARCHAR".to_string(),
                    to: "TINYINT".to_string(),
                })?,
                Value::Null => return Ok(Value::Null),
                _ => return Err(Error::InvalidCast {
                    from: val.logical_type().to_string(),
                    to: target.to_string(),
                }),
            };
            Ok(Value::TinyInt(i))
        }
        LogicalType::Float => {
            let f = match val {
                Value::Integer(i) => *i as f32,
                Value::BigInt(i) => *i as f32,
                Value::Float(f) => *f,
                Value::Double(f) => *f as f32,
                Value::Varchar(s) => s.parse().map_err(|_| Error::InvalidCast {
                    from: "VARCHAR".to_string(),
                    to: "FLOAT".to_string(),
                })?,
                Value::Null => return Ok(Value::Null),
                _ => return Err(Error::InvalidCast {
                    from: val.logical_type().to_string(),
                    to: target.to_string(),
                }),
            };
            Ok(Value::Float(f))
        }
        LogicalType::Date => {
            use chrono::NaiveDate;
            let d = match val {
                Value::Date(d) => *d,
                Value::Timestamp(ts) => ts.date(),
                Value::TimestampTz(ts) => ts.date_naive(),
                Value::Varchar(s) => {
                    // Try parsing common date formats
                    NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                        .or_else(|_| NaiveDate::parse_from_str(s.trim(), "%Y/%m/%d"))
                        .or_else(|_| NaiveDate::parse_from_str(s.trim(), "%d-%m-%Y"))
                        .map_err(|_| Error::InvalidCast {
                            from: "VARCHAR".to_string(),
                            to: "DATE".to_string(),
                        })?
                }
                _ => return Err(Error::InvalidCast {
                    from: val.logical_type().to_string(),
                    to: target.to_string(),
                }),
            };
            Ok(Value::Date(d))
        }
        LogicalType::Timestamp => {
            use chrono::{NaiveDateTime, NaiveDate};
            let ts = match val {
                Value::Timestamp(ts) => *ts,
                Value::Date(d) => d.and_hms_opt(0, 0, 0).unwrap(),
                Value::TimestampTz(ts) => ts.naive_utc(),
                Value::Varchar(s) => {
                    // Try parsing common timestamp formats
                    NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S%.f"))
                        .or_else(|_| NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%dT%H:%M:%S"))
                        .or_else(|_| {
                            // Try as date only
                            NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                                .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                        })
                        .map_err(|_| Error::InvalidCast {
                            from: "VARCHAR".to_string(),
                            to: "TIMESTAMP".to_string(),
                        })?
                }
                _ => return Err(Error::InvalidCast {
                    from: val.logical_type().to_string(),
                    to: target.to_string(),
                }),
            };
            Ok(Value::Timestamp(ts))
        }
        LogicalType::Time => {
            use chrono::NaiveTime;
            let t = match val {
                Value::Time(t) => *t,
                Value::Timestamp(ts) => ts.time(),
                Value::TimestampTz(ts) => ts.time(),
                Value::Varchar(s) => {
                    NaiveTime::parse_from_str(s.trim(), "%H:%M:%S")
                        .or_else(|_| NaiveTime::parse_from_str(s.trim(), "%H:%M:%S%.f"))
                        .or_else(|_| NaiveTime::parse_from_str(s.trim(), "%H:%M"))
                        .map_err(|_| Error::InvalidCast {
                            from: "VARCHAR".to_string(),
                            to: "TIME".to_string(),
                        })?
                }
                _ => return Err(Error::InvalidCast {
                    from: val.logical_type().to_string(),
                    to: target.to_string(),
                }),
            };
            Ok(Value::Time(t))
        }
        LogicalType::TimeTz => {
            use chrono::NaiveTime;
            let (time, offset) = match val {
                Value::TimeTz(t, off) => (*t, *off),
                Value::Time(t) => (*t, 0), // Assume UTC for plain time
                Value::Timestamp(ts) => (ts.time(), 0),
                Value::TimestampTz(ts) => (ts.time(), 0), // Already UTC
                Value::Varchar(s) => {
                    // Parse TIMETZ format like "00:00:00+1559" or "12:30:45-0500"
                    let s = s.trim();
                    // Find the timezone offset (+ or -)
                    let (time_part, offset_secs) = if let Some(pos) = s.rfind('+') {
                        let time_str = &s[..pos];
                        let offset_str = &s[pos + 1..];
                        let offset = parse_tz_offset(offset_str);
                        (time_str, offset)
                    } else if let Some(pos) = s.rfind('-') {
                        // Make sure we don't split on a date separator
                        if pos > 5 { // Likely timezone, not date
                            let time_str = &s[..pos];
                            let offset_str = &s[pos + 1..];
                            let offset = -parse_tz_offset(offset_str);
                            (time_str, offset)
                        } else {
                            (s, 0)
                        }
                    } else {
                        (s, 0)
                    };

                    let time = NaiveTime::parse_from_str(time_part, "%H:%M:%S")
                        .or_else(|_| NaiveTime::parse_from_str(time_part, "%H:%M:%S%.f"))
                        .or_else(|_| NaiveTime::parse_from_str(time_part, "%H:%M"))
                        .map_err(|_| Error::InvalidCast {
                            from: "VARCHAR".to_string(),
                            to: "TIMETZ".to_string(),
                        })?;
                    (time, offset_secs)
                }
                _ => return Err(Error::InvalidCast {
                    from: val.logical_type().to_string(),
                    to: target.to_string(),
                }),
            };
            Ok(Value::TimeTz(time, offset))
        }
        LogicalType::TimestampTz => {
            use chrono::{DateTime, Utc, NaiveDateTime, NaiveDate};
            let ts = match val {
                Value::TimestampTz(ts) => *ts,
                Value::Timestamp(ts) => DateTime::from_naive_utc_and_offset(*ts, Utc),
                Value::Date(d) => {
                    let naive = d.and_hms_opt(0, 0, 0).unwrap();
                    DateTime::from_naive_utc_and_offset(naive, Utc)
                }
                Value::Varchar(s) => {
                    // Try parsing with timezone
                    DateTime::parse_from_rfc3339(s.trim())
                        .map(|dt| dt.with_timezone(&Utc))
                        .or_else(|_| {
                            // Try as naive timestamp and assume UTC
                            NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S")
                                .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
                        })
                        .or_else(|_| {
                            NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                                .map(|d| DateTime::from_naive_utc_and_offset(d.and_hms_opt(0, 0, 0).unwrap(), Utc))
                        })
                        .map_err(|_| Error::InvalidCast {
                            from: "VARCHAR".to_string(),
                            to: "TIMESTAMPTZ".to_string(),
                        })?
                }
                _ => return Err(Error::InvalidCast {
                    from: val.logical_type().to_string(),
                    to: target.to_string(),
                }),
            };
            Ok(Value::TimestampTz(ts))
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
