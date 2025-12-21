//! Query executor

use crate::expression::evaluate;
use ironduck_catalog::Catalog;
use ironduck_common::{Error, LogicalType, Result, Value};
use ironduck_planner::{Expression, LogicalOperator, LogicalPlan, WindowFunction};
use ironduck_storage::TableStorage;
use std::sync::Arc;

/// The main query executor
pub struct Executor {
    catalog: Arc<Catalog>,
    storage: Arc<TableStorage>,
}

impl Executor {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Executor {
            catalog,
            storage: Arc::new(TableStorage::new()),
        }
    }

    /// Create executor with shared storage
    pub fn with_storage(catalog: Arc<Catalog>, storage: Arc<TableStorage>) -> Self {
        Executor { catalog, storage }
    }

    /// Get storage reference
    pub fn storage(&self) -> &TableStorage {
        &self.storage
    }

    /// Execute a logical plan and return results
    pub fn execute(&self, plan: &LogicalPlan) -> Result<QueryResult> {
        let rows = self.execute_operator(&plan.root)?;

        Ok(QueryResult {
            columns: plan.output_names.clone(),
            rows,
        })
    }

    /// Execute a logical operator recursively
    fn execute_operator(&self, op: &LogicalOperator) -> Result<Vec<Vec<Value>>> {
        match op {
            LogicalOperator::DummyScan => {
                // Return a single empty row for SELECT without FROM
                Ok(vec![vec![]])
            }

            LogicalOperator::Scan {
                schema,
                table,
                column_names,
                output_types,
            } => {
                // Get table data from storage
                match self.storage.get(schema, table) {
                    Some(table_data) => Ok(table_data.scan()),
                    None => {
                        // Table exists in catalog but no data yet
                        Ok(vec![])
                    }
                }
            }

            LogicalOperator::Project {
                input,
                expressions,
                ..
            } => {
                let input_rows = self.execute_operator(input)?;
                let mut output_rows = Vec::new();

                for row in &input_rows {
                    let mut output_row = Vec::new();
                    for expr in expressions {
                        let value = evaluate(expr, row)?;
                        output_row.push(value);
                    }
                    output_rows.push(output_row);
                }

                // Handle case of no input rows but constant expressions
                if input_rows.is_empty() && expressions.iter().all(is_constant) {
                    let mut output_row = Vec::new();
                    for expr in expressions {
                        let value = evaluate(expr, &[])?;
                        output_row.push(value);
                    }
                    output_rows.push(output_row);
                }

                Ok(output_rows)
            }

            LogicalOperator::Filter { input, predicate } => {
                let input_rows = self.execute_operator(input)?;
                let has_subquery = contains_subquery(predicate);
                let mut output_rows = Vec::new();

                for row in input_rows {
                    let result = if has_subquery {
                        evaluate_with_subqueries(self, predicate, &row)?
                    } else {
                        evaluate(predicate, &row)?
                    };
                    if matches!(result, Value::Boolean(true)) {
                        output_rows.push(row);
                    }
                }

                Ok(output_rows)
            }

            LogicalOperator::Limit {
                input,
                limit,
                offset,
            } => {
                let input_rows = self.execute_operator(input)?;
                let skip = offset.unwrap_or(0) as usize;
                let take = limit.unwrap_or(u64::MAX) as usize;

                Ok(input_rows.into_iter().skip(skip).take(take).collect())
            }

            LogicalOperator::Sort { input, order_by } => {
                let mut rows = self.execute_operator(input)?;

                rows.sort_by(|a, b| {
                    for order in order_by {
                        let left = evaluate(&order.expr, a).unwrap_or(Value::Null);
                        let right = evaluate(&order.expr, b).unwrap_or(Value::Null);

                        // Handle NULL values according to nulls_first setting
                        let left_is_null = left.is_null();
                        let right_is_null = right.is_null();

                        let cmp = match (left_is_null, right_is_null) {
                            (true, true) => std::cmp::Ordering::Equal,
                            (true, false) => {
                                // Left is NULL
                                if order.nulls_first {
                                    std::cmp::Ordering::Less
                                } else {
                                    std::cmp::Ordering::Greater
                                }
                            }
                            (false, true) => {
                                // Right is NULL
                                if order.nulls_first {
                                    std::cmp::Ordering::Greater
                                } else {
                                    std::cmp::Ordering::Less
                                }
                            }
                            (false, false) => {
                                // Neither is NULL, compare normally
                                let cmp = left.partial_cmp(&right).unwrap_or(std::cmp::Ordering::Equal);
                                if order.ascending { cmp } else { cmp.reverse() }
                            }
                        };

                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                    std::cmp::Ordering::Equal
                });

                Ok(rows)
            }

            LogicalOperator::Distinct { input, on_exprs } => {
                let input_rows = self.execute_operator(input)?;
                let mut seen = std::collections::HashSet::new();
                let mut output_rows = Vec::new();

                for row in input_rows {
                    // For DISTINCT ON, compute key only from specified expressions
                    // For plain DISTINCT, use all columns
                    let key = match on_exprs {
                        Some(exprs) => {
                            let key_values: Vec<Value> = exprs
                                .iter()
                                .map(|expr| evaluate(expr, &row))
                                .collect::<Result<Vec<_>>>()?;
                            format!("{:?}", key_values)
                        }
                        None => format!("{:?}", row),
                    };
                    if seen.insert(key) {
                        output_rows.push(row);
                    }
                }

                Ok(output_rows)
            }

            LogicalOperator::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let input_rows = self.execute_operator(input)?;

                if group_by.is_empty() {
                    // Simple aggregation without GROUP BY
                    let mut result_row = Vec::new();

                    for agg in aggregates {
                        let value = compute_aggregate(agg, &input_rows)?;
                        result_row.push(value);
                    }

                    Ok(vec![result_row])
                } else {
                    // GROUP BY aggregation
                    let mut groups: std::collections::HashMap<String, Vec<Vec<Value>>> =
                        std::collections::HashMap::new();

                    for row in &input_rows {
                        let mut key_parts = Vec::new();
                        for expr in group_by {
                            let val = evaluate(expr, row)?;
                            key_parts.push(format!("{:?}", val));
                        }
                        let key = key_parts.join("|");
                        groups.entry(key).or_default().push(row.clone());
                    }

                    let mut result_rows = Vec::new();
                    for (_, group_rows) in groups {
                        let mut result_row = Vec::new();

                        // Add group by values
                        if let Some(first_row) = group_rows.first() {
                            for expr in group_by {
                                result_row.push(evaluate(expr, first_row)?);
                            }
                        }

                        // Add aggregate values
                        for agg in aggregates {
                            let value = compute_aggregate(agg, &group_rows)?;
                            result_row.push(value);
                        }

                        result_rows.push(result_row);
                    }

                    Ok(result_rows)
                }
            }

            LogicalOperator::Join {
                left,
                right,
                join_type,
                condition,
            } => {
                let left_rows = self.execute_operator(left)?;
                let right_rows = self.execute_operator(right)?;
                let mut result = Vec::new();

                // Determine the width of right side for NULL padding
                let right_width = right_rows.first().map(|r| r.len()).unwrap_or(0);
                let left_width = left_rows.first().map(|r| r.len()).unwrap_or(0);

                match join_type {
                    ironduck_planner::JoinType::Cross => {
                        // Cross join - cartesian product
                        for l in &left_rows {
                            for r in &right_rows {
                                let mut row = l.clone();
                                row.extend(r.clone());
                                result.push(row);
                            }
                        }
                    }
                    ironduck_planner::JoinType::Inner => {
                        for l in &left_rows {
                            for r in &right_rows {
                                let mut row = l.clone();
                                row.extend(r.clone());

                                // Check condition
                                let matches = match condition {
                                    Some(cond) => {
                                        matches!(evaluate(cond, &row)?, Value::Boolean(true))
                                    }
                                    None => true,
                                };

                                if matches {
                                    result.push(row);
                                }
                            }
                        }
                    }
                    ironduck_planner::JoinType::Left => {
                        // LEFT JOIN - all rows from left, matching rows from right (or NULLs)
                        for l in &left_rows {
                            let mut matched = false;
                            for r in &right_rows {
                                let mut row = l.clone();
                                row.extend(r.clone());

                                let matches = match condition {
                                    Some(cond) => {
                                        matches!(evaluate(cond, &row)?, Value::Boolean(true))
                                    }
                                    None => true,
                                };

                                if matches {
                                    result.push(row);
                                    matched = true;
                                }
                            }
                            // If no match, add left row with NULLs for right columns
                            if !matched {
                                let mut row = l.clone();
                                row.extend(vec![Value::Null; right_width]);
                                result.push(row);
                            }
                        }
                    }
                    ironduck_planner::JoinType::Right => {
                        // RIGHT JOIN - all rows from right, matching rows from left (or NULLs)
                        for r in &right_rows {
                            let mut matched = false;
                            for l in &left_rows {
                                let mut row = l.clone();
                                row.extend(r.clone());

                                let matches = match condition {
                                    Some(cond) => {
                                        matches!(evaluate(cond, &row)?, Value::Boolean(true))
                                    }
                                    None => true,
                                };

                                if matches {
                                    result.push(row);
                                    matched = true;
                                }
                            }
                            // If no match, add NULLs for left columns with right row
                            if !matched {
                                let mut row = vec![Value::Null; left_width];
                                row.extend(r.clone());
                                result.push(row);
                            }
                        }
                    }
                    ironduck_planner::JoinType::Full => {
                        // FULL OUTER JOIN - all rows from both sides
                        let mut right_matched = vec![false; right_rows.len()];

                        for l in &left_rows {
                            let mut left_matched = false;
                            for (ri, r) in right_rows.iter().enumerate() {
                                let mut row = l.clone();
                                row.extend(r.clone());

                                let matches = match condition {
                                    Some(cond) => {
                                        matches!(evaluate(cond, &row)?, Value::Boolean(true))
                                    }
                                    None => true,
                                };

                                if matches {
                                    result.push(row);
                                    left_matched = true;
                                    right_matched[ri] = true;
                                }
                            }
                            // Left row with no match
                            if !left_matched {
                                let mut row = l.clone();
                                row.extend(vec![Value::Null; right_width]);
                                result.push(row);
                            }
                        }

                        // Add unmatched right rows
                        for (ri, r) in right_rows.iter().enumerate() {
                            if !right_matched[ri] {
                                let mut row = vec![Value::Null; left_width];
                                row.extend(r.clone());
                                result.push(row);
                            }
                        }
                    }
                    _ => return Err(Error::NotImplemented(format!("Join type: {:?}", join_type))),
                }

                Ok(result)
            }

            LogicalOperator::Values { values, .. } => {
                let mut rows = Vec::new();
                for row_exprs in values {
                    let mut row = Vec::new();
                    for expr in row_exprs {
                        row.push(evaluate(expr, &[])?);
                    }
                    rows.push(row);
                }
                Ok(rows)
            }

            LogicalOperator::CreateTable {
                schema,
                name,
                columns,
                if_not_exists,
            } => {
                // Check if table exists
                if self.catalog.get_table(schema, name).is_some() {
                    if *if_not_exists {
                        return Ok(vec![vec![Value::Varchar("Table already exists".to_string())]]);
                    } else {
                        return Err(Error::TableAlreadyExists(name.clone()));
                    }
                }

                // Create the table in catalog
                self.catalog.create_table(schema, name, columns.clone())?;

                // Create storage for the table
                let types: Vec<_> = columns.iter().map(|(_, t)| t.clone()).collect();
                self.storage.get_or_create(schema, name, &types);

                Ok(vec![vec![Value::Varchar(format!("Created table {}", name))]])
            }

            LogicalOperator::CreateSchema { name, if_not_exists } => {
                if self.catalog.get_schema(name).is_some() {
                    if *if_not_exists {
                        return Ok(vec![vec![Value::Varchar("Schema already exists".to_string())]]);
                    } else {
                        return Err(Error::SchemaAlreadyExists(name.clone()));
                    }
                }

                self.catalog.create_schema(name)?;
                Ok(vec![vec![Value::Varchar(format!("Created schema {}", name))]])
            }

            LogicalOperator::Insert {
                schema,
                table,
                columns,
                values,
                source,
            } => {
                // Get table from catalog
                let table_info = self
                    .catalog
                    .get_table(schema, table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;

                // Get column types
                let types: Vec<_> = table_info.columns.iter().map(|c| c.logical_type.clone()).collect();

                // Get or create storage
                let table_data = self.storage.get_or_create(schema, table, &types);

                let mut count = 0;

                // Handle INSERT ... SELECT ...
                if let Some(source_op) = source {
                    let source_rows = self.execute_operator(source_op)?;
                    for row in source_rows {
                        table_data.insert(row);
                        count += 1;
                    }
                } else {
                    // Handle INSERT ... VALUES (...)
                    for row_exprs in values {
                        let mut row = Vec::new();
                        for expr in row_exprs {
                            row.push(evaluate(expr, &[])?);
                        }
                        table_data.insert(row);
                        count += 1;
                    }
                }

                Ok(vec![vec![Value::BigInt(count)]])
            }

            LogicalOperator::Drop {
                object_type,
                schema,
                name,
                if_exists,
            } => {
                let schema_name = schema.as_deref().unwrap_or("main");

                match object_type.as_str() {
                    "Table" => {
                        self.storage.drop_table(schema_name, name);
                        // TODO: Drop from catalog
                    }
                    _ => {}
                }

                Ok(vec![vec![Value::Varchar(format!("Dropped {} {}", object_type, name))]])
            }

            LogicalOperator::Delete {
                schema,
                table,
                predicate,
            } => {
                // Get table data
                let table_data = self
                    .storage
                    .get(schema, table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;

                let count = table_data.delete(|row| {
                    if let Some(pred) = predicate {
                        matches!(evaluate(pred, row).unwrap_or(Value::Boolean(false)), Value::Boolean(true))
                    } else {
                        true // No WHERE clause means delete all
                    }
                });

                Ok(vec![vec![Value::BigInt(count as i64)]])
            }

            LogicalOperator::Update {
                schema,
                table,
                assignments,
                predicate,
            } => {
                // Get table info for column name to index mapping
                let table_info = self
                    .catalog
                    .get_table(schema, table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;

                // Build column index map
                let col_indices: std::collections::HashMap<_, _> = table_info
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (c.name.clone(), i))
                    .collect();

                // Get table data
                let table_data = self
                    .storage
                    .get(schema, table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;

                let count = table_data.update(
                    |row| {
                        if let Some(pred) = predicate {
                            matches!(evaluate(pred, row).unwrap_or(Value::Boolean(false)), Value::Boolean(true))
                        } else {
                            true // No WHERE clause means update all
                        }
                    },
                    |row| {
                        let mut new_row = row.to_vec();
                        for (col_name, expr) in assignments {
                            if let Some(&idx) = col_indices.get(col_name) {
                                if let Ok(val) = evaluate(expr, row) {
                                    new_row[idx] = val;
                                }
                            }
                        }
                        new_row
                    },
                );

                Ok(vec![vec![Value::BigInt(count as i64)]])
            }

            LogicalOperator::SetOperation { left, right, op, all } => {
                let left_rows = self.execute_operator(left)?;
                let right_rows = self.execute_operator(right)?;

                match op {
                    ironduck_planner::SetOperationType::Union => {
                        let mut result = left_rows;
                        result.extend(right_rows);

                        if !all {
                            // Remove duplicates
                            let mut seen = std::collections::HashSet::new();
                            result.retain(|row| {
                                let key = format!("{:?}", row);
                                seen.insert(key)
                            });
                        }

                        Ok(result)
                    }
                    ironduck_planner::SetOperationType::Intersect => {
                        // Keep only rows that appear in both
                        let right_set: std::collections::HashSet<_> = right_rows
                            .iter()
                            .map(|row| format!("{:?}", row))
                            .collect();

                        let mut result: Vec<_> = left_rows
                            .into_iter()
                            .filter(|row| right_set.contains(&format!("{:?}", row)))
                            .collect();

                        if !all {
                            // Remove duplicates
                            let mut seen = std::collections::HashSet::new();
                            result.retain(|row| {
                                let key = format!("{:?}", row);
                                seen.insert(key)
                            });
                        }

                        Ok(result)
                    }
                    ironduck_planner::SetOperationType::Except => {
                        // Keep only rows from left that don't appear in right
                        let right_set: std::collections::HashSet<_> = right_rows
                            .iter()
                            .map(|row| format!("{:?}", row))
                            .collect();

                        let mut result: Vec<_> = left_rows
                            .into_iter()
                            .filter(|row| !right_set.contains(&format!("{:?}", row)))
                            .collect();

                        if !all {
                            // Remove duplicates
                            let mut seen = std::collections::HashSet::new();
                            result.retain(|row| {
                                let key = format!("{:?}", row);
                                seen.insert(key)
                            });
                        }

                        Ok(result)
                    }
                }
            }

            LogicalOperator::Window {
                input,
                window_exprs,
                ..
            } => {
                let input_rows = self.execute_operator(input)?;

                if input_rows.is_empty() {
                    return Ok(vec![]);
                }

                let num_rows = input_rows.len();

                // Process each window expression
                let mut window_results: Vec<Vec<Value>> = vec![vec![Value::Null; num_rows]; window_exprs.len()];

                for (win_idx, win_expr) in window_exprs.iter().enumerate() {
                    // Collect rows with original indices
                    let mut indexed_rows: Vec<(usize, Vec<Value>)> = input_rows
                        .iter()
                        .enumerate()
                        .map(|(i, r)| (i, r.clone()))
                        .collect();

                    // Group by partition key if present
                    let partitions: Vec<Vec<(usize, Vec<Value>)>> = if win_expr.partition_by.is_empty() {
                        // Single partition containing all rows
                        vec![indexed_rows]
                    } else {
                        // Group rows by partition key
                        use std::collections::HashMap;
                        let mut partition_map: HashMap<String, Vec<(usize, Vec<Value>)>> = HashMap::new();

                        for (orig_idx, row) in indexed_rows {
                            let key: Vec<String> = win_expr.partition_by.iter()
                                .map(|e| format!("{:?}", evaluate(e, &row).unwrap_or(Value::Null)))
                                .collect();
                            let key_str = key.join("|");
                            partition_map.entry(key_str).or_default().push((orig_idx, row));
                        }

                        partition_map.into_values().collect()
                    };

                    // Process each partition
                    for mut partition in partitions {
                        // Sort partition by ORDER BY if present
                        if !win_expr.order_by.is_empty() {
                            partition.sort_by(|a, b| {
                                for ob in &win_expr.order_by {
                                    let a_val = evaluate(&ob.expr, &a.1).unwrap_or(Value::Null);
                                    let b_val = evaluate(&ob.expr, &b.1).unwrap_or(Value::Null);

                                    // Handle NULL values according to nulls_first setting
                                    let a_is_null = a_val.is_null();
                                    let b_is_null = b_val.is_null();

                                    let cmp = match (a_is_null, b_is_null) {
                                        (true, true) => std::cmp::Ordering::Equal,
                                        (true, false) => {
                                            if ob.nulls_first {
                                                std::cmp::Ordering::Less
                                            } else {
                                                std::cmp::Ordering::Greater
                                            }
                                        }
                                        (false, true) => {
                                            if ob.nulls_first {
                                                std::cmp::Ordering::Greater
                                            } else {
                                                std::cmp::Ordering::Less
                                            }
                                        }
                                        (false, false) => {
                                            let cmp = a_val.partial_cmp(&b_val).unwrap_or(std::cmp::Ordering::Equal);
                                            if ob.ascending { cmp } else { cmp.reverse() }
                                        }
                                    };

                                    if cmp != std::cmp::Ordering::Equal {
                                        return cmp;
                                    }
                                }
                                std::cmp::Ordering::Equal
                            });
                        }

                        // Compute window function values for this partition
                        match win_expr.function {
                            WindowFunction::RowNumber => {
                                for (idx, (orig_idx, _)) in partition.iter().enumerate() {
                                    window_results[win_idx][*orig_idx] = Value::BigInt((idx + 1) as i64);
                                }
                            }
                            WindowFunction::Rank => {
                                let mut rank = 1i64;
                                for (idx, (orig_idx, row)) in partition.iter().enumerate() {
                                    if idx > 0 && !win_expr.order_by.is_empty() {
                                        let prev_row = &partition[idx - 1].1;
                                        let same = win_expr.order_by.iter().all(|ob| {
                                            evaluate(&ob.expr, row).unwrap_or(Value::Null)
                                                == evaluate(&ob.expr, prev_row).unwrap_or(Value::Null)
                                        });
                                        if !same {
                                            rank = (idx + 1) as i64;
                                        }
                                    }
                                    window_results[win_idx][*orig_idx] = Value::BigInt(rank);
                                }
                            }
                            WindowFunction::DenseRank => {
                                let mut rank = 1i64;
                                for (idx, (orig_idx, row)) in partition.iter().enumerate() {
                                    if idx > 0 && !win_expr.order_by.is_empty() {
                                        let prev_row = &partition[idx - 1].1;
                                        let same = win_expr.order_by.iter().all(|ob| {
                                            evaluate(&ob.expr, row).unwrap_or(Value::Null)
                                                == evaluate(&ob.expr, prev_row).unwrap_or(Value::Null)
                                        });
                                        if !same {
                                            rank += 1;
                                        }
                                    }
                                    window_results[win_idx][*orig_idx] = Value::BigInt(rank);
                                }
                            }
                            WindowFunction::Lag => {
                                // LAG(expr, offset, default) - get value from previous row
                                let offset = if win_expr.args.len() > 1 {
                                    match evaluate(&win_expr.args[1], &partition[0].1).unwrap_or(Value::Integer(1)) {
                                        Value::Integer(n) => n as usize,
                                        Value::BigInt(n) => n as usize,
                                        _ => 1,
                                    }
                                } else {
                                    1
                                };
                                let default_val = if win_expr.args.len() > 2 {
                                    evaluate(&win_expr.args[2], &partition[0].1).unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                };

                                for (idx, (orig_idx, _)) in partition.iter().enumerate() {
                                    let val = if idx >= offset {
                                        if let Some(arg) = win_expr.args.first() {
                                            evaluate(arg, &partition[idx - offset].1).unwrap_or(Value::Null)
                                        } else {
                                            Value::Null
                                        }
                                    } else {
                                        default_val.clone()
                                    };
                                    window_results[win_idx][*orig_idx] = val;
                                }
                            }
                            WindowFunction::Lead => {
                                // LEAD(expr, offset, default) - get value from next row
                                let offset = if win_expr.args.len() > 1 {
                                    match evaluate(&win_expr.args[1], &partition[0].1).unwrap_or(Value::Integer(1)) {
                                        Value::Integer(n) => n as usize,
                                        Value::BigInt(n) => n as usize,
                                        _ => 1,
                                    }
                                } else {
                                    1
                                };
                                let default_val = if win_expr.args.len() > 2 {
                                    evaluate(&win_expr.args[2], &partition[0].1).unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                };

                                for (idx, (orig_idx, _)) in partition.iter().enumerate() {
                                    let val = if idx + offset < partition.len() {
                                        if let Some(arg) = win_expr.args.first() {
                                            evaluate(arg, &partition[idx + offset].1).unwrap_or(Value::Null)
                                        } else {
                                            Value::Null
                                        }
                                    } else {
                                        default_val.clone()
                                    };
                                    window_results[win_idx][*orig_idx] = val;
                                }
                            }
                            WindowFunction::FirstValue => {
                                // FIRST_VALUE(expr) - get first value in partition
                                let first_val = if let Some(arg) = win_expr.args.first() {
                                    evaluate(arg, &partition[0].1).unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                };
                                for (orig_idx, _) in &partition {
                                    window_results[win_idx][*orig_idx] = first_val.clone();
                                }
                            }
                            WindowFunction::LastValue => {
                                // LAST_VALUE(expr) - get last value in partition (up to current row by default)
                                for (idx, (orig_idx, _)) in partition.iter().enumerate() {
                                    let val = if let Some(arg) = win_expr.args.first() {
                                        evaluate(arg, &partition[idx].1).unwrap_or(Value::Null)
                                    } else {
                                        Value::Null
                                    };
                                    window_results[win_idx][*orig_idx] = val;
                                }
                            }
                            _ => {
                                // For unsupported window functions, return NULL
                                for (orig_idx, _) in &partition {
                                    window_results[win_idx][*orig_idx] = Value::Null;
                                }
                            }
                        }
                    }
                }

                // Build output: original columns + window function results
                let mut output_rows = Vec::new();
                for (row_idx, row) in input_rows.into_iter().enumerate() {
                    let mut output_row = row;
                    for win_result in &window_results {
                        output_row.push(win_result[row_idx].clone());
                    }
                    output_rows.push(output_row);
                }

                Ok(output_rows)
            }
        }
    }
}

/// Evaluate an expression with subquery support
fn evaluate_with_subqueries(
    executor: &Executor,
    expr: &Expression,
    row: &[Value],
) -> Result<Value> {
    match expr {
        Expression::InSubquery { expr: val_expr, subquery, negated } => {
            let val = evaluate_with_subqueries(executor, val_expr, row)?;
            if val.is_null() {
                return Ok(Value::Null);
            }

            // Execute subquery
            let subquery_rows = executor.execute_operator(subquery)?;

            // Check if value is in subquery results
            let mut found = false;
            for subquery_row in &subquery_rows {
                if let Some(first_col) = subquery_row.first() {
                    if !first_col.is_null() {
                        if val == *first_col {
                            found = true;
                            break;
                        }
                    }
                }
            }

            let result = if *negated { !found } else { found };
            Ok(Value::Boolean(result))
        }

        Expression::Exists { subquery, negated } => {
            // Execute subquery
            let subquery_rows = executor.execute_operator(subquery)?;

            let exists = !subquery_rows.is_empty();
            let result = if *negated { !exists } else { exists };
            Ok(Value::Boolean(result))
        }

        Expression::Subquery(subquery) => {
            // Execute scalar subquery
            let subquery_rows = executor.execute_operator(subquery)?;

            // Return the first column of the first row
            if let Some(first_row) = subquery_rows.first() {
                if let Some(first_col) = first_row.first() {
                    return Ok(first_col.clone());
                }
            }
            Ok(Value::Null)
        }

        // Handle expressions that may contain subqueries
        Expression::BinaryOp { left, op, right } => {
            let left_val = evaluate_with_subqueries(executor, left, row)?;
            let right_val = evaluate_with_subqueries(executor, right, row)?;
            // Use the simple evaluate for the actual operation
            use ironduck_planner::BinaryOperator;
            match (left_val.is_null() || right_val.is_null(), op) {
                (true, BinaryOperator::And) => {
                    if matches!(left_val, Value::Boolean(false)) || matches!(right_val, Value::Boolean(false)) {
                        Ok(Value::Boolean(false))
                    } else {
                        Ok(Value::Null)
                    }
                }
                (true, BinaryOperator::Or) => {
                    if matches!(left_val, Value::Boolean(true)) || matches!(right_val, Value::Boolean(true)) {
                        Ok(Value::Boolean(true))
                    } else {
                        Ok(Value::Null)
                    }
                }
                (true, _) => Ok(Value::Null),
                (false, _) => {
                    // Build a simple expression and evaluate it
                    let simple_expr = Expression::BinaryOp {
                        left: Box::new(Expression::Constant(left_val)),
                        op: *op,
                        right: Box::new(Expression::Constant(right_val)),
                    };
                    evaluate(&simple_expr, &[])
                }
            }
        }

        Expression::UnaryOp { op, expr: inner } => {
            let val = evaluate_with_subqueries(executor, inner, row)?;
            let simple_expr = Expression::UnaryOp {
                op: *op,
                expr: Box::new(Expression::Constant(val)),
            };
            evaluate(&simple_expr, &[])
        }

        // Fall back to regular evaluate for non-subquery expressions
        _ => evaluate(expr, row),
    }
}

/// Check if expression contains subqueries
fn contains_subquery(expr: &Expression) -> bool {
    match expr {
        Expression::InSubquery { .. } | Expression::Exists { .. } | Expression::Subquery(_) => true,
        Expression::BinaryOp { left, right, .. } => contains_subquery(left) || contains_subquery(right),
        Expression::UnaryOp { expr, .. } => contains_subquery(expr),
        Expression::Function { args, .. } => args.iter().any(contains_subquery),
        Expression::Cast { expr, .. } => contains_subquery(expr),
        Expression::IsNull(expr) | Expression::IsNotNull(expr) => contains_subquery(expr),
        Expression::Case { operand, conditions, results, else_result } => {
            operand.as_ref().map_or(false, |e| contains_subquery(e))
                || conditions.iter().any(contains_subquery)
                || results.iter().any(contains_subquery)
                || else_result.as_ref().map_or(false, |e| contains_subquery(e))
        }
        Expression::InList { expr, list, .. } => contains_subquery(expr) || list.iter().any(contains_subquery),
        _ => false,
    }
}

/// Check if an expression is constant (no column references)
fn is_constant(expr: &Expression) -> bool {
    match expr {
        Expression::Constant(_) => true,
        Expression::ColumnRef { .. } => false,
        Expression::BinaryOp { left, right, .. } => is_constant(left) && is_constant(right),
        Expression::UnaryOp { expr, .. } => is_constant(expr),
        Expression::Function { args, .. } => args.iter().all(is_constant),
        Expression::Cast { expr, .. } => is_constant(expr),
        Expression::IsNull(expr) | Expression::IsNotNull(expr) => is_constant(expr),
        Expression::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_ref().map_or(true, |e| is_constant(e))
                && conditions.iter().all(is_constant)
                && results.iter().all(is_constant)
                && else_result.as_ref().map_or(true, |e| is_constant(e))
        }
        Expression::Subquery(_) => false,
        Expression::InList { expr, list, .. } => is_constant(expr) && list.iter().all(is_constant),
        Expression::InSubquery { .. } | Expression::Exists { .. } => false,
    }
}

/// Compute an aggregate function
fn compute_aggregate(
    agg: &ironduck_planner::AggregateExpression,
    rows: &[Vec<Value>],
) -> Result<Value> {
    use ironduck_planner::AggregateFunction::*;

    let args = &agg.args;
    let distinct = agg.distinct;

    match agg.function {
        Count => {
            if args.is_empty() || matches!(args.first(), Some(Expression::Constant(Value::Null))) {
                // COUNT(*) or COUNT(literal)
                Ok(Value::BigInt(rows.len() as i64))
            } else if distinct {
                // COUNT(DISTINCT expr) - count distinct non-NULL values
                let mut seen = std::collections::HashSet::new();
                for row in rows {
                    let val = evaluate(&args[0], row)?;
                    if !val.is_null() {
                        seen.insert(format!("{:?}", val));
                    }
                }
                Ok(Value::BigInt(seen.len() as i64))
            } else {
                // COUNT(expr) - count non-NULL values
                let mut count = 0i64;
                for row in rows {
                    let val = evaluate(&args[0], row)?;
                    if !val.is_null() {
                        count += 1;
                    }
                }
                Ok(Value::BigInt(count))
            }
        }

        Sum => {
            let mut sum = 0i64;
            let mut has_float = false;
            let mut float_sum = 0f64;

            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                match val {
                    Value::Integer(i) => sum += i as i64,
                    Value::BigInt(i) => sum += i,
                    Value::Float(f) => {
                        has_float = true;
                        float_sum += f as f64;
                    }
                    Value::Double(f) => {
                        has_float = true;
                        float_sum += f;
                    }
                    Value::Null => {}
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "numeric".to_string(),
                            got: format!("{:?}", val),
                        })
                    }
                }
            }

            if has_float {
                Ok(Value::Double(float_sum + sum as f64))
            } else {
                Ok(Value::BigInt(sum))
            }
        }

        Avg => {
            let mut sum = 0f64;
            let mut count = 0i64;

            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if let Some(f) = val.as_f64() {
                    sum += f;
                    count += 1;
                }
            }

            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Double(sum / count as f64))
            }
        }

        Min => {
            let mut min: Option<Value> = None;

            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if val.is_null() {
                    continue;
                }
                min = Some(match min {
                    None => val,
                    Some(m) => {
                        if val.partial_cmp(&m) == Some(std::cmp::Ordering::Less) {
                            val
                        } else {
                            m
                        }
                    }
                });
            }

            Ok(min.unwrap_or(Value::Null))
        }

        Max => {
            let mut max: Option<Value> = None;

            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if val.is_null() {
                    continue;
                }
                max = Some(match max {
                    None => val,
                    Some(m) => {
                        if val.partial_cmp(&m) == Some(std::cmp::Ordering::Greater) {
                            val
                        } else {
                            m
                        }
                    }
                });
            }

            Ok(max.unwrap_or(Value::Null))
        }

        First => {
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if !val.is_null() {
                    return Ok(val);
                }
            }
            Ok(Value::Null)
        }

        Last => {
            let mut last = Value::Null;
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if !val.is_null() {
                    last = val;
                }
            }
            Ok(last)
        }

        StringAgg => {
            // STRING_AGG(expr, delimiter) or GROUP_CONCAT(expr)
            let delimiter = if args.len() > 1 {
                match evaluate(&args[1], &[])? {
                    Value::Varchar(s) => s,
                    _ => ",".to_string(),
                }
            } else {
                ",".to_string()
            };

            let mut values: Vec<String> = Vec::new();
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if !val.is_null() {
                    values.push(val.to_string());
                }
            }

            // Handle DISTINCT
            if distinct {
                let mut seen = std::collections::HashSet::new();
                values.retain(|v| seen.insert(v.clone()));
            }

            Ok(Value::Varchar(values.join(&delimiter)))
        }

        ArrayAgg => {
            let mut values: Vec<Value> = Vec::new();
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if !val.is_null() {
                    // Handle DISTINCT
                    if distinct {
                        let key = format!("{:?}", val);
                        if !values.iter().any(|v| format!("{:?}", v) == key) {
                            values.push(val);
                        }
                    } else {
                        values.push(val);
                    }
                }
            }
            Ok(Value::List(values))
        }
    }
}

/// Result of query execution
#[derive(Debug)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Rows of values
    pub rows: Vec<Vec<Value>>,
}

impl QueryResult {
    pub fn empty() -> Self {
        QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}
