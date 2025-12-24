//! Query executor

use crate::expression::{evaluate, evaluate_with_ctx, EvalContext};
use ironduck_catalog::Catalog;
use ironduck_common::{Error, LogicalType, Result, Value};
use ironduck_planner::{Expression, LogicalOperator, LogicalPlan, TableFunctionKind, WindowFrame, WindowFrameBound, WindowFunction};
use ironduck_storage::TableStorage;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

// Thread-local catalog reference for expression evaluation in functions like compute_aggregate
thread_local! {
    static CURRENT_CATALOG: RefCell<Option<Arc<Catalog>>> = const { RefCell::new(None) };
    static OUTER_ROW: RefCell<Option<Vec<Value>>> = const { RefCell::new(None) };
}

/// Set the catalog for the current thread during execution
fn with_catalog<T>(catalog: Arc<Catalog>, f: impl FnOnce() -> T) -> T {
    CURRENT_CATALOG.with(|c| {
        let old = c.borrow_mut().replace(catalog);
        let result = f();
        *c.borrow_mut() = old;
        result
    })
}

/// Get the current thread's catalog (for expression evaluation)
fn current_catalog() -> Option<Arc<Catalog>> {
    CURRENT_CATALOG.with(|c| c.borrow().clone())
}

/// Set the outer row for the current thread during correlated subquery execution
fn with_outer_row<T>(outer_row: Vec<Value>, f: impl FnOnce() -> T) -> T {
    OUTER_ROW.with(|r| {
        let old = r.borrow_mut().replace(outer_row);
        let result = f();
        *r.borrow_mut() = old;
        result
    })
}

/// Get the current thread's outer row (for correlated subqueries)
fn current_outer_row() -> Option<Vec<Value>> {
    OUTER_ROW.with(|r| r.borrow().clone())
}

/// Evaluate expression using the thread-local catalog and outer row if available
fn eval_with_catalog(expr: &Expression, row: &[Value]) -> Result<Value> {
    let mut ctx = EvalContext::new();
    if let Some(catalog) = current_catalog() {
        ctx = EvalContext::with_catalog(catalog);
    }
    if let Some(outer_row) = current_outer_row() {
        ctx = ctx.with_outer_row(outer_row);
    }
    evaluate_with_ctx(expr, row, &ctx)
}

/// The main query executor
pub struct Executor {
    catalog: Arc<Catalog>,
    storage: Arc<TableStorage>,
    /// Working tables for recursive CTE execution
    /// Maps CTE name to the current iteration's rows
    recursive_working_tables: RefCell<HashMap<String, Vec<Vec<Value>>>>,
}

impl Executor {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Executor {
            catalog,
            storage: Arc::new(TableStorage::new()),
            recursive_working_tables: RefCell::new(HashMap::new()),
        }
    }

    /// Create executor with shared storage
    pub fn with_storage(catalog: Arc<Catalog>, storage: Arc<TableStorage>) -> Self {
        Executor {
            catalog,
            storage,
            recursive_working_tables: RefCell::new(HashMap::new()),
        }
    }

    /// Get storage reference
    pub fn storage(&self) -> &TableStorage {
        &self.storage
    }

    /// Get evaluation context with catalog access
    fn eval_ctx(&self) -> EvalContext {
        EvalContext::with_catalog(self.catalog.clone())
    }

    /// Evaluate expression with catalog access (for NEXTVAL, etc.)
    fn eval(&self, expr: &Expression, row: &[Value]) -> Result<Value> {
        evaluate_with_ctx(expr, row, &self.eval_ctx())
    }

    /// Execute a LATERAL join - the right side is re-executed for each left row
    /// with the left row available as outer context for OuterColumnRef expressions
    fn execute_lateral_join(
        &self,
        _left: &LogicalOperator,
        right: &LogicalOperator,
        join_type: &ironduck_planner::JoinType,
        condition: &Option<Expression>,
        left_rows: &[Vec<Value>],
    ) -> Result<Vec<Vec<Value>>> {
        let mut result = Vec::new();
        let right_width = right.output_types().len();

        for left_row in left_rows {
            // Substitute OuterColumnRef in the right plan with values from left_row
            let substituted_right = substitute_outer_refs(right, left_row);

            // Execute the modified right plan
            let right_rows = self.execute_operator(&substituted_right)?;

            // Combine results based on join type
            match join_type {
                ironduck_planner::JoinType::Cross | ironduck_planner::JoinType::Inner => {
                    for r in &right_rows {
                        let mut row = left_row.clone();
                        row.extend(r.clone());

                        let matches = match condition {
                            Some(cond) => matches!(evaluate(cond, &row)?, Value::Boolean(true)),
                            None => true,
                        };

                        if matches {
                            result.push(row);
                        }
                    }
                }
                ironduck_planner::JoinType::Left => {
                    let mut matched = false;
                    for r in &right_rows {
                        let mut row = left_row.clone();
                        row.extend(r.clone());

                        let matches = match condition {
                            Some(cond) => matches!(evaluate(cond, &row)?, Value::Boolean(true)),
                            None => true,
                        };

                        if matches {
                            result.push(row);
                            matched = true;
                        }
                    }
                    if !matched {
                        let mut row = left_row.clone();
                        row.extend(vec![Value::Null; right_width]);
                        result.push(row);
                    }
                }
                _ => {
                    // For other join types, fall back to cross product behavior
                    for r in &right_rows {
                        let mut row = left_row.clone();
                        row.extend(r.clone());
                        result.push(row);
                    }
                }
            }
        }

        Ok(result)
    }

    /// Execute a logical plan and return results
    pub fn execute(&self, plan: &LogicalPlan) -> Result<QueryResult> {
        // Set the catalog for the current thread so aggregate functions can access it
        let catalog = self.catalog.clone();
        let rows = with_catalog(catalog, || self.execute_operator(&plan.root))?;

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
                let has_subquery = expressions.iter().any(contains_subquery);
                let has_rowid = expressions.iter().any(contains_rowid);

                for (row_idx, row) in input_rows.iter().enumerate() {
                    let mut output_row = Vec::new();
                    for expr in expressions {
                        let value = if has_subquery {
                            evaluate_with_subqueries(self, expr, row)?
                        } else if has_rowid {
                            let ctx = EvalContext::with_row_index(row_idx as i64);
                            evaluate_with_ctx(expr, row, &ctx)?
                        } else {
                            // Use eval_with_catalog to support outer column references in correlated subqueries
                            eval_with_catalog(expr, row)?
                        };
                        output_row.push(value);
                    }
                    output_rows.push(output_row);
                }

                // Handle case of DummyScan with constant expressions (SELECT 1)
                // Note: We only do this for DummyScan, not for empty results from Filter
                // because SELECT 1 WHERE FALSE should return no rows
                if input_rows.is_empty()
                    && expressions.iter().all(is_constant)
                    && matches!(input.as_ref(), LogicalOperator::DummyScan)
                {
                    let mut output_row = Vec::new();
                    for expr in expressions {
                        let value = if has_subquery {
                            evaluate_with_subqueries(self, expr, &[])?
                        } else {
                            // Use eval_with_catalog to support outer column references in correlated subqueries
                            eval_with_catalog(expr, &[])?
                        };
                        output_row.push(value);
                    }
                    output_rows.push(output_row);
                }

                Ok(output_rows)
            }

            LogicalOperator::Filter { input, predicate } => {
                let input_rows = self.execute_operator(input)?;
                let has_subquery = contains_subquery(predicate);
                let has_rowid = contains_rowid(predicate);
                let mut output_rows = Vec::new();

                for (row_idx, row) in input_rows.into_iter().enumerate() {
                    let result = if has_subquery {
                        evaluate_with_subqueries(self, predicate, &row)?
                    } else if has_rowid {
                        let ctx = EvalContext::with_row_index(row_idx as i64);
                        evaluate_with_ctx(predicate, &row, &ctx)?
                    } else {
                        // Use eval_with_catalog to support outer column references in correlated subqueries
                        eval_with_catalog(predicate, &row)?
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

                    // Sort group keys for deterministic output
                    let mut sorted_keys: Vec<_> = groups.keys().cloned().collect();
                    sorted_keys.sort();

                    for key in sorted_keys {
                        let group_rows = groups.get(&key).unwrap();
                        let mut result_row = Vec::new();

                        // Add group by values
                        if let Some(first_row) = group_rows.first() {
                            for expr in group_by {
                                result_row.push(evaluate(expr, first_row)?);
                            }
                        }

                        // Add aggregate values
                        for agg in aggregates {
                            let value = compute_aggregate(agg, group_rows)?;
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
                is_lateral,
            } => {
                let left_rows = self.execute_operator(left)?;

                // For LATERAL joins, we need to re-execute the right side for each left row
                // with the left row as outer context
                if *is_lateral {
                    return self.execute_lateral_join(left, right, join_type, condition, &left_rows);
                }

                let right_rows = self.execute_operator(right)?;
                let mut result = Vec::new();

                // Determine the width of left/right sides from schema (not from rows, which may be empty)
                let right_width = right.output_types().len();
                let left_width = left.output_types().len();

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
                    ironduck_planner::JoinType::Semi => {
                        // SEMI JOIN - return left rows that have at least one match in right
                        // Only returns columns from the left side
                        for l in &left_rows {
                            let mut has_match = false;
                            for r in &right_rows {
                                let mut combined = l.clone();
                                combined.extend(r.clone());

                                let matches = match condition {
                                    Some(cond) => {
                                        matches!(evaluate(cond, &combined)?, Value::Boolean(true))
                                    }
                                    None => true,
                                };

                                if matches {
                                    has_match = true;
                                    break; // Only need to find one match
                                }
                            }
                            if has_match {
                                result.push(l.clone()); // Only left side columns
                            }
                        }
                    }
                    ironduck_planner::JoinType::Anti => {
                        // ANTI JOIN - return left rows that have NO match in right
                        // Only returns columns from the left side
                        for l in &left_rows {
                            let mut has_match = false;
                            for r in &right_rows {
                                let mut combined = l.clone();
                                combined.extend(r.clone());

                                let matches = match condition {
                                    Some(cond) => {
                                        matches!(evaluate(cond, &combined)?, Value::Boolean(true))
                                    }
                                    None => true,
                                };

                                if matches {
                                    has_match = true;
                                    break;
                                }
                            }
                            if !has_match {
                                result.push(l.clone()); // Only left side columns
                            }
                        }
                    }
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
                default_values,
                if_not_exists,
                source,
            } => {
                // Check if table exists
                if self.catalog.get_table(schema, name).is_some() {
                    if *if_not_exists {
                        return Ok(vec![vec![Value::Varchar("Table already exists".to_string())]]);
                    } else {
                        return Err(Error::TableAlreadyExists(name.clone()));
                    }
                }

                // Evaluate default value expressions
                let defaults: Vec<Option<Value>> = default_values
                    .iter()
                    .map(|opt_expr| {
                        opt_expr.as_ref().map(|expr| evaluate(expr, &[]).unwrap_or(Value::Null))
                    })
                    .collect();

                // Create the table in catalog with defaults
                self.catalog.create_table_with_defaults(schema, name, columns.clone(), defaults)?;

                // Create storage for the table
                let types: Vec<_> = columns.iter().map(|(_, t)| t.clone()).collect();
                let table_data = self.storage.get_or_create(schema, name, &types);

                // For CREATE TABLE ... AS SELECT ..., insert the data
                if let Some(source_op) = source {
                    let source_rows = self.execute_operator(source_op)?;
                    for row in source_rows {
                        table_data.insert(row);
                    }
                }

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

            LogicalOperator::CreateView {
                schema,
                name,
                sql,
                column_names,
                or_replace,
            } => {
                self.catalog.create_view(
                    schema,
                    name,
                    sql.clone(),
                    column_names.clone(),
                    *or_replace,
                )?;
                Ok(vec![vec![Value::Varchar(format!("Created view {}", name))]])
            }

            LogicalOperator::CreateSequence {
                schema,
                name,
                start,
                increment,
                min_value,
                max_value,
                cycle,
                if_not_exists,
            } => {
                // Check if sequence exists
                if self.catalog.get_sequence(schema, name).is_some() {
                    if *if_not_exists {
                        return Ok(vec![vec![Value::Varchar("Sequence already exists".to_string())]]);
                    } else {
                        return Err(Error::SequenceAlreadyExists(name.clone()));
                    }
                }

                self.catalog.create_sequence(
                    schema,
                    name,
                    *start,
                    *increment,
                    *min_value,
                    *max_value,
                    *cycle,
                )?;
                Ok(vec![vec![Value::Varchar(format!("Created sequence {}", name))]])
            }

            LogicalOperator::Insert {
                schema,
                table,
                columns: insert_columns,
                values,
                source,
            } => {
                // Get table from catalog
                let table_info = self
                    .catalog
                    .get_table(schema, table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;

                // Get column types and defaults
                let types: Vec<_> = table_info.columns.iter().map(|c| c.logical_type.clone()).collect();
                let defaults: Vec<Option<Value>> = table_info.columns.iter().map(|c| c.default_value.clone()).collect();
                let table_column_names: Vec<_> = table_info.columns.iter().map(|c| c.name.to_lowercase()).collect();

                // Get or create storage
                let table_data = self.storage.get_or_create(schema, table, &types);

                let mut count = 0;

                // Build column mapping if we have a partial column list
                let column_indices: Option<Vec<usize>> = if !insert_columns.is_empty() {
                    Some(insert_columns.iter().map(|col| {
                        table_column_names.iter().position(|tc| tc == &col.to_lowercase())
                            .unwrap_or(0) // fallback
                    }).collect())
                } else {
                    None
                };

                // Handle INSERT ... SELECT ...
                if let Some(source_op) = source {
                    let source_rows = self.execute_operator(source_op)?;
                    for source_row in source_rows {
                        let row = if let Some(indices) = &column_indices {
                            // Partial column list - map values to correct positions
                            let mut full_row = vec![Value::Null; types.len()];
                            for (val_idx, &col_idx) in indices.iter().enumerate() {
                                if val_idx < source_row.len() && col_idx < full_row.len() {
                                    full_row[col_idx] = coerce_value(source_row[val_idx].clone(), &types[col_idx])?;
                                }
                            }
                            // Fill in defaults for missing columns
                            for (idx, val) in full_row.iter_mut().enumerate() {
                                if *val == Value::Null {
                                    if let Some(default) = &defaults[idx] {
                                        *val = default.clone();
                                    }
                                }
                            }
                            full_row
                        } else {
                            source_row
                        };
                        table_data.insert(row);
                        count += 1;
                    }
                } else {
                    // Handle INSERT ... VALUES (...)
                    for row_exprs in values {
                        let row = if let Some(indices) = &column_indices {
                            // Partial column list - map values to correct positions
                            let mut full_row = vec![Value::Null; types.len()];
                            for (val_idx, &col_idx) in indices.iter().enumerate() {
                                if val_idx < row_exprs.len() && col_idx < full_row.len() {
                                    let val = evaluate(&row_exprs[val_idx], &[])?;
                                    full_row[col_idx] = coerce_value(val, &types[col_idx])?;
                                }
                            }
                            // Fill in defaults for missing columns
                            for (idx, val) in full_row.iter_mut().enumerate() {
                                if *val == Value::Null {
                                    if let Some(default) = &defaults[idx] {
                                        *val = default.clone();
                                    }
                                }
                            }
                            full_row
                        } else {
                            // Full row - evaluate all expressions
                            let mut row = Vec::new();
                            for (idx, expr) in row_exprs.iter().enumerate() {
                                let val = evaluate(expr, &[])?;
                                let coerced = if idx < types.len() {
                                    coerce_value(val, &types[idx])?
                                } else {
                                    val
                                };
                                row.push(coerced);
                            }
                            row
                        };
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

            LogicalOperator::AlterTable {
                schema,
                table_name,
                operation,
            } => {
                use ironduck_planner::AlterTableOp;

                // Get the table
                let table_data = self
                    .storage
                    .get(schema, table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

                match operation {
                    AlterTableOp::AddColumn { name, data_type, nullable, default } => {
                        // Add the column to storage
                        let default_val = default.as_ref()
                            .map(|e| evaluate(e, &[]))
                            .transpose()?
                            .unwrap_or(Value::Null);
                        table_data.add_column(name, data_type.clone(), default_val.clone());

                        // Update the catalog
                        let col_name = name.clone();
                        let col_type = data_type.clone();
                        let col_nullable = *nullable;
                        let col_default = default_val;
                        self.catalog.alter_table(schema, table_name, |t| {
                            let next_id = t.columns.len() as u32;
                            let mut col = ironduck_catalog::Column::new(next_id, col_name, col_type);
                            col.nullable = col_nullable;
                            if col_default != Value::Null {
                                col.default_value = Some(col_default);
                            }
                            t.add_column(col);
                        })?;

                        Ok(vec![vec![Value::Varchar(format!("Added column {} to {}", name, table_name))]])
                    }
                    AlterTableOp::DropColumn { column_name, if_exists } => {
                        // Update storage
                        let dropped = table_data.drop_column(column_name);

                        if dropped {
                            // Update the catalog
                            let col_name = column_name.clone();
                            self.catalog.alter_table(schema, table_name, |t| {
                                t.drop_column(&col_name);
                            })?;
                            Ok(vec![vec![Value::Varchar(format!("Dropped column {} from {}", column_name, table_name))]])
                        } else if *if_exists {
                            Ok(vec![vec![Value::Varchar(format!("Column {} does not exist (skipped)", column_name))]])
                        } else {
                            Err(Error::ColumnNotFound(column_name.clone()))
                        }
                    }
                    AlterTableOp::RenameColumn { old_name, new_name } => {
                        if table_data.rename_column(old_name, new_name) {
                            // Update the catalog
                            let old = old_name.clone();
                            let new = new_name.clone();
                            self.catalog.alter_table(schema, table_name, |t| {
                                t.rename_column(&old, &new);
                            })?;
                            Ok(vec![vec![Value::Varchar(format!("Renamed column {} to {}", old_name, new_name))]])
                        } else {
                            Err(Error::ColumnNotFound(old_name.clone()))
                        }
                    }
                    AlterTableOp::RenameTable { new_name } => {
                        // Update storage
                        self.storage.rename_table(schema, table_name, new_name);
                        // Update catalog
                        self.catalog.rename_table(schema, table_name, new_name)?;
                        Ok(vec![vec![Value::Varchar(format!("Renamed table {} to {}", table_name, new_name))]])
                    }
                    AlterTableOp::AlterColumnType { column_name, new_type } => {
                        if table_data.alter_column_type(column_name, new_type.clone()) {
                            // Update the catalog
                            let col_name = column_name.clone();
                            let col_type = new_type.clone();
                            self.catalog.alter_table(schema, table_name, |t| {
                                t.alter_column_type(&col_name, col_type);
                            })?;
                            Ok(vec![vec![Value::Varchar(format!("Changed column {} type to {:?}", column_name, new_type))]])
                        } else {
                            Err(Error::ColumnNotFound(column_name.clone()))
                        }
                    }
                    AlterTableOp::SetColumnDefault { column_name, default } => {
                        let msg = if default.is_some() {
                            format!("Set default for column {}", column_name)
                        } else {
                            format!("Dropped default for column {}", column_name)
                        };
                        // Note: defaults are not enforced in current in-memory storage
                        Ok(vec![vec![Value::Varchar(msg)]])
                    }
                    AlterTableOp::SetColumnNotNull { column_name, not_null } => {
                        let msg = if *not_null {
                            format!("Set NOT NULL for column {}", column_name)
                        } else {
                            format!("Dropped NOT NULL for column {}", column_name)
                        };
                        // Note: constraints are not enforced in current in-memory storage
                        Ok(vec![vec![Value::Varchar(msg)]])
                    }
                }
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
                            WindowFunction::PercentRank => {
                                // PERCENT_RANK = (rank - 1) / (partition_size - 1)
                                let n = partition.len() as f64;
                                if n <= 1.0 {
                                    for (orig_idx, _) in &partition {
                                        window_results[win_idx][*orig_idx] = Value::Double(0.0);
                                    }
                                } else {
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
                                        let pct = (rank - 1) as f64 / (n - 1.0);
                                        window_results[win_idx][*orig_idx] = Value::Double(pct);
                                    }
                                }
                            }
                            WindowFunction::CumeDist => {
                                // CUME_DIST = rows_up_to_and_including / partition_size
                                let n = partition.len() as f64;
                                for (idx, (orig_idx, row)) in partition.iter().enumerate() {
                                    // Count rows with same or smaller order values
                                    let mut count = 0;
                                    for (_, other_row) in &partition {
                                        let le = win_expr.order_by.iter().all(|ob| {
                                            let curr = evaluate(&ob.expr, row).unwrap_or(Value::Null);
                                            let other = evaluate(&ob.expr, other_row).unwrap_or(Value::Null);
                                            other <= curr
                                        });
                                        if le || win_expr.order_by.is_empty() {
                                            count += 1;
                                        }
                                    }
                                    let cume = count as f64 / n;
                                    window_results[win_idx][*orig_idx] = Value::Double(cume);
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
                            WindowFunction::NthValue => {
                                // NTH_VALUE(expr, n) - get nth value in partition
                                let n = win_expr.args.get(1)
                                    .and_then(|e| evaluate(e, &partition[0].1).ok())
                                    .and_then(|v| v.as_i64())
                                    .unwrap_or(1) as usize;
                                let nth_val = if n > 0 && n <= partition.len() {
                                    if let Some(arg) = win_expr.args.first() {
                                        evaluate(arg, &partition[n - 1].1).unwrap_or(Value::Null)
                                    } else {
                                        Value::Null
                                    }
                                } else {
                                    Value::Null
                                };
                                for (orig_idx, _) in &partition {
                                    window_results[win_idx][*orig_idx] = nth_val.clone();
                                }
                            }
                            WindowFunction::Ntile => {
                                // NTILE(n) - divide partition into n buckets
                                let n = win_expr.args.first()
                                    .and_then(|e| evaluate(e, &partition[0].1).ok())
                                    .and_then(|v| v.as_i64())
                                    .unwrap_or(1) as usize;
                                let n = n.max(1);
                                let rows_per_bucket = partition.len() / n;
                                let extra_rows = partition.len() % n;
                                let mut bucket = 1;
                                let mut count = 0;
                                let bucket_size = if extra_rows > 0 { rows_per_bucket + 1 } else { rows_per_bucket };
                                let mut current_bucket_size = bucket_size;
                                for (orig_idx, _) in &partition {
                                    window_results[win_idx][*orig_idx] = Value::BigInt(bucket as i64);
                                    count += 1;
                                    if count >= current_bucket_size && bucket < n {
                                        bucket += 1;
                                        count = 0;
                                        current_bucket_size = if bucket <= extra_rows { rows_per_bucket + 1 } else { rows_per_bucket };
                                    }
                                }
                            }
                            WindowFunction::Sum => {
                                // SUM as window function
                                // By default (no frame), compute cumulative sum up to current row
                                let mut running_sum: Option<f64> = None;
                                for (idx, (orig_idx, row)) in partition.iter().enumerate() {
                                    let (start, end) = get_frame_bounds(&win_expr.frame, idx, partition.len());

                                    // For no frame or ROWS UNBOUNDED PRECEDING TO CURRENT ROW
                                    // we use cumulative sum for efficiency
                                    if win_expr.frame.is_none() && start == 0 {
                                        // Cumulative sum
                                        if let Some(arg) = win_expr.args.first() {
                                            let val = evaluate(arg, row).unwrap_or(Value::Null);
                                            if !val.is_null() {
                                                let num = val.as_f64().unwrap_or(0.0);
                                                running_sum = Some(running_sum.unwrap_or(0.0) + num);
                                            }
                                        }
                                        window_results[win_idx][*orig_idx] = running_sum.map_or(Value::Null, Value::Double);
                                    } else {
                                        // Frame-aware sum
                                        let mut sum: Option<f64> = None;
                                        for i in start..=end.min(partition.len() - 1) {
                                            if let Some(arg) = win_expr.args.first() {
                                                let val = evaluate(arg, &partition[i].1).unwrap_or(Value::Null);
                                                if !val.is_null() {
                                                    let num = val.as_f64().unwrap_or(0.0);
                                                    sum = Some(sum.unwrap_or(0.0) + num);
                                                }
                                            }
                                        }
                                        window_results[win_idx][*orig_idx] = sum.map_or(Value::Null, Value::Double);
                                    }
                                }
                            }
                            WindowFunction::Count => {
                                // COUNT as window function
                                for (idx, (orig_idx, row)) in partition.iter().enumerate() {
                                    let (start, end) = get_frame_bounds(&win_expr.frame, idx, partition.len());

                                    // Check if counting all (*) or specific expression
                                    let count = if win_expr.args.is_empty() {
                                        // COUNT(*) - count all rows in frame
                                        (end.min(partition.len() - 1) - start + 1) as i64
                                    } else {
                                        // COUNT(expr) - count non-null values in frame
                                        let mut cnt = 0i64;
                                        for i in start..=end.min(partition.len() - 1) {
                                            if let Some(arg) = win_expr.args.first() {
                                                let val = evaluate(arg, &partition[i].1).unwrap_or(Value::Null);
                                                if !val.is_null() {
                                                    cnt += 1;
                                                }
                                            }
                                        }
                                        cnt
                                    };
                                    window_results[win_idx][*orig_idx] = Value::BigInt(count);
                                }
                            }
                            WindowFunction::Avg => {
                                // AVG as window function
                                for (idx, (orig_idx, row)) in partition.iter().enumerate() {
                                    let (start, end) = get_frame_bounds(&win_expr.frame, idx, partition.len());

                                    let mut sum = 0.0;
                                    let mut count = 0i64;
                                    for i in start..=end.min(partition.len() - 1) {
                                        if let Some(arg) = win_expr.args.first() {
                                            let val = evaluate(arg, &partition[i].1).unwrap_or(Value::Null);
                                            if !val.is_null() {
                                                sum += val.as_f64().unwrap_or(0.0);
                                                count += 1;
                                            }
                                        }
                                    }
                                    window_results[win_idx][*orig_idx] = if count > 0 {
                                        Value::Double(sum / count as f64)
                                    } else {
                                        Value::Null
                                    };
                                }
                            }
                            WindowFunction::Min => {
                                // MIN as window function
                                for (idx, (orig_idx, row)) in partition.iter().enumerate() {
                                    let (start, end) = get_frame_bounds(&win_expr.frame, idx, partition.len());

                                    let mut min_val: Option<Value> = None;
                                    for i in start..=end.min(partition.len() - 1) {
                                        if let Some(arg) = win_expr.args.first() {
                                            let val = evaluate(arg, &partition[i].1).unwrap_or(Value::Null);
                                            if !val.is_null() {
                                                min_val = Some(match &min_val {
                                                    None => val,
                                                    Some(current) => {
                                                        if val.partial_cmp(current) == Some(std::cmp::Ordering::Less) {
                                                            val
                                                        } else {
                                                            current.clone()
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                    }
                                    window_results[win_idx][*orig_idx] = min_val.unwrap_or(Value::Null);
                                }
                            }
                            WindowFunction::Max => {
                                // MAX as window function
                                for (idx, (orig_idx, row)) in partition.iter().enumerate() {
                                    let (start, end) = get_frame_bounds(&win_expr.frame, idx, partition.len());

                                    let mut max_val: Option<Value> = None;
                                    for i in start..=end.min(partition.len() - 1) {
                                        if let Some(arg) = win_expr.args.first() {
                                            let val = evaluate(arg, &partition[i].1).unwrap_or(Value::Null);
                                            if !val.is_null() {
                                                max_val = Some(match &max_val {
                                                    None => val,
                                                    Some(current) => {
                                                        if val.partial_cmp(current) == Some(std::cmp::Ordering::Greater) {
                                                            val
                                                        } else {
                                                            current.clone()
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                    }
                                    window_results[win_idx][*orig_idx] = max_val.unwrap_or(Value::Null);
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

            LogicalOperator::Explain { input } => {
                // Format the query plan as a string
                let plan_str = format_plan(input, 0);
                Ok(vec![vec![Value::Varchar(plan_str)]])
            }

            LogicalOperator::RecursiveCTE {
                name,
                base_case,
                recursive_case,
                output_names: _,
                output_types: _,
                union_all,
            } => {
                // Execute the recursive CTE using iterative fixpoint computation
                // 1. Execute the base case (anchor)
                let base_result = self.execute_operator(base_case)?;

                // Initialize accumulator with base case results
                let mut result = base_result.clone();

                // The working table starts with the base case results
                // This is what RecursiveCTEScan will read from
                let mut working_table = base_result;

                // 2. Iteratively execute the recursive case until no new rows are produced
                let max_iterations = 1000; // Prevent infinite loops
                let mut iteration = 0;

                loop {
                    if iteration >= max_iterations {
                        return Err(Error::Execution(
                            "Recursive CTE exceeded maximum iteration limit (1000)".to_string()
                        ));
                    }
                    iteration += 1;

                    // Set the working table for this CTE so RecursiveCTEScan can access it
                    self.recursive_working_tables
                        .borrow_mut()
                        .insert(name.clone(), working_table.clone());

                    // Execute the recursive part - it will read from working_table via RecursiveCTEScan
                    let new_rows = self.execute_operator(recursive_case)?;

                    // Clear the working table entry after execution
                    self.recursive_working_tables.borrow_mut().remove(name);

                    if new_rows.is_empty() {
                        break;
                    }

                    // Add new rows to result and update working table for next iteration
                    if *union_all {
                        // UNION ALL: append all rows, working table becomes new rows only
                        result.extend(new_rows.clone());
                        working_table = new_rows;
                    } else {
                        // UNION: deduplicate, working table becomes only truly new rows
                        let mut truly_new = Vec::new();
                        for row in new_rows {
                            if !result.contains(&row) {
                                result.push(row.clone());
                                truly_new.push(row);
                            }
                        }
                        if truly_new.is_empty() {
                            break;
                        }
                        working_table = truly_new;
                    }
                }

                Ok(result)
            }

            LogicalOperator::NoOp => {
                // No-op statements return empty result
                Ok(vec![vec![Value::Varchar("OK".to_string())]])
            }

            LogicalOperator::TableFunction { function, .. } => {
                match function {
                    TableFunctionKind::Range { start, stop, step } => {
                        let start_val = evaluate(start, &[])?;
                        let stop_val = evaluate(stop, &[])?;
                        let step_val = evaluate(step, &[])?;

                        // Handle different types for range
                        match (&start_val, &stop_val, &step_val) {
                            // Date range with interval step
                            (Value::Date(start_date), Value::Date(stop_date), Value::Interval(interval)) => {
                                use chrono::Duration;

                                let mut rows = Vec::new();

                                // Convert interval to microseconds for sub-day precision
                                let micros_per_step = interval.micros
                                    + (interval.days as i64 * 24 * 60 * 60 * 1_000_000)
                                    + (interval.months as i64 * 30 * 24 * 60 * 60 * 1_000_000);

                                if micros_per_step == 0 {
                                    return Err(Error::Execution("range() step cannot be zero".to_string()));
                                }

                                // Convert dates to timestamps for sub-day interval support
                                let start_ts = start_date.and_hms_opt(0, 0, 0).unwrap();
                                let stop_ts = stop_date.and_hms_opt(0, 0, 0).unwrap();
                                let mut current = start_ts;

                                if micros_per_step > 0 {
                                    while current < stop_ts {
                                        rows.push(vec![Value::Timestamp(current)]);
                                        current = current + Duration::microseconds(micros_per_step);
                                    }
                                } else {
                                    while current > stop_ts {
                                        rows.push(vec![Value::Timestamp(current)]);
                                        current = current + Duration::microseconds(micros_per_step);
                                    }
                                }

                                Ok(rows)
                            }

                            // Timestamp range with interval step
                            (Value::Timestamp(start_ts), Value::Timestamp(stop_ts), Value::Interval(interval)) => {
                                use chrono::Duration;

                                let mut rows = Vec::new();
                                let mut current = *start_ts;

                                // Convert interval to microseconds
                                let micros_per_step = interval.micros
                                    + (interval.days as i64 * 24 * 60 * 60 * 1_000_000)
                                    + (interval.months as i64 * 30 * 24 * 60 * 60 * 1_000_000);

                                if micros_per_step == 0 {
                                    return Err(Error::Execution("range() step cannot be zero".to_string()));
                                }

                                if micros_per_step > 0 {
                                    while current < *stop_ts {
                                        rows.push(vec![Value::Timestamp(current)]);
                                        current = current + Duration::microseconds(micros_per_step);
                                    }
                                } else {
                                    while current > *stop_ts {
                                        rows.push(vec![Value::Timestamp(current)]);
                                        current = current + Duration::microseconds(micros_per_step);
                                    }
                                }

                                Ok(rows)
                            }

                            // Integer range (original behavior)
                            _ => {
                                let start = start_val.as_i64().unwrap_or(0);
                                let stop = stop_val.as_i64().unwrap_or(0);
                                let step = step_val.as_i64().unwrap_or(1);

                                if step == 0 {
                                    return Err(Error::Execution("range() step cannot be zero".to_string()));
                                }

                                let mut rows = Vec::new();
                                let mut current = start;

                                if step > 0 {
                                    while current < stop {
                                        rows.push(vec![Value::BigInt(current)]);
                                        current += step;
                                    }
                                } else {
                                    while current > stop {
                                        rows.push(vec![Value::BigInt(current)]);
                                        current += step;
                                    }
                                }

                                Ok(rows)
                            }
                        }
                    }

                    TableFunctionKind::Unnest { array_expr } => {
                        // Evaluate the array expression
                        let array_val = evaluate(array_expr, &[])?;

                        match array_val {
                            Value::List(elements) => {
                                // Expand each element into a row
                                Ok(elements.into_iter().map(|elem| vec![elem]).collect())
                            }
                            Value::Null => {
                                // NULL array produces empty result
                                Ok(vec![])
                            }
                            _ => {
                                // Non-array value produces single row
                                Ok(vec![vec![array_val]])
                            }
                        }
                    }

                    TableFunctionKind::GenerateSubscripts { array_expr, dim } => {
                        // Evaluate the array expression
                        let array_val = evaluate(array_expr, &[])?;

                        match array_val {
                            Value::List(elements) => {
                                // For dimension 1, generate 1-based indices for the array
                                if *dim == 1 {
                                    let len = elements.len() as i64;
                                    Ok((1..=len).map(|i| vec![Value::BigInt(i)]).collect())
                                } else {
                                    // Higher dimensions not yet supported for simple arrays
                                    Ok(vec![])
                                }
                            }
                            Value::Null => {
                                Ok(vec![])
                            }
                            _ => {
                                Err(Error::Execution("generate_subscripts requires an array argument".to_string()))
                            }
                        }
                    }
                }
            }

            LogicalOperator::RecursiveCTEScan { cte_name, .. } => {
                // RecursiveCTEScan reads from the working table set by the parent RecursiveCTE operator
                // The working table contains rows from the previous iteration
                let working_tables = self.recursive_working_tables.borrow();
                if let Some(rows) = working_tables.get(cte_name) {
                    Ok(rows.clone())
                } else {
                    // If no working table is set, this means RecursiveCTEScan is being
                    // executed outside of a RecursiveCTE context, which is an error
                    Err(Error::Execution(format!(
                        "RecursiveCTEScan for '{}' executed outside of RecursiveCTE context",
                        cte_name
                    )))
                }
            }

            LogicalOperator::Pivot {
                input,
                aggregate_function,
                aggregate_arg,
                value_column: _,
                value_column_index,
                pivot_values,
                group_columns,
            } => {
                let input_rows = self.execute_operator(input)?;

                // Group rows by the group columns - use String key since Value doesn't implement Hash
                let mut groups: std::collections::HashMap<String, (Vec<Value>, Vec<Vec<Value>>)> = std::collections::HashMap::new();

                for row in &input_rows {
                    let group_key_values: Vec<Value> = group_columns.iter()
                        .map(|(_, _, idx)| row.get(*idx).cloned().unwrap_or(Value::Null))
                        .collect();
                    let group_key = format!("{:?}", group_key_values);
                    groups.entry(group_key).or_insert_with(|| (group_key_values.clone(), Vec::new())).1.push(row.clone());
                }

                // For each group, compute aggregates for each pivot value
                let mut result = Vec::new();

                for (_key, (group_key_values, group_rows)) in groups {
                    let mut output_row = group_key_values;

                    // For each pivot value, filter rows and compute aggregate
                    for pivot_val in pivot_values {
                        // Filter rows where value_column matches pivot_val
                        let matching_rows: Vec<&Vec<Value>> = group_rows.iter()
                            .filter(|row| {
                                if let Some(val) = row.get(*value_column_index) {
                                    val.to_string() == *pivot_val || format!("{:?}", val) == *pivot_val
                                } else {
                                    false
                                }
                            })
                            .collect();

                        // Compute aggregate on matching rows
                        let agg_result = if matching_rows.is_empty() {
                            Value::Null
                        } else {
                            let agg_values: Vec<Value> = matching_rows.iter()
                                .map(|row| evaluate(aggregate_arg, row).unwrap_or(Value::Null))
                                .collect();
                            compute_pivot_aggregate(aggregate_function, &agg_values)
                        };

                        output_row.push(agg_result);
                    }

                    result.push(output_row);
                }

                Ok(result)
            }

            LogicalOperator::Unpivot {
                input,
                value_column: _,
                name_column: _,
                unpivot_columns,
                keep_columns,
            } => {
                let input_rows = self.execute_operator(input)?;
                let mut result = Vec::new();

                for row in &input_rows {
                    // For each unpivot column, create a new row
                    for (col_name, col_idx, _) in unpivot_columns {
                        let mut output_row = Vec::new();

                        // Add keep columns
                        for (_, idx, _) in keep_columns {
                            output_row.push(row.get(*idx).cloned().unwrap_or(Value::Null));
                        }

                        // Order: keep columns, then name column (column name), then value column (value)
                        // Add name column (the column name) first
                        output_row.push(Value::Varchar(col_name.clone()));

                        // Add value column (the actual value from the unpivoted column) second
                        output_row.push(row.get(*col_idx).cloned().unwrap_or(Value::Null));

                        result.push(output_row);
                    }
                }

                Ok(result)
            }
        }
    }
}

/// Compute a simple aggregate value from a list of values (for PIVOT)
fn compute_pivot_aggregate(func: &ironduck_planner::AggregateFunction, values: &[Value]) -> Value {
    use ironduck_planner::AggregateFunction;

    match func {
        AggregateFunction::Count => Value::BigInt(values.len() as i64),
        AggregateFunction::Sum => {
            let mut sum = 0.0f64;
            let mut has_value = false;
            for v in values {
                if let Some(n) = v.as_f64() {
                    sum += n;
                    has_value = true;
                }
            }
            if has_value { Value::Double(sum) } else { Value::Null }
        }
        AggregateFunction::Avg => {
            let mut sum = 0.0f64;
            let mut count = 0;
            for v in values {
                if let Some(n) = v.as_f64() {
                    sum += n;
                    count += 1;
                }
            }
            if count > 0 { Value::Double(sum / count as f64) } else { Value::Null }
        }
        AggregateFunction::Min => {
            values.iter()
                .filter(|v| !matches!(v, Value::Null))
                .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .cloned()
                .unwrap_or(Value::Null)
        }
        AggregateFunction::Max => {
            values.iter()
                .filter(|v| !matches!(v, Value::Null))
                .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                .cloned()
                .unwrap_or(Value::Null)
        }
        AggregateFunction::First => {
            values.first().cloned().unwrap_or(Value::Null)
        }
        AggregateFunction::Last => {
            values.last().cloned().unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

/// Get frame bounds for a window function at a given row index
/// Returns (start_idx, end_idx) for the frame
fn get_frame_bounds(frame: &Option<WindowFrame>, current_idx: usize, partition_len: usize) -> (usize, usize) {
    match frame {
        None => {
            // Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            (0, current_idx)
        }
        Some(f) => {
            let start = match &f.start {
                WindowFrameBound::UnboundedPreceding => 0,
                WindowFrameBound::CurrentRow => current_idx,
                WindowFrameBound::Preceding(n) => {
                    // n is an expression - evaluate it
                    if let Expression::Constant(val) = n {
                        let offset = val.as_i64().unwrap_or(0) as usize;
                        current_idx.saturating_sub(offset)
                    } else {
                        current_idx
                    }
                }
                WindowFrameBound::Following(n) => {
                    if let Expression::Constant(val) = n {
                        let offset = val.as_i64().unwrap_or(0) as usize;
                        (current_idx + offset).min(partition_len - 1)
                    } else {
                        current_idx
                    }
                }
                WindowFrameBound::UnboundedFollowing => partition_len - 1,
            };

            let end = match &f.end {
                WindowFrameBound::UnboundedPreceding => 0,
                WindowFrameBound::CurrentRow => current_idx,
                WindowFrameBound::Preceding(n) => {
                    if let Expression::Constant(val) = n {
                        let offset = val.as_i64().unwrap_or(0) as usize;
                        current_idx.saturating_sub(offset)
                    } else {
                        current_idx
                    }
                }
                WindowFrameBound::Following(n) => {
                    if let Expression::Constant(val) = n {
                        let offset = val.as_i64().unwrap_or(0) as usize;
                        (current_idx + offset).min(partition_len - 1)
                    } else {
                        current_idx
                    }
                }
                WindowFrameBound::UnboundedFollowing => partition_len - 1,
            };

            (start, end)
        }
    }
}

/// Format a logical operator as a string for EXPLAIN output
fn format_plan(op: &LogicalOperator, indent: usize) -> String {
    let prefix = "  ".repeat(indent);
    match op {
        LogicalOperator::Scan { schema, table, .. } => {
            format!("{}Scan: {}.{}", prefix, schema, table)
        }
        LogicalOperator::DummyScan => {
            format!("{}DummyScan", prefix)
        }
        LogicalOperator::Filter { input, .. } => {
            format!("{}Filter\n{}", prefix, format_plan(input, indent + 1))
        }
        LogicalOperator::Project { input, output_names, .. } => {
            format!(
                "{}Project: [{}]\n{}",
                prefix,
                output_names.join(", "),
                format_plan(input, indent + 1)
            )
        }
        LogicalOperator::Aggregate { input, group_by, aggregates, .. } => {
            format!(
                "{}Aggregate (groups: {}, aggs: {})\n{}",
                prefix,
                group_by.len(),
                aggregates.len(),
                format_plan(input, indent + 1)
            )
        }
        LogicalOperator::Join { left, right, join_type, .. } => {
            format!(
                "{}{:?}Join\n{}\n{}",
                prefix,
                join_type,
                format_plan(left, indent + 1),
                format_plan(right, indent + 1)
            )
        }
        LogicalOperator::Sort { input, order_by, .. } => {
            format!(
                "{}Sort ({} columns)\n{}",
                prefix,
                order_by.len(),
                format_plan(input, indent + 1)
            )
        }
        LogicalOperator::Limit { input, limit, offset } => {
            format!(
                "{}Limit (limit: {:?}, offset: {:?})\n{}",
                prefix,
                limit,
                offset,
                format_plan(input, indent + 1)
            )
        }
        LogicalOperator::Distinct { input, .. } => {
            format!("{}Distinct\n{}", prefix, format_plan(input, indent + 1))
        }
        LogicalOperator::Values { values, .. } => {
            format!("{}Values ({} rows)", prefix, values.len())
        }
        LogicalOperator::CreateTable { name, .. } => {
            format!("{}CreateTable: {}", prefix, name)
        }
        LogicalOperator::CreateSchema { name, .. } => {
            format!("{}CreateSchema: {}", prefix, name)
        }
        LogicalOperator::CreateView { name, .. } => {
            format!("{}CreateView: {}", prefix, name)
        }
        LogicalOperator::CreateSequence { name, .. } => {
            format!("{}CreateSequence: {}", prefix, name)
        }
        LogicalOperator::Insert { table, .. } => {
            format!("{}Insert: {}", prefix, table)
        }
        LogicalOperator::Delete { table, .. } => {
            format!("{}Delete: {}", prefix, table)
        }
        LogicalOperator::Update { table, .. } => {
            format!("{}Update: {}", prefix, table)
        }
        LogicalOperator::Drop { name, .. } => {
            format!("{}Drop: {}", prefix, name)
        }
        LogicalOperator::AlterTable { table_name, operation, .. } => {
            format!("{}AlterTable: {} ({:?})", prefix, table_name, operation)
        }
        LogicalOperator::SetOperation { left, right, op, all } => {
            format!(
                "{}{:?}{}\n{}\n{}",
                prefix,
                op,
                if *all { " ALL" } else { "" },
                format_plan(left, indent + 1),
                format_plan(right, indent + 1)
            )
        }
        LogicalOperator::Window { input, window_exprs, .. } => {
            format!(
                "{}Window ({} expressions)\n{}",
                prefix,
                window_exprs.len(),
                format_plan(input, indent + 1)
            )
        }
        LogicalOperator::Explain { input } => {
            format!("{}Explain\n{}", prefix, format_plan(input, indent + 1))
        }
        LogicalOperator::RecursiveCTE { name, base_case, recursive_case, .. } => {
            format!(
                "{}RecursiveCTE: {}\n{}Base:\n{}\n{}Recursive:\n{}",
                prefix,
                name,
                prefix,
                format_plan(base_case, indent + 1),
                prefix,
                format_plan(recursive_case, indent + 1)
            )
        }
        LogicalOperator::NoOp => {
            format!("{}NoOp", prefix)
        }
        LogicalOperator::TableFunction { function, column_name, .. } => {
            let func_name = match function {
                TableFunctionKind::Range { .. } => "range",
                TableFunctionKind::Unnest { .. } => "unnest",
                TableFunctionKind::GenerateSubscripts { .. } => "generate_subscripts",
            };
            format!("{}TableFunction: {}() -> {}", prefix, func_name, column_name)
        }
        LogicalOperator::RecursiveCTEScan { cte_name, .. } => {
            format!("{}RecursiveCTEScan: {}", prefix, cte_name)
        }
        LogicalOperator::Pivot { input, aggregate_function, value_column, pivot_values, .. } => {
            format!(
                "{}Pivot ({:?}({}) FOR {} IN ({}))\n{}",
                prefix,
                aggregate_function,
                value_column,
                value_column,
                pivot_values.join(", "),
                format_plan(input, indent + 1)
            )
        }
        LogicalOperator::Unpivot { input, value_column, name_column, unpivot_columns, .. } => {
            let col_names: Vec<_> = unpivot_columns.iter().map(|(name, _, _)| name.as_str()).collect();
            format!(
                "{}Unpivot ({} FOR {} IN ({}))\n{}",
                prefix,
                value_column,
                name_column,
                col_names.join(", "),
                format_plan(input, indent + 1)
            )
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

            // Execute subquery with outer row for correlated subqueries
            let subquery_rows = with_outer_row(row.to_vec(), || executor.execute_operator(subquery))?;

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
            // Execute subquery with outer row for correlated subqueries
            let subquery_rows = with_outer_row(row.to_vec(), || executor.execute_operator(subquery))?;

            let exists = !subquery_rows.is_empty();
            let result = if *negated { !exists } else { exists };
            Ok(Value::Boolean(result))
        }

        Expression::Subquery(subquery) => {
            // Execute scalar subquery with outer row for correlated subqueries
            let subquery_rows = with_outer_row(row.to_vec(), || executor.execute_operator(subquery))?;

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
        // Use eval_with_catalog to get outer row context for correlated subqueries
        _ => eval_with_catalog(expr, row),
    }
}

/// Check if expression contains subqueries
fn contains_subquery(expr: &Expression) -> bool {
    match expr {
        Expression::InSubquery { .. } | Expression::Exists { .. } | Expression::Subquery(_) => true,
        Expression::BinaryOp { left, right, .. } => contains_subquery(left) || contains_subquery(right),
        Expression::UnaryOp { expr, .. } => contains_subquery(expr),
        Expression::Function { args, .. } => args.iter().any(contains_subquery),
        Expression::Cast { expr, .. } | Expression::TryCast { expr, .. } => contains_subquery(expr),
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

/// Check if expression contains rowid references
fn contains_rowid(expr: &Expression) -> bool {
    match expr {
        Expression::RowId { .. } => true,
        Expression::BinaryOp { left, right, .. } => contains_rowid(left) || contains_rowid(right),
        Expression::UnaryOp { expr, .. } => contains_rowid(expr),
        Expression::Function { args, .. } => args.iter().any(contains_rowid),
        Expression::Cast { expr, .. } | Expression::TryCast { expr, .. } => contains_rowid(expr),
        Expression::IsNull(expr) | Expression::IsNotNull(expr) => contains_rowid(expr),
        Expression::Case { operand, conditions, results, else_result } => {
            operand.as_ref().map_or(false, |e| contains_rowid(e))
                || conditions.iter().any(contains_rowid)
                || results.iter().any(contains_rowid)
                || else_result.as_ref().map_or(false, |e| contains_rowid(e))
        }
        Expression::InList { expr, list, .. } => contains_rowid(expr) || list.iter().any(contains_rowid),
        Expression::InSubquery { expr, .. } => contains_rowid(expr),
        _ => false,
    }
}

/// Substitute OuterColumnRef expressions in a logical operator with constant values
/// This is used for LATERAL joins where we need to execute the right side with
/// values from the current left row
fn substitute_outer_refs(op: &LogicalOperator, outer_row: &[Value]) -> LogicalOperator {
    match op {
        LogicalOperator::Project { input, expressions, output_names, output_types } => {
            LogicalOperator::Project {
                input: Box::new(substitute_outer_refs(input, outer_row)),
                expressions: expressions.iter().map(|e| substitute_expr_outer_refs(e, outer_row)).collect(),
                output_names: output_names.clone(),
                output_types: output_types.clone(),
            }
        }
        LogicalOperator::Filter { input, predicate } => {
            LogicalOperator::Filter {
                input: Box::new(substitute_outer_refs(input, outer_row)),
                predicate: substitute_expr_outer_refs(predicate, outer_row),
            }
        }
        LogicalOperator::Aggregate { input, group_by, aggregates } => {
            LogicalOperator::Aggregate {
                input: Box::new(substitute_outer_refs(input, outer_row)),
                group_by: group_by.iter().map(|e| substitute_expr_outer_refs(e, outer_row)).collect(),
                aggregates: aggregates.iter().map(|agg| {
                    ironduck_planner::AggregateExpression {
                        function: agg.function.clone(),
                        args: agg.args.iter().map(|e| substitute_expr_outer_refs(e, outer_row)).collect(),
                        distinct: agg.distinct,
                        filter: agg.filter.as_ref().map(|e| substitute_expr_outer_refs(e, outer_row)),
                        order_by: agg.order_by.iter().map(|(e, asc, nf)| (substitute_expr_outer_refs(e, outer_row), *asc, *nf)).collect(),
                    }
                }).collect(),
            }
        }
        LogicalOperator::Scan { schema, table, column_names, output_types } => {
            LogicalOperator::Scan {
                schema: schema.clone(),
                table: table.clone(),
                column_names: column_names.clone(),
                output_types: output_types.clone(),
            }
        }
        LogicalOperator::Join { left, right, join_type, condition, is_lateral } => {
            LogicalOperator::Join {
                left: Box::new(substitute_outer_refs(left, outer_row)),
                right: Box::new(substitute_outer_refs(right, outer_row)),
                join_type: *join_type,
                condition: condition.as_ref().map(|e| substitute_expr_outer_refs(e, outer_row)),
                is_lateral: *is_lateral,
            }
        }
        LogicalOperator::Sort { input, order_by } => {
            LogicalOperator::Sort {
                input: Box::new(substitute_outer_refs(input, outer_row)),
                order_by: order_by.iter().map(|o| ironduck_planner::OrderByExpression {
                    expr: substitute_expr_outer_refs(&o.expr, outer_row),
                    ascending: o.ascending,
                    nulls_first: o.nulls_first,
                }).collect(),
            }
        }
        LogicalOperator::Limit { input, limit, offset } => {
            LogicalOperator::Limit {
                input: Box::new(substitute_outer_refs(input, outer_row)),
                limit: *limit,
                offset: *offset,
            }
        }
        LogicalOperator::Distinct { input, on_exprs } => {
            LogicalOperator::Distinct {
                input: Box::new(substitute_outer_refs(input, outer_row)),
                on_exprs: on_exprs.as_ref().map(|es| es.iter().map(|e| substitute_expr_outer_refs(e, outer_row)).collect()),
            }
        }
        // For operators that don't contain expressions, just return as-is
        _ => op.clone(),
    }
}

/// Substitute OuterColumnRef in an expression with constant values
fn substitute_expr_outer_refs(expr: &Expression, outer_row: &[Value]) -> Expression {
    match expr {
        Expression::OuterColumnRef { column_index, .. } => {
            // Replace with constant value from outer row
            if let Some(value) = outer_row.get(*column_index) {
                Expression::Constant(value.clone())
            } else {
                Expression::Constant(Value::Null)
            }
        }
        Expression::BinaryOp { left, op, right } => {
            Expression::BinaryOp {
                left: Box::new(substitute_expr_outer_refs(left, outer_row)),
                op: *op,
                right: Box::new(substitute_expr_outer_refs(right, outer_row)),
            }
        }
        Expression::UnaryOp { op, expr } => {
            Expression::UnaryOp {
                op: *op,
                expr: Box::new(substitute_expr_outer_refs(expr, outer_row)),
            }
        }
        Expression::Function { name, args } => {
            Expression::Function {
                name: name.clone(),
                args: args.iter().map(|e| substitute_expr_outer_refs(e, outer_row)).collect(),
            }
        }
        Expression::Cast { expr, target_type } => {
            Expression::Cast {
                expr: Box::new(substitute_expr_outer_refs(expr, outer_row)),
                target_type: target_type.clone(),
            }
        }
        Expression::TryCast { expr, target_type } => {
            Expression::TryCast {
                expr: Box::new(substitute_expr_outer_refs(expr, outer_row)),
                target_type: target_type.clone(),
            }
        }
        Expression::IsNull(e) => Expression::IsNull(Box::new(substitute_expr_outer_refs(e, outer_row))),
        Expression::IsNotNull(e) => Expression::IsNotNull(Box::new(substitute_expr_outer_refs(e, outer_row))),
        Expression::Case { operand, conditions, results, else_result } => {
            Expression::Case {
                operand: operand.as_ref().map(|e| Box::new(substitute_expr_outer_refs(e, outer_row))),
                conditions: conditions.iter().map(|e| substitute_expr_outer_refs(e, outer_row)).collect(),
                results: results.iter().map(|e| substitute_expr_outer_refs(e, outer_row)).collect(),
                else_result: else_result.as_ref().map(|e| Box::new(substitute_expr_outer_refs(e, outer_row))),
            }
        }
        Expression::InList { expr, list, negated } => {
            Expression::InList {
                expr: Box::new(substitute_expr_outer_refs(expr, outer_row)),
                list: list.iter().map(|e| substitute_expr_outer_refs(e, outer_row)).collect(),
                negated: *negated,
            }
        }
        // For other expressions (Constant, ColumnRef, etc.), return as-is
        _ => expr.clone(),
    }
}

/// Check if an expression is constant (no column references)
fn is_constant(expr: &Expression) -> bool {
    match expr {
        Expression::Constant(_) => true,
        Expression::ColumnRef { .. } => false,
        Expression::OuterColumnRef { .. } => false, // Depends on outer row
        Expression::RowId { .. } => false,
        Expression::BinaryOp { left, right, .. } => is_constant(left) && is_constant(right),
        Expression::UnaryOp { expr, .. } => is_constant(expr),
        Expression::Function { args, .. } => args.iter().all(is_constant),
        Expression::Cast { expr, .. } | Expression::TryCast { expr, .. } => is_constant(expr),
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

/// Sort rows based on ORDER BY expressions for ordered aggregates
fn sort_rows_for_aggregate(
    rows: &[Vec<Value>],
    order_by: &[(Expression, bool, bool)],
) -> Result<Vec<Vec<Value>>> {
    use std::cmp::Ordering;

    let mut indexed_rows: Vec<(usize, Vec<Value>)> = rows
        .iter()
        .enumerate()
        .map(|(i, row)| (i, row.clone()))
        .collect();

    // Sort with error handling
    let mut sort_error: Option<Error> = None;
    indexed_rows.sort_by(|(_, row_a), (_, row_b)| {
        if sort_error.is_some() {
            return Ordering::Equal;
        }

        for (expr, ascending, nulls_first) in order_by {
            let val_a = match evaluate(expr, row_a) {
                Ok(v) => v,
                Err(e) => {
                    sort_error = Some(e);
                    return Ordering::Equal;
                }
            };
            let val_b = match evaluate(expr, row_b) {
                Ok(v) => v,
                Err(e) => {
                    sort_error = Some(e);
                    return Ordering::Equal;
                }
            };

            // Handle nulls
            match (&val_a, &val_b) {
                (Value::Null, Value::Null) => continue,
                (Value::Null, _) => {
                    return if *nulls_first { Ordering::Less } else { Ordering::Greater };
                }
                (_, Value::Null) => {
                    return if *nulls_first { Ordering::Greater } else { Ordering::Less };
                }
                _ => {}
            }

            let cmp = val_a.partial_cmp(&val_b).unwrap_or(Ordering::Equal);
            let cmp = if *ascending { cmp } else { cmp.reverse() };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    });

    if let Some(e) = sort_error {
        return Err(e);
    }

    Ok(indexed_rows.into_iter().map(|(_, row)| row).collect())
}

/// Compute an aggregate function
fn compute_aggregate(
    agg: &ironduck_planner::AggregateExpression,
    rows: &[Vec<Value>],
) -> Result<Value> {
    use ironduck_planner::AggregateFunction::*;

    let args = &agg.args;
    let distinct = agg.distinct;
    let has_order_by = !agg.order_by.is_empty();

    // Apply FILTER clause if present - filter out rows that don't match
    let filtered_rows: Vec<Vec<Value>>;
    let rows = if let Some(filter) = &agg.filter {
        filtered_rows = rows
            .iter()
            .filter(|row| {
                match eval_with_catalog(filter, row) {
                    Ok(Value::Boolean(true)) => true,
                    _ => false, // NULL or false means exclude the row
                }
            })
            .cloned()
            .collect();
        &filtered_rows[..]
    } else {
        rows
    };

    // If there's an ORDER BY clause, sort the rows first
    let sorted_rows: Vec<Vec<Value>>;
    let rows = if !agg.order_by.is_empty() {
        sorted_rows = sort_rows_for_aggregate(rows, &agg.order_by)?;
        &sorted_rows[..]
    } else {
        rows
    };

    match agg.function {
        Count => {
            if args.is_empty() {
                // COUNT(*) counts all rows
                Ok(Value::BigInt(rows.len() as i64))
            } else if distinct {
                // COUNT(DISTINCT expr) - count distinct non-NULL values
                let mut seen = std::collections::HashSet::new();
                for row in rows {
                    let val = eval_with_catalog(&args[0], row)?;
                    if !val.is_null() {
                        seen.insert(format!("{:?}", val));
                    }
                }
                Ok(Value::BigInt(seen.len() as i64))
            } else {
                // COUNT(expr) - count non-NULL values
                let mut count = 0i64;
                for row in rows {
                    let val = eval_with_catalog(&args[0], row)?;
                    if !val.is_null() {
                        count += 1;
                    }
                }
                Ok(Value::BigInt(count))
            }
        }

        Sum => {
            // Use i128 for integer sums to preserve precision, f64 for floats
            let mut int_sum: i128 = 0;
            let mut float_sum: f64 = 0.0;
            let mut has_value = false;
            let mut has_float = false;

            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = eval_with_catalog(&args[0], row)?;
                match val {
                    Value::TinyInt(i) => {
                        has_value = true;
                        int_sum += i as i128;
                    }
                    Value::SmallInt(i) => {
                        has_value = true;
                        int_sum += i as i128;
                    }
                    Value::Integer(i) => {
                        has_value = true;
                        int_sum += i as i128;
                    }
                    Value::BigInt(i) => {
                        has_value = true;
                        int_sum += i as i128;
                    }
                    Value::HugeInt(i) => {
                        has_value = true;
                        int_sum += i;
                    }
                    Value::Float(f) => {
                        has_value = true;
                        has_float = true;
                        float_sum += f as f64;
                    }
                    Value::Double(f) => {
                        has_value = true;
                        has_float = true;
                        float_sum += f;
                    }
                    Value::Boolean(b) => {
                        // SUM(boolean) counts TRUE as 1, FALSE as 0
                        has_value = true;
                        if b {
                            int_sum += 1;
                        }
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

            if !has_value {
                Ok(Value::Null)
            } else if has_float {
                // If we had any floats, return as double
                Ok(Value::Double(float_sum + int_sum as f64))
            } else {
                // For pure integer sums, return HugeInt to preserve precision
                Ok(Value::HugeInt(int_sum))
            }
        }

        Avg => {
            use chrono::{NaiveDateTime, NaiveDate, NaiveTime, DateTime, Utc, TimeZone, Timelike};
            use ironduck_common::value::Interval;

            // Track what type we're averaging
            enum AvgType {
                Numeric,
                Timestamp,
                Date,
                Time,
                TimeTz,
                TimestampTz,
                Interval,
            }

            // Collect values (applying DISTINCT if needed)
            let mut values: Vec<Value> = Vec::new();
            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = eval_with_catalog(&args[0], row)?;
                if val.is_null() {
                    continue;
                }

                if distinct {
                    let key = format!("{:?}", val);
                    if seen.insert(key) {
                        values.push(val);
                    }
                } else {
                    values.push(val);
                }
            }

            let mut sum = 0f64;
            let mut count = 0i64;
            let mut avg_type = None;

            // For intervals, we need separate accumulators
            let mut interval_months: i64 = 0;
            let mut interval_days: i64 = 0;
            let mut interval_micros: i128 = 0;

            for val in values {
                match val {
                    Value::Null => {} // Skip nulls
                    Value::TinyInt(i) => {
                        avg_type.get_or_insert(AvgType::Numeric);
                        sum += i as f64;
                        count += 1;
                    }
                    Value::SmallInt(i) => {
                        avg_type.get_or_insert(AvgType::Numeric);
                        sum += i as f64;
                        count += 1;
                    }
                    Value::Integer(i) => {
                        avg_type.get_or_insert(AvgType::Numeric);
                        sum += i as f64;
                        count += 1;
                    }
                    Value::BigInt(i) => {
                        avg_type.get_or_insert(AvgType::Numeric);
                        sum += i as f64;
                        count += 1;
                    }
                    Value::HugeInt(i) => {
                        avg_type.get_or_insert(AvgType::Numeric);
                        sum += i as f64;
                        count += 1;
                    }
                    Value::Float(f) => {
                        avg_type.get_or_insert(AvgType::Numeric);
                        sum += f as f64;
                        count += 1;
                    }
                    Value::Double(f) => {
                        avg_type.get_or_insert(AvgType::Numeric);
                        sum += f;
                        count += 1;
                    }
                    Value::Timestamp(ts) => {
                        avg_type.get_or_insert(AvgType::Timestamp);
                        // Convert to microseconds since epoch
                        let micros = ts.and_utc().timestamp_micros();
                        sum += micros as f64;
                        count += 1;
                    }
                    Value::Date(d) => {
                        avg_type.get_or_insert(AvgType::Date);
                        // Convert to days since Unix epoch, then to microseconds for precision
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let days = (d - epoch).num_days();
                        // Store as microseconds (days * 24 * 60 * 60 * 1_000_000)
                        let micros = days as f64 * 86400.0 * 1_000_000.0;
                        sum += micros;
                        count += 1;
                    }
                    Value::Time(t) => {
                        avg_type.get_or_insert(AvgType::Time);
                        // Convert to nanoseconds since midnight
                        let nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                            + t.nanosecond() as i64;
                        sum += nanos as f64;
                        count += 1;
                    }
                    Value::TimeTz(t, offset_secs) => {
                        avg_type.get_or_insert(AvgType::TimeTz);
                        // Convert to nanoseconds since midnight, then normalize to UTC
                        // by subtracting the offset
                        let local_nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                            + t.nanosecond() as i64;
                        // Offset is in seconds, convert to nanos and subtract to get UTC
                        let offset_nanos = offset_secs as i64 * 1_000_000_000;
                        let utc_nanos = local_nanos - offset_nanos;
                        // Wrap around 24 hours (handle times that cross midnight)
                        let day_nanos: i64 = 24 * 60 * 60 * 1_000_000_000;
                        let utc_nanos = ((utc_nanos % day_nanos) + day_nanos) % day_nanos;
                        sum += utc_nanos as f64;
                        count += 1;
                    }
                    Value::TimestampTz(ts) => {
                        avg_type.get_or_insert(AvgType::TimestampTz);
                        let micros = ts.timestamp_micros();
                        sum += micros as f64;
                        count += 1;
                    }
                    Value::Interval(i) => {
                        avg_type.get_or_insert(AvgType::Interval);
                        interval_months += i.months as i64;
                        interval_days += i.days as i64;
                        interval_micros += i.micros as i128;
                        count += 1;
                    }
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "numeric or temporal".to_string(),
                            got: format!("{:?}", val),
                        })
                    }
                }
            }

            if count == 0 {
                Ok(Value::Null)
            } else {
                match avg_type {
                    Some(AvgType::Numeric) | None => {
                        Ok(Value::Double(sum / count as f64))
                    }
                    Some(AvgType::Timestamp) => {
                        let avg_micros = (sum / count as f64) as i64;
                        let secs = avg_micros / 1_000_000;
                        let micros = (avg_micros % 1_000_000) as u32;
                        let ts = DateTime::from_timestamp(secs, micros * 1000)
                            .map(|dt| dt.naive_utc())
                            .unwrap_or_else(|| NaiveDateTime::default());
                        Ok(Value::Timestamp(ts))
                    }
                    Some(AvgType::Date) => {
                        // Average of dates returns a timestamp (to handle fractional days)
                        let avg_micros = (sum / count as f64) as i64;
                        let secs = avg_micros / 1_000_000;
                        let micros = ((avg_micros % 1_000_000).abs()) as u32;
                        let ts = DateTime::from_timestamp(secs, micros * 1000)
                            .map(|dt| dt.naive_utc())
                            .unwrap_or_else(|| NaiveDateTime::default());
                        Ok(Value::Timestamp(ts))
                    }
                    Some(AvgType::Time) => {
                        let avg_nanos = (sum / count as f64) as i64;
                        let secs = (avg_nanos / 1_000_000_000) as u32;
                        let nanos = (avg_nanos % 1_000_000_000) as u32;
                        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                            .unwrap_or_else(|| NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                        Ok(Value::Time(time))
                    }
                    Some(AvgType::TimeTz) => {
                        let avg_nanos = (sum / count as f64) as i64;
                        let secs = (avg_nanos / 1_000_000_000) as u32;
                        let nanos = (avg_nanos % 1_000_000_000) as u32;
                        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                            .unwrap_or_else(|| NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                        // Return with UTC offset (0)
                        Ok(Value::TimeTz(time, 0))
                    }
                    Some(AvgType::TimestampTz) => {
                        let avg_micros = (sum / count as f64) as i64;
                        let secs = avg_micros / 1_000_000;
                        let micros = (avg_micros % 1_000_000) as u32;
                        let ts = DateTime::from_timestamp(secs, micros * 1000)
                            .unwrap_or_else(|| Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap());
                        Ok(Value::TimestampTz(ts))
                    }
                    Some(AvgType::Interval) => {
                        // Calculate averages keeping fractional parts
                        let avg_months_f = interval_months as f64 / count as f64;
                        let avg_days_f = interval_days as f64 / count as f64;
                        let avg_micros_f = interval_micros as f64 / count as f64;

                        // Whole months, fractional months become days
                        let whole_months = avg_months_f.trunc() as i32;
                        let frac_months = avg_months_f.fract();
                        let extra_days_from_months = frac_months * 30.0; // Approximate month = 30 days

                        // Whole days, fractional days become microseconds
                        let total_days = avg_days_f + extra_days_from_months;
                        let whole_days = total_days.trunc() as i32;
                        let frac_days = total_days.fract();
                        let extra_micros_from_days = (frac_days * 24.0 * 60.0 * 60.0 * 1_000_000.0).round() as i64;

                        let total_micros = avg_micros_f.round() as i64 + extra_micros_from_days;

                        Ok(Value::Interval(Interval::new(whole_months, whole_days, total_micros)))
                    }
                }
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

            // Return NULL if no non-null values
            if values.is_empty() {
                return Ok(Value::Null);
            }

            // Handle DISTINCT while preserving order
            if distinct {
                let mut seen = std::collections::HashSet::new();
                values.retain(|v| seen.insert(v.clone()));
            }

            // Sort values alphabetically if DISTINCT and no ORDER BY was specified
            // (for consistent ordering when no explicit order is requested)
            if distinct && !has_order_by {
                values.sort();
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

        StdDev => {
            // Sample standard deviation: sqrt(sum((x - mean)^2) / (n - 1))
            let mut values: Vec<f64> = Vec::new();
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if let Some(f) = val.as_f64() {
                    values.push(f);
                }
            }
            if values.len() < 2 {
                return Ok(Value::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance: f64 = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            Ok(Value::Double(variance.sqrt()))
        }

        StdDevPop => {
            // Population standard deviation: sqrt(sum((x - mean)^2) / n)
            let mut values: Vec<f64> = Vec::new();
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if let Some(f) = val.as_f64() {
                    values.push(f);
                }
            }
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance: f64 = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;
            Ok(Value::Double(variance.sqrt()))
        }

        Variance => {
            // Sample variance: sum((x - mean)^2) / (n - 1)
            let mut values: Vec<f64> = Vec::new();
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if let Some(f) = val.as_f64() {
                    values.push(f);
                }
            }
            if values.len() < 2 {
                return Ok(Value::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance: f64 = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            Ok(Value::Double(variance))
        }

        VariancePop => {
            // Population variance: sum((x - mean)^2) / n
            let mut values: Vec<f64> = Vec::new();
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if let Some(f) = val.as_f64() {
                    values.push(f);
                }
            }
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance: f64 = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;
            Ok(Value::Double(variance))
        }

        BoolAnd => {
            // Logical AND of all boolean values (returns false if any is false)
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                match val {
                    Value::Boolean(false) => return Ok(Value::Boolean(false)),
                    Value::Null => continue, // Skip NULLs
                    _ => {}
                }
            }
            Ok(Value::Boolean(true))
        }

        BoolOr => {
            // Logical OR of all boolean values (returns true if any is true)
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                match val {
                    Value::Boolean(true) => return Ok(Value::Boolean(true)),
                    Value::Null => continue, // Skip NULLs
                    _ => {}
                }
            }
            Ok(Value::Boolean(false))
        }

        BitAnd => {
            // Bitwise AND of all integer values
            let mut result: Option<i64> = None;
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if let Some(i) = val.as_i64() {
                    result = Some(match result {
                        None => i,
                        Some(r) => r & i,
                    });
                }
            }
            Ok(result.map(Value::BigInt).unwrap_or(Value::Null))
        }

        BitOr => {
            // Bitwise OR of all integer values
            let mut result: i64 = 0;
            let mut has_value = false;
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if let Some(i) = val.as_i64() {
                    result |= i;
                    has_value = true;
                }
            }
            if has_value {
                Ok(Value::BigInt(result))
            } else {
                Ok(Value::Null)
            }
        }

        BitXor => {
            // Bitwise XOR of all integer values
            let mut result: i64 = 0;
            let mut has_value = false;
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if let Some(i) = val.as_i64() {
                    result ^= i;
                    has_value = true;
                }
            }
            if has_value {
                Ok(Value::BigInt(result))
            } else {
                Ok(Value::Null)
            }
        }

        Product => {
            // Product of all numeric values
            let mut result: f64 = 1.0;
            let mut has_value = false;
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if let Some(f) = val.as_f64() {
                    result *= f;
                    has_value = true;
                }
            }
            if has_value {
                Ok(Value::Double(result))
            } else {
                Ok(Value::Null)
            }
        }

        Median => {
            // Median = 50th percentile
            let mut values: Vec<f64> = Vec::new();
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if let Some(f) = val.as_f64() {
                    values.push(f);
                }
            }
            if values.is_empty() {
                return Ok(Value::Null);
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let n = values.len();
            let median = if n % 2 == 0 {
                // Average of two middle values
                (values[n / 2 - 1] + values[n / 2]) / 2.0
            } else {
                values[n / 2]
            };
            Ok(Value::Double(median))
        }

        PercentileCont => {
            // PERCENTILE_CONT(percentile) WITHIN GROUP (ORDER BY column) - continuous percentile
            // OR PERCENTILE_CONT(percentile, column) for backwards compatibility
            // First arg is percentile (0.0-1.0)
            if args.is_empty() {
                return Ok(Value::Null);
            }
            let percentile = evaluate(&args[0], &[])?.as_f64().unwrap_or(0.5);

            // Get the column expression: either from args[1] or from ORDER BY (WITHIN GROUP)
            let column_expr = if args.len() >= 2 {
                &args[1]
            } else if !agg.order_by.is_empty() {
                &agg.order_by[0].0
            } else {
                return Ok(Value::Null);
            };

            let mut values: Vec<f64> = Vec::new();
            for row in rows {
                let val = evaluate(column_expr, row)?;
                if let Some(f) = val.as_f64() {
                    values.push(f);
                }
            }
            if values.is_empty() {
                return Ok(Value::Null);
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            // Use linear interpolation
            let n = values.len();
            if n == 1 {
                return Ok(Value::Double(values[0]));
            }

            let idx = percentile * (n - 1) as f64;
            let lower_idx = idx.floor() as usize;
            let upper_idx = idx.ceil() as usize;
            let frac = idx - lower_idx as f64;

            let result = if lower_idx == upper_idx {
                values[lower_idx]
            } else {
                values[lower_idx] * (1.0 - frac) + values[upper_idx] * frac
            };
            Ok(Value::Double(result))
        }

        PercentileDisc => {
            // PERCENTILE_DISC(percentile) WITHIN GROUP (ORDER BY column) - discrete percentile
            // OR PERCENTILE_DISC(percentile, column) for backwards compatibility
            // First arg is percentile (0.0-1.0)
            if args.is_empty() {
                return Ok(Value::Null);
            }
            let percentile = evaluate(&args[0], &[])?.as_f64().unwrap_or(0.5);

            // Get the column expression: either from args[1] or from ORDER BY (WITHIN GROUP)
            let column_expr = if args.len() >= 2 {
                &args[1]
            } else if !agg.order_by.is_empty() {
                &agg.order_by[0].0
            } else {
                return Ok(Value::Null);
            };

            let mut values: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate(column_expr, row)?;
                if !val.is_null() {
                    values.push(val);
                }
            }
            if values.is_empty() {
                return Ok(Value::Null);
            }

            // Sort values
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            // Find the index for the given percentile
            let n = values.len();
            let idx = ((percentile * n as f64).ceil() as usize).saturating_sub(1).min(n - 1);
            Ok(values[idx].clone())
        }

        Mode => {
            // MODE - returns the most frequent value
            if args.is_empty() {
                return Ok(Value::Null);
            }

            // Collect all non-null values
            let mut values: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate(&args[0], row)?;
                if !val.is_null() {
                    values.push(val);
                }
            }
            if values.is_empty() {
                return Ok(Value::Null);
            }

            // Count occurrences using a HashMap
            let mut counts: std::collections::HashMap<String, (usize, Value)> = std::collections::HashMap::new();
            for val in values {
                let key = format!("{:?}", val);
                counts.entry(key)
                    .and_modify(|(count, _)| *count += 1)
                    .or_insert((1, val));
            }

            // Find the value with the highest count
            let mode = counts.into_iter()
                .max_by_key(|(_, (count, _))| *count)
                .map(|(_, (_, val))| val)
                .unwrap_or(Value::Null);

            Ok(mode)
        }

        CovarPop => {
            // Population covariance: sum((x - mean_x) * (y - mean_y)) / n
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut x_values: Vec<f64> = Vec::new();
            let mut y_values: Vec<f64> = Vec::new();

            for row in rows {
                let x = evaluate(&args[0], row)?;
                let y = evaluate(&args[1], row)?;
                if let (Some(xf), Some(yf)) = (x.as_f64(), y.as_f64()) {
                    x_values.push(xf);
                    y_values.push(yf);
                }
            }

            if x_values.is_empty() {
                return Ok(Value::Null);
            }

            let n = x_values.len() as f64;
            let mean_x: f64 = x_values.iter().sum::<f64>() / n;
            let mean_y: f64 = y_values.iter().sum::<f64>() / n;

            let covar: f64 = x_values.iter().zip(y_values.iter())
                .map(|(x, y)| (x - mean_x) * (y - mean_y))
                .sum::<f64>() / n;

            Ok(Value::Double(covar))
        }

        CovarSamp => {
            // Sample covariance: sum((x - mean_x) * (y - mean_y)) / (n - 1)
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut x_values: Vec<f64> = Vec::new();
            let mut y_values: Vec<f64> = Vec::new();

            for row in rows {
                let x = evaluate(&args[0], row)?;
                let y = evaluate(&args[1], row)?;
                if let (Some(xf), Some(yf)) = (x.as_f64(), y.as_f64()) {
                    x_values.push(xf);
                    y_values.push(yf);
                }
            }

            if x_values.len() < 2 {
                return Ok(Value::Null);
            }

            let n = x_values.len() as f64;
            let mean_x: f64 = x_values.iter().sum::<f64>() / n;
            let mean_y: f64 = y_values.iter().sum::<f64>() / n;

            let covar: f64 = x_values.iter().zip(y_values.iter())
                .map(|(x, y)| (x - mean_x) * (y - mean_y))
                .sum::<f64>() / (n - 1.0);

            Ok(Value::Double(covar))
        }

        Corr => {
            // Pearson correlation coefficient
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut x_values: Vec<f64> = Vec::new();
            let mut y_values: Vec<f64> = Vec::new();

            for row in rows {
                let x = evaluate(&args[0], row)?;
                let y = evaluate(&args[1], row)?;
                if let (Some(xf), Some(yf)) = (x.as_f64(), y.as_f64()) {
                    x_values.push(xf);
                    y_values.push(yf);
                }
            }

            if x_values.is_empty() {
                return Ok(Value::Null);
            }

            let n = x_values.len() as f64;
            let mean_x: f64 = x_values.iter().sum::<f64>() / n;
            let mean_y: f64 = y_values.iter().sum::<f64>() / n;

            let covar: f64 = x_values.iter().zip(y_values.iter())
                .map(|(x, y)| (x - mean_x) * (y - mean_y))
                .sum::<f64>();

            let std_x: f64 = x_values.iter()
                .map(|x| (x - mean_x).powi(2))
                .sum::<f64>()
                .sqrt();

            let std_y: f64 = y_values.iter()
                .map(|y| (y - mean_y).powi(2))
                .sum::<f64>()
                .sqrt();

            if std_x == 0.0 || std_y == 0.0 {
                return Ok(Value::Null);
            }

            let corr = covar / (std_x * std_y);
            Ok(Value::Double(corr))
        }

        // New aggregate functions
        ArgMax => {
            // ARG_MAX(arg, val) - return arg value when val is maximum
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut max_val: Option<f64> = None;
            let mut max_arg: Value = Value::Null;

            for row in rows {
                let arg = evaluate(&args[0], row)?;
                let val = evaluate(&args[1], row)?;
                if let Some(v) = val.as_f64() {
                    if max_val.is_none() || v > max_val.unwrap() {
                        max_val = Some(v);
                        max_arg = arg;
                    }
                }
            }

            Ok(max_arg)
        }

        ArgMin => {
            // ARG_MIN(arg, val) - return arg value when val is minimum
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut min_val: Option<f64> = None;
            let mut min_arg: Value = Value::Null;

            for row in rows {
                let arg = evaluate(&args[0], row)?;
                let val = evaluate(&args[1], row)?;
                if let Some(v) = val.as_f64() {
                    if min_val.is_none() || v < min_val.unwrap() {
                        min_val = Some(v);
                        min_arg = arg;
                    }
                }
            }

            Ok(min_arg)
        }

        Histogram => {
            // HISTOGRAM(col) - returns a list of structs with value counts
            let mut counts: std::collections::HashMap<String, i64> = std::collections::HashMap::new();

            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if !val.is_null() {
                    let key = format!("{}", val);
                    *counts.entry(key).or_insert(0) += 1;
                }
            }

            // Convert to list of structs (represented as nested lists)
            let mut result: Vec<Value> = Vec::new();
            for (key, count) in counts {
                // Represent as a list [key, count]
                result.push(Value::List(vec![Value::Varchar(key), Value::BigInt(count)]));
            }
            Ok(Value::List(result))
        }

        Entropy => {
            // Information entropy: -sum(p * log2(p))
            let mut counts: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
            let mut total: i64 = 0;

            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if !val.is_null() {
                    let key = format!("{:?}", val);
                    *counts.entry(key).or_insert(0) += 1;
                    total += 1;
                }
            }

            if total == 0 {
                return Ok(Value::Null);
            }

            let n = total as f64;
            let entropy: f64 = counts.values()
                .map(|&count| {
                    let p = count as f64 / n;
                    if p > 0.0 { -p * p.log2() } else { 0.0 }
                })
                .sum();

            Ok(Value::Double(entropy))
        }

        Kurtosis => {
            // Excess kurtosis: E[(X-μ)^4] / σ^4 - 3
            let mut values: Vec<f64> = Vec::new();
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if let Some(f) = val.as_f64() {
                    values.push(f);
                }
            }

            if values.len() < 4 {
                return Ok(Value::Null);
            }

            let n = values.len() as f64;
            let mean: f64 = values.iter().sum::<f64>() / n;
            let variance: f64 = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;

            if variance == 0.0 {
                return Ok(Value::Null);
            }

            let std_dev = variance.sqrt();
            let m4: f64 = values.iter().map(|x| ((x - mean) / std_dev).powi(4)).sum::<f64>() / n;
            let kurtosis = m4 - 3.0;

            Ok(Value::Double(kurtosis))
        }

        Skewness => {
            // Skewness: E[(X-μ)^3] / σ^3
            let mut values: Vec<f64> = Vec::new();
            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if let Some(f) = val.as_f64() {
                    values.push(f);
                }
            }

            if values.len() < 3 {
                return Ok(Value::Null);
            }

            let n = values.len() as f64;
            let mean: f64 = values.iter().sum::<f64>() / n;
            let variance: f64 = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;

            if variance == 0.0 {
                return Ok(Value::Null);
            }

            let std_dev = variance.sqrt();
            let m3: f64 = values.iter().map(|x| ((x - mean) / std_dev).powi(3)).sum::<f64>() / n;

            Ok(Value::Double(m3))
        }

        ApproxCountDistinct => {
            // Approximate count distinct using simple hash-based approach
            // (A proper implementation would use HyperLogLog)
            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

            for row in rows {
                if args.is_empty() {
                    continue;
                }
                let val = evaluate(&args[0], row)?;
                if !val.is_null() {
                    seen.insert(format!("{:?}", val));
                }
            }

            Ok(Value::BigInt(seen.len() as i64))
        }

        RegrSlope => {
            // Linear regression slope: Cov(X,Y) / Var(X)
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut x_values: Vec<f64> = Vec::new();
            let mut y_values: Vec<f64> = Vec::new();

            for row in rows {
                let y = evaluate(&args[0], row)?;
                let x = evaluate(&args[1], row)?;
                if let (Some(xf), Some(yf)) = (x.as_f64(), y.as_f64()) {
                    x_values.push(xf);
                    y_values.push(yf);
                }
            }

            if x_values.len() < 2 {
                return Ok(Value::Null);
            }

            let n = x_values.len() as f64;
            let mean_x: f64 = x_values.iter().sum::<f64>() / n;
            let mean_y: f64 = y_values.iter().sum::<f64>() / n;

            let sxx: f64 = x_values.iter().map(|x| (x - mean_x).powi(2)).sum();
            let sxy: f64 = x_values.iter().zip(y_values.iter())
                .map(|(x, y)| (x - mean_x) * (y - mean_y))
                .sum();

            if sxx == 0.0 {
                return Ok(Value::Null);
            }

            Ok(Value::Double(sxy / sxx))
        }

        RegrIntercept => {
            // Linear regression intercept: mean(Y) - slope * mean(X)
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut x_values: Vec<f64> = Vec::new();
            let mut y_values: Vec<f64> = Vec::new();

            for row in rows {
                let y = evaluate(&args[0], row)?;
                let x = evaluate(&args[1], row)?;
                if let (Some(xf), Some(yf)) = (x.as_f64(), y.as_f64()) {
                    x_values.push(xf);
                    y_values.push(yf);
                }
            }

            if x_values.len() < 2 {
                return Ok(Value::Null);
            }

            let n = x_values.len() as f64;
            let mean_x: f64 = x_values.iter().sum::<f64>() / n;
            let mean_y: f64 = y_values.iter().sum::<f64>() / n;

            let sxx: f64 = x_values.iter().map(|x| (x - mean_x).powi(2)).sum();
            let sxy: f64 = x_values.iter().zip(y_values.iter())
                .map(|(x, y)| (x - mean_x) * (y - mean_y))
                .sum();

            if sxx == 0.0 {
                return Ok(Value::Null);
            }

            let slope = sxy / sxx;
            Ok(Value::Double(mean_y - slope * mean_x))
        }

        RegrCount => {
            // Count of non-null pairs
            if args.len() < 2 {
                return Ok(Value::BigInt(0));
            }

            let mut count: i64 = 0;
            for row in rows {
                let y = evaluate(&args[0], row)?;
                let x = evaluate(&args[1], row)?;
                if !y.is_null() && !x.is_null() {
                    count += 1;
                }
            }

            Ok(Value::BigInt(count))
        }

        RegrR2 => {
            // Coefficient of determination (R²)
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut x_values: Vec<f64> = Vec::new();
            let mut y_values: Vec<f64> = Vec::new();

            for row in rows {
                let y = evaluate(&args[0], row)?;
                let x = evaluate(&args[1], row)?;
                if let (Some(xf), Some(yf)) = (x.as_f64(), y.as_f64()) {
                    x_values.push(xf);
                    y_values.push(yf);
                }
            }

            if x_values.len() < 2 {
                return Ok(Value::Null);
            }

            let n = x_values.len() as f64;
            let mean_x: f64 = x_values.iter().sum::<f64>() / n;
            let mean_y: f64 = y_values.iter().sum::<f64>() / n;

            let sxx: f64 = x_values.iter().map(|x| (x - mean_x).powi(2)).sum();
            let syy: f64 = y_values.iter().map(|y| (y - mean_y).powi(2)).sum();
            let sxy: f64 = x_values.iter().zip(y_values.iter())
                .map(|(x, y)| (x - mean_x) * (y - mean_y))
                .sum();

            if sxx == 0.0 || syy == 0.0 {
                return Ok(Value::Null);
            }

            let r2 = (sxy * sxy) / (sxx * syy);
            Ok(Value::Double(r2))
        }

        RegrAvgX => {
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut sum: f64 = 0.0;
            let mut count: i64 = 0;

            for row in rows {
                let y = evaluate(&args[0], row)?;
                let x = evaluate(&args[1], row)?;
                if let (Some(xf), Some(_yf)) = (x.as_f64(), y.as_f64()) {
                    sum += xf;
                    count += 1;
                }
            }

            if count == 0 {
                return Ok(Value::Null);
            }

            Ok(Value::Double(sum / count as f64))
        }

        RegrAvgY => {
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut sum: f64 = 0.0;
            let mut count: i64 = 0;

            for row in rows {
                let y = evaluate(&args[0], row)?;
                let x = evaluate(&args[1], row)?;
                if let (Some(_xf), Some(yf)) = (x.as_f64(), y.as_f64()) {
                    sum += yf;
                    count += 1;
                }
            }

            if count == 0 {
                return Ok(Value::Null);
            }

            Ok(Value::Double(sum / count as f64))
        }

        RegrSXX => {
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut x_values: Vec<f64> = Vec::new();

            for row in rows {
                let y = evaluate(&args[0], row)?;
                let x = evaluate(&args[1], row)?;
                if let (Some(xf), Some(_yf)) = (x.as_f64(), y.as_f64()) {
                    x_values.push(xf);
                }
            }

            if x_values.is_empty() {
                return Ok(Value::Null);
            }

            let n = x_values.len() as f64;
            let mean_x: f64 = x_values.iter().sum::<f64>() / n;
            let sxx: f64 = x_values.iter().map(|x| (x - mean_x).powi(2)).sum();

            Ok(Value::Double(sxx))
        }

        RegrSYY => {
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut y_values: Vec<f64> = Vec::new();

            for row in rows {
                let y = evaluate(&args[0], row)?;
                let x = evaluate(&args[1], row)?;
                if let (Some(_xf), Some(yf)) = (x.as_f64(), y.as_f64()) {
                    y_values.push(yf);
                }
            }

            if y_values.is_empty() {
                return Ok(Value::Null);
            }

            let n = y_values.len() as f64;
            let mean_y: f64 = y_values.iter().sum::<f64>() / n;
            let syy: f64 = y_values.iter().map(|y| (y - mean_y).powi(2)).sum();

            Ok(Value::Double(syy))
        }

        RegrSXY => {
            if args.len() < 2 {
                return Ok(Value::Null);
            }

            let mut x_values: Vec<f64> = Vec::new();
            let mut y_values: Vec<f64> = Vec::new();

            for row in rows {
                let y = evaluate(&args[0], row)?;
                let x = evaluate(&args[1], row)?;
                if let (Some(xf), Some(yf)) = (x.as_f64(), y.as_f64()) {
                    x_values.push(xf);
                    y_values.push(yf);
                }
            }

            if x_values.is_empty() {
                return Ok(Value::Null);
            }

            let n = x_values.len() as f64;
            let mean_x: f64 = x_values.iter().sum::<f64>() / n;
            let mean_y: f64 = y_values.iter().sum::<f64>() / n;
            let sxy: f64 = x_values.iter().zip(y_values.iter())
                .map(|(x, y)| (x - mean_x) * (y - mean_y))
                .sum();

            Ok(Value::Double(sxy))
        }

        // Additional aggregate functions
        AnyValue | Arbitrary => {
            // Return the first non-null value
            for row in rows {
                if !args.is_empty() {
                    let val = evaluate(&args[0], row)?;
                    if !val.is_null() {
                        return Ok(val);
                    }
                }
            }
            Ok(Value::Null)
        }

        ListAgg | GroupConcat => {
            // Like StringAgg - concatenate values with separator
            let separator = if args.len() > 1 {
                match evaluate(&args[1], &rows.first().cloned().unwrap_or_default())? {
                    Value::Varchar(s) => s,
                    _ => ",".to_string(),
                }
            } else {
                ",".to_string()
            };

            let mut parts: Vec<String> = Vec::new();
            for row in rows {
                if !args.is_empty() {
                    let val = evaluate(&args[0], row)?;
                    if !val.is_null() {
                        parts.push(format!("{}", val));
                    }
                }
            }

            if parts.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(Value::Varchar(parts.join(&separator)))
            }
        }

        FSum => {
            // Kahan summation for better precision with floats
            let mut sum = 0.0f64;
            let mut c = 0.0f64; // Compensation for lost low-order bits
            let mut has_value = false;

            for row in rows {
                if !args.is_empty() {
                    let val = evaluate(&args[0], row)?;
                    if let Some(f) = val.as_f64() {
                        has_value = true;
                        let y = f - c;
                        let t = sum + y;
                        c = (t - sum) - y;
                        sum = t;
                    }
                }
            }

            if has_value {
                Ok(Value::Double(sum))
            } else {
                Ok(Value::Null)
            }
        }

        Quantile | ApproxQuantile => {
            // Quantile is an alias for percentile
            // quantile(col, p) returns the p-th quantile (0-1)
            let p = if args.len() > 1 {
                match evaluate(&args[1], &rows.first().cloned().unwrap_or_default())? {
                    Value::Float(f) => f as f64,
                    Value::Double(f) => f,
                    _ => 0.5,
                }
            } else {
                0.5 // Default to median
            };

            let mut values: Vec<f64> = Vec::new();
            for row in rows {
                if !args.is_empty() {
                    let val = evaluate(&args[0], row)?;
                    if let Some(f) = val.as_f64() {
                        values.push(f);
                    }
                }
            }

            if values.is_empty() {
                return Ok(Value::Null);
            }

            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let idx = ((values.len() - 1) as f64 * p).round() as usize;
            Ok(Value::Double(values[idx.min(values.len() - 1)]))
        }

        CountIf => {
            // COUNT_IF(condition) - count rows where condition is true
            let mut count: i64 = 0;
            for row in rows {
                if !args.is_empty() {
                    let val = evaluate(&args[0], row)?;
                    if matches!(val, Value::Boolean(true)) {
                        count += 1;
                    }
                }
            }
            Ok(Value::BigInt(count))
        }

        SumIf => {
            // SUM_IF(value, condition) - sum values where condition is true
            let mut sum: i128 = 0;
            let mut float_sum: f64 = 0.0;
            let mut has_float = false;
            let mut has_value = false;

            for row in rows {
                if args.len() >= 2 {
                    let cond = evaluate(&args[1], row)?;
                    if matches!(cond, Value::Boolean(true)) {
                        let val = evaluate(&args[0], row)?;
                        match val {
                            Value::Integer(i) => { has_value = true; sum += i as i128; }
                            Value::BigInt(i) => { has_value = true; sum += i as i128; }
                            Value::Float(f) => { has_value = true; has_float = true; float_sum += f as f64; }
                            Value::Double(f) => { has_value = true; has_float = true; float_sum += f; }
                            _ => {}
                        }
                    }
                }
            }

            if !has_value {
                Ok(Value::Null)
            } else if has_float {
                Ok(Value::Double(float_sum + sum as f64))
            } else {
                Ok(Value::HugeInt(sum))
            }
        }

        AvgIf => {
            // AVG_IF(value, condition) - average values where condition is true
            let mut sum: f64 = 0.0;
            let mut count: i64 = 0;

            for row in rows {
                if args.len() >= 2 {
                    let cond = evaluate(&args[1], row)?;
                    if matches!(cond, Value::Boolean(true)) {
                        let val = evaluate(&args[0], row)?;
                        if let Some(f) = val.as_f64() {
                            sum += f;
                            count += 1;
                        }
                    }
                }
            }

            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Double(sum / count as f64))
            }
        }

        MinIf => {
            // MIN_IF(value, condition) - minimum value where condition is true
            let mut min_val: Option<Value> = None;

            for row in rows {
                if args.len() >= 2 {
                    let cond = evaluate(&args[1], row)?;
                    if matches!(cond, Value::Boolean(true)) {
                        let val = evaluate(&args[0], row)?;
                        if !val.is_null() {
                            min_val = Some(match min_val {
                                None => val,
                                Some(m) => if val < m { val } else { m },
                            });
                        }
                    }
                }
            }

            Ok(min_val.unwrap_or(Value::Null))
        }

        MaxIf => {
            // MAX_IF(value, condition) - maximum value where condition is true
            let mut max_val: Option<Value> = None;

            for row in rows {
                if args.len() >= 2 {
                    let cond = evaluate(&args[1], row)?;
                    if matches!(cond, Value::Boolean(true)) {
                        let val = evaluate(&args[0], row)?;
                        if !val.is_null() {
                            max_val = Some(match max_val {
                                None => val,
                                Some(m) => if val > m { val } else { m },
                            });
                        }
                    }
                }
            }

            Ok(max_val.unwrap_or(Value::Null))
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

/// Coerce a value to a target type
fn coerce_value(value: Value, target_type: &LogicalType) -> Result<Value> {
    use ironduck_common::value::Interval;

    // If already the right type or NULL, return as-is
    if value.is_null() {
        return Ok(value);
    }

    match (value.clone(), target_type) {
        // String to Interval conversion
        (Value::Varchar(s), LogicalType::Interval) => {
            parse_interval(&s).map(Value::Interval)
        }
        // Keep value if types match
        (Value::Interval(_), LogicalType::Interval) => Ok(value),
        (Value::Timestamp(_), LogicalType::Timestamp) => Ok(value),
        (Value::Date(_), LogicalType::Date) => Ok(value),
        (Value::Time(_), LogicalType::Time) => Ok(value),
        (Value::TimeTz(_, _), LogicalType::TimeTz) => Ok(value),
        (Value::TimestampTz(_), LogicalType::TimestampTz) => Ok(value),
        (Value::Integer(_), LogicalType::Integer) => Ok(value),
        (Value::BigInt(_), LogicalType::BigInt) => Ok(value),
        (Value::Double(_), LogicalType::Double) => Ok(value),
        (Value::Varchar(_), LogicalType::Varchar) => Ok(value),
        (Value::Boolean(_), LogicalType::Boolean) => Ok(value),
        // String to TimeTz conversion
        (Value::Varchar(s), LogicalType::TimeTz) => {
            parse_timetz(&s).map(|(time, offset)| Value::TimeTz(time, offset))
        }
        // For other types, just return the value (may need more conversions later)
        _ => Ok(value),
    }
}

/// Parse an interval string like "1 day", "30 days", "1 year 2 months"
fn parse_interval(s: &str) -> Result<ironduck_common::value::Interval> {
    use ironduck_common::value::Interval;

    let s = s.trim();

    // Handle PostgreSQL-style intervals like "@ 1 minute"
    let s = s.strip_prefix("@ ").unwrap_or(s);
    let s = s.strip_prefix('@').unwrap_or(s).trim();

    // Handle "ago" suffix for negative intervals
    let (s, negate) = if s.ends_with(" ago") {
        (s.strip_suffix(" ago").unwrap(), true)
    } else {
        (s, false)
    };

    let mut months = 0i32;
    let mut days = 0i32;
    let mut micros = 0i64;

    // Split on whitespace and process pairs
    let parts: Vec<&str> = s.split_whitespace().collect();
    let mut i = 0;

    while i < parts.len() {
        // Try to parse as number + unit
        if i + 1 < parts.len() {
            if let Ok(num) = parts[i].parse::<i64>() {
                let unit = parts[i + 1].to_lowercase();
                match unit.as_str() {
                    "year" | "years" => months += (num * 12) as i32,
                    "month" | "months" => months += num as i32,
                    "week" | "weeks" => days += (num * 7) as i32,
                    "day" | "days" => days += num as i32,
                    "hour" | "hours" => micros += num * 3600 * 1_000_000,
                    "minute" | "minutes" => micros += num * 60 * 1_000_000,
                    "second" | "seconds" => micros += num * 1_000_000,
                    "millisecond" | "milliseconds" => micros += num * 1_000,
                    "microsecond" | "microseconds" => micros += num,
                    _ => {}
                }
                i += 2;
                continue;
            }
        }

        // Try parsing composite format like "1 day 2 hours 3 minutes 4 seconds"
        // or simple format like "1 day"
        if let Ok(num) = parts[i].parse::<i64>() {
            // Just a number, skip
            i += 1;
        } else {
            // Might be a time format like "04:18:23"
            if parts[i].contains(':') {
                let time_parts: Vec<&str> = parts[i].split(':').collect();
                if time_parts.len() >= 2 {
                    if let (Ok(h), Ok(m)) = (time_parts[0].parse::<i64>(), time_parts[1].parse::<i64>()) {
                        micros += h * 3600 * 1_000_000;
                        micros += m * 60 * 1_000_000;
                        if time_parts.len() >= 3 {
                            if let Ok(s) = time_parts[2].parse::<f64>() {
                                micros += (s * 1_000_000.0) as i64;
                            }
                        }
                    }
                }
            }
            i += 1;
        }
    }

    if negate {
        months = -months;
        days = -days;
        micros = -micros;
    }

    Ok(Interval::new(months, days, micros))
}

/// Parse a TIMETZ string like "00:00:00+1559" or "12:30:45-0500"
fn parse_timetz(s: &str) -> Result<(chrono::NaiveTime, i32)> {
    use chrono::NaiveTime;

    let s = s.trim();

    // Find the timezone offset (+ or -)
    let (time_part, offset_secs) = if let Some(pos) = s.rfind('+') {
        let time_str = &s[..pos];
        let offset_str = &s[pos + 1..];
        let offset = parse_tz_offset_str(offset_str);
        (time_str, offset)
    } else if let Some(pos) = s.rfind('-') {
        // Make sure we don't split on something that's not a timezone
        if pos > 5 {
            let time_str = &s[..pos];
            let offset_str = &s[pos + 1..];
            let offset = -parse_tz_offset_str(offset_str);
            (time_str, offset)
        } else {
            (s, 0)
        }
    } else {
        (s, 0)
    };

    // Handle 24:00:00 which is valid in SQL but not in chrono
    // It represents midnight at the end of the day (same as 00:00:00 the next day)
    let time_part_owned: String;
    let time_part_ref = if time_part.starts_with("24:") {
        time_part_owned = time_part.replacen("24:", "00:", 1);
        time_part_owned.as_str()
    } else {
        time_part
    };

    let time = NaiveTime::parse_from_str(time_part_ref, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(time_part_ref, "%H:%M:%S%.f"))
        .or_else(|_| NaiveTime::parse_from_str(time_part_ref, "%H:%M"))
        .map_err(|_| Error::Parse(format!("Invalid TIMETZ format: {}", s)))?;

    Ok((time, offset_secs))
}

/// Parse a timezone offset string like "0530" or "05:30" or "04:30:45" to seconds
fn parse_tz_offset_str(s: &str) -> i32 {
    // Split by colons first to handle HH:MM:SS format
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() >= 3 {
        // HH:MM:SS format
        let hours: i32 = parts[0].parse().unwrap_or(0);
        let mins: i32 = parts[1].parse().unwrap_or(0);
        let secs: i32 = parts[2].parse().unwrap_or(0);
        return hours * 3600 + mins * 60 + secs;
    } else if parts.len() == 2 {
        // HH:MM format
        let hours: i32 = parts[0].parse().unwrap_or(0);
        let mins: i32 = parts[1].parse().unwrap_or(0);
        return hours * 3600 + mins * 60;
    }

    // No colons - try HHMM or HHMMSS format
    let s = s.replace(':', "");
    if s.len() >= 6 {
        let hours: i32 = s[..2].parse().unwrap_or(0);
        let mins: i32 = s[2..4].parse().unwrap_or(0);
        let secs: i32 = s[4..6].parse().unwrap_or(0);
        hours * 3600 + mins * 60 + secs
    } else if s.len() >= 4 {
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
