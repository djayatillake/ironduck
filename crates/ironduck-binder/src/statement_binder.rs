//! Statement binding

use super::expression_binder::{bind_data_type, bind_expression, ExpressionBinderContext};
use super::{
    BoundCTE, BoundColumnDef, BoundCreateSchema, BoundCreateSequence, BoundCreateTable,
    BoundCreateView, BoundDelete, BoundDrop, BoundExpression, BoundInsert, BoundJoinType,
    BoundOrderBy, BoundRecursiveCTE, BoundSelect, BoundSetOperation, BoundStatement,
    BoundTableRef, BoundUpdate, Binder, DistinctKind, DropObjectType, SetOperationType,
    TableFunctionType,
};
use ironduck_common::{Error, Result};
use sqlparser::ast as sql;

/// Bind a statement
pub fn bind_statement(binder: &Binder, stmt: &sql::Statement) -> Result<BoundStatement> {
    match stmt {
        sql::Statement::Query(query) => bind_query(binder, query),

        sql::Statement::CreateTable(create) => {
            let bound = bind_create_table(binder, create)?;
            Ok(BoundStatement::CreateTable(bound))
        }

        sql::Statement::CreateSchema { schema_name, if_not_exists, .. } => {
            Ok(BoundStatement::CreateSchema(BoundCreateSchema {
                name: schema_name.to_string(),
                if_not_exists: *if_not_exists,
            }))
        }

        sql::Statement::Insert(insert) => {
            let bound = bind_insert(binder, insert)?;
            Ok(BoundStatement::Insert(bound))
        }

        sql::Statement::Drop { object_type, names, if_exists, .. } => {
            let obj_type = match object_type {
                sql::ObjectType::Table => DropObjectType::Table,
                sql::ObjectType::Schema => DropObjectType::Schema,
                sql::ObjectType::View => DropObjectType::View,
                _ => return Err(Error::NotImplemented(format!("DROP {:?}", object_type))),
            };

            let name = names.first().ok_or_else(|| Error::Parse("Missing object name".to_string()))?;
            let parts: Vec<_> = name.0.iter().map(|i| i.value.clone()).collect();

            let (schema, table_name) = if parts.len() == 2 {
                (Some(parts[0].clone()), parts[1].clone())
            } else {
                (None, parts[0].clone())
            };

            Ok(BoundStatement::Drop(BoundDrop {
                object_type: obj_type,
                schema,
                name: table_name,
                if_exists: *if_exists,
            }))
        }

        sql::Statement::Delete(delete) => {
            let bound = bind_delete(binder, delete)?;
            Ok(BoundStatement::Delete(bound))
        }

        sql::Statement::Update { table, assignments, selection, .. } => {
            let bound = bind_update(binder, table, assignments, selection)?;
            Ok(BoundStatement::Update(bound))
        }

        sql::Statement::Explain { statement, .. } => {
            let inner = bind_statement(binder, statement)?;
            Ok(BoundStatement::Explain(Box::new(inner)))
        }

        // Configuration statements - treat as no-ops for compatibility
        sql::Statement::SetVariable { .. } => Ok(BoundStatement::NoOp),
        sql::Statement::SetNames { .. } => Ok(BoundStatement::NoOp),
        sql::Statement::SetNamesDefault { .. } => Ok(BoundStatement::NoOp),
        sql::Statement::SetTimeZone { .. } => Ok(BoundStatement::NoOp),

        // PRAGMA statements - treat as no-ops for compatibility
        sql::Statement::Pragma { .. } => Ok(BoundStatement::NoOp),

        sql::Statement::CreateView { name, query, or_replace, .. } => {
            bind_create_view(binder, name, query, *or_replace)
        }

        sql::Statement::CreateSequence {
            name,
            if_not_exists,
            sequence_options,
            ..
        } => {
            bind_create_sequence(binder, name, *if_not_exists, sequence_options)
        }

        _ => Err(Error::NotImplemented(format!("Statement: {:?}", stmt))),
    }
}

/// Bind a query - returns either a Select or SetOperation
fn bind_query(binder: &Binder, query: &sql::Query) -> Result<BoundStatement> {
    // First, bind any CTEs in the WITH clause
    let ctes = bind_ctes(binder, &query.with)?;

    // Handle the body - check for set operations first
    match query.body.as_ref() {
        sql::SetExpr::SetOperation { op, set_quantifier, left, right } => {
            // Recursively bind left and right sides
            let left_select = bind_set_expr_with_ctes(binder, left, &ctes)?;
            let right_select = bind_set_expr_with_ctes(binder, right, &ctes)?;

            let set_op = match op {
                sql::SetOperator::Union => SetOperationType::Union,
                sql::SetOperator::Intersect => SetOperationType::Intersect,
                sql::SetOperator::Except => SetOperationType::Except,
            };

            let all = matches!(set_quantifier, sql::SetQuantifier::All);

            // Build order by, limit, offset
            let mut order_by = Vec::new();
            if let Some(ob) = &query.order_by {
                let ctx = ExpressionBinderContext::new(&left_select.from);
                for order in &ob.exprs {
                    let expr = bind_expression(binder, &order.expr, &ctx)?;
                    order_by.push(BoundOrderBy {
                        expr,
                        ascending: order.asc.unwrap_or(true),
                        nulls_first: order.nulls_first.unwrap_or(false),
                    });
                }
            }

            let limit = if let Some(l) = &query.limit {
                if let sql::Expr::Value(sql::Value::Number(n, _)) = l {
                    Some(n.parse().map_err(|_| Error::Parse("Invalid LIMIT".to_string()))?)
                } else {
                    None
                }
            } else {
                None
            };

            let offset = if let Some(o) = &query.offset {
                if let sql::Expr::Value(sql::Value::Number(n, _)) = &o.value {
                    Some(n.parse().map_err(|_| Error::Parse("Invalid OFFSET".to_string()))?)
                } else {
                    None
                }
            } else {
                None
            };

            Ok(BoundStatement::SetOperation(BoundSetOperation {
                left: Box::new(left_select),
                right: Box::new(right_select),
                set_op,
                all,
                order_by,
                limit,
                offset,
            }))
        }
        _ => {
            // Regular SELECT
            let select = bind_query_select_with_ctes(binder, query, &ctes)?;
            Ok(BoundStatement::Select(select))
        }
    }
}

/// Bind CTEs from WITH clause
fn bind_ctes(binder: &Binder, with: &Option<sql::With>) -> Result<Vec<BoundCTE>> {
    let Some(with_clause) = with else {
        return Ok(Vec::new());
    };

    let mut ctes = Vec::new();

    for cte in &with_clause.cte_tables {
        // Get the CTE name
        let name = cte.alias.name.value.clone();

        // Get column aliases if any
        let column_aliases: Vec<String> = cte.alias.columns.iter().map(|c| c.name.value.clone()).collect();

        if with_clause.recursive {
            // For recursive CTEs, the body should be a UNION/UNION ALL
            // with a base case and a recursive case
            let bound_cte = bind_recursive_cte(binder, &name, &column_aliases, &cte.query, &ctes)?;
            ctes.push(bound_cte);
        } else {
            // Bind the CTE query - CTEs can reference earlier CTEs in the same WITH clause
            let query = bind_query_select_with_ctes(binder, &cte.query, &ctes)?;

            ctes.push(BoundCTE {
                name,
                column_aliases,
                query,
                is_recursive: false,
            });
        }
    }

    Ok(ctes)
}

/// Bind a recursive CTE
fn bind_recursive_cte(
    binder: &Binder,
    name: &str,
    column_aliases: &[String],
    query: &sql::Query,
    prior_ctes: &[BoundCTE],
) -> Result<BoundCTE> {
    // For recursive CTEs, the body should be a UNION/UNION ALL
    let (left, right, all) = match query.body.as_ref() {
        sql::SetExpr::SetOperation {
            op: sql::SetOperator::Union,
            set_quantifier,
            left,
            right,
        } => {
            let is_all = matches!(set_quantifier, sql::SetQuantifier::All);
            (left, right, is_all)
        }
        _ => {
            // Not a UNION, might be a simple recursive (self-referential) CTE
            // Try to bind it with the CTE already visible (for simple recursion)
            // For now, just treat as non-recursive
            let query = bind_query_select_with_ctes(binder, query, prior_ctes)?;
            return Ok(BoundCTE {
                name: name.to_string(),
                column_aliases: column_aliases.to_vec(),
                query,
                is_recursive: false,
            });
        }
    };

    // Determine which side is the base case (doesn't reference the CTE)
    // and which is the recursive case (references the CTE)
    let left_references_cte = set_expr_references_cte(left, name);
    let right_references_cte = set_expr_references_cte(right, name);

    let (base_expr, recursive_expr, union_all) = match (left_references_cte, right_references_cte) {
        (false, true) => (left, right, all),
        (true, false) => (right, left, all),
        (false, false) => {
            // Neither side references the CTE - not actually recursive
            // Just bind as a regular UNION
            let left_select = bind_set_expr_with_ctes(binder, left, prior_ctes)?;
            let right_select = bind_set_expr_with_ctes(binder, right, prior_ctes)?;

            // Combine as a set operation
            let combined = BoundSelect {
                select_list: left_select.select_list.clone(),
                from: vec![BoundTableRef::Subquery {
                    subquery: Box::new(left_select),
                    alias: "union_left".to_string(),
                }],
                where_clause: None,
                group_by: Vec::new(),
                having: None,
                order_by: Vec::new(),
                limit: None,
                offset: None,
                distinct: if all { DistinctKind::None } else { DistinctKind::All },
            };

            return Ok(BoundCTE {
                name: name.to_string(),
                column_aliases: column_aliases.to_vec(),
                query: combined,
                is_recursive: false,
            });
        }
        (true, true) => {
            return Err(Error::InvalidArguments(
                "Both sides of recursive CTE UNION reference the CTE".to_string(),
            ));
        }
    };

    // First, bind the base case without the CTE in scope
    let base_case = bind_set_expr_with_ctes(binder, base_expr, prior_ctes)?;

    // Determine the output column types from the base case
    let column_types: Vec<_> = base_case.select_list.iter()
        .map(|e| e.return_type.clone())
        .collect();

    // Get column names (either from aliases or from base case)
    let column_names: Vec<String> = if column_aliases.is_empty() {
        base_case.select_list.iter().map(|e| e.name()).collect()
    } else {
        column_aliases.to_vec()
    };

    // Create a temporary BoundCTE to allow the recursive case to reference it
    let temp_cte = BoundCTE {
        name: name.to_string(),
        column_aliases: column_names.clone(),
        query: base_case.clone(),
        is_recursive: true,
    };

    // Create extended CTE list including this recursive CTE
    let mut extended_ctes = prior_ctes.to_vec();
    extended_ctes.push(temp_cte);

    // Now bind the recursive case with the CTE in scope
    let recursive_case = bind_set_expr_with_ctes(binder, recursive_expr, &extended_ctes)?;

    // Create a combined result that represents the recursive structure
    // We'll use a special marker in the select list to indicate recursion
    let combined = BoundSelect {
        select_list: base_case.select_list.clone(),
        from: vec![BoundTableRef::Subquery {
            subquery: Box::new(base_case.clone()),
            alias: format!("{}_base", name),
        }],
        where_clause: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        limit: None,
        offset: None,
        distinct: DistinctKind::None, // We handle UNION vs UNION ALL in execution
    };

    Ok(BoundCTE {
        name: name.to_string(),
        column_aliases: column_names,
        query: combined,
        is_recursive: true,
    })
}

/// Check if a set expression references a CTE by name
fn set_expr_references_cte(set_expr: &sql::SetExpr, cte_name: &str) -> bool {
    match set_expr {
        sql::SetExpr::Select(select) => {
            // Check FROM clause for references to the CTE
            for table_with_joins in &select.from {
                if table_factor_references_cte(&table_with_joins.relation, cte_name) {
                    return true;
                }
                for join in &table_with_joins.joins {
                    if table_factor_references_cte(&join.relation, cte_name) {
                        return true;
                    }
                }
            }
            false
        }
        sql::SetExpr::Query(query) => set_expr_references_cte(&query.body, cte_name),
        sql::SetExpr::SetOperation { left, right, .. } => {
            set_expr_references_cte(left, cte_name) || set_expr_references_cte(right, cte_name)
        }
        _ => false,
    }
}

/// Check if a table factor references a CTE by name
fn table_factor_references_cte(factor: &sql::TableFactor, cte_name: &str) -> bool {
    match factor {
        sql::TableFactor::Table { name, .. } => {
            // Check if this table name matches the CTE name
            if name.0.len() == 1 {
                let table_name = &name.0[0].value;
                table_name.eq_ignore_ascii_case(cte_name)
            } else {
                false
            }
        }
        sql::TableFactor::Derived { subquery, .. } => {
            set_expr_references_cte(&subquery.body, cte_name)
        }
        sql::TableFactor::NestedJoin { table_with_joins, .. } => {
            if table_factor_references_cte(&table_with_joins.relation, cte_name) {
                return true;
            }
            for join in &table_with_joins.joins {
                if table_factor_references_cte(&join.relation, cte_name) {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

/// Bind a set expression to a BoundSelect
fn bind_set_expr(binder: &Binder, set_expr: &sql::SetExpr) -> Result<BoundSelect> {
    bind_set_expr_with_ctes(binder, set_expr, &[])
}

/// Bind a set expression to a BoundSelect (with CTE support)
fn bind_set_expr_with_ctes(
    binder: &Binder,
    set_expr: &sql::SetExpr,
    ctes: &[BoundCTE],
) -> Result<BoundSelect> {
    match set_expr {
        sql::SetExpr::Select(select) => bind_select_with_ctes(binder, select, ctes),
        sql::SetExpr::Values(values) => bind_values(binder, values),
        sql::SetExpr::Query(query) => bind_query_select_with_ctes(binder, query, ctes),
        sql::SetExpr::SetOperation { .. } => {
            // Nested set operations - for now, wrap in a query
            Err(Error::NotImplemented("Nested set operations".to_string()))
        }
        _ => Err(Error::NotImplemented(format!("Set expression: {:?}", set_expr))),
    }
}

/// Bind a subquery (exposed for expression binder to use)
pub fn bind_subquery(binder: &Binder, query: &sql::Query) -> Result<BoundSelect> {
    bind_query_select(binder, query)
}

/// Bind a query to a BoundSelect (for non-set-operation queries)
fn bind_query_select(binder: &Binder, query: &sql::Query) -> Result<BoundSelect> {
    bind_query_select_with_ctes(binder, query, &[])
}

/// Bind a query to a BoundSelect (with CTE support)
fn bind_query_select_with_ctes(
    binder: &Binder,
    query: &sql::Query,
    ctes: &[BoundCTE],
) -> Result<BoundSelect> {
    // Handle the body
    let select = match query.body.as_ref() {
        sql::SetExpr::Select(select) => bind_select_with_ctes(binder, select, ctes)?,
        sql::SetExpr::Values(values) => bind_values(binder, values)?,
        _ => return Err(Error::NotImplemented("Complex query body".to_string())),
    };

    // Apply ORDER BY, LIMIT, OFFSET from the query
    let mut result = select;

    if let Some(order_by) = &query.order_by {
        let ctx = ExpressionBinderContext::new(&result.from);
        for order in &order_by.exprs {
            let expr = bind_expression(binder, &order.expr, &ctx)?;
            result.order_by.push(BoundOrderBy {
                expr,
                ascending: order.asc.unwrap_or(true),
                nulls_first: order.nulls_first.unwrap_or(false),
            });
        }
    }

    if let Some(limit) = &query.limit {
        if let sql::Expr::Value(sql::Value::Number(n, _)) = limit {
            result.limit = Some(n.parse().map_err(|_| Error::Parse("Invalid LIMIT".to_string()))?);
        }
    }

    if let Some(offset) = &query.offset {
        if let sql::Expr::Value(sql::Value::Number(n, _)) = &offset.value {
            result.offset = Some(n.parse().map_err(|_| Error::Parse("Invalid OFFSET".to_string()))?);
        }
    }

    Ok(result)
}

/// Bind a SELECT clause
fn bind_select(binder: &Binder, select: &sql::Select) -> Result<BoundSelect> {
    bind_select_with_ctes(binder, select, &[])
}

/// Bind a SELECT clause (with CTE support)
fn bind_select_with_ctes(
    binder: &Binder,
    select: &sql::Select,
    ctes: &[BoundCTE],
) -> Result<BoundSelect> {
    // First, bind FROM clause to know available columns
    let from = bind_from_with_ctes(binder, &select.from, ctes)?;

    // Bind SELECT list
    let mut select_list = Vec::new();
    {
        let ctx = ExpressionBinderContext::new(&from);
        for item in &select.projection {
            match item {
                sql::SelectItem::UnnamedExpr(expr) => {
                    let bound = bind_expression(binder, expr, &ctx)?;
                    select_list.push(bound);
                }
                sql::SelectItem::ExprWithAlias { expr, alias } => {
                    let mut bound = bind_expression(binder, expr, &ctx)?;
                    bound.alias = Some(alias.value.clone());
                    select_list.push(bound);
                }
                sql::SelectItem::Wildcard(_) => {
                    // Expand * to all columns from all tables
                    // Track global column offset across all tables in FROM
                    let mut global_col_offset = 0;
                    for (table_idx, table_ref) in from.iter().enumerate() {
                        expand_wildcard_with_offset(table_ref, table_idx, &mut global_col_offset, &mut select_list);
                    }
                }
                sql::SelectItem::QualifiedWildcard(name, _) => {
                    // Expand table.* to all columns from the specified table
                    let table_name = name.0.last()
                        .map(|i| i.value.clone())
                        .unwrap_or_default();

                    let mut found = false;
                    let mut global_col_offset = 0;
                    for (table_idx, table_ref) in from.iter().enumerate() {
                        if expand_qualified_wildcard(table_ref, &table_name, table_idx, &mut global_col_offset, &mut select_list) {
                            found = true;
                            // Don't break - we need to continue counting columns for proper offset
                        }
                    }

                    if !found {
                        return Err(Error::TableNotFound(table_name));
                    }
                }
            }
        }
    }

    let mut result = BoundSelect::new(select_list);

    // Bind DISTINCT or DISTINCT ON
    result.distinct = match &select.distinct {
        None => DistinctKind::None,
        Some(sql::Distinct::Distinct) => DistinctKind::All,
        Some(sql::Distinct::On(exprs)) => {
            let ctx = ExpressionBinderContext::new(&from);
            let mut bound_exprs = Vec::new();
            for expr in exprs {
                bound_exprs.push(bind_expression(binder, expr, &ctx)?);
            }
            DistinctKind::On(bound_exprs)
        }
    };

    // Bind WHERE, GROUP BY, HAVING using a fresh context
    {
        let ctx = ExpressionBinderContext::new(&from);

        // Bind WHERE clause
        if let Some(selection) = &select.selection {
            result.where_clause = Some(bind_expression(binder, selection, &ctx)?);
        }

        // Bind GROUP BY
        match &select.group_by {
            sql::GroupByExpr::All(_) => {
                return Err(Error::NotImplemented("GROUP BY ALL".to_string()));
            }
            sql::GroupByExpr::Expressions(exprs, _) => {
                for expr in exprs {
                    result.group_by.push(bind_expression(binder, expr, &ctx)?);
                }
            }
        }

        // Bind HAVING
        if let Some(having) = &select.having {
            result.having = Some(bind_expression(binder, having, &ctx)?);
        }
    }

    // Now we can move from into result
    result.from = from;

    Ok(result)
}

/// Bind VALUES clause
fn bind_values(_binder: &Binder, values: &sql::Values) -> Result<BoundSelect> {
    let ctx = ExpressionBinderContext::empty();
    let mut all_rows = Vec::new();

    for row in &values.rows {
        let mut bound_row = Vec::new();
        for expr in row {
            bound_row.push(bind_expression(_binder, expr, &ctx)?);
        }
        all_rows.push(bound_row);
    }

    // For VALUES, create a select with constant expressions
    // The first row determines the column types
    let select_list = all_rows.into_iter().next().unwrap_or_default();
    Ok(BoundSelect::new(select_list))
}

/// Collect all base tables from a table reference (for building expression context)
fn collect_tables_from_ref(table_ref: &BoundTableRef) -> Vec<BoundTableRef> {
    match table_ref {
        BoundTableRef::BaseTable { .. } => vec![table_ref.clone()],
        BoundTableRef::Join { left, right, .. } => {
            let mut tables = collect_tables_from_ref(left);
            tables.extend(collect_tables_from_ref(right));
            tables
        }
        BoundTableRef::Subquery { .. } => vec![table_ref.clone()],
        BoundTableRef::TableFunction { .. } => vec![table_ref.clone()],
        BoundTableRef::Empty => vec![],
    }
}

/// Bind FROM clause
fn bind_from(binder: &Binder, from: &[sql::TableWithJoins]) -> Result<Vec<BoundTableRef>> {
    bind_from_with_ctes(binder, from, &[])
}

/// Bind FROM clause (with CTE support)
fn bind_from_with_ctes(
    binder: &Binder,
    from: &[sql::TableWithJoins],
    ctes: &[BoundCTE],
) -> Result<Vec<BoundTableRef>> {
    if from.is_empty() {
        // SELECT without FROM (e.g., SELECT 1)
        return Ok(vec![BoundTableRef::Empty]);
    }

    let mut result = Vec::new();
    for table_with_joins in from {
        let base = bind_table_factor_with_ctes(binder, &table_with_joins.relation, ctes)?;

        // Handle joins
        let mut current = base;
        for join in &table_with_joins.joins {
            let right = bind_table_factor_with_ctes(binder, &join.relation, ctes)?;
            let join_type = match &join.join_operator {
                sql::JoinOperator::Inner(_) => BoundJoinType::Inner,
                sql::JoinOperator::LeftOuter(_) => BoundJoinType::Left,
                sql::JoinOperator::RightOuter(_) => BoundJoinType::Right,
                sql::JoinOperator::FullOuter(_) => BoundJoinType::Full,
                sql::JoinOperator::CrossJoin => BoundJoinType::Cross,
                _ => return Err(Error::NotImplemented("Join type".to_string())),
            };

            let (condition, using_columns) = match &join.join_operator {
                sql::JoinOperator::Inner(constraint)
                | sql::JoinOperator::LeftOuter(constraint)
                | sql::JoinOperator::RightOuter(constraint)
                | sql::JoinOperator::FullOuter(constraint) => {
                    match constraint {
                        sql::JoinConstraint::On(expr) => {
                            // Build context with both left and right tables
                            let join_tables = collect_tables_from_ref(&current);
                            let right_tables = collect_tables_from_ref(&right);
                            let mut all_tables = join_tables;
                            all_tables.extend(right_tables);
                            let ctx = ExpressionBinderContext::new(&all_tables);
                            (Some(bind_expression(binder, expr, &ctx)?), Vec::new())
                        }
                        sql::JoinConstraint::Using(columns) => {
                            // USING (col1, col2, ...) is equivalent to:
                            // ON left.col1 = right.col1 AND left.col2 = right.col2 ...
                            let join_tables = collect_tables_from_ref(&current);
                            let right_tables = collect_tables_from_ref(&right);
                            let mut all_tables = join_tables;
                            all_tables.extend(right_tables);
                            let ctx = ExpressionBinderContext::new(&all_tables);

                            // Collect USING column names (for wildcard expansion)
                            let using_cols: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();

                            // Build equality conditions for each column
                            let mut conditions: Vec<BoundExpression> = Vec::new();
                            for col in columns {
                                // Create left.col = right.col expression
                                let left_ref = sql::Expr::Identifier(col.clone());
                                let right_ref = sql::Expr::Identifier(col.clone());
                                let eq_expr = sql::Expr::BinaryOp {
                                    left: Box::new(left_ref),
                                    op: sql::BinaryOperator::Eq,
                                    right: Box::new(right_ref),
                                };
                                conditions.push(bind_expression(binder, &eq_expr, &ctx)?);
                            }

                            // Combine with AND
                            let cond = if conditions.is_empty() {
                                None
                            } else {
                                let mut result = conditions.remove(0);
                                for cond in conditions {
                                    result = BoundExpression {
                                        expr: super::BoundExpressionKind::BinaryOp {
                                            left: Box::new(result),
                                            op: super::BoundBinaryOperator::And,
                                            right: Box::new(cond),
                                        },
                                        return_type: ironduck_common::LogicalType::Boolean,
                                        alias: None,
                                    };
                                }
                                Some(result)
                            };
                            (cond, using_cols)
                        }
                        sql::JoinConstraint::None => (None, Vec::new()),
                        sql::JoinConstraint::Natural => {
                            return Err(Error::NotImplemented("NATURAL join".to_string()))
                        }
                    }
                }
                _ => (None, Vec::new()),
            };

            current = BoundTableRef::Join {
                left: Box::new(current),
                right: Box::new(right),
                join_type,
                condition,
                using_columns,
            };
        }

        result.push(current);
    }

    Ok(result)
}

/// Bind a table factor (table reference in FROM)
fn bind_table_factor(binder: &Binder, factor: &sql::TableFactor) -> Result<BoundTableRef> {
    bind_table_factor_with_ctes(binder, factor, &[])
}

/// Bind a table factor (with CTE support)
fn bind_table_factor_with_ctes(
    binder: &Binder,
    factor: &sql::TableFactor,
    ctes: &[BoundCTE],
) -> Result<BoundTableRef> {
    match factor {
        sql::TableFactor::Table { name, alias, args, .. } => {
            let parts: Vec<_> = name.0.iter().map(|i| i.value.clone()).collect();

            // Check if this is a table-valued function (has args and name is a known function)
            if let Some(table_args) = args {
                if parts.len() == 1 {
                    let func_name = parts[0].to_uppercase();
                    if matches!(func_name.as_str(), "RANGE" | "GENERATE_SERIES" | "UNNEST" | "GENERATE_SUBSCRIPTS") {
                        // TableFunctionArgs.args is already Vec<FunctionArg>
                        return bind_table_function(binder, name, &table_args.args, alias);
                    }
                }
            }

            // For single-part names, check CTEs first
            if parts.len() == 1 {
                let table_name = &parts[0];
                // Check if this is a CTE reference
                if let Some(cte) = ctes.iter().find(|c| c.name.eq_ignore_ascii_case(table_name)) {
                    let alias_name = alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| cte.name.clone());

                    // Return the CTE as a subquery reference
                    return Ok(BoundTableRef::Subquery {
                        subquery: Box::new(cte.query.clone()),
                        alias: alias_name,
                    });
                }
            }

            let (schema_name, table_name) = if parts.len() == 2 {
                (parts[0].clone(), parts[1].clone())
            } else {
                (binder.current_schema().to_string(), parts[0].clone())
            };

            // First, try to look up as a table
            if let Some(table) = binder.catalog().get_table(&schema_name, &table_name) {
                let column_names: Vec<_> = table.columns.iter().map(|c| c.name.clone()).collect();
                let column_types: Vec<_> = table.columns.iter().map(|c| c.logical_type.clone()).collect();

                return Ok(BoundTableRef::BaseTable {
                    schema: schema_name,
                    name: table_name,
                    alias: alias.as_ref().map(|a| a.name.value.clone()),
                    column_names,
                    column_types,
                });
            }

            // If not a table, try to look up as a view
            if let Some(view) = binder.catalog().get_view(&schema_name, &table_name) {
                // Parse and bind the view's SQL query
                let dialect = sqlparser::dialect::DuckDbDialect {};
                let statements = sqlparser::parser::Parser::parse_sql(&dialect, &view.sql)
                    .map_err(|e| Error::Parse(e.to_string()))?;

                if statements.is_empty() {
                    return Err(Error::Internal("View has no query".to_string()));
                }

                // The view SQL should be a SELECT query
                if let sql::Statement::Query(query) = &statements[0] {
                    let bound_stmt = bind_query(binder, query)?;
                    let bound_select = match bound_stmt {
                        BoundStatement::Select(sel) => sel,
                        _ => return Err(Error::Internal("View query must be SELECT".to_string())),
                    };

                    let alias_name = alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| table_name.clone());

                    return Ok(BoundTableRef::Subquery {
                        subquery: Box::new(bound_select),
                        alias: alias_name,
                    });
                } else {
                    return Err(Error::Internal("View query is not a SELECT".to_string()));
                }
            }

            // Neither table nor view found
            Err(Error::TableNotFound(table_name.clone()))
        }

        sql::TableFactor::Derived { subquery, alias, .. } => {
            let bound_stmt = bind_query(binder, subquery)?;
            let bound_select = match bound_stmt {
                BoundStatement::Select(sel) => sel,
                BoundStatement::SetOperation(_) => {
                    return Err(Error::NotImplemented("Set operation in subquery".to_string()))
                }
                _ => return Err(Error::NotImplemented("Unsupported subquery type".to_string())),
            };
            let alias_name = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| "subquery".to_string());

            Ok(BoundTableRef::Subquery {
                subquery: Box::new(bound_select),
                alias: alias_name,
            })
        }

        sql::TableFactor::Function { name, args, alias, .. } => {
            bind_table_function(binder, name, args, alias)
        }

        _ => Err(Error::NotImplemented(format!("Table factor: {:?}", factor))),
    }
}

/// Bind a table-valued function (e.g., range(), generate_series())
fn bind_table_function(
    binder: &Binder,
    name: &sql::ObjectName,
    args: &[sql::FunctionArg],
    alias: &Option<sql::TableAlias>,
) -> Result<BoundTableRef> {
    use super::bound_expression::BoundExpressionKind;
    use ironduck_common::{LogicalType, Value};

    let func_name = name.to_string().to_uppercase();

    match func_name.as_str() {
        "RANGE" | "GENERATE_SERIES" => {
            // range() can have 1, 2, or 3 arguments:
            // range(stop) - generates 0 to stop-1
            // range(start, stop) - generates start to stop-1
            // range(start, stop, step) - generates start to stop-1 with step

            let ctx = ExpressionBinderContext::new(&[]);

            let bound_args: Vec<BoundExpression> = args
                .iter()
                .filter_map(|arg| {
                    let expr_arg = match arg {
                        sql::FunctionArg::Unnamed(expr) => Some(expr),
                        sql::FunctionArg::Named { arg, .. } => Some(arg),
                        _ => None,
                    };
                    expr_arg.and_then(|expr| match expr {
                        sql::FunctionArgExpr::Expr(e) => bind_expression(binder, e, &ctx).ok(),
                        _ => None,
                    })
                })
                .collect();

            let (start, stop, step) = match bound_args.len() {
                1 => {
                    // range(stop) - start=0, step=1
                    let zero = BoundExpression {
                        expr: BoundExpressionKind::Constant(Value::BigInt(0)),
                        return_type: LogicalType::BigInt,
                        alias: None,
                    };
                    let one = BoundExpression {
                        expr: BoundExpressionKind::Constant(Value::BigInt(1)),
                        return_type: LogicalType::BigInt,
                        alias: None,
                    };
                    (zero, bound_args.into_iter().next().unwrap(), one)
                }
                2 => {
                    // range(start, stop) - step=1
                    let one = BoundExpression {
                        expr: BoundExpressionKind::Constant(Value::BigInt(1)),
                        return_type: LogicalType::BigInt,
                        alias: None,
                    };
                    let mut iter = bound_args.into_iter();
                    (iter.next().unwrap(), iter.next().unwrap(), one)
                }
                3 => {
                    // range(start, stop, step)
                    let mut iter = bound_args.into_iter();
                    (iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())
                }
                _ => {
                    return Err(Error::InvalidArguments(format!(
                        "range() requires 1-3 arguments, got {}",
                        bound_args.len()
                    )));
                }
            };

            // Extract column alias from table alias if provided
            let (table_alias, column_alias) = if let Some(a) = alias {
                let col_alias = a.columns.first().map(|c| c.name.value.clone());
                (Some(a.name.value.clone()), col_alias)
            } else {
                (None, None)
            };

            Ok(BoundTableRef::TableFunction {
                function: TableFunctionType::Range { start, stop, step },
                alias: table_alias,
                column_alias,
            })
        }
        "UNNEST" => {
            // unnest(array) - expands an array into rows
            let ctx = ExpressionBinderContext::new(&[]);

            let bound_args: Vec<BoundExpression> = args
                .iter()
                .filter_map(|arg| {
                    let expr_arg = match arg {
                        sql::FunctionArg::Unnamed(expr) => Some(expr),
                        sql::FunctionArg::Named { arg, .. } => Some(arg),
                        _ => None,
                    };
                    expr_arg.and_then(|expr| match expr {
                        sql::FunctionArgExpr::Expr(e) => bind_expression(binder, e, &ctx).ok(),
                        _ => None,
                    })
                })
                .collect();

            if bound_args.is_empty() {
                return Err(Error::InvalidArguments("unnest() requires an array argument".to_string()));
            }

            let array_expr = bound_args.into_iter().next().unwrap();

            // Extract column alias from table alias if provided
            let (table_alias, column_alias) = if let Some(a) = alias {
                let col_alias = a.columns.first().map(|c| c.name.value.clone());
                (Some(a.name.value.clone()), col_alias)
            } else {
                (None, None)
            };

            Ok(BoundTableRef::TableFunction {
                function: TableFunctionType::Unnest { array_expr },
                alias: table_alias,
                column_alias,
            })
        }
        "GENERATE_SUBSCRIPTS" => {
            // generate_subscripts(array, dim) - generates subscripts for an array dimension
            let ctx = ExpressionBinderContext::new(&[]);

            let bound_args: Vec<BoundExpression> = args
                .iter()
                .filter_map(|arg| {
                    let expr_arg = match arg {
                        sql::FunctionArg::Unnamed(expr) => Some(expr),
                        sql::FunctionArg::Named { arg, .. } => Some(arg),
                        _ => None,
                    };
                    expr_arg.and_then(|expr| match expr {
                        sql::FunctionArgExpr::Expr(e) => bind_expression(binder, e, &ctx).ok(),
                        _ => None,
                    })
                })
                .collect();

            if bound_args.is_empty() {
                return Err(Error::InvalidArguments("generate_subscripts() requires an array argument".to_string()));
            }

            let array_expr = bound_args.into_iter().next().unwrap();
            let dim = 1; // Default dimension

            // Extract column alias from table alias if provided
            let (table_alias, column_alias) = if let Some(a) = alias {
                let col_alias = a.columns.first().map(|c| c.name.value.clone());
                (Some(a.name.value.clone()), col_alias)
            } else {
                (None, None)
            };

            Ok(BoundTableRef::TableFunction {
                function: TableFunctionType::GenerateSubscripts { array_expr, dim },
                alias: table_alias,
                column_alias,
            })
        }
        _ => Err(Error::NotImplemented(format!("Table function: {}", func_name))),
    }
}

/// Bind CREATE TABLE
fn bind_create_table(binder: &Binder, create: &sql::CreateTable) -> Result<BoundCreateTable> {
    let parts: Vec<_> = create.name.0.iter().map(|i| i.value.clone()).collect();
    let (schema, name) = if parts.len() == 2 {
        (parts[0].clone(), parts[1].clone())
    } else {
        (binder.current_schema().to_string(), parts[0].clone())
    };

    // Check for CREATE TABLE ... AS SELECT ...
    if let Some(query) = &create.query {
        let bound_stmt = bind_query(binder, query)?;
        let bound_select = match bound_stmt {
            BoundStatement::Select(sel) => sel,
            _ => return Err(Error::NotImplemented("CREATE TABLE AS requires SELECT".to_string())),
        };

        // Derive columns from the query's output
        let columns: Vec<_> = bound_select.select_list.iter().map(|expr| {
            BoundColumnDef {
                name: expr.name(),
                data_type: expr.return_type.clone(),
                nullable: true,
                default: None,
            }
        }).collect();

        return Ok(BoundCreateTable {
            schema,
            name,
            columns,
            if_not_exists: create.if_not_exists,
            source_query: Some(Box::new(bound_select)),
        });
    }

    // Regular CREATE TABLE with column definitions
    let mut columns = Vec::new();
    for col in &create.columns {
        let data_type = bind_data_type(&col.data_type)?;
        let nullable = !col.options.iter().any(|opt| {
            matches!(opt.option, sql::ColumnOption::NotNull)
        });

        columns.push(BoundColumnDef {
            name: col.name.value.clone(),
            data_type,
            nullable,
            default: None, // TODO: Handle defaults
        });
    }

    Ok(BoundCreateTable {
        schema,
        name,
        columns,
        if_not_exists: create.if_not_exists,
        source_query: None,
    })
}

/// Bind CREATE VIEW
fn bind_create_view(
    binder: &Binder,
    name: &sql::ObjectName,
    query: &sql::Query,
    or_replace: bool,
) -> Result<BoundStatement> {
    let parts: Vec<_> = name.0.iter().map(|i| i.value.clone()).collect();
    let (schema, view_name) = if parts.len() == 2 {
        (parts[0].clone(), parts[1].clone())
    } else {
        (binder.current_schema().to_string(), parts[0].clone())
    };

    // Bind the query to get column names
    let bound_stmt = bind_query(binder, query)?;
    let column_names = match &bound_stmt {
        BoundStatement::Select(sel) => sel.select_list.iter().map(|e| e.name()).collect(),
        BoundStatement::SetOperation(op) => op.left.select_list.iter().map(|e| e.name()).collect(),
        _ => return Err(Error::NotImplemented("View query must be SELECT".to_string())),
    };

    // Store the original SQL text
    let sql = format!("{}", query);

    Ok(BoundStatement::CreateView(BoundCreateView {
        schema,
        name: view_name,
        sql,
        column_names,
        or_replace,
    }))
}

/// Bind INSERT
fn bind_insert(binder: &Binder, insert: &sql::Insert) -> Result<BoundInsert> {
    let parts: Vec<_> = insert.table_name.0.iter().map(|i| i.value.clone()).collect();
    let (schema, table) = if parts.len() == 2 {
        (parts[0].clone(), parts[1].clone())
    } else {
        (binder.current_schema().to_string(), parts[0].clone())
    };

    let columns: Vec<_> = insert.columns.iter().map(|c| c.value.clone()).collect();

    // Bind source - either VALUES or SELECT
    let mut values = Vec::new();
    let mut source_query = None;

    if let Some(source) = &insert.source {
        match source.body.as_ref() {
            sql::SetExpr::Values(vals) => {
                // INSERT ... VALUES (...)
                let ctx = ExpressionBinderContext::empty();
                for row in &vals.rows {
                    let mut bound_row = Vec::new();
                    for expr in row {
                        bound_row.push(bind_expression(binder, expr, &ctx)?);
                    }
                    values.push(bound_row);
                }
            }
            sql::SetExpr::Select(select) => {
                // INSERT ... SELECT ...
                let bound_select = bind_select(binder, select)?;
                source_query = Some(Box::new(bound_select));
            }
            sql::SetExpr::Query(query) => {
                // INSERT ... (SELECT ...)
                if let sql::SetExpr::Select(select) = query.body.as_ref() {
                    let bound_select = bind_select(binder, select)?;
                    source_query = Some(Box::new(bound_select));
                } else {
                    return Err(Error::NotImplemented(
                        "INSERT with complex query source".to_string(),
                    ));
                }
            }
            _ => {
                return Err(Error::NotImplemented(format!(
                    "INSERT source type: {:?}",
                    source.body
                )));
            }
        }
    }

    Ok(BoundInsert {
        schema,
        table,
        columns,
        values,
        source_query,
    })
}

/// Bind DELETE
fn bind_delete(binder: &Binder, delete: &sql::Delete) -> Result<BoundDelete> {
    // Get table name from FROM clause
    // FromTable is either WithFromKeyword(Vec<TableWithJoins>) or WithoutKeyword(Vec<TableWithJoins>)
    let tables = match &delete.from {
        sql::FromTable::WithFromKeyword(tables) => tables,
        sql::FromTable::WithoutKeyword(tables) => tables,
    };

    let from = tables.first().ok_or_else(|| Error::Parse("DELETE requires FROM clause".to_string()))?;

    let (schema, table) = match &from.relation {
        sql::TableFactor::Table { name, .. } => {
            let parts: Vec<_> = name.0.iter().map(|i| i.value.clone()).collect();
            if parts.len() == 2 {
                (parts[0].clone(), parts[1].clone())
            } else {
                (binder.current_schema().to_string(), parts[0].clone())
            }
        }
        _ => return Err(Error::NotImplemented("Complex DELETE source".to_string())),
    };

    // Get table info for binding WHERE clause
    let table_info = binder
        .catalog()
        .get_table(&schema, &table)
        .ok_or_else(|| Error::TableNotFound(table.clone()))?;

    // Build table reference for expression binding context
    let table_ref = BoundTableRef::BaseTable {
        schema: schema.clone(),
        name: table.clone(),
        alias: None,
        column_names: table_info.columns.iter().map(|c| c.name.clone()).collect(),
        column_types: table_info.columns.iter().map(|c| c.logical_type.clone()).collect(),
    };
    let from_refs = [table_ref];

    // Bind WHERE clause
    let where_clause = if let Some(selection) = &delete.selection {
        let ctx = ExpressionBinderContext::new(&from_refs);
        Some(bind_expression(binder, selection, &ctx)?)
    } else {
        None
    };

    Ok(BoundDelete {
        schema,
        table,
        where_clause,
    })
}

/// Bind UPDATE
fn bind_update(
    binder: &Binder,
    table: &sql::TableWithJoins,
    assignments: &[sql::Assignment],
    selection: &Option<sql::Expr>,
) -> Result<BoundUpdate> {
    // Get table name
    let (schema, table_name) = match &table.relation {
        sql::TableFactor::Table { name, .. } => {
            let parts: Vec<_> = name.0.iter().map(|i| i.value.clone()).collect();
            if parts.len() == 2 {
                (parts[0].clone(), parts[1].clone())
            } else {
                (binder.current_schema().to_string(), parts[0].clone())
            }
        }
        _ => return Err(Error::NotImplemented("Complex UPDATE source".to_string())),
    };

    // Get table info for binding
    let table_info = binder
        .catalog()
        .get_table(&schema, &table_name)
        .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

    // Build table reference for expression binding context
    let table_ref = BoundTableRef::BaseTable {
        schema: schema.clone(),
        name: table_name.clone(),
        alias: None,
        column_names: table_info.columns.iter().map(|c| c.name.clone()).collect(),
        column_types: table_info.columns.iter().map(|c| c.logical_type.clone()).collect(),
    };
    let from_refs = [table_ref];
    let ctx = ExpressionBinderContext::new(&from_refs);

    // Bind assignments
    let mut bound_assignments = Vec::new();
    for assignment in assignments {
        // AssignmentTarget is either ColumnName(ObjectName) or Tuple(Vec<ObjectName>)
        let col_name = match &assignment.target {
            sql::AssignmentTarget::ColumnName(name) => {
                name.0.iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join(".")
            }
            sql::AssignmentTarget::Tuple(names) => {
                // For tuple assignments like (a, b) = (1, 2), just use first column for now
                names.first()
                    .map(|name| name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."))
                    .unwrap_or_default()
            }
        };
        let value = bind_expression(binder, &assignment.value, &ctx)?;
        bound_assignments.push((col_name, value));
    }

    // Bind WHERE clause
    let where_clause = if let Some(sel) = selection {
        Some(bind_expression(binder, sel, &ctx)?)
    } else {
        None
    };

    Ok(BoundUpdate {
        schema,
        table: table_name,
        assignments: bound_assignments,
        where_clause,
    })
}

/// Expand qualified wildcard (table.*) - only expand columns from the matching table
fn expand_qualified_wildcard(
    table_ref: &BoundTableRef,
    target_name: &str,
    table_idx: usize,
    col_offset: &mut usize,
    select_list: &mut Vec<BoundExpression>,
) -> bool {
    match table_ref {
        BoundTableRef::BaseTable {
            name,
            alias,
            column_names,
            column_types,
            ..
        } => {
            let matches = alias.as_ref().map(|a| a.eq_ignore_ascii_case(target_name)).unwrap_or(false)
                || name.eq_ignore_ascii_case(target_name);

            if matches {
                for (idx, (col_name, typ)) in column_names.iter().zip(column_types.iter()).enumerate() {
                    select_list.push(BoundExpression::new(
                        super::BoundExpressionKind::ColumnRef {
                            table_idx,
                            column_idx: *col_offset + idx,
                            name: col_name.clone(),
                        },
                        typ.clone(),
                    ));
                }
            }
            *col_offset += column_names.len();
            matches
        }
        BoundTableRef::Subquery { subquery, alias } => {
            let matches = alias.eq_ignore_ascii_case(target_name);

            if matches {
                for (idx, expr) in subquery.select_list.iter().enumerate() {
                    select_list.push(BoundExpression::new(
                        super::BoundExpressionKind::ColumnRef {
                            table_idx,
                            column_idx: *col_offset + idx,
                            name: expr.name(),
                        },
                        expr.return_type.clone(),
                    ));
                }
            }
            *col_offset += subquery.select_list.len();
            matches
        }
        BoundTableRef::Join { left, right, .. } => {
            let left_matched = expand_qualified_wildcard(left, target_name, table_idx, col_offset, select_list);
            let right_matched = expand_qualified_wildcard(right, target_name, table_idx, col_offset, select_list);
            left_matched || right_matched
        }
        BoundTableRef::TableFunction { alias, column_alias, .. } => {
            let matches = alias.as_ref().map(|a| a.eq_ignore_ascii_case(target_name)).unwrap_or(false);

            if matches {
                let col_name = column_alias.clone().unwrap_or_else(|| "range".to_string());
                select_list.push(BoundExpression::new(
                    super::BoundExpressionKind::ColumnRef {
                        table_idx,
                        column_idx: *col_offset,
                        name: col_name,
                    },
                    ironduck_common::LogicalType::BigInt,
                ));
            }
            *col_offset += 1;
            matches
        }
        BoundTableRef::Empty => false,
    }
}

/// Expand qualified wildcard with skip columns (for JOIN USING deduplication)
fn expand_qualified_wildcard_with_skip(
    table_ref: &BoundTableRef,
    target_name: &str,
    table_idx: usize,
    col_offset: &mut usize,
    select_list: &mut Vec<BoundExpression>,
    skip_columns: &[String],
) -> bool {
    match table_ref {
        BoundTableRef::BaseTable {
            name,
            alias,
            column_names,
            column_types,
            ..
        } => {
            let matches = alias.as_ref().map(|a| a.eq_ignore_ascii_case(target_name)).unwrap_or(false)
                || name.eq_ignore_ascii_case(target_name);

            let mut local_col_idx = 0;
            for (col_name, typ) in column_names.iter().zip(column_types.iter()) {
                // Check if this column should be skipped (USING column)
                let should_skip = skip_columns.iter().any(|sc| sc.eq_ignore_ascii_case(col_name));

                if matches && !should_skip {
                    select_list.push(BoundExpression::new(
                        super::BoundExpressionKind::ColumnRef {
                            table_idx,
                            column_idx: *col_offset + local_col_idx,
                            name: col_name.clone(),
                        },
                        typ.clone(),
                    ));
                }

                // Only increment local_col_idx for non-skipped columns
                if !should_skip {
                    local_col_idx += 1;
                }
            }
            // Increment col_offset by the number of non-skipped columns
            *col_offset += local_col_idx;
            matches
        }
        BoundTableRef::Subquery { subquery, alias } => {
            let matches = alias.eq_ignore_ascii_case(target_name);

            let mut local_col_idx = 0;
            for expr in subquery.select_list.iter() {
                let col_name = expr.name();
                let should_skip = skip_columns.iter().any(|sc| sc.eq_ignore_ascii_case(&col_name));

                if matches && !should_skip {
                    select_list.push(BoundExpression::new(
                        super::BoundExpressionKind::ColumnRef {
                            table_idx,
                            column_idx: *col_offset + local_col_idx,
                            name: col_name,
                        },
                        expr.return_type.clone(),
                    ));
                }

                if !should_skip {
                    local_col_idx += 1;
                }
            }
            *col_offset += local_col_idx;
            matches
        }
        BoundTableRef::Join { left, right, using_columns, .. } => {
            // For nested joins, pass through skip_columns but also add using_columns for right side
            let left_matched = expand_qualified_wildcard_with_skip(left, target_name, table_idx, col_offset, select_list, skip_columns);
            // For right side, combine parent skip_columns with this join's using_columns
            let mut combined_skip = skip_columns.to_vec();
            combined_skip.extend(using_columns.iter().cloned());
            let right_matched = expand_qualified_wildcard_with_skip(right, target_name, table_idx, col_offset, select_list, &combined_skip);
            left_matched || right_matched
        }
        BoundTableRef::TableFunction { alias, column_alias, .. } => {
            let matches = alias.as_ref().map(|a| a.eq_ignore_ascii_case(target_name)).unwrap_or(false);

            let col_name = column_alias.clone().unwrap_or_else(|| "range".to_string());
            let should_skip = skip_columns.iter().any(|sc| sc.eq_ignore_ascii_case(&col_name));

            if matches && !should_skip {
                select_list.push(BoundExpression::new(
                    super::BoundExpressionKind::ColumnRef {
                        table_idx,
                        column_idx: *col_offset,
                        name: col_name,
                    },
                    ironduck_common::LogicalType::BigInt,
                ));
            }
            if !should_skip {
                *col_offset += 1;
            }
            matches
        }
        BoundTableRef::Empty => false,
    }
}

/// Check if a table reference matches a given name (for qualified wildcards)
fn matches_table_name(table_ref: &BoundTableRef, name: &str) -> bool {
    match table_ref {
        BoundTableRef::BaseTable { name: table_name, alias, .. } => {
            alias.as_ref().map(|a| a.eq_ignore_ascii_case(name)).unwrap_or(false)
                || table_name.eq_ignore_ascii_case(name)
        }
        BoundTableRef::Subquery { alias, .. } => {
            alias.eq_ignore_ascii_case(name)
        }
        BoundTableRef::Join { left, right, .. } => {
            matches_table_name(left, name) || matches_table_name(right, name)
        }
        BoundTableRef::TableFunction { alias, .. } => {
            alias.as_ref().map(|a| a.eq_ignore_ascii_case(name)).unwrap_or(false)
        }
        BoundTableRef::Empty => false,
    }
}

/// Recursively expand wildcard (*) for a table reference, handling joins and subqueries
fn expand_wildcard_for_table(
    table_ref: &BoundTableRef,
    table_idx: usize,
    select_list: &mut Vec<BoundExpression>,
) {
    let mut col_offset = 0;
    expand_wildcard_rec(table_ref, table_idx, &mut col_offset, select_list);
}

/// Expand wildcard with a global column offset (for multiple tables in FROM)
fn expand_wildcard_with_offset(
    table_ref: &BoundTableRef,
    table_idx: usize,
    global_col_offset: &mut usize,
    select_list: &mut Vec<BoundExpression>,
) {
    expand_wildcard_rec(table_ref, table_idx, global_col_offset, select_list);
}

fn expand_wildcard_rec(
    table_ref: &BoundTableRef,
    table_idx: usize,
    col_offset: &mut usize,
    select_list: &mut Vec<BoundExpression>,
) {
    expand_wildcard_rec_with_skip(table_ref, table_idx, col_offset, select_list, &[]);
}

fn expand_wildcard_rec_with_skip(
    table_ref: &BoundTableRef,
    table_idx: usize,
    col_offset: &mut usize,
    select_list: &mut Vec<BoundExpression>,
    skip_columns: &[String],
) {
    match table_ref {
        BoundTableRef::BaseTable {
            column_names,
            column_types,
            ..
        } => {
            let mut local_col_idx = 0;
            for (name, typ) in column_names.iter().zip(column_types.iter()) {
                let should_skip = skip_columns.iter().any(|sc| sc.eq_ignore_ascii_case(name));
                if !should_skip {
                    select_list.push(BoundExpression::new(
                        super::BoundExpressionKind::ColumnRef {
                            table_idx,
                            column_idx: *col_offset + local_col_idx,
                            name: name.clone(),
                        },
                        typ.clone(),
                    ));
                    local_col_idx += 1;
                }
            }
            *col_offset += local_col_idx;
        }
        BoundTableRef::Subquery { subquery, .. } => {
            // For subqueries (including views), get columns from the select list
            let mut local_col_idx = 0;
            for expr in subquery.select_list.iter() {
                let col_name = expr.name();
                let should_skip = skip_columns.iter().any(|sc| sc.eq_ignore_ascii_case(&col_name));
                if !should_skip {
                    select_list.push(BoundExpression::new(
                        super::BoundExpressionKind::ColumnRef {
                            table_idx,
                            column_idx: *col_offset + local_col_idx,
                            name: col_name,
                        },
                        expr.return_type.clone(),
                    ));
                    local_col_idx += 1;
                }
            }
            *col_offset += local_col_idx;
        }
        BoundTableRef::Join { left, right, .. } => {
            // For left side, use parent skip_columns
            expand_wildcard_rec_with_skip(left, table_idx, col_offset, select_list, skip_columns);
            // For right side, just use parent skip_columns (no special USING handling here)
            expand_wildcard_rec_with_skip(right, table_idx, col_offset, select_list, skip_columns);
        }
        BoundTableRef::TableFunction { column_alias, .. } => {
            let col_name = column_alias.clone().unwrap_or_else(|| "range".to_string());
            let should_skip = skip_columns.iter().any(|sc| sc.eq_ignore_ascii_case(&col_name));
            if !should_skip {
                select_list.push(BoundExpression::new(
                    super::BoundExpressionKind::ColumnRef {
                        table_idx,
                        column_idx: *col_offset,
                        name: col_name,
                    },
                    ironduck_common::LogicalType::BigInt,
                ));
                *col_offset += 1;
            }
        }
        BoundTableRef::Empty => {}
    }
}

/// Bind CREATE SEQUENCE
fn bind_create_sequence(
    binder: &Binder,
    name: &sql::ObjectName,
    if_not_exists: bool,
    options: &[sql::SequenceOptions],
) -> Result<BoundStatement> {
    let parts: Vec<_> = name.0.iter().map(|i| i.value.clone()).collect();
    let (schema, seq_name) = if parts.len() == 2 {
        (parts[0].clone(), parts[1].clone())
    } else {
        (binder.current_schema().to_string(), parts[0].clone())
    };

    // Parse sequence options
    let mut start: i64 = 1;
    let mut increment: i64 = 1;
    let mut min_value: i64 = 1;
    let mut max_value: i64 = i64::MAX;
    let mut cycle = false;

    for opt in options {
        match opt {
            sql::SequenceOptions::StartWith(val, _) => {
                if let sql::Expr::Value(sql::Value::Number(n, _)) = val {
                    start = n.parse().unwrap_or(1);
                }
            }
            sql::SequenceOptions::IncrementBy(val, _) => {
                if let sql::Expr::Value(sql::Value::Number(n, _)) = val {
                    increment = n.parse().unwrap_or(1);
                }
            }
            sql::SequenceOptions::MinValue(Some(val)) => {
                if let sql::Expr::Value(sql::Value::Number(n, _)) = val {
                    min_value = n.parse().unwrap_or(1);
                }
            }
            sql::SequenceOptions::MaxValue(Some(val)) => {
                if let sql::Expr::Value(sql::Value::Number(n, _)) = val {
                    max_value = n.parse().unwrap_or(i64::MAX);
                }
            }
            sql::SequenceOptions::Cycle(is_cycle) => {
                cycle = *is_cycle;
            }
            _ => {}
        }
    }

    Ok(BoundStatement::CreateSequence(BoundCreateSequence {
        schema,
        name: seq_name,
        start,
        increment,
        min_value,
        max_value,
        cycle,
        if_not_exists,
    }))
}
