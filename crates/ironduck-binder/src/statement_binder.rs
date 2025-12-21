//! Statement binding

use super::expression_binder::{bind_data_type, bind_expression, ExpressionBinderContext};
use super::{
    BoundCTE, BoundColumnDef, BoundCreateSchema, BoundCreateTable, BoundDelete, BoundDrop,
    BoundExpression, BoundInsert, BoundJoinType, BoundOrderBy, BoundSelect, BoundSetOperation,
    BoundStatement, BoundTableRef, BoundUpdate, Binder, DistinctKind, DropObjectType, SetOperationType,
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

    if with_clause.recursive {
        return Err(Error::NotImplemented("Recursive CTEs".to_string()));
    }

    let mut ctes = Vec::new();

    for cte in &with_clause.cte_tables {
        // Get the CTE name
        let name = cte.alias.name.value.clone();

        // Get column aliases if any
        let column_aliases: Vec<String> = cte.alias.columns.iter().map(|c| c.name.value.clone()).collect();

        // Bind the CTE query - CTEs can reference earlier CTEs in the same WITH clause
        let query = bind_query_select_with_ctes(binder, &cte.query, &ctes)?;

        ctes.push(BoundCTE {
            name,
            column_aliases,
            query,
        });
    }

    Ok(ctes)
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
                    for table_ref in &from {
                        if let BoundTableRef::BaseTable {
                            column_names,
                            column_types,
                            ..
                        } = table_ref
                        {
                            for (idx, (name, typ)) in column_names.iter().zip(column_types.iter()).enumerate() {
                                select_list.push(BoundExpression::new(
                                    super::BoundExpressionKind::ColumnRef {
                                        table_idx: 0,
                                        column_idx: idx,
                                        name: name.clone(),
                                    },
                                    typ.clone(),
                                ));
                            }
                        }
                    }
                }
                sql::SelectItem::QualifiedWildcard(_, _) => {
                    return Err(Error::NotImplemented("Qualified wildcard".to_string()));
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

            let condition = match &join.join_operator {
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
                            Some(bind_expression(binder, expr, &ctx)?)
                        }
                        sql::JoinConstraint::None => None,
                        _ => return Err(Error::NotImplemented("Join constraint".to_string())),
                    }
                }
                _ => None,
            };

            current = BoundTableRef::Join {
                left: Box::new(current),
                right: Box::new(right),
                join_type,
                condition,
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
        sql::TableFactor::Table { name, alias, .. } => {
            let parts: Vec<_> = name.0.iter().map(|i| i.value.clone()).collect();

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

            // Look up table in catalog
            let table = binder
                .catalog()
                .get_table(&schema_name, &table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

            let column_names: Vec<_> = table.columns.iter().map(|c| c.name.clone()).collect();
            let column_types: Vec<_> = table.columns.iter().map(|c| c.logical_type.clone()).collect();

            Ok(BoundTableRef::BaseTable {
                schema: schema_name,
                name: table_name,
                alias: alias.as_ref().map(|a| a.name.value.clone()),
                column_names,
                column_types,
            })
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

        _ => Err(Error::NotImplemented(format!("Table factor: {:?}", factor))),
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
    })
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
