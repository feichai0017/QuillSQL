use crate::catalog::{Column, Schema};
use crate::expression::{columnize_expr, Alias, ColumnExpr, Expr, ExprTrait};
use crate::plan::logical_plan::{
    build_join_schema, project_schema, EmptyRelation, Filter, Join, LogicalPlan, Project,
    TableScan, Values,
};
use crate::plan::logical_plan::{Aggregate, JoinType};
use crate::plan::LogicalPlanner;
use crate::error::{QuillSQLError, QuillSQLResult};
use std::sync::Arc;
use std::vec;

impl LogicalPlanner<'_> {
    pub fn plan_set_expr(&self, set_expr: &sqlparser::ast::SetExpr) -> QuillSQLResult<LogicalPlan> {
        match set_expr {
            sqlparser::ast::SetExpr::Select(select) => self.plan_select(select),
            sqlparser::ast::SetExpr::Values(values) => self.plan_values(values),
            _ => Err(QuillSQLError::Plan(format!(
                "Failed to plan set expr: {}",
                set_expr
            ))),
        }
    }

    pub fn plan_select(&self, select: &sqlparser::ast::Select) -> QuillSQLResult<LogicalPlan> {
        let table_scan = self.plan_from_tables(&select.from)?;
        let selection = self.plan_selection(table_scan, &select.selection)?;
        let aggregate = self.plan_aggregate(selection, &select.projection, &select.group_by)?;
        self.plan_project(aggregate, &select.projection)
    }

    pub fn plan_aggregate(
        &self,
        input: LogicalPlan,
        project: &Vec<sqlparser::ast::SelectItem>,
        group_by: &[sqlparser::ast::Expr],
    ) -> QuillSQLResult<LogicalPlan> {
        let mut exprs = vec![];
        for select_item in project {
            exprs.extend(self.bind_select_item(&input, select_item)?);
        }

        let aggr_exprs = exprs
            .iter()
            .filter(|e| matches!(e, Expr::AggregateFunction(_)))
            .cloned()
            .collect::<Vec<Expr>>();
        let group_exprs = group_by
            .iter()
            .map(|e| self.bind_expr(e))
            .collect::<QuillSQLResult<Vec<Expr>>>()?;

        if aggr_exprs.is_empty() && group_exprs.is_empty() {
            Ok(input)
        } else {
            let mut columns = aggr_exprs
                .iter()
                .map(|e| e.to_column(input.schema()))
                .collect::<QuillSQLResult<Vec<Column>>>()?;
            columns.extend(
                group_exprs
                    .iter()
                    .map(|e| e.to_column(input.schema()))
                    .collect::<QuillSQLResult<Vec<Column>>>()?,
            );
            Ok(LogicalPlan::Aggregate(Aggregate {
                input: Arc::new(input),
                group_exprs,
                aggr_exprs,
                schema: Arc::new(Schema::new(columns)),
            }))
        }
    }

    pub fn plan_project(
        &self,
        input: LogicalPlan,
        project: &Vec<sqlparser::ast::SelectItem>,
    ) -> QuillSQLResult<LogicalPlan> {
        let mut exprs = vec![];
        for select_item in project {
            exprs.extend(self.bind_select_item(&input, select_item)?);
        }
        let columnized_exprs = exprs
            .into_iter()
            .map(|e| {
                if let Ok(new_expr) = columnize_expr(&e, input.schema()) {
                    new_expr
                } else {
                    e
                }
            })
            .collect::<Vec<Expr>>();

        let schema = Arc::new(project_schema(&input, &columnized_exprs)?);
        Ok(LogicalPlan::Project(Project {
            exprs: columnized_exprs,
            input: Arc::new(input),
            schema,
        }))
    }

    pub fn bind_select_item(
        &self,
        input: &LogicalPlan,
        item: &sqlparser::ast::SelectItem,
    ) -> QuillSQLResult<Vec<Expr>> {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => Ok(vec![self.bind_expr(expr)?]),
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                Ok(vec![Expr::Alias(Alias {
                    name: alias.value.clone(),
                    expr: Box::new(self.bind_expr(expr)?),
                })])
            }
            sqlparser::ast::SelectItem::Wildcard(_) => {
                let all_columns = input
                    .schema()
                    .columns
                    .iter()
                    .map(|col| {
                        Expr::Column(ColumnExpr {
                            relation: col.relation.clone(),
                            name: col.name.clone(),
                        })
                    })
                    .collect::<Vec<Expr>>();
                Ok(all_columns)
            }
            _ => Err(QuillSQLError::Plan(format!(
                "sqlparser select item {} not supported",
                item
            ))),
        }
    }

    pub fn plan_selection(
        &self,
        input: LogicalPlan,
        selection: &Option<sqlparser::ast::Expr>,
    ) -> QuillSQLResult<LogicalPlan> {
        match selection {
            None => Ok(input),
            Some(predicate) => {
                let predicate = self.bind_expr(predicate)?;
                Ok(LogicalPlan::Filter(Filter {
                    input: Arc::new(input),
                    predicate,
                }))
            }
        }
    }

    pub fn plan_from_tables(
        &self,
        from: &Vec<sqlparser::ast::TableWithJoins>,
    ) -> QuillSQLResult<LogicalPlan> {
        match from.len() {
            0 => Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            1 => self.plan_table_with_joins(&from[0]),
            _ => {
                let mut left = self.plan_table_with_joins(&from[0])?;
                for t in from.iter().skip(1) {
                    let right = self.plan_table_with_joins(t)?;
                    left = self.plan_cross_join(left, right)?;
                }
                Ok(left)
            }
        }
    }

    pub fn plan_table_with_joins(
        &self,
        t: &sqlparser::ast::TableWithJoins,
    ) -> QuillSQLResult<LogicalPlan> {
        let mut left = self.plan_relation(&t.relation)?;
        match t.joins.len() {
            0 => Ok(left),
            _ => {
                for join in t.joins.iter() {
                    left = self.plan_relation_join(left, join)?;
                }
                Ok(left)
            }
        }
    }

    pub fn plan_relation_join(
        &self,
        left: LogicalPlan,
        join: &sqlparser::ast::Join,
    ) -> QuillSQLResult<LogicalPlan> {
        let right = self.plan_relation(&join.relation)?;
        match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(constraint) => {
                self.plan_join(left, right, constraint, JoinType::Inner)
            }
            sqlparser::ast::JoinOperator::LeftOuter(constraint) => {
                self.plan_join(left, right, constraint, JoinType::Inner)
            }
            sqlparser::ast::JoinOperator::RightOuter(constraint) => {
                self.plan_join(left, right, constraint, JoinType::Inner)
            }
            sqlparser::ast::JoinOperator::FullOuter(constraint) => {
                self.plan_join(left, right, constraint, JoinType::Inner)
            }
            sqlparser::ast::JoinOperator::CrossJoin => self.plan_cross_join(left, right),
            _ => Err(QuillSQLError::Plan(format!(
                "sqlparser join operator {:?} not supported",
                join.join_operator
            ))),
        }
    }

    pub fn plan_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        constraint: &sqlparser::ast::JoinConstraint,
        join_type: JoinType,
    ) -> QuillSQLResult<LogicalPlan> {
        match constraint {
            sqlparser::ast::JoinConstraint::On(expr) => {
                let expr = self.bind_expr(expr)?;
                let schema = Arc::new(build_join_schema(left.schema(), right.schema(), join_type)?);
                Ok(LogicalPlan::Join(Join {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    join_type,
                    condition: Some(expr),
                    schema,
                }))
            }
            _ => Err(QuillSQLError::Plan(format!(
                "Only support join on constraint, {:?}",
                constraint
            ))),
        }
    }

    pub fn plan_cross_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
    ) -> QuillSQLResult<LogicalPlan> {
        let schema = Arc::new(build_join_schema(
            left.schema(),
            right.schema(),
            JoinType::Cross,
        )?);
        Ok(LogicalPlan::Join(Join {
            left: Arc::new(left),
            right: Arc::new(right),
            join_type: JoinType::Cross,
            condition: None,
            schema,
        }))
    }

    pub fn plan_relation(
        &self,
        relation: &sqlparser::ast::TableFactor,
    ) -> QuillSQLResult<LogicalPlan> {
        match relation {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                // TODO handle alias
                let table_ref = self.bind_table_name(name)?;
                let schema = self.context.catalog.table_heap(&table_ref)?.schema.clone();
                Ok(LogicalPlan::TableScan(TableScan {
                    table_ref,
                    table_schema: schema,
                    filters: vec![],
                    limit: None,
                }))
            }
            sqlparser::ast::TableFactor::NestedJoin {
                table_with_joins,
                alias: _,
            } => {
                // TODO handle alias
                self.plan_table_with_joins(table_with_joins)
            }
            sqlparser::ast::TableFactor::Derived { subquery, .. } => self.plan_query(subquery),
            _ => Err(QuillSQLError::Plan(format!(
                "sqlparser relation {} not supported",
                relation
            ))),
        }
    }

    pub fn plan_values(&self, values: &sqlparser::ast::Values) -> QuillSQLResult<LogicalPlan> {
        let mut result = vec![];
        for row in values.rows.iter() {
            let mut record = vec![];
            for item in row {
                record.push(self.bind_expr(item)?);
            }
            result.push(record);
        }

        // schema will be replaced later
        Ok(LogicalPlan::Values(Values {
            schema: Arc::new(Schema::empty()),
            values: result,
        }))
    }
}
