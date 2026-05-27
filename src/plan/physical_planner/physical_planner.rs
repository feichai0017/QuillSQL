use crate::catalog::{Catalog, CatalogIndex, Schema};
use std::ops::Bound;
use std::sync::Arc;

use crate::expression::{BinaryExpr, BinaryOp, ColumnExpr, Expr, Literal};
use crate::plan::logical_plan::{
    Aggregate, CreateIndex, CreateTable, DropIndex, DropTable, EmptyRelation, Filter, Insert, Join,
    Limit, LogicalPlan, Project, Sort, TableScan, Values,
};
use crate::storage::tuple::Tuple;

use crate::execution::physical_plan::{
    PhysicalAggregate, PhysicalAnalyze, PhysicalCreateIndex, PhysicalCreateTable, PhysicalDelete,
    PhysicalDropIndex, PhysicalDropTable, PhysicalEmpty, PhysicalFilter, PhysicalIndexScan,
    PhysicalInsert, PhysicalLimit, PhysicalNestedLoopJoin, PhysicalPlan, PhysicalProject,
    PhysicalSeqScan, PhysicalSort, PhysicalUpdate, PhysicalValues,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct PhysicalPlanner<'a> {
    catalog: Option<&'a Catalog>,
}

impl<'a> PhysicalPlanner<'a> {
    pub fn new() -> Self {
        Self { catalog: None }
    }

    pub fn with_catalog(catalog: &'a Catalog) -> Self {
        Self {
            catalog: Some(catalog),
        }
    }
}

impl<'a> PhysicalPlanner<'a> {
    pub fn create_physical_plan(&self, logical_plan: LogicalPlan) -> PhysicalPlan {
        let logical_plan = Arc::new(logical_plan);
        self.build_plan(logical_plan)
    }

    fn build_plan(&self, logical_plan: Arc<LogicalPlan>) -> PhysicalPlan {
        let plan = match logical_plan.as_ref() {
            LogicalPlan::CreateTable(CreateTable {
                name,
                columns,
                if_not_exists,
                engine,
            }) => PhysicalPlan::CreateTable(PhysicalCreateTable::new(
                name.clone(),
                Schema::new(columns.clone()),
                *if_not_exists,
                *engine,
            )),
            LogicalPlan::CreateIndex(CreateIndex {
                index_name,
                table,
                table_schema,
                columns,
                using,
            }) => PhysicalPlan::CreateIndex(PhysicalCreateIndex::new(
                index_name.clone(),
                table.clone(),
                table_schema.clone(),
                columns.clone(),
                *using,
            )),
            LogicalPlan::DropTable(DropTable { name, if_exists }) => {
                PhysicalPlan::DropTable(PhysicalDropTable::new(name.clone(), *if_exists))
            }
            LogicalPlan::DropIndex(DropIndex {
                name,
                schema,
                catalog,
                if_exists,
            }) => PhysicalPlan::DropIndex(PhysicalDropIndex::new(
                name.clone(),
                schema.clone(),
                catalog.clone(),
                *if_exists,
            )),
            LogicalPlan::Insert(Insert {
                table,
                table_schema,
                projected_schema,
                input,
            }) => {
                let input_physical_plan = self.build_plan(input.clone());
                PhysicalPlan::Insert(PhysicalInsert::new(
                    table.clone(),
                    table_schema.clone(),
                    projected_schema.clone(),
                    Arc::new(input_physical_plan),
                ))
            }
            LogicalPlan::Values(Values { schema, values }) => {
                PhysicalPlan::Values(PhysicalValues::new(schema.clone(), values.clone()))
            }
            LogicalPlan::Project(Project {
                exprs,
                input,
                schema,
            }) => {
                let input_physical_plan = self.build_plan(input.clone());
                PhysicalPlan::Project(PhysicalProject::new(
                    exprs.clone(),
                    schema.clone(),
                    Arc::new(input_physical_plan),
                ))
            }
            LogicalPlan::Filter(Filter { predicate, input }) => {
                let input_physical_plan = self.build_plan(input.clone());
                PhysicalPlan::Filter(PhysicalFilter::new(
                    predicate.clone(),
                    Arc::new(input_physical_plan),
                ))
            }
            LogicalPlan::TableScan(scan) => self.build_table_scan(scan),
            LogicalPlan::Limit(Limit {
                limit,
                offset,
                input,
            }) => {
                let input_physical_plan = self.build_plan((*input).clone());
                PhysicalPlan::Limit(PhysicalLimit::new(
                    *limit,
                    *offset,
                    Arc::new(input_physical_plan),
                ))
            }
            LogicalPlan::Join(Join {
                left,
                right,
                join_type,
                condition,
                schema,
            }) => {
                let left_physical_plan = self.build_plan((*left).clone());
                let right_physical_plan = self.build_plan((*right).clone());
                PhysicalPlan::NestedLoopJoin(PhysicalNestedLoopJoin::new(
                    *join_type,
                    condition.clone(),
                    Arc::new(left_physical_plan),
                    Arc::new(right_physical_plan),
                    schema.clone(),
                ))
            }
            LogicalPlan::Sort(Sort {
                order_by: expr,
                ref input,
                limit: _,
            }) => {
                // TODO limit
                let input_physical_plan = self.build_plan(Arc::clone(input));
                PhysicalPlan::Sort(PhysicalSort::new(
                    expr.clone(),
                    Arc::new(input_physical_plan),
                ))
            }
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row,
                schema,
            }) => PhysicalPlan::Empty(PhysicalEmpty::new(
                if *produce_one_row { 1 } else { 0 },
                schema.clone(),
            )),
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_exprs,
                aggr_exprs,
                schema,
            }) => {
                let input_physical_plan = self.build_plan(Arc::clone(input));
                PhysicalPlan::Aggregate(PhysicalAggregate::new(
                    Arc::new(input_physical_plan),
                    group_exprs.clone(),
                    aggr_exprs.clone(),
                    schema.clone(),
                ))
            }
            LogicalPlan::Update(update) => PhysicalPlan::Update(PhysicalUpdate::new(
                update.table.clone(),
                update.table_schema.clone(),
                update.assignments.clone(),
                update.selection.clone(),
            )),
            LogicalPlan::Delete(delete) => PhysicalPlan::Delete(PhysicalDelete::new(
                delete.table.clone(),
                delete.table_schema.clone(),
                delete.selection.clone(),
            )),
            LogicalPlan::Analyze(analyze) => {
                PhysicalPlan::Analyze(PhysicalAnalyze::new(analyze.table.clone()))
            }
            LogicalPlan::BeginTransaction(_)
            | LogicalPlan::CommitTransaction
            | LogicalPlan::RollbackTransaction
            | LogicalPlan::SetTransaction { .. } => {
                PhysicalPlan::Empty(PhysicalEmpty::new(0, Schema::empty().into()))
            }
        };
        plan
    }

    fn build_table_scan(&self, scan: &TableScan) -> PhysicalPlan {
        let mut plan = self
            .new_index_scan(scan)
            .unwrap_or_else(|| self.new_seq_scan(scan));

        if let Some(limit_value) = scan.limit {
            plan = PhysicalPlan::Limit(PhysicalLimit::new(Some(limit_value), 0, Arc::new(plan)));
        }

        if let Some(predicate) = conjunction(&scan.filters) {
            plan = PhysicalPlan::Filter(PhysicalFilter::new(predicate, Arc::new(plan)));
        }

        plan
    }
}

impl<'a> PhysicalPlanner<'a> {
    fn new_seq_scan(&self, scan: &TableScan) -> PhysicalPlan {
        let op = PhysicalSeqScan::new(scan.table_ref.clone(), scan.table_schema.clone());
        PhysicalPlan::SeqScan(op)
    }

    fn new_index_scan(&self, scan: &TableScan) -> Option<PhysicalPlan> {
        let catalog = self.catalog?;
        let indexes = catalog.table_indexes(&scan.table_ref).ok()?;
        indexes.into_iter().find_map(|index| {
            let bounds = bounds_for_index(&index, &scan.filters)?;
            Some(PhysicalPlan::IndexScan(PhysicalIndexScan::new(
                scan.table_ref.clone(),
                index.name,
                scan.table_schema.clone(),
                bounds,
            )))
        })
    }
}

fn conjunction(predicates: &[Expr]) -> Option<Expr> {
    let mut iter = predicates.iter();
    let first = iter.next()?.clone();
    Some(iter.fold(first, |acc, expr| {
        Expr::Binary(BinaryExpr {
            left: Box::new(acc),
            op: BinaryOp::And,
            right: Box::new(expr.clone()),
        })
    }))
}

fn bounds_for_index(
    index: &CatalogIndex,
    filters: &[Expr],
) -> Option<(Bound<Tuple>, Bound<Tuple>)> {
    if index.key_schema.column_count() != 1 {
        return None;
    }
    let column = index.key_schema.columns[0].clone();
    let mut lower = Bound::Unbounded;
    let mut upper = Bound::Unbounded;
    let mut matched = false;
    for predicate in flattened_conjuncts(filters) {
        if let Some((next_lower, next_upper)) =
            bounds_from_predicate(predicate, column.name.as_str(), &index.key_schema)
        {
            lower = merge_lower(lower, next_lower);
            upper = merge_upper(upper, next_upper);
            matched = true;
        }
    }
    matched.then_some((lower, upper))
}

fn flattened_conjuncts(filters: &[Expr]) -> Vec<&Expr> {
    let mut out = Vec::new();
    for filter in filters {
        flatten_conjunct(filter, &mut out);
    }
    out
}

fn flatten_conjunct<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::Binary(BinaryExpr {
        left,
        op: BinaryOp::And,
        right,
    }) = expr
    {
        flatten_conjunct(left, out);
        flatten_conjunct(right, out);
    } else {
        out.push(expr);
    }
}

fn bounds_from_predicate(
    predicate: &Expr,
    column_name: &str,
    key_schema: &crate::catalog::SchemaRef,
) -> Option<(Bound<Tuple>, Bound<Tuple>)> {
    let Expr::Binary(BinaryExpr { left, op, right }) = predicate else {
        return None;
    };
    if let Some(value) = column_literal(left, right, column_name, key_schema) {
        return bounds_for_op(*op, value);
    }
    if let Some(value) = column_literal(right, left, column_name, key_schema) {
        return bounds_for_op(invert_comparison(*op)?, value);
    }
    None
}

fn column_literal(
    column_expr: &Expr,
    literal_expr: &Expr,
    column_name: &str,
    key_schema: &crate::catalog::SchemaRef,
) -> Option<Tuple> {
    let Expr::Column(ColumnExpr { name, .. }) = column_expr else {
        return None;
    };
    if !name.eq_ignore_ascii_case(column_name) {
        return None;
    }
    let Expr::Literal(Literal { value }) = literal_expr else {
        return None;
    };
    let data_type = key_schema.columns[0].data_type;
    let value = value.cast_to(&data_type).ok()?;
    Some(Tuple::new(key_schema.clone(), vec![value]))
}

fn bounds_for_op(op: BinaryOp, value: Tuple) -> Option<(Bound<Tuple>, Bound<Tuple>)> {
    match op {
        BinaryOp::Eq => Some((Bound::Included(value.clone()), Bound::Included(value))),
        BinaryOp::Gt => Some((Bound::Excluded(value), Bound::Unbounded)),
        BinaryOp::GtEq => Some((Bound::Included(value), Bound::Unbounded)),
        BinaryOp::Lt => Some((Bound::Unbounded, Bound::Excluded(value))),
        BinaryOp::LtEq => Some((Bound::Unbounded, Bound::Included(value))),
        _ => None,
    }
}

fn invert_comparison(op: BinaryOp) -> Option<BinaryOp> {
    match op {
        BinaryOp::Eq => Some(BinaryOp::Eq),
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::GtEq => Some(BinaryOp::LtEq),
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::LtEq => Some(BinaryOp::GtEq),
        _ => None,
    }
}

fn merge_lower(current: Bound<Tuple>, next: Bound<Tuple>) -> Bound<Tuple> {
    match (current, next) {
        (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound,
        (left, right) => {
            let left_tuple = bound_tuple(&left);
            let right_tuple = bound_tuple(&right);
            match left_tuple.cmp(right_tuple) {
                std::cmp::Ordering::Less => right,
                std::cmp::Ordering::Greater => left,
                std::cmp::Ordering::Equal => {
                    if matches!(left, Bound::Excluded(_)) || matches!(right, Bound::Excluded(_)) {
                        Bound::Excluded(left_tuple.clone())
                    } else {
                        Bound::Included(left_tuple.clone())
                    }
                }
            }
        }
    }
}

fn merge_upper(current: Bound<Tuple>, next: Bound<Tuple>) -> Bound<Tuple> {
    match (current, next) {
        (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound,
        (left, right) => {
            let left_tuple = bound_tuple(&left);
            let right_tuple = bound_tuple(&right);
            match left_tuple.cmp(right_tuple) {
                std::cmp::Ordering::Less => left,
                std::cmp::Ordering::Greater => right,
                std::cmp::Ordering::Equal => {
                    if matches!(left, Bound::Excluded(_)) || matches!(right, Bound::Excluded(_)) {
                        Bound::Excluded(left_tuple.clone())
                    } else {
                        Bound::Included(left_tuple.clone())
                    }
                }
            }
        }
    }
}

fn bound_tuple(bound: &Bound<Tuple>) -> &Tuple {
    match bound {
        Bound::Included(tuple) | Bound::Excluded(tuple) => tuple,
        Bound::Unbounded => unreachable!("unbounded handled before bound_tuple"),
    }
}
