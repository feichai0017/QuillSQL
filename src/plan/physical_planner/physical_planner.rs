use crate::catalog::{Catalog, Schema, DEFAULT_SCHEMA_NAME};
use std::sync::Arc;

use crate::plan::logical_plan::{
    Aggregate, CreateIndex, CreateTable, DropIndex, DropTable, EmptyRelation, Filter, Insert, Join,
    Limit, LogicalPlan, Project, Sort, TableScan, Values,
};

use crate::execution::physical_plan::{
    PhysicalAggregate, PhysicalCreateIndex, PhysicalCreateTable, PhysicalDelete, PhysicalDropIndex,
    PhysicalDropTable, PhysicalEmpty, PhysicalFilter, PhysicalIndexScan, PhysicalInsert,
    PhysicalLimit, PhysicalNestedLoopJoin, PhysicalPlan, PhysicalProject, PhysicalSeqScan,
    PhysicalSort, PhysicalUpdate, PhysicalValues,
};

pub struct PhysicalPlanner<'a> {
    pub catalog: &'a Catalog,
}

impl PhysicalPlanner<'_> {
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
            }) => PhysicalPlan::CreateTable(PhysicalCreateTable::new(
                name.clone(),
                Schema::new(columns.clone()),
                *if_not_exists,
            )),
            LogicalPlan::CreateIndex(CreateIndex {
                index_name,
                table,
                table_schema,
                columns,
            }) => PhysicalPlan::CreateIndex(PhysicalCreateIndex::new(
                index_name.clone(),
                table.clone(),
                table_schema.clone(),
                columns.clone(),
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
            LogicalPlan::TableScan(TableScan {
                table_ref,
                table_schema,
                filters: _,
                limit: _,
                streaming_hint,
            }) => {
                // TODO fix testing
                if let Some(catalog_table) = self
                    .catalog
                    .schemas
                    .get(table_ref.schema().unwrap_or(DEFAULT_SCHEMA_NAME))
                    .unwrap()
                    .tables
                    .get(table_ref.table())
                {
                    if !catalog_table.indexes.is_empty() {
                        PhysicalPlan::IndexScan(PhysicalIndexScan::new(
                            table_ref.clone(),
                            catalog_table.indexes.keys().next().unwrap().clone(),
                            table_schema.clone(),
                            ..,
                        ))
                    } else {
                        {
                            let mut op =
                                PhysicalSeqScan::new(table_ref.clone(), table_schema.clone());
                            op.streaming_hint = *streaming_hint;
                            PhysicalPlan::SeqScan(op)
                        }
                    }
                } else {
                    {
                        let mut op = PhysicalSeqScan::new(table_ref.clone(), table_schema.clone());
                        op.streaming_hint = *streaming_hint;
                        PhysicalPlan::SeqScan(op)
                    }
                }
            }
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
            LogicalPlan::BeginTransaction(_)
            | LogicalPlan::CommitTransaction
            | LogicalPlan::RollbackTransaction
            | LogicalPlan::SetTransaction { .. } => {
                PhysicalPlan::Empty(PhysicalEmpty::new(0, Schema::empty().into()))
            }
        };
        plan
    }
}
