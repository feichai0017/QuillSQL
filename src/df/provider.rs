use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::common::{DFSchema, DataFusionError, SchemaExt, Statistics};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::{common, ExecutionPlan, ExecutionPlanProperties};

use crate::catalog::{SchemaRef as QuillSchemaRef, DEFAULT_CATALOG_NAME};
use crate::utils::table_ref::TableReference;

use super::arrow::{arrow_schema_from_quill, record_batch_from_tuples, tuples_from_batch};
use super::exec::{DmlResultExec, HoltScanExec};
use super::filter::{evaluate_filters_to_mask, indexed_filter_for_expr};
use super::{map_quill_to_df, EngineState};

#[derive(Debug)]
pub struct HoltCatalogProvider {
    pub(super) state: EngineState,
}

impl CatalogProvider for HoltCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let mut names = self
            .state
            .catalog
            .lock()
            .schemas
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if self.state.catalog.lock().schemas.contains_key(name) {
            Some(Arc::new(HoltSchemaProvider {
                state: self.state.clone(),
                schema_name: name.to_string(),
            }))
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct HoltSchemaProvider {
    state: EngineState,
    schema_name: String,
}

#[async_trait]
impl SchemaProvider for HoltSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let catalog = self.state.catalog.lock();
        let mut names = catalog
            .schemas
            .get(&self.schema_name)
            .map(|schema| schema.tables.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let table = {
            let catalog = self.state.catalog.lock();
            let Some(schema) = catalog.schemas.get(&self.schema_name) else {
                return Ok(None);
            };
            let Some(table) = schema.tables.get(name) else {
                return Ok(None);
            };
            (table.schema.clone(), table.table_id)
        };

        let table_ref = TableReference::Full {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: self.schema_name.clone(),
            table: name.to_string(),
        };
        let arrow_schema = Arc::new(arrow_schema_from_quill(table.0.as_ref()));
        Ok(Some(Arc::new(HoltTableProvider {
            state: self.state.clone(),
            table_ref,
            schema: table.0,
            arrow_schema,
            table_id: table.1,
        })))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.state
            .catalog
            .lock()
            .schemas
            .get(&self.schema_name)
            .map(|schema| schema.tables.contains_key(name))
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
pub struct HoltTableProvider {
    state: EngineState,
    table_ref: TableReference,
    schema: QuillSchemaRef,
    arrow_schema: ArrowSchemaRef,
    table_id: u64,
}

#[async_trait]
impl TableProvider for HoltTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(HoltScanExec::new(
            self.state.clone(),
            self.table_ref.clone(),
            self.schema.clone(),
            self.arrow_schema.clone(),
            self.table_id,
            projection.cloned(),
            _filters.to_vec(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(filters
            .iter()
            .map(|filter| {
                if indexed_filter_for_expr(
                    self.schema.as_ref(),
                    &self.state,
                    &self.table_ref,
                    filter,
                )
                .is_some()
                {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if insert_op != InsertOp::Append {
            return Err(DataFusionError::NotImplemented(format!(
                "{insert_op} is not supported by Holt tables"
            )));
        }
        self.arrow_schema
            .logically_equivalent_names_and_types(&input.schema())?;

        let mut batches = Vec::new();
        for partition in 0..input.output_partitioning().partition_count() {
            let stream = input.execute(partition, state.task_ctx())?;
            batches.extend(common::collect(stream).await?);
        }

        let row_count = self
            .state
            .with_write_txn("INSERT", |txn| {
                let binding = self.state.table_binding(&self.table_ref)?;
                let mut inserted = 0u64;
                for batch in &batches {
                    for tuple in tuples_from_batch(self.schema.clone(), batch)? {
                        binding.insert(txn, &tuple)?;
                        inserted += 1;
                    }
                }
                Ok(inserted)
            })
            .map_err(map_quill_to_df)?;

        Ok(Arc::new(DmlResultExec::new(row_count)))
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let rows = self
            .state
            .visible_rows(&self.table_ref)
            .map_err(map_quill_to_df)?;
        if rows.is_empty() {
            return Ok(Arc::new(DmlResultExec::new(0)));
        }
        let tuples = rows.iter().map(|row| row.tuple.clone()).collect::<Vec<_>>();
        let batch =
            record_batch_from_tuples(self.arrow_schema.clone(), self.schema.as_ref(), &tuples)
                .map_err(map_quill_to_df)?;
        let mask = evaluate_filters_to_mask(&filters, &batch, state)?;
        let delete_mask = match mask {
            Some(mask) => mask
                .iter()
                .map(|value| Some(value == Some(true)))
                .collect::<BooleanArray>(),
            None => BooleanArray::from(vec![true; batch.num_rows()]),
        };

        let deleted = self
            .state
            .with_write_txn("DELETE", |txn| {
                let binding = self.state.table_binding(&self.table_ref)?;
                let mut deleted = 0u64;
                for (idx, row) in rows.iter().enumerate() {
                    if delete_mask.value(idx) {
                        if let Some((current_meta, current_tuple)) =
                            binding.prepare_row_for_write(txn, row.rid, &row.meta)?
                        {
                            binding.delete(txn, row.rid, current_meta, current_tuple)?;
                            deleted += 1;
                        }
                    }
                }
                Ok(deleted)
            })
            .map_err(map_quill_to_df)?;

        Ok(Arc::new(DmlResultExec::new(deleted)))
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let rows = self
            .state
            .visible_rows(&self.table_ref)
            .map_err(map_quill_to_df)?;
        if rows.is_empty() {
            return Ok(Arc::new(DmlResultExec::new(0)));
        }

        let tuples = rows.iter().map(|row| row.tuple.clone()).collect::<Vec<_>>();
        let batch =
            record_batch_from_tuples(self.arrow_schema.clone(), self.schema.as_ref(), &tuples)
                .map_err(map_quill_to_df)?;
        let mask = evaluate_filters_to_mask(&filters, &batch, state)?;
        let update_mask = match mask {
            Some(mask) => mask
                .iter()
                .map(|value| Some(value == Some(true)))
                .collect::<BooleanArray>(),
            None => BooleanArray::from(vec![true; batch.num_rows()]),
        };

        let df_schema = DFSchema::try_from(self.arrow_schema.clone())?;
        let physical_assignments = assignments
            .iter()
            .map(|(name, expr)| {
                let physical_expr =
                    create_physical_expr(expr, &df_schema, state.execution_props())?;
                Ok((name.clone(), physical_expr))
            })
            .collect::<Result<
                HashMap<String, Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
                DataFusionError,
            >>()?;

        let mut columns = Vec::with_capacity(batch.num_columns());
        for field in self.arrow_schema.fields() {
            let original = batch.column_by_name(field.name()).ok_or_else(|| {
                DataFusionError::Internal(format!("column {} not found", field.name()))
            })?;
            if let Some(expr) = physical_assignments.get(field.name()) {
                let values = expr.evaluate_selection(&batch, &update_mask)?;
                columns.push(values.into_array(batch.num_rows())?);
            } else {
                columns.push(original.clone());
            }
        }
        let updated_batch = RecordBatch::try_new(self.arrow_schema.clone(), columns)?;
        let updated_tuples =
            tuples_from_batch(self.schema.clone(), &updated_batch).map_err(map_quill_to_df)?;

        let updated = self
            .state
            .with_write_txn("UPDATE", |txn| {
                let binding = self.state.table_binding(&self.table_ref)?;
                let mut updated = 0u64;
                for (idx, row) in rows.iter().enumerate() {
                    if update_mask.value(idx) {
                        if let Some((current_meta, current_tuple)) =
                            binding.prepare_row_for_write(txn, row.rid, &row.meta)?
                        {
                            binding.update(
                                txn,
                                row.rid,
                                updated_tuples[idx].clone(),
                                current_meta,
                                current_tuple,
                            )?;
                            updated += 1;
                        }
                    }
                }
                Ok(updated)
            })
            .map_err(map_quill_to_df)?;

        Ok(Arc::new(DmlResultExec::new(updated)))
    }
}
