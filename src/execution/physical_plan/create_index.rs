use crate::catalog::{IndexEngine, SchemaRef, EMPTY_SCHEMA_REF};
use crate::error::QuillSQLError;
use crate::expression::{ColumnExpr, Expr};
use crate::plan::logical_plan::OrderByExpr;
use crate::storage::tuple::Tuple;
use crate::utils::table_ref::TableReference;
use crate::{
    error::QuillSQLResult,
    execution::{ExecutionContext, VolcanoExecutor},
};
use std::sync::Arc;

#[derive(Debug, derive_new::new)]
pub struct PhysicalCreateIndex {
    pub name: String,
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub columns: Vec<OrderByExpr>,
    pub using: Option<IndexEngine>,
}

impl VolcanoExecutor for PhysicalCreateIndex {
    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        let mut key_indices = vec![];
        for col in self.columns.iter() {
            match col.expr.as_ref() {
                Expr::Column(ColumnExpr { name, .. }) => {
                    key_indices.push(self.table_schema.index_of(None, name)?);
                }
                _ => {
                    return Err(QuillSQLError::Execution(format!(
                        "The expr should be column instead of {}",
                        col.expr
                    )))
                }
            }
        }
        let key_schema = Arc::new(self.table_schema.project(&key_indices)?);
        let _ = self.using.unwrap_or(IndexEngine::Holt);
        context
            .catalog
            .create_holt_index(self.name.clone(), &self.table, key_schema)?;
        backfill_index(context, &self.table, &self.name)?;
        Ok(None)
    }
    fn output_schema(&self) -> SchemaRef {
        EMPTY_SCHEMA_REF.clone()
    }
}

fn backfill_index(
    context: &mut ExecutionContext,
    table: &TableReference,
    index_name: &str,
) -> QuillSQLResult<()> {
    let binding = context.table(table)?;
    let index = binding
        .indexes()
        .iter()
        .find(|index| index.name() == index_name)
        .cloned()
        .ok_or_else(|| QuillSQLError::Execution(format!("index {index_name} not found")))?;
    let mut stream = binding.scan()?;
    while let Some((rid, meta, tuple)) = stream.next()? {
        if meta.is_deleted || !context.txn_ctx().is_visible(&meta) {
            continue;
        }
        if let Ok(key) = tuple.project_with_schema(index.key_schema()) {
            index.insert(&key, rid, context.txn_ctx().txn_id())?;
        }
    }
    Ok(())
}

impl std::fmt::Display for PhysicalCreateIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateIndex: {}", self.name)
    }
}
