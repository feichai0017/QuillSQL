use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::TableReference as DfTableRef;
use datafusion::logical_expr::{DdlStatement, LogicalPlan};

use crate::catalog::DEFAULT_SCHEMA_NAME;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::holt::HoltIndexHandle;

use super::arrow::{
    default_index_name, index_schema_from_sort_exprs, quill_schema_from_arrow, quill_table_ref,
    tuples_from_batch,
};
use super::{map_df_err, EngineState};

pub(crate) async fn execute_logical_plan(
    state: &EngineState,
    plan: LogicalPlan,
) -> QuillSQLResult<Vec<RecordBatch>> {
    match plan {
        LogicalPlan::Ddl(ddl) => execute_ddl(state, ddl).await,
        plan => {
            let ctx = state.session_context();
            let df = ctx.execute_logical_plan(plan).await.map_err(map_df_err)?;
            df.collect().await.map_err(map_df_err)
        }
    }
}

async fn execute_ddl(state: &EngineState, ddl: DdlStatement) -> QuillSQLResult<Vec<RecordBatch>> {
    match ddl {
        DdlStatement::CreateMemoryTable(cmd) => {
            create_holt_table(
                state,
                cmd.name,
                cmd.input,
                cmd.if_not_exists,
                cmd.or_replace,
            )
            .await
        }
        DdlStatement::CreateIndex(cmd) => create_holt_index(state, cmd).await,
        DdlStatement::DropTable(cmd) => {
            let table_ref = quill_table_ref(&cmd.name);
            let dropped = state.catalog.lock().drop_table(&table_ref)?;
            if !dropped && !cmd.if_exists {
                return Err(QuillSQLError::Execution(format!(
                    "table {} does not exist",
                    table_ref.to_log_string()
                )));
            }
            Ok(Vec::new())
        }
        DdlStatement::CreateCatalogSchema(cmd) => {
            let mut catalog = state.catalog.lock();
            if catalog.schemas.contains_key(&cmd.schema_name) {
                if cmd.if_not_exists {
                    return Ok(Vec::new());
                }
                return Err(QuillSQLError::Storage(format!(
                    "schema {} already exists",
                    cmd.schema_name
                )));
            }
            catalog.create_schema(cmd.schema_name)?;
            Ok(Vec::new())
        }
        other => Err(QuillSQLError::NotSupport(format!(
            "DataFusion DDL {} is not supported by Holt",
            other.name()
        ))),
    }
}

async fn create_holt_table(
    state: &EngineState,
    name: DfTableRef,
    input: Arc<LogicalPlan>,
    if_not_exists: bool,
    or_replace: bool,
) -> QuillSQLResult<Vec<RecordBatch>> {
    let table_ref = quill_table_ref(&name);
    let schema = Arc::new(quill_schema_from_arrow(input.schema().as_arrow())?);
    {
        let mut catalog = state.catalog.lock();
        let schema_name = table_ref
            .schema()
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string();
        if !catalog.schemas.contains_key(&schema_name) {
            catalog.create_schema(schema_name.clone())?;
        }
        if catalog.try_table_schema(&table_ref).is_some() {
            if if_not_exists {
                return Ok(Vec::new());
            }
            if or_replace {
                catalog.drop_table(&table_ref)?;
            } else {
                return Err(QuillSQLError::Storage(format!(
                    "table {} already exists",
                    table_ref.to_log_string()
                )));
            }
        }
        catalog.create_table(table_ref.clone(), schema)?;
    }

    let ctx = state.session_context();
    let df = ctx
        .execute_logical_plan((*input).clone())
        .await
        .map_err(map_df_err)?;
    let batches = df.collect().await.map_err(map_df_err)?;
    if batches.iter().any(|batch| batch.num_rows() > 0) {
        let quill_schema = state.table_schema(&table_ref)?;
        state.with_write_txn("CREATE TABLE AS", |txn| {
            let binding = state.table_binding(&table_ref)?;
            for batch in &batches {
                for tuple in tuples_from_batch(quill_schema.clone(), batch)? {
                    binding.insert(txn, &tuple)?;
                }
            }
            Ok(())
        })?;
    }
    Ok(Vec::new())
}

async fn create_holt_index(
    state: &EngineState,
    cmd: datafusion::logical_expr::CreateIndex,
) -> QuillSQLResult<Vec<RecordBatch>> {
    if let Some(using) = &cmd.using {
        return Err(QuillSQLError::NotSupport(format!(
            "CREATE INDEX USING {using} is not supported; Holt is the only index backend"
        )));
    }

    let table_ref = quill_table_ref(&cmd.table);
    let table_schema = state.table_schema(&table_ref)?;
    let key_schema = Arc::new(index_schema_from_sort_exprs(
        table_schema.as_ref(),
        &cmd.columns,
    )?);
    let index_name = cmd
        .name
        .unwrap_or_else(|| default_index_name(&table_ref, key_schema.as_ref()));
    let index_id = {
        let mut catalog = state.catalog.lock();
        if catalog
            .table_indexes(&table_ref)?
            .iter()
            .any(|idx| idx.name == index_name)
        {
            if cmd.if_not_exists {
                return Ok(Vec::new());
            }
            return Err(QuillSQLError::Storage(format!(
                "index {index_name} already exists"
            )));
        }
        catalog.create_index(index_name.clone(), &table_ref, key_schema.clone())?
    };

    let index = HoltIndexHandle::new(
        index_name,
        key_schema.clone(),
        index_id,
        state.holt_store.clone(),
    );
    for row in state.visible_rows(&table_ref)? {
        let key = row.tuple.project_with_schema(key_schema.clone())?;
        crate::storage::IndexHandle::insert(&index, &key, row.rid, 0)?;
    }
    Ok(Vec::new())
}
