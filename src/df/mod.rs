use std::fmt;
use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::context::SessionContext as DfSessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use parking_lot::Mutex;
use sqlparser::ast::TransactionAccessMode;

use crate::catalog::{
    Catalog, SchemaRef as QuillSchemaRef, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::engine::{TableBinding, TupleStream};
use crate::storage::holt::HoltStore;
use crate::storage::record::{RecordId, TupleMeta};
use crate::storage::tuple::Tuple;
use crate::transaction::TxnContext;
use crate::transaction::{IsolationLevel, Transaction, TransactionManager, TransactionStatus};
use crate::utils::table_ref::TableReference;

mod arrow;
mod ddl;
mod exec;
mod filter;
mod provider;

pub(crate) use arrow::{pretty_format_batches, record_batches_to_string_rows};
pub(crate) use ddl::execute_logical_plan;

#[derive(Clone)]
pub struct EngineState {
    pub catalog: Arc<Mutex<Catalog>>,
    pub holt_store: Arc<HoltStore>,
    pub transaction_manager: Arc<TransactionManager>,
    pub active_txn: Arc<Mutex<Option<Transaction>>>,
    pub default_isolation: IsolationLevel,
}

impl fmt::Debug for EngineState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EngineState").finish_non_exhaustive()
    }
}

impl EngineState {
    pub fn session_context(&self) -> DfSessionContext {
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(crate::jit::MlirJitRule::new()))
            .build();
        let ctx = DfSessionContext::new_with_state(state);
        ctx.register_catalog(
            DEFAULT_CATALOG_NAME,
            Arc::new(provider::HoltCatalogProvider {
                state: self.clone(),
            }),
        );
        ctx
    }

    pub fn table_binding(&self, table_ref: &TableReference) -> QuillSQLResult<TableBinding> {
        let catalog = self.catalog.lock();
        crate::storage::HoltStorage::new(self.holt_store.clone()).table(&catalog, table_ref)
    }

    fn table_schema(&self, table_ref: &TableReference) -> QuillSQLResult<QuillSchemaRef> {
        self.catalog.lock().table_schema(table_ref)
    }

    pub fn begin_transaction(
        &self,
        isolation: IsolationLevel,
        access_mode: TransactionAccessMode,
    ) -> QuillSQLResult<()> {
        let mut active = self.active_txn.lock();
        if active.is_some() {
            return Err(QuillSQLError::Execution(
                "transaction already active".to_string(),
            ));
        }
        let txn = self.transaction_manager.begin(isolation, access_mode)?;
        self.holt_store
            .put_txn_status(txn.id(), TransactionStatus::InProgress)?;
        *active = Some(txn);
        Ok(())
    }

    pub fn commit_transaction(&self) -> QuillSQLResult<()> {
        let mut active = self.active_txn.lock();
        let mut txn = active
            .take()
            .ok_or_else(|| QuillSQLError::Execution("no active transaction".to_string()))?;
        let txn_id = txn.id();
        self.transaction_manager.commit(&mut txn)?;
        self.holt_store
            .put_txn_status(txn_id, TransactionStatus::Committed)
    }

    pub fn rollback_transaction(&self) -> QuillSQLResult<()> {
        let mut active = self.active_txn.lock();
        let mut txn = active
            .take()
            .ok_or_else(|| QuillSQLError::Execution("no active transaction".to_string()))?;
        let txn_id = txn.id();
        self.transaction_manager.abort(&mut txn)?;
        self.holt_store
            .put_txn_status(txn_id, TransactionStatus::Aborted)
    }

    fn with_write_txn<T>(
        &self,
        operation: &str,
        f: impl FnOnce(&mut TxnContext<'_>) -> QuillSQLResult<T>,
    ) -> QuillSQLResult<T> {
        if let Some(txn) = self.active_txn.lock().as_mut() {
            let mut ctx = TxnContext::new(self.transaction_manager.clone(), txn);
            return f(&mut ctx);
        }

        let mut txn = self
            .transaction_manager
            .begin(self.default_isolation, TransactionAccessMode::ReadWrite)?;
        self.holt_store
            .put_txn_status(txn.id(), TransactionStatus::InProgress)?;
        let txn_id = txn.id();
        let result = {
            let mut ctx = TxnContext::new(self.transaction_manager.clone(), &mut txn);
            f(&mut ctx)
        };

        match result {
            Ok(value) => {
                self.transaction_manager.commit(&mut txn)?;
                self.holt_store
                    .put_txn_status(txn_id, TransactionStatus::Committed)?;
                Ok(value)
            }
            Err(err) => {
                let abort_result = self.transaction_manager.abort(&mut txn).and_then(|_| {
                    self.holt_store
                        .put_txn_status(txn_id, TransactionStatus::Aborted)
                });
                if let Err(abort_err) = abort_result {
                    return Err(QuillSQLError::Execution(format!(
                        "{operation} failed: {err}; abort also failed: {abort_err}"
                    )));
                }
                Err(err)
            }
        }
    }

    fn visible_rows(&self, table_ref: &TableReference) -> QuillSQLResult<Vec<VisibleRow>> {
        let binding = self.table_binding(table_ref)?;
        let mut stream = binding.scan()?;
        self.visible_rows_from_stream(stream.as_mut())
    }

    fn visible_rows_from_stream(
        &self,
        stream: &mut dyn TupleStream,
    ) -> QuillSQLResult<Vec<VisibleRow>> {
        let mut rows = Vec::new();

        if let Some(txn) = self.active_txn.lock().as_mut() {
            let ctx = TxnContext::new(self.transaction_manager.clone(), txn);
            while let Some((rid, meta, tuple)) = stream.next()? {
                if ctx.is_visible(&meta) {
                    rows.push(VisibleRow { rid, meta, tuple });
                }
            }
            return Ok(rows);
        }

        let snapshot = self
            .transaction_manager
            .snapshot(self.transaction_manager.next_txn_id_hint());
        while let Some((rid, meta, tuple)) = stream.next()? {
            if snapshot.is_visible(&meta, 0, |txn_id| {
                self.transaction_manager.transaction_status(txn_id)
            }) {
                rows.push(VisibleRow { rid, meta, tuple });
            }
        }
        Ok(rows)
    }
}

#[derive(Clone)]
struct VisibleRow {
    rid: RecordId,
    meta: TupleMeta,
    tuple: Tuple,
}

pub(super) fn map_df_err(err: DataFusionError) -> QuillSQLError {
    QuillSQLError::Execution(format!("DataFusion error: {err}"))
}

pub(super) fn map_quill_to_df(err: QuillSQLError) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}
