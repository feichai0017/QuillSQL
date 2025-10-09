pub mod physical_plan;

use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::execution::physical_plan::PhysicalPlan;
use crate::transaction::{CommandId, TransactionManager, TransactionSnapshot};
use crate::{
    catalog::Catalog,
    storage::tuple::Tuple,
    transaction::{LockMode, Transaction},
    utils::table_ref::TableReference,
};
use log::warn;
use sqlparser::ast::TransactionAccessMode;
pub trait VolcanoExecutor {
    fn init(&self, _context: &mut ExecutionContext) -> QuillSQLResult<()> {
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>>;

    fn output_schema(&self) -> SchemaRef;
}

pub struct ExecutionContext<'a> {
    pub catalog: &'a mut Catalog,
    pub txn: &'a mut Transaction,
    pub txn_mgr: &'a TransactionManager,
    command_id: CommandId,
    snapshot: TransactionSnapshot,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(
        catalog: &'a mut Catalog,
        txn: &'a mut Transaction,
        txn_mgr: &'a TransactionManager,
    ) -> Self {
        let command_id = txn.begin_command();
        let snapshot = txn_mgr.snapshot(txn.id());
        Self {
            catalog,
            txn,
            txn_mgr,
            command_id,
            snapshot,
        }
    }

    pub fn command_id(&self) -> CommandId {
        self.command_id
    }

    pub fn snapshot(&self) -> &TransactionSnapshot {
        &self.snapshot
    }

    pub fn is_visible(&self, meta: &crate::storage::page::TupleMeta) -> bool {
        self.snapshot.is_visible(meta, self.command_id, |txn_id| {
            self.txn_mgr.transaction_status(txn_id)
        })
    }

    pub fn lock_table(&mut self, table: TableReference, mode: LockMode) -> QuillSQLResult<()> {
        self.txn_mgr
            .acquire_table_lock(self.txn, table.clone(), mode)
            .map_err(|e| QuillSQLError::Execution(format!("lock error: {}", e)))?;
        Ok(())
    }

    pub fn lock_row_shared(
        &mut self,
        table: &TableReference,
        rid: crate::storage::page::RecordId,
        retain: bool,
    ) -> QuillSQLResult<()> {
        let acquired =
            self.txn_mgr
                .try_acquire_row_lock(self.txn, table.clone(), rid, LockMode::Shared)?;
        if !acquired {
            return Err(QuillSQLError::Execution(
                "failed to acquire shared row lock".to_string(),
            ));
        }
        if retain {
            self.txn_mgr
                .record_shared_row_lock(self.txn.id(), table.clone(), rid);
        } else {
            // Track transient shared locks so subsequent attempts still go through the lock manager.
            self.txn_mgr
                .remove_row_key_marker(self.txn.id(), table, rid);
        }
        Ok(())
    }

    pub fn unlock_row_shared(
        &mut self,
        table: &TableReference,
        rid: crate::storage::page::RecordId,
    ) -> QuillSQLResult<()> {
        self.txn_mgr
            .try_unlock_shared_row(self.txn.id(), table, rid)
    }

    pub fn lock_row_exclusive(
        &mut self,
        table: &TableReference,
        rid: crate::storage::page::RecordId,
    ) -> QuillSQLResult<()> {
        if !self
            .txn_mgr
            .try_acquire_row_lock(self.txn, table.clone(), rid, LockMode::Exclusive)?
        {
            return Err(QuillSQLError::Execution(
                "failed to acquire row exclusive lock".to_string(),
            ));
        }
        Ok(())
    }

    /// Ensure that the current transaction is allowed to perform a write on the given table.
    pub fn ensure_writable(&self, table: &TableReference, operation: &str) -> QuillSQLResult<()> {
        if matches!(self.txn.access_mode(), TransactionAccessMode::ReadOnly) {
            warn!(
                "read-only txn {} attempted '{}' on {}",
                self.txn.id(),
                operation,
                table.to_log_string()
            );
            return Err(QuillSQLError::Execution(format!(
                "operation '{}' on table {} is not allowed in READ ONLY transaction",
                operation,
                table.to_log_string()
            )));
        }
        Ok(())
    }
}

pub struct ExecutionEngine<'a> {
    pub context: ExecutionContext<'a>,
}
impl<'a> ExecutionEngine<'a> {
    pub fn execute(&mut self, plan: Arc<PhysicalPlan>) -> QuillSQLResult<Vec<Tuple>> {
        plan.init(&mut self.context)?;
        let mut result = Vec::new();
        loop {
            let next_tuple = plan.next(&mut self.context)?;
            if let Some(tuple) = next_tuple {
                result.push(tuple);
            } else {
                break;
            }
        }
        Ok(result)
    }
}
