pub mod physical_plan;

use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::execution::physical_plan::PhysicalPlan;
use crate::transaction::TransactionManager;
use crate::{
    catalog::Catalog,
    storage::tuple::Tuple,
    transaction::{LockMode, Transaction},
    utils::table_ref::TableReference,
};
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
}

impl<'a> ExecutionContext<'a> {
    pub fn new(
        catalog: &'a mut Catalog,
        txn: &'a mut Transaction,
        txn_mgr: &'a TransactionManager,
    ) -> Self {
        Self {
            catalog,
            txn,
            txn_mgr,
        }
    }

    pub fn lock_table(&mut self, table: TableReference, mode: LockMode) -> QuillSQLResult<()> {
        self.txn_mgr
            .acquire_table_lock(self.txn, table.clone(), mode)
            .map_err(|e| QuillSQLError::Execution(format!("lock error: {}", e)))?;
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
