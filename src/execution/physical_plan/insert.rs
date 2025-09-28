use log::debug;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicU32, Arc};

use crate::catalog::{SchemaRef, INSERT_OUTPUT_SCHEMA_REF};
use crate::error::QuillSQLError;
use crate::storage::page::TupleMeta;
use crate::storage::tuple::Tuple;
use crate::transaction::LockMode;
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;
use crate::{
    error::QuillSQLResult,
    execution::{ExecutionContext, VolcanoExecutor},
};

use super::PhysicalPlan;

#[derive(Debug)]
pub struct PhysicalInsert {
    pub table: TableReference,
    pub table_schema: SchemaRef,
    pub projected_schema: SchemaRef,
    pub input: Arc<PhysicalPlan>,

    insert_rows: AtomicU32,
}
impl PhysicalInsert {
    pub fn new(
        table: TableReference,
        table_schema: SchemaRef,
        projected_schema: SchemaRef,
        input: Arc<PhysicalPlan>,
    ) -> Self {
        Self {
            table,
            table_schema,
            projected_schema,
            input,
            insert_rows: AtomicU32::new(0),
        }
    }
}
impl VolcanoExecutor for PhysicalInsert {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        debug!("init insert executor");
        self.input.init(context)?;
        self.insert_rows.store(0, Ordering::SeqCst);
        if context
            .txn_mgr
            .acquire_table_lock(
                context.txn,
                self.table.clone(),
                LockMode::IntentionExclusive,
            )
            .is_err()
        {
            return Err(QuillSQLError::Execution(format!(
                "failed to acquire IX lock on table {}",
                self.table
            )));
        }
        Ok(())
    }
    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        loop {
            let next_tuple = self.input.next(context)?;
            if next_tuple.is_none() {
                // only return insert_rows when input exhausted
                return if self.insert_rows.load(Ordering::SeqCst) == 0 {
                    Ok(None)
                } else {
                    let insert_rows = self.insert_rows.swap(0, Ordering::SeqCst);
                    Ok(Some(Tuple::new(
                        self.output_schema(),
                        vec![ScalarValue::Int32(Some(insert_rows as i32))],
                    )))
                };
            }
            let tuple = next_tuple.unwrap();

            // cast values
            let mut casted_data = vec![];
            for (idx, value) in tuple.data.iter().enumerate() {
                let target_type = self.projected_schema.column_with_index(idx)?.data_type;
                casted_data.push(value.cast_to(&target_type)?);
            }

            // fill default values
            let mut full_data = vec![];
            for col in self.table_schema.columns.iter() {
                if let Ok(idx) = self
                    .projected_schema
                    .index_of(col.relation.as_ref(), &col.name)
                {
                    full_data.push(casted_data[idx].clone());
                } else {
                    full_data.push(col.default.clone())
                }
            }

            let tuple = Tuple::new(self.table_schema.clone(), full_data);

            let table_heap = context.catalog.table_heap(&self.table)?;
            let meta = TupleMeta {
                insert_txn_id: context.txn.id(),
                delete_txn_id: 0,
                is_deleted: false,
            };
            let rid = table_heap.insert_tuple(&meta, &tuple)?;
            context.txn_mgr.record_row_lock(
                context.txn.id(),
                self.table.clone(),
                rid,
                LockMode::Exclusive,
            );

            let indexes = context.catalog.table_indexes(&self.table)?;
            for index in indexes {
                if let Ok(key_tuple) = tuple.project_with_schema(index.key_schema.clone()) {
                    let root_page_id = index.get_root_page_id()?;
                    index.insert(&key_tuple, rid)?;
                    let new_root_page_id = index.get_root_page_id()?;
                    if new_root_page_id != root_page_id {
                        // The root page ID has changed, which means a split propagated to the root.
                        // With the new header page model, we no longer need to update the catalog here,
                        // as the header_page_id is immutable. The root_page_id is updated within
                        // the header page automatically.
                    }
                }
            }

            self.insert_rows.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn output_schema(&self) -> SchemaRef {
        INSERT_OUTPUT_SCHEMA_REF.clone()
    }
}

impl std::fmt::Display for PhysicalInsert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Insert")
    }
}
