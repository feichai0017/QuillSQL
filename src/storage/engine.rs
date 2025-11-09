use std::sync::Arc;

use crate::{
    catalog::{Catalog, SchemaRef},
    error::QuillSQLResult,
    storage::{
        index::btree_index::BPlusTreeIndex,
        mvcc_heap::MvccHeap,
        page::{RecordId, TupleMeta},
        table_heap::TableHeap,
        tuple::Tuple,
    },
    transaction::TxnContext,
    utils::table_ref::TableReference,
};

pub trait TableHandle: Send + Sync {
    fn table_ref(&self) -> &TableReference;
    fn schema(&self) -> SchemaRef;
    fn table_heap(&self) -> Arc<TableHeap>;

    fn insert(
        &self,
        txn: &mut TxnContext<'_>,
        tuple: &Tuple,
        indexes: &[Arc<dyn IndexHandle>],
    ) -> QuillSQLResult<()>;

    fn delete(
        &self,
        txn: &mut TxnContext<'_>,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
    ) -> QuillSQLResult<()>;

    fn update(
        &self,
        txn: &mut TxnContext<'_>,
        rid: RecordId,
        new_tuple: Tuple,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
        indexes: &[Arc<dyn IndexHandle>],
    ) -> QuillSQLResult<RecordId>;

    fn prepare_row_for_write(
        &self,
        txn: &mut TxnContext<'_>,
        rid: RecordId,
        observed_meta: &TupleMeta,
    ) -> QuillSQLResult<Option<(TupleMeta, Tuple)>>;
}

pub trait IndexHandle: Send + Sync {
    fn name(&self) -> &str;
    fn key_schema(&self) -> SchemaRef;
    fn index(&self) -> Arc<BPlusTreeIndex>;
}

pub trait StorageEngine: Send + Sync {
    fn table(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Arc<dyn TableHandle>>;

    fn indexes(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Vec<Arc<dyn IndexHandle>>>;
}

#[derive(Default)]
pub struct DefaultStorageEngine;

struct HeapTableHandle {
    table_ref: TableReference,
    heap: Arc<TableHeap>,
}

impl HeapTableHandle {
    fn new(table_ref: TableReference, heap: Arc<TableHeap>) -> Self {
        Self { table_ref, heap }
    }
}

impl TableHandle for HeapTableHandle {
    fn table_ref(&self) -> &TableReference {
        &self.table_ref
    }

    fn schema(&self) -> SchemaRef {
        self.heap.schema.clone()
    }

    fn table_heap(&self) -> Arc<TableHeap> {
        self.heap.clone()
    }

    fn insert(
        &self,
        txn: &mut TxnContext<'_>,
        tuple: &Tuple,
        indexes: &[Arc<dyn IndexHandle>],
    ) -> QuillSQLResult<()> {
        txn.ensure_writable(self.table_ref(), "INSERT")?;
        let mvcc = MvccHeap::new(self.heap.clone());
        let (rid, _) = mvcc.insert(tuple, txn.txn_id(), txn.command_id())?;

        let mut index_links = Vec::new();
        for handle in indexes {
            if let Ok(key_tuple) = tuple.project_with_schema(handle.key_schema()) {
                let index = handle.index();
                index.insert(&key_tuple, rid)?;
                index_links.push((index, key_tuple));
            }
        }

        txn.transaction_mut()
            .push_insert_undo(self.heap.clone(), rid, index_links);
        Ok(())
    }

    fn delete(
        &self,
        txn: &mut TxnContext<'_>,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
    ) -> QuillSQLResult<()> {
        txn.ensure_writable(self.table_ref(), "DELETE")?;
        let mvcc = MvccHeap::new(self.heap.clone());
        mvcc.mark_deleted(rid, txn.txn_id(), txn.command_id())?;
        txn.transaction_mut()
            .push_delete_undo(self.heap.clone(), rid, prev_meta, prev_tuple);
        Ok(())
    }

    fn update(
        &self,
        txn: &mut TxnContext<'_>,
        rid: RecordId,
        new_tuple: Tuple,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
        indexes: &[Arc<dyn IndexHandle>],
    ) -> QuillSQLResult<RecordId> {
        txn.ensure_writable(self.table_ref(), "UPDATE")?;
        let mvcc = MvccHeap::new(self.heap.clone());
        let (new_rid, _) = mvcc.update(rid, new_tuple.clone(), txn.txn_id(), txn.command_id())?;

        let mut new_keys = Vec::new();
        for handle in indexes {
            if let Ok(new_key_tuple) = new_tuple.project_with_schema(handle.key_schema()) {
                let index = handle.index();
                index.insert(&new_key_tuple, new_rid)?;
                new_keys.push((index, new_key_tuple));
            }
        }

        txn.transaction_mut().push_update_undo(
            self.heap.clone(),
            rid,
            new_rid,
            prev_meta,
            prev_tuple,
            new_keys,
        );
        Ok(new_rid)
    }

    fn prepare_row_for_write(
        &self,
        txn: &mut TxnContext<'_>,
        rid: RecordId,
        observed_meta: &TupleMeta,
    ) -> QuillSQLResult<Option<(TupleMeta, Tuple)>> {
        if !txn.is_visible(observed_meta) {
            return Ok(None);
        }
        txn.lock_row_exclusive(self.table_ref(), rid)?;
        let mvcc = MvccHeap::new(self.heap.clone());
        let (current_meta, current_tuple) = mvcc.full_tuple(rid)?;
        if !txn.is_visible(&current_meta) {
            txn.unlock_row(self.table_ref(), rid);
            return Ok(None);
        }
        Ok(Some((current_meta, current_tuple)))
    }
}

struct BTreeIndexHandle {
    name: String,
    index: Arc<BPlusTreeIndex>,
}

impl BTreeIndexHandle {
    fn new(name: String, index: Arc<BPlusTreeIndex>) -> Self {
        Self { name, index }
    }
}

impl IndexHandle for BTreeIndexHandle {
    fn name(&self) -> &str {
        &self.name
    }

    fn key_schema(&self) -> SchemaRef {
        self.index.key_schema.clone()
    }

    fn index(&self) -> Arc<BPlusTreeIndex> {
        self.index.clone()
    }
}

impl StorageEngine for DefaultStorageEngine {
    fn table(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Arc<dyn TableHandle>> {
        let heap = catalog.table_heap(table)?;
        Ok(Arc::new(HeapTableHandle::new(table.clone(), heap)))
    }

    fn indexes(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Vec<Arc<dyn IndexHandle>>> {
        let indexes = catalog.table_indexes(table)?;
        Ok(indexes
            .into_iter()
            .map(|(name, index)| {
                Arc::new(BTreeIndexHandle::new(name, index)) as Arc<dyn IndexHandle>
            })
            .collect())
    }
}
