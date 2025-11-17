use std::fmt;
use std::ops::Bound;
use std::sync::Arc;

use crate::{
    catalog::{Catalog, SchemaRef},
    error::QuillSQLResult,
    storage::{
        index::btree_index::{BPlusTreeIndex, TreeIndexIterator},
        mvcc_heap::MvccHeap,
        page::{RecordId, TupleMeta},
        table_heap::{TableHeap, TableIterator},
        tuple::Tuple,
    },
    transaction::TxnContext,
    utils::table_ref::TableReference,
};

#[derive(Debug, Clone)]
pub struct IndexScanRequest {
    pub start: Bound<Tuple>,
    pub end: Bound<Tuple>,
}

impl IndexScanRequest {
    pub fn new(start: Bound<Tuple>, end: Bound<Tuple>) -> Self {
        Self { start, end }
    }
}

pub trait TupleStream {
    fn next(&mut self) -> QuillSQLResult<Option<(RecordId, TupleMeta, Tuple)>>;
}

pub trait TableHandle: Send + Sync {
    fn table_ref(&self) -> &TableReference;
    fn schema(&self) -> SchemaRef;
    fn table_heap(&self) -> Arc<TableHeap>;
    fn full_scan(&self) -> QuillSQLResult<Box<dyn TupleStream>>;

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
        indexes: &[Arc<dyn IndexHandle>],
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
    fn range_scan(
        &self,
        table: Arc<dyn TableHandle>,
        request: IndexScanRequest,
    ) -> QuillSQLResult<Box<dyn TupleStream>>;
}

pub trait StorageEngine: Send + Sync {
    fn table(&self, catalog: &Catalog, table: &TableReference) -> QuillSQLResult<TableBinding>;
}

#[derive(Default)]
pub struct DefaultStorageEngine;

#[derive(Clone)]
pub struct TableBinding {
    table: Arc<dyn TableHandle>,
    indexes: Arc<Vec<Arc<dyn IndexHandle>>>,
}

impl TableBinding {
    fn new(table: Arc<dyn TableHandle>, indexes: Vec<Arc<dyn IndexHandle>>) -> Self {
        Self {
            table,
            indexes: Arc::new(indexes),
        }
    }

    pub fn table(&self) -> Arc<dyn TableHandle> {
        self.table.clone()
    }

    pub fn table_heap(&self) -> Arc<TableHeap> {
        self.table.table_heap()
    }

    pub fn indexes(&self) -> &[Arc<dyn IndexHandle>] {
        self.indexes.as_ref()
    }

    pub fn scan(&self) -> QuillSQLResult<Box<dyn TupleStream>> {
        self.table.full_scan()
    }

    pub fn insert(&self, txn: &mut TxnContext<'_>, tuple: &Tuple) -> QuillSQLResult<()> {
        self.table.insert(txn, tuple, self.indexes())
    }

    pub fn delete(
        &self,
        txn: &mut TxnContext<'_>,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
    ) -> QuillSQLResult<()> {
        self.table
            .delete(txn, rid, prev_meta, prev_tuple, self.indexes())
    }

    pub fn update(
        &self,
        txn: &mut TxnContext<'_>,
        rid: RecordId,
        new_tuple: Tuple,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
    ) -> QuillSQLResult<RecordId> {
        self.table
            .update(txn, rid, new_tuple, prev_meta, prev_tuple, self.indexes())
    }

    pub fn prepare_row_for_write(
        &self,
        txn: &mut TxnContext<'_>,
        rid: RecordId,
        observed_meta: &TupleMeta,
    ) -> QuillSQLResult<Option<(TupleMeta, Tuple)>> {
        self.table.prepare_row_for_write(txn, rid, observed_meta)
    }

    pub fn index_scan(
        &self,
        name: &str,
        request: IndexScanRequest,
    ) -> QuillSQLResult<Box<dyn TupleStream>> {
        let handle = self
            .indexes()
            .iter()
            .find(|idx| idx.name() == name)
            .ok_or_else(|| {
                crate::error::QuillSQLError::Execution(format!("index {} not found", name))
            })?;
        handle.range_scan(self.table(), request)
    }
}

impl fmt::Debug for TableBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableBinding")
            .field("table", &self.table.table_ref())
            .field("index_count", &self.indexes.len())
            .finish()
    }
}

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

    fn full_scan(&self) -> QuillSQLResult<Box<dyn TupleStream>> {
        let iterator = TableIterator::new(self.heap.clone(), ..);
        Ok(Box::new(HeapTableStream { iterator }))
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
        _prev_meta: TupleMeta,
        prev_tuple: Tuple,
        indexes: &[Arc<dyn IndexHandle>],
    ) -> QuillSQLResult<()> {
        txn.ensure_writable(self.table_ref(), "DELETE")?;
        let mvcc = MvccHeap::new(self.heap.clone());
        let prev_meta = mvcc.mark_deleted(rid, txn.txn_id(), txn.command_id())?;

        let mut index_links = Vec::new();
        for handle in indexes {
            if let Ok(key_tuple) = prev_tuple.project_with_schema(handle.key_schema()) {
                let index = handle.index();
                index.delete(&key_tuple)?;
                index_links.push((index, key_tuple));
            }
        }

        txn.transaction_mut().push_delete_undo(
            self.heap.clone(),
            rid,
            prev_meta,
            prev_tuple,
            index_links,
        );
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

        let mut old_keys = Vec::new();
        for handle in indexes {
            if let Ok(old_key_tuple) = prev_tuple.project_with_schema(handle.key_schema()) {
                let index = handle.index();
                index.delete(&old_key_tuple)?;
                old_keys.push((index, old_key_tuple));
            }
        }

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
            old_keys,
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

struct HeapTableStream {
    iterator: TableIterator,
}

impl TupleStream for HeapTableStream {
    fn next(&mut self) -> QuillSQLResult<Option<(RecordId, TupleMeta, Tuple)>> {
        self.iterator.next()
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

    fn range_scan(
        &self,
        table: Arc<dyn TableHandle>,
        request: IndexScanRequest,
    ) -> QuillSQLResult<Box<dyn TupleStream>> {
        let iterator = TreeIndexIterator::new(self.index.clone(), (request.start, request.end));
        Ok(Box::new(BTreeIndexStream { iterator, table }))
    }
}

struct BTreeIndexStream {
    iterator: TreeIndexIterator,
    table: Arc<dyn TableHandle>,
}

impl TupleStream for BTreeIndexStream {
    fn next(&mut self) -> QuillSQLResult<Option<(RecordId, TupleMeta, Tuple)>> {
        loop {
            let Some(rid) = self.iterator.next()? else {
                return Ok(None);
            };
            if let Ok((meta, tuple)) = self.table.table_heap().full_tuple(rid) {
                return Ok(Some((rid, meta, tuple)));
            }
        }
    }
}

impl StorageEngine for DefaultStorageEngine {
    fn table(&self, catalog: &Catalog, table: &TableReference) -> QuillSQLResult<TableBinding> {
        let heap = catalog.table_heap(table)?;
        let handle: Arc<dyn TableHandle> = Arc::new(HeapTableHandle::new(table.clone(), heap));
        let indexes = catalog.table_indexes(table)?;
        let index_handles = indexes
            .into_iter()
            .map(|(name, index)| {
                Arc::new(BTreeIndexHandle::new(name, index)) as Arc<dyn IndexHandle>
            })
            .collect();
        Ok(TableBinding::new(handle, index_handles))
    }
}
