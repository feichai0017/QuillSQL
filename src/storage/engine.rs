use std::fmt;
use std::ops::Bound;
use std::sync::Arc;

use crate::transaction::TransactionId;
use crate::{
    catalog::{Catalog, IndexBackend, SchemaRef, TableBackend},
    error::{QuillSQLError, QuillSQLResult},
    storage::{
        holt::{HoltIndexHandle, HoltStore, HoltTableHandle},
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
    fn full_scan(&self) -> QuillSQLResult<Box<dyn TupleStream>>;
    fn full_tuple(&self, rid: RecordId) -> QuillSQLResult<(TupleMeta, Tuple)>;

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

    fn undo_insert(&self, rid: RecordId, txn_id: TransactionId) -> QuillSQLResult<()>;

    fn undo_delete(
        &self,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
    ) -> QuillSQLResult<()>;

    fn undo_insert_payload(
        &self,
        rid: RecordId,
        txn_id: TransactionId,
    ) -> QuillSQLResult<Option<crate::storage::heap::wal_codec::HeapRecordPayload>>;

    fn undo_delete_payload(
        &self,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: &Tuple,
        txn_id: TransactionId,
    ) -> QuillSQLResult<Option<crate::storage::heap::wal_codec::HeapRecordPayload>>;
}

pub trait IndexHandle: Send + Sync {
    fn name(&self) -> &str;
    fn key_schema(&self) -> SchemaRef;
    fn holt_index_id(&self) -> Option<u64> {
        None
    }
    fn insert(&self, key: &Tuple, rid: RecordId, txn_id: TransactionId) -> QuillSQLResult<()>;
    fn delete(&self, key: &Tuple, rid: RecordId, txn_id: TransactionId) -> QuillSQLResult<()>;
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
pub struct PageStoreEngine;

#[derive(Default)]
pub struct DefaultStorageEngine {
    holt_store: Option<Arc<HoltStore>>,
}

impl DefaultStorageEngine {
    pub fn new(holt_store: Option<Arc<HoltStore>>) -> Self {
        Self { holt_store }
    }
}

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

    fn full_scan(&self) -> QuillSQLResult<Box<dyn TupleStream>> {
        let iterator = TableIterator::new(self.heap.clone(), ..);
        Ok(Box::new(HeapTableStream { iterator }))
    }

    fn full_tuple(&self, rid: RecordId) -> QuillSQLResult<(TupleMeta, Tuple)> {
        self.heap.full_tuple(rid)
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
                handle.insert(&key_tuple, rid, txn.txn_id())?;
                index_links.push((handle.clone(), key_tuple, rid));
            }
        }

        txn.transaction_mut()
            .push_insert_undo(Arc::new(self.clone()), rid, index_links);
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
                handle.delete(&key_tuple, rid, txn.txn_id())?;
                index_links.push((handle.clone(), key_tuple, rid));
            }
        }

        txn.transaction_mut().push_delete_undo(
            Arc::new(self.clone()),
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
                handle.delete(&old_key_tuple, rid, txn.txn_id())?;
                old_keys.push((handle.clone(), old_key_tuple, rid));
            }
        }

        let mut new_keys = Vec::new();
        for handle in indexes {
            if let Ok(new_key_tuple) = new_tuple.project_with_schema(handle.key_schema()) {
                handle.insert(&new_key_tuple, new_rid, txn.txn_id())?;
                new_keys.push((handle.clone(), new_key_tuple, new_rid));
            }
        }

        txn.transaction_mut().push_update_undo(
            Arc::new(self.clone()),
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

    fn undo_insert(&self, rid: RecordId, txn_id: TransactionId) -> QuillSQLResult<()> {
        self.heap.recover_delete_tuple(rid, txn_id, 0)
    }

    fn undo_delete(
        &self,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
    ) -> QuillSQLResult<()> {
        self.heap.recover_restore_tuple(rid, prev_meta, &prev_tuple)
    }

    fn undo_insert_payload(
        &self,
        rid: RecordId,
        txn_id: TransactionId,
    ) -> QuillSQLResult<Option<crate::storage::heap::wal_codec::HeapRecordPayload>> {
        use crate::storage::codec::TupleCodec;
        use crate::storage::heap::wal_codec::{
            HeapDeletePayload, HeapRecordPayload, TupleMetaRepr,
        };
        let (meta, tuple) = self.heap.full_tuple(rid)?;
        let mut deleted_meta = meta;
        deleted_meta.mark_deleted(txn_id, 0);
        Ok(Some(HeapRecordPayload::Delete(HeapDeletePayload {
            relation: self.heap.relation_ident(),
            page_id: rid.page_id,
            slot_id: rid.slot_num as u16,
            op_txn_id: meta.insert_txn_id,
            new_tuple_meta: TupleMetaRepr::from(deleted_meta),
            old_tuple_meta: TupleMetaRepr::from(meta),
            old_tuple_data: TupleCodec::encode(&tuple),
        })))
    }

    fn undo_delete_payload(
        &self,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: &Tuple,
        txn_id: TransactionId,
    ) -> QuillSQLResult<Option<crate::storage::heap::wal_codec::HeapRecordPayload>> {
        use crate::storage::codec::TupleCodec;
        use crate::storage::heap::wal_codec::{
            HeapInsertPayload, HeapRecordPayload, TupleMetaRepr,
        };
        Ok(Some(HeapRecordPayload::Insert(HeapInsertPayload {
            relation: self.heap.relation_ident(),
            page_id: rid.page_id,
            slot_id: rid.slot_num as u16,
            op_txn_id: txn_id,
            tuple_meta: TupleMetaRepr::from(prev_meta),
            tuple_data: TupleCodec::encode(prev_tuple),
        })))
    }
}

impl Clone for HeapTableHandle {
    fn clone(&self) -> Self {
        Self {
            table_ref: self.table_ref.clone(),
            heap: self.heap.clone(),
        }
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

    fn insert(&self, key: &Tuple, rid: RecordId, txn_id: TransactionId) -> QuillSQLResult<()> {
        self.index.insert_with_txn(key, rid, txn_id)
    }

    fn delete(&self, key: &Tuple, _rid: RecordId, txn_id: TransactionId) -> QuillSQLResult<()> {
        self.index.delete_with_txn(key, txn_id)
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
            if let Ok((meta, tuple)) = self.table.full_tuple(rid) {
                return Ok(Some((rid, meta, tuple)));
            }
        }
    }
}

impl StorageEngine for PageStoreEngine {
    fn table(&self, catalog: &Catalog, table: &TableReference) -> QuillSQLResult<TableBinding> {
        match catalog.table_backend(table)? {
            TableBackend::Page => {}
            TableBackend::Holt { .. } => {
                return Err(QuillSQLError::Storage(format!(
                    "table {} is not backed by PageStoreEngine",
                    table
                )));
            }
        }
        let heap = catalog.table_heap(table)?;
        let handle: Arc<dyn TableHandle> = Arc::new(HeapTableHandle::new(table.clone(), heap));
        let indexes = catalog.table_indexes(table)?;
        let index_handles = indexes
            .into_iter()
            .map(|index| match index.backend {
                IndexBackend::BTree => {
                    let btree = index.btree.ok_or_else(|| {
                        QuillSQLError::Storage(format!(
                            "BTree index {} has no B+Tree descriptor",
                            index.name
                        ))
                    })?;
                    Ok(Arc::new(BTreeIndexHandle::new(index.name, btree)) as Arc<dyn IndexHandle>)
                }
                IndexBackend::Holt { .. } => Err(QuillSQLError::Storage(format!(
                    "index {} is not backed by PageStoreEngine",
                    index.name
                ))),
            })
            .collect::<QuillSQLResult<Vec<_>>>()?;
        Ok(TableBinding::new(handle, index_handles))
    }
}

impl StorageEngine for DefaultStorageEngine {
    fn table(&self, catalog: &Catalog, table: &TableReference) -> QuillSQLResult<TableBinding> {
        let handle: Arc<dyn TableHandle> = match catalog.table_backend(table)? {
            TableBackend::Page => {
                let heap = catalog.table_heap(table)?;
                Arc::new(HeapTableHandle::new(table.clone(), heap))
            }
            TableBackend::Holt { table_id } => {
                let holt = self.holt_store.as_ref().ok_or_else(|| {
                    QuillSQLError::Storage("Holt store is not configured".to_string())
                })?;
                let schema = catalog.table_schema(table)?;
                Arc::new(HoltTableHandle::new(
                    table.clone(),
                    schema,
                    table_id,
                    holt.clone(),
                ))
            }
        };
        let indexes = catalog.table_indexes(table)?;
        let index_handles = indexes
            .into_iter()
            .map(|index| match index.backend {
                IndexBackend::BTree => {
                    let btree = index.btree.ok_or_else(|| {
                        QuillSQLError::Storage(format!(
                            "BTree index {} has no B+Tree descriptor",
                            index.name
                        ))
                    })?;
                    Ok(Arc::new(BTreeIndexHandle::new(index.name, btree)) as Arc<dyn IndexHandle>)
                }
                IndexBackend::Holt { index_id } => {
                    let holt = self.holt_store.as_ref().ok_or_else(|| {
                        QuillSQLError::Storage("Holt store is not configured".to_string())
                    })?;
                    Ok(Arc::new(HoltIndexHandle::new(
                        index.name,
                        index.key_schema,
                        index_id,
                        holt.clone(),
                    )) as Arc<dyn IndexHandle>)
                }
            })
            .collect::<QuillSQLResult<Vec<_>>>()?;
        Ok(TableBinding::new(handle, index_handles))
    }
}
