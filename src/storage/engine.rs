use std::sync::Arc;

use crate::{
    catalog::Catalog,
    error::QuillSQLResult,
    storage::{index::btree_index::BPlusTreeIndex, table_heap::TableHeap, tuple::Tuple},
    transaction::{CommandId, TransactionId},
    utils::table_ref::TableReference,
};

pub trait StorageEngine: Send + Sync {
    fn table_heap(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Arc<TableHeap>>;

    fn table_indexes(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Vec<Arc<BPlusTreeIndex>>>;

    fn mvcc_insert(
        &self,
        catalog: &Catalog,
        table: &TableReference,
        tuple: &Tuple,
        txn_id: TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<(Arc<TableHeap>, crate::storage::page::RecordId)>;

    fn mvcc_update(
        &self,
        heap: &Arc<TableHeap>,
        rid: crate::storage::page::RecordId,
        new_tuple: Tuple,
        txn_id: TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<(
        crate::storage::page::RecordId,
        crate::storage::page::TupleMeta,
    )>;

    fn mvcc_delete(
        &self,
        heap: &Arc<TableHeap>,
        rid: crate::storage::page::RecordId,
        txn_id: TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<crate::storage::page::TupleMeta>;
}

#[derive(Default)]
pub struct DefaultStorageEngine;

impl StorageEngine for DefaultStorageEngine {
    fn table_heap(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Arc<TableHeap>> {
        catalog.table_heap(table)
    }

    fn table_indexes(
        &self,
        catalog: &Catalog,
        table: &TableReference,
    ) -> QuillSQLResult<Vec<Arc<BPlusTreeIndex>>> {
        catalog.table_indexes(table)
    }

    fn mvcc_insert(
        &self,
        catalog: &Catalog,
        table: &TableReference,
        tuple: &Tuple,
        txn_id: crate::transaction::TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<(Arc<TableHeap>, crate::storage::page::RecordId)> {
        let heap = catalog.table_heap(table)?;
        let (rid, _) = heap.mvcc_insert_version(tuple, txn_id, cid, None)?;
        Ok((heap, rid))
    }

    fn mvcc_update(
        &self,
        heap: &Arc<TableHeap>,
        rid: crate::storage::page::RecordId,
        new_tuple: Tuple,
        txn_id: crate::transaction::TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<(
        crate::storage::page::RecordId,
        crate::storage::page::TupleMeta,
    )> {
        heap.mvcc_update(rid, new_tuple, txn_id, cid)
    }

    fn mvcc_delete(
        &self,
        heap: &Arc<TableHeap>,
        rid: crate::storage::page::RecordId,
        txn_id: crate::transaction::TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<crate::storage::page::TupleMeta> {
        heap.mvcc_mark_deleted(rid, txn_id, cid)
    }
}
