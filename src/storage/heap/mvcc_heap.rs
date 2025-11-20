use crate::error::QuillSQLResult;
use crate::recovery::wal_record::WalRecordPayload;
use crate::recovery::Lsn;
use crate::storage::codec::TupleCodec;
use crate::storage::heap::table_heap::TableHeap;
use crate::storage::heap::wal_codec::{
    HeapDeletePayload, HeapInsertPayload, HeapRecordPayload, RelationIdent, TupleMetaRepr,
};
use crate::storage::page::{RecordId, TupleMeta};
use crate::storage::tuple::Tuple;
use crate::transaction::{CommandId, TransactionId};
use std::sync::Arc;

pub struct MvccHeap {
    heap: Arc<TableHeap>,
}

impl MvccHeap {
    pub fn new(heap: Arc<TableHeap>) -> Self {
        Self { heap }
    }

    pub fn heap(&self) -> Arc<TableHeap> {
        self.heap.clone()
    }

    pub fn insert(
        &self,
        tuple: &Tuple,
        txn_id: TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<(RecordId, TupleMeta)> {
        self.insert_version(tuple, txn_id, cid, None)
    }

    pub fn insert_version(
        &self,
        tuple: &Tuple,
        txn_id: TransactionId,
        cid: CommandId,
        prev_version: Option<RecordId>,
    ) -> QuillSQLResult<(RecordId, TupleMeta)> {
        let mut meta = TupleMeta::new(txn_id, cid);
        meta.set_prev_version(prev_version);
        let rid = self
            .heap
            .insert_tuple_with(&meta, tuple, |rid, meta_ref, tuple_ref| {
                self.log_insert(rid, meta_ref, tuple_ref)
            })?;
        Ok((rid, meta))
    }

    pub fn update(
        &self,
        current_rid: RecordId,
        new_tuple: Tuple,
        txn_id: TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<(RecordId, TupleMeta)> {
        let (current_meta, _) = self.heap.full_tuple(current_rid)?;
        if current_meta.is_deleted && current_meta.next_version.is_some() {
            return Err(crate::error::QuillSQLError::Execution(format!(
                "tuple {} has already been updated",
                current_rid
            )));
        }

        let prev_meta = current_meta;
        let (new_rid, mut new_meta) =
            self.insert_version(&new_tuple, txn_id, cid, Some(current_rid))?;
        new_meta.set_prev_version(Some(current_rid));

        self.mark_deleted_version(current_rid, txn_id, cid, Some(new_rid))?;
        Ok((new_rid, prev_meta))
    }

    pub fn mark_deleted(
        &self,
        rid: RecordId,
        txn_id: TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<TupleMeta> {
        self.mark_deleted_version(rid, txn_id, cid, None)
    }

    pub fn full_tuple(&self, rid: RecordId) -> QuillSQLResult<(TupleMeta, Tuple)> {
        self.heap.full_tuple(rid)
    }

    fn mark_deleted_version(
        &self,
        rid: RecordId,
        txn_id: TransactionId,
        cid: CommandId,
        next_version: Option<RecordId>,
    ) -> QuillSQLResult<TupleMeta> {
        let (current_meta, tuple) = self.heap.full_tuple(rid)?;
        if current_meta.is_deleted {
            return Ok(current_meta);
        }
        let prev_meta = current_meta;
        let mut new_meta = current_meta;
        if let Some(next) = next_version {
            new_meta.set_next_version(Some(next));
        }
        new_meta.mark_deleted(txn_id, cid);
        let wal_lsn = self.log_delete(rid, txn_id, &new_meta, &prev_meta, &tuple)?;
        self.heap
            .write_tuple_meta_with_lsn(rid, new_meta, wal_lsn)?;
        Ok(prev_meta)
    }

    fn log_insert(
        &self,
        rid: RecordId,
        meta: &TupleMeta,
        tuple: &Tuple,
    ) -> QuillSQLResult<Option<Lsn>> {
        let payload = HeapRecordPayload::Insert(HeapInsertPayload {
            relation: self.relation_ident(),
            page_id: rid.page_id,
            slot_id: rid.slot_num as u16,
            op_txn_id: meta.insert_txn_id,
            tuple_meta: TupleMetaRepr::from(*meta),
            tuple_data: TupleCodec::encode(tuple),
        });
        self.append_heap_record(payload)
    }

    fn log_delete(
        &self,
        rid: RecordId,
        txn_id: TransactionId,
        new_meta: &TupleMeta,
        old_meta: &TupleMeta,
        tuple: &Tuple,
    ) -> QuillSQLResult<Option<Lsn>> {
        let payload = HeapRecordPayload::Delete(HeapDeletePayload {
            relation: self.relation_ident(),
            page_id: rid.page_id,
            slot_id: rid.slot_num as u16,
            op_txn_id: txn_id,
            new_tuple_meta: TupleMetaRepr::from(*new_meta),
            old_tuple_meta: TupleMetaRepr::from(*old_meta),
            old_tuple_data: TupleCodec::encode(tuple),
        });
        self.append_heap_record(payload)
    }

    fn append_heap_record(&self, payload: HeapRecordPayload) -> QuillSQLResult<Option<Lsn>> {
        if let Some(wal) = self.heap.buffer_pool.wal_manager() {
            let res = wal.append_record_with(|_| WalRecordPayload::Heap(payload.clone()))?;
            Ok(Some(res.end_lsn))
        } else {
            Ok(None)
        }
    }

    fn relation_ident(&self) -> RelationIdent {
        self.heap.relation_ident()
    }
}

#[cfg(test)]
mod tests {
    use super::MvccHeap;
    use crate::buffer::BufferManager;
    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::{
        disk_manager::DiskManager, disk_scheduler::DiskScheduler, table_heap::TableHeap,
        tuple::Tuple,
    };
    use crate::utils::scalar::ScalarValue;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn make_heap() -> (Arc<TableHeap>, Arc<Schema>) {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().join("mvcc_test.db");
        let schema = Arc::new(Schema::new(vec![
            Column::new("id", DataType::Int32, false),
            Column::new("val", DataType::Int32, false),
        ]));
        let disk_manager = Arc::new(DiskManager::try_new(temp_path).unwrap());
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager));
        let buffer_pool = Arc::new(BufferManager::new(64, disk_scheduler));
        let heap = Arc::new(TableHeap::try_new(schema.clone(), buffer_pool).unwrap());
        (heap, schema)
    }

    #[test]
    fn mvcc_insert_and_update_chain() {
        let (heap, schema) = make_heap();
        let mvcc = MvccHeap::new(heap.clone());
        let base = Tuple::new(
            schema.clone(),
            vec![ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(10))],
        );
        let (rid, _) = mvcc.insert(&base, 1, 0).unwrap();

        let updated = Tuple::new(
            schema.clone(),
            vec![ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(20))],
        );
        let (new_rid, _) = mvcc.update(rid, updated, 2, 0).unwrap();

        let old_meta = heap.tuple_meta(rid).unwrap();
        assert!(old_meta.is_deleted);
        assert_eq!(old_meta.next_version, Some(new_rid));

        let new_meta = heap.tuple_meta(new_rid).unwrap();
        assert_eq!(new_meta.prev_version, Some(rid));
        assert!(!new_meta.is_deleted);
    }

    #[test]
    fn mvcc_mark_deleted_returns_previous_meta() {
        let (heap, schema) = make_heap();
        let mvcc = MvccHeap::new(heap.clone());
        let base = Tuple::new(
            schema.clone(),
            vec![ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(10))],
        );

        let (rid, meta) = mvcc.insert(&base, 1, 0).unwrap();
        let prev_meta = mvcc.mark_deleted(rid, 1, 1).unwrap();
        assert_eq!(prev_meta.insert_txn_id, meta.insert_txn_id);
        assert!(heap.tuple_meta(rid).unwrap().is_deleted);
    }
}
