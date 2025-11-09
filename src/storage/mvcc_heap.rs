use crate::error::QuillSQLResult;
use crate::storage::page::{RecordId, TupleMeta};
use crate::storage::table_heap::TableHeap;
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
        let rid = self.heap.insert_tuple(&meta, tuple)?;
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
        let mut updated_meta = current_meta;
        updated_meta.mark_deleted(txn_id, cid);
        updated_meta.set_next_version(Some(new_rid));
        self.heap.update_tuple_meta(updated_meta, current_rid)?;
        Ok((new_rid, prev_meta))
    }

    pub fn mark_deleted(
        &self,
        rid: RecordId,
        txn_id: TransactionId,
        cid: CommandId,
    ) -> QuillSQLResult<TupleMeta> {
        let (mut current_meta, _) = self.heap.full_tuple(rid)?;
        if current_meta.is_deleted {
            return Ok(current_meta);
        }
        let prev_meta = current_meta;
        current_meta.mark_deleted(txn_id, cid);
        self.heap.update_tuple_meta(current_meta, rid)?;
        Ok(prev_meta)
    }

    pub fn full_tuple(&self, rid: RecordId) -> QuillSQLResult<(TupleMeta, Tuple)> {
        self.heap.full_tuple(rid)
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
