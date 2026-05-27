use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::{cmp::Ordering, ops::Bound};

use crate::catalog::{
    Column, DataType, Schema, SchemaRef, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::codec::TupleCodec;
use crate::storage::engine::{IndexHandle, IndexScanRequest, TableHandle, TupleStream};
use crate::storage::page::{RecordId, TupleMeta};
use crate::storage::tuple::Tuple;
use crate::transaction::{TransactionId, TransactionStatus, TxnContext};
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;

const CATALOG_TREE: &str = "catalog";
const TXN_TREE: &str = "txn";
const NEXT_TABLE_ID: &[u8] = b"next/table-id";
const NEXT_INDEX_ID: &[u8] = b"next/index-id";
const NEXT_RID: &[u8] = b"next/rid";
const TABLE_PREFIX: &str = "table";
const INDEX_PREFIX: &str = "index";
const TABLE_SCHEMA_PREFIX: &str = "table-schema";
const INDEX_SCHEMA_PREFIX: &str = "index-schema";

#[derive(Clone)]
pub struct HoltStore {
    db: Arc<holt::DB>,
    next_table_id: Arc<AtomicU64>,
    next_index_id: Arc<AtomicU64>,
    next_rid: Arc<AtomicU64>,
    dir: PathBuf,
}

impl std::fmt::Debug for HoltStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HoltStore")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl HoltStore {
    pub fn open(dir: impl AsRef<Path>) -> QuillSQLResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        let db = Arc::new(holt::DB::open(holt::TreeConfig::new(&dir)).map_err(map_holt_err)?);
        db.open_or_create_tree(CATALOG_TREE).map_err(map_holt_err)?;
        db.open_or_create_tree(TXN_TREE).map_err(map_holt_err)?;

        let next_table_id = read_counter(&db, NEXT_TABLE_ID, 1)?;
        let next_index_id = read_counter(&db, NEXT_INDEX_ID, 1)?;
        let next_rid = read_counter(&db, NEXT_RID, 1)?;

        Ok(Self {
            db,
            next_table_id: Arc::new(AtomicU64::new(next_table_id)),
            next_index_id: Arc::new(AtomicU64::new(next_index_id)),
            next_rid: Arc::new(AtomicU64::new(next_rid)),
            dir,
        })
    }

    pub fn db(&self) -> Arc<holt::DB> {
        self.db.clone()
    }

    pub fn table_tree_name(table_id: u64) -> String {
        format!("table/{table_id}")
    }

    pub fn index_tree_name(index_id: u64) -> String {
        format!("index/{index_id}")
    }

    pub fn canonical_table_name(table_ref: &TableReference) -> String {
        let catalog = table_ref.catalog().unwrap_or(DEFAULT_CATALOG_NAME);
        let schema = table_ref.schema().unwrap_or(DEFAULT_SCHEMA_NAME);
        format!("{catalog}.{schema}.{}", table_ref.table())
    }

    pub fn table_descriptor(&self, table_ref: &TableReference) -> QuillSQLResult<Option<u64>> {
        let key = table_key(&Self::canonical_table_name(table_ref));
        let catalog = self.db.open_tree(CATALOG_TREE).map_err(map_holt_err)?;
        catalog
            .get(&key)
            .map_err(map_holt_err)?
            .map(|raw| decode_u64(&raw))
            .transpose()
    }

    pub fn index_descriptor(
        &self,
        table_ref: &TableReference,
        index_name: &str,
    ) -> QuillSQLResult<Option<u64>> {
        let table = Self::canonical_table_name(table_ref);
        let key = index_key(&table, index_name);
        let catalog = self.db.open_tree(CATALOG_TREE).map_err(map_holt_err)?;
        catalog
            .get(&key)
            .map_err(map_holt_err)?
            .map(|raw| decode_u64(&raw))
            .transpose()
    }

    pub fn create_table_descriptor(
        &self,
        table_ref: &TableReference,
        schema: &Schema,
    ) -> QuillSQLResult<u64> {
        let table = Self::canonical_table_name(table_ref);
        let key = table_key(&table);
        let schema_key = table_schema_key(&table);
        let schema_value = encode_schema(schema)?;
        let table_id = self.next_table_id.fetch_add(1, AtomicOrdering::SeqCst);
        let next_id = table_id
            .checked_add(1)
            .ok_or_else(|| QuillSQLError::Storage("Holt table id overflow".to_string()))?;
        let tree_name = Self::table_tree_name(table_id);
        self.db
            .open_or_create_tree(&tree_name)
            .map_err(map_holt_err)?;
        self.db
            .atomic(|batch| {
                batch.put_if_absent(CATALOG_TREE, &key, &encode_u64(table_id));
                batch.put(CATALOG_TREE, &schema_key, &schema_value);
                batch.put(CATALOG_TREE, NEXT_TABLE_ID, &encode_u64(next_id));
            })
            .map_err(map_holt_err)
            .and_then(committed)?;
        Ok(table_id)
    }

    pub fn create_index_descriptor(
        &self,
        table_ref: &TableReference,
        index_name: &str,
        key_schema: &Schema,
    ) -> QuillSQLResult<u64> {
        let table = Self::canonical_table_name(table_ref);
        let key = index_key(&table, index_name);
        let schema_key = index_schema_key(&table, index_name);
        let schema_value = encode_schema(key_schema)?;
        let index_id = self.next_index_id.fetch_add(1, AtomicOrdering::SeqCst);
        let next_id = index_id
            .checked_add(1)
            .ok_or_else(|| QuillSQLError::Storage("Holt index id overflow".to_string()))?;
        let tree_name = Self::index_tree_name(index_id);
        self.db
            .open_or_create_tree(&tree_name)
            .map_err(map_holt_err)?;
        self.db
            .atomic(|batch| {
                batch.put_if_absent(CATALOG_TREE, &key, &encode_u64(index_id));
                batch.put(CATALOG_TREE, &schema_key, &schema_value);
                batch.put(CATALOG_TREE, NEXT_INDEX_ID, &encode_u64(next_id));
            })
            .map_err(map_holt_err)
            .and_then(committed)?;
        Ok(index_id)
    }

    pub fn allocate_rid(&self) -> QuillSQLResult<RecordId> {
        let (rid, next) = self.reserve_rid()?;
        self.db
            .atomic(|batch| {
                batch.put(CATALOG_TREE, NEXT_RID, &encode_u64(next));
            })
            .map_err(map_holt_err)
            .and_then(committed)?;
        Ok(rid)
    }

    pub fn reserve_rid(&self) -> QuillSQLResult<(RecordId, u64)> {
        let raw = self.next_rid.fetch_add(1, AtomicOrdering::SeqCst);
        let next = raw
            .checked_add(1)
            .ok_or_else(|| QuillSQLError::Storage("Holt RID overflow".to_string()))?;
        let page_id = (raw >> 32) as u32;
        let slot_num = raw as u32;
        Ok((RecordId::new(page_id, slot_num), next))
    }

    pub fn put_txn_status(
        &self,
        txn_id: TransactionId,
        status: TransactionStatus,
    ) -> QuillSQLResult<()> {
        let tree = self.db.open_tree(TXN_TREE).map_err(map_holt_err)?;
        tree.put(&encode_u64(txn_id), &[encode_txn_status(status)])
            .map_err(map_holt_err)
    }

    pub fn txn_status(&self, txn_id: TransactionId) -> QuillSQLResult<Option<TransactionStatus>> {
        let tree = self.db.open_tree(TXN_TREE).map_err(map_holt_err)?;
        tree.get(&encode_u64(txn_id))
            .map_err(map_holt_err)?
            .map(|raw| decode_txn_status(raw.first().copied().unwrap_or(0)))
            .transpose()
    }

    pub fn recover_txn_statuses(&self) -> QuillSQLResult<Vec<(TransactionId, TransactionStatus)>> {
        let tree = self.db.open_tree(TXN_TREE).map_err(map_holt_err)?;
        let mut statuses = Vec::new();
        for entry in tree.range().into_iter() {
            let entry = entry.map_err(map_holt_err)?;
            let holt::RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let txn_id = decode_u64(&key)?;
            let mut status = decode_txn_status(value.first().copied().unwrap_or(0))?;
            if matches!(
                status,
                TransactionStatus::InProgress | TransactionStatus::Unknown
            ) {
                status = TransactionStatus::Aborted;
                tree.put(&key, &[encode_txn_status(status)])
                    .map_err(map_holt_err)?;
            }
            statuses.push((txn_id, status));
        }
        Ok(statuses)
    }

    pub fn table_descriptors(&self) -> QuillSQLResult<Vec<HoltTableDescriptor>> {
        let catalog = self.db.open_tree(CATALOG_TREE).map_err(map_holt_err)?;
        let mut descriptors = Vec::new();
        for entry in catalog.scan(b"table/").into_iter() {
            let entry = entry.map_err(map_holt_err)?;
            let holt::RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let canonical = std::str::from_utf8(&key[b"table/".len()..])
                .map_err(|err| QuillSQLError::Storage(format!("invalid Holt table key: {err}")))?;
            let schema_key = table_schema_key(canonical);
            let Some(schema_raw) = catalog.get(&schema_key).map_err(map_holt_err)? else {
                continue;
            };
            let table_ref = parse_canonical_table_name(canonical)?;
            let schema = Arc::new(decode_schema(&schema_raw)?);
            let table_id = decode_u64(&value)?;
            descriptors.push(HoltTableDescriptor {
                table_name: table_ref.table().to_string(),
                table_ref,
                schema,
                table_id,
            });
        }
        Ok(descriptors)
    }

    pub fn index_descriptors(&self) -> QuillSQLResult<Vec<HoltIndexDescriptor>> {
        let catalog = self.db.open_tree(CATALOG_TREE).map_err(map_holt_err)?;
        let mut descriptors = Vec::new();
        for entry in catalog.scan(b"index/").into_iter() {
            let entry = entry.map_err(map_holt_err)?;
            let holt::RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let raw = std::str::from_utf8(&key[b"index/".len()..])
                .map_err(|err| QuillSQLError::Storage(format!("invalid Holt index key: {err}")))?;
            let Some((canonical, index_name)) = raw.rsplit_once('/') else {
                continue;
            };
            let schema_key = index_schema_key(canonical, index_name);
            let Some(schema_raw) = catalog.get(&schema_key).map_err(map_holt_err)? else {
                continue;
            };
            descriptors.push(HoltIndexDescriptor {
                table_ref: parse_canonical_table_name(canonical)?,
                index_name: index_name.to_string(),
                key_schema: Arc::new(decode_schema(&schema_raw)?),
                index_id: decode_u64(&value)?,
            });
        }
        Ok(descriptors)
    }
}

#[derive(Debug, Clone)]
pub struct HoltTableDescriptor {
    pub table_name: String,
    pub table_ref: TableReference,
    pub schema: SchemaRef,
    pub table_id: u64,
}

#[derive(Debug, Clone)]
pub struct HoltIndexDescriptor {
    pub table_ref: TableReference,
    pub index_name: String,
    pub key_schema: SchemaRef,
    pub index_id: u64,
}

#[derive(Clone)]
pub struct HoltTableHandle {
    table_ref: TableReference,
    schema: SchemaRef,
    table_id: u64,
    store: Arc<HoltStore>,
}

impl HoltTableHandle {
    pub fn new(
        table_ref: TableReference,
        schema: SchemaRef,
        table_id: u64,
        store: Arc<HoltStore>,
    ) -> Self {
        Self {
            table_ref,
            schema,
            table_id,
            store,
        }
    }

    fn tree_name(&self) -> String {
        HoltStore::table_tree_name(self.table_id)
    }

    fn row_value(&self, meta: TupleMeta, tuple: &Tuple) -> Vec<u8> {
        encode_row(meta, tuple)
    }

    fn put_txn_in_progress(&self, txn_id: TransactionId) -> QuillSQLResult<()> {
        self.store
            .put_txn_status(txn_id, TransactionStatus::InProgress)
    }
}

impl TableHandle for HoltTableHandle {
    fn table_ref(&self) -> &TableReference {
        &self.table_ref
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn full_scan(&self) -> QuillSQLResult<Box<dyn TupleStream>> {
        let tree = self
            .store
            .db
            .open_tree(&self.tree_name())
            .map_err(map_holt_err)?;
        Ok(Box::new(HoltTableStream {
            iter: tree.range().into_iter(),
            schema: self.schema.clone(),
        }))
    }

    fn full_tuple(&self, rid: RecordId) -> QuillSQLResult<(TupleMeta, Tuple)> {
        let tree = self
            .store
            .db
            .open_tree(&self.tree_name())
            .map_err(map_holt_err)?;
        let key = encode_rid(rid);
        let Some(value) = tree.get(&key).map_err(map_holt_err)? else {
            return Err(QuillSQLError::Storage(format!(
                "Holt tuple {} not found",
                rid
            )));
        };
        decode_row(&value, self.schema.clone())
    }

    fn insert(
        &self,
        txn: &mut TxnContext<'_>,
        tuple: &Tuple,
        indexes: &[Arc<dyn IndexHandle>],
    ) -> QuillSQLResult<()> {
        txn.ensure_writable(self.table_ref(), "INSERT")?;
        self.put_txn_in_progress(txn.txn_id())?;
        let (rid, next_rid) = self.store.reserve_rid()?;
        let meta = TupleMeta::new(txn.txn_id(), txn.command_id());
        let row_key = encode_rid(rid);
        let row_value = self.row_value(meta, tuple);
        let mut index_links = Vec::new();
        let mut index_puts = Vec::new();
        for handle in indexes {
            let Some(index_id) = handle.holt_index_id() else {
                return Err(QuillSQLError::Storage(format!(
                    "Holt table {} cannot maintain non-Holt index {}",
                    self.table_ref,
                    handle.name()
                )));
            };
            if let Ok(key_tuple) = tuple.project_with_schema(handle.key_schema()) {
                let encoded_key = encode_index_key(&key_tuple, rid)?;
                index_puts.push((index_id, encoded_key));
                index_links.push((handle.clone(), key_tuple, rid));
            }
        }
        let table_tree = self.tree_name();
        let rid_value = encode_rid(rid);
        self.store
            .db
            .atomic(|batch| {
                batch.put(CATALOG_TREE, NEXT_RID, &encode_u64(next_rid));
                batch.put(&table_tree, &row_key, &row_value);
                for (index_id, key) in &index_puts {
                    let index_tree = HoltStore::index_tree_name(*index_id);
                    batch.put(&index_tree, key, &rid_value);
                }
            })
            .map_err(map_holt_err)
            .and_then(committed)?;
        txn.transaction_mut()
            .push_insert_undo(Arc::new(self.clone()), rid, index_links);
        Ok(())
    }

    fn delete(
        &self,
        txn: &mut TxnContext<'_>,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
        indexes: &[Arc<dyn IndexHandle>],
    ) -> QuillSQLResult<()> {
        txn.ensure_writable(self.table_ref(), "DELETE")?;
        self.put_txn_in_progress(txn.txn_id())?;
        let mut deleted_meta = prev_meta;
        if deleted_meta.is_deleted {
            return Ok(());
        }
        deleted_meta.mark_deleted(txn.txn_id(), txn.command_id());
        let row_key = encode_rid(rid);
        let row_value = self.row_value(deleted_meta, &prev_tuple);
        let mut index_links = Vec::new();
        let mut index_deletes = Vec::new();
        for handle in indexes {
            let Some(index_id) = handle.holt_index_id() else {
                return Err(QuillSQLError::Storage(format!(
                    "Holt table {} cannot maintain non-Holt index {}",
                    self.table_ref,
                    handle.name()
                )));
            };
            if let Ok(key_tuple) = prev_tuple.project_with_schema(handle.key_schema()) {
                index_deletes.push((index_id, encode_index_key(&key_tuple, rid)?));
                index_links.push((handle.clone(), key_tuple, rid));
            }
        }
        let table_tree = self.tree_name();
        self.store
            .db
            .atomic(|batch| {
                batch.put(&table_tree, &row_key, &row_value);
                for (index_id, key) in &index_deletes {
                    let index_tree = HoltStore::index_tree_name(*index_id);
                    batch.delete(&index_tree, key);
                }
            })
            .map_err(map_holt_err)
            .and_then(committed)?;
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
        self.put_txn_in_progress(txn.txn_id())?;
        if prev_meta.is_deleted && prev_meta.next_version.is_some() {
            return Err(QuillSQLError::Execution(format!(
                "tuple {} has already been updated",
                rid
            )));
        }
        let (new_rid, next_rid) = self.store.reserve_rid()?;
        let mut old_meta = prev_meta;
        old_meta.set_next_version(Some(new_rid));
        old_meta.mark_deleted(txn.txn_id(), txn.command_id());
        let mut new_meta = TupleMeta::new(txn.txn_id(), txn.command_id());
        new_meta.set_prev_version(Some(rid));

        let mut old_links = Vec::new();
        let mut new_links = Vec::new();
        let mut old_index_deletes = Vec::new();
        let mut new_index_puts = Vec::new();
        for handle in indexes {
            let Some(index_id) = handle.holt_index_id() else {
                return Err(QuillSQLError::Storage(format!(
                    "Holt table {} cannot maintain non-Holt index {}",
                    self.table_ref,
                    handle.name()
                )));
            };
            if let Ok(old_key) = prev_tuple.project_with_schema(handle.key_schema()) {
                old_index_deletes.push((index_id, encode_index_key(&old_key, rid)?));
                old_links.push((handle.clone(), old_key, rid));
            }
            if let Ok(new_key) = new_tuple.project_with_schema(handle.key_schema()) {
                new_index_puts.push((index_id, encode_index_key(&new_key, new_rid)?));
                new_links.push((handle.clone(), new_key, new_rid));
            }
        }

        let table_tree = self.tree_name();
        let old_row_key = encode_rid(rid);
        let new_row_key = encode_rid(new_rid);
        let old_row_value = self.row_value(old_meta, &prev_tuple);
        let new_row_value = self.row_value(new_meta, &new_tuple);
        let new_rid_value = encode_rid(new_rid);
        self.store
            .db
            .atomic(|batch| {
                batch.put(CATALOG_TREE, NEXT_RID, &encode_u64(next_rid));
                batch.put(&table_tree, &old_row_key, &old_row_value);
                batch.put(&table_tree, &new_row_key, &new_row_value);
                for (index_id, key) in &old_index_deletes {
                    let index_tree = HoltStore::index_tree_name(*index_id);
                    batch.delete(&index_tree, key);
                }
                for (index_id, key) in &new_index_puts {
                    let index_tree = HoltStore::index_tree_name(*index_id);
                    batch.put(&index_tree, key, &new_rid_value);
                }
            })
            .map_err(map_holt_err)
            .and_then(committed)?;
        txn.transaction_mut().push_update_undo(
            Arc::new(self.clone()),
            rid,
            new_rid,
            prev_meta,
            prev_tuple,
            new_links,
            old_links,
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
        let (current_meta, current_tuple) = self.full_tuple(rid)?;
        if !txn.is_visible(&current_meta) {
            txn.unlock_row(self.table_ref(), rid);
            return Ok(None);
        }
        Ok(Some((current_meta, current_tuple)))
    }

    fn undo_insert(&self, rid: RecordId, txn_id: TransactionId) -> QuillSQLResult<()> {
        self.store
            .put_txn_status(txn_id, TransactionStatus::InProgress)?;
        let (mut meta, tuple) = self.full_tuple(rid)?;
        meta.mark_deleted(txn_id, 0);
        let tree = self
            .store
            .db
            .open_tree(&self.tree_name())
            .map_err(map_holt_err)?;
        tree.put(&encode_rid(rid), &self.row_value(meta, &tuple))
            .map_err(map_holt_err)
    }

    fn undo_delete(
        &self,
        rid: RecordId,
        prev_meta: TupleMeta,
        prev_tuple: Tuple,
    ) -> QuillSQLResult<()> {
        let tree = self
            .store
            .db
            .open_tree(&self.tree_name())
            .map_err(map_holt_err)?;
        tree.put(&encode_rid(rid), &self.row_value(prev_meta, &prev_tuple))
            .map_err(map_holt_err)
    }

    fn undo_insert_payload(
        &self,
        _rid: RecordId,
        _txn_id: TransactionId,
    ) -> QuillSQLResult<Option<crate::storage::heap::wal_codec::HeapRecordPayload>> {
        Ok(None)
    }

    fn undo_delete_payload(
        &self,
        _rid: RecordId,
        _prev_meta: TupleMeta,
        _prev_tuple: &Tuple,
        _txn_id: TransactionId,
    ) -> QuillSQLResult<Option<crate::storage::heap::wal_codec::HeapRecordPayload>> {
        Ok(None)
    }
}

pub struct HoltIndexHandle {
    name: String,
    key_schema: SchemaRef,
    index_id: u64,
    store: Arc<HoltStore>,
}

impl HoltIndexHandle {
    pub fn new(name: String, key_schema: SchemaRef, index_id: u64, store: Arc<HoltStore>) -> Self {
        Self {
            name,
            key_schema,
            index_id,
            store,
        }
    }

    fn tree_name(&self) -> String {
        HoltStore::index_tree_name(self.index_id)
    }
}

impl IndexHandle for HoltIndexHandle {
    fn name(&self) -> &str {
        &self.name
    }

    fn key_schema(&self) -> SchemaRef {
        self.key_schema.clone()
    }

    fn holt_index_id(&self) -> Option<u64> {
        Some(self.index_id)
    }

    fn insert(&self, key: &Tuple, rid: RecordId, txn_id: TransactionId) -> QuillSQLResult<()> {
        self.store
            .put_txn_status(txn_id, TransactionStatus::InProgress)?;
        let tree = self
            .store
            .db
            .open_tree(&self.tree_name())
            .map_err(map_holt_err)?;
        tree.put(&encode_index_key(key, rid)?, &encode_rid(rid))
            .map_err(map_holt_err)
    }

    fn delete(&self, key: &Tuple, rid: RecordId, txn_id: TransactionId) -> QuillSQLResult<()> {
        self.store
            .put_txn_status(txn_id, TransactionStatus::InProgress)?;
        let tree = self
            .store
            .db
            .open_tree(&self.tree_name())
            .map_err(map_holt_err)?;
        tree.delete(&encode_index_key(key, rid)?)
            .map_err(map_holt_err)?;
        Ok(())
    }

    fn range_scan(
        &self,
        table: Arc<dyn TableHandle>,
        request: IndexScanRequest,
    ) -> QuillSQLResult<Box<dyn TupleStream>> {
        let tree = self
            .store
            .db
            .open_tree(&self.tree_name())
            .map_err(map_holt_err)?;
        Ok(Box::new(HoltIndexStream {
            iter: tree.range().into_iter(),
            table,
            key_schema: self.key_schema.clone(),
            request,
        }))
    }
}

struct HoltTableStream {
    iter: holt::RangeIter,
    schema: SchemaRef,
}

impl TupleStream for HoltTableStream {
    fn next(&mut self) -> QuillSQLResult<Option<(RecordId, TupleMeta, Tuple)>> {
        for entry in self.iter.by_ref() {
            let entry = entry.map_err(map_holt_err)?;
            let holt::RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let rid = decode_rid(&key)?;
            let (meta, tuple) = decode_row(&value, self.schema.clone())?;
            return Ok(Some((rid, meta, tuple)));
        }
        Ok(None)
    }
}

struct HoltIndexStream {
    iter: holt::RangeIter,
    table: Arc<dyn TableHandle>,
    key_schema: SchemaRef,
    request: IndexScanRequest,
}

impl TupleStream for HoltIndexStream {
    fn next(&mut self) -> QuillSQLResult<Option<(RecordId, TupleMeta, Tuple)>> {
        for entry in self.iter.by_ref() {
            let entry = entry.map_err(map_holt_err)?;
            let holt::RangeEntry::Key { value, .. } = entry else {
                continue;
            };
            let rid = decode_rid(&value)?;
            let Ok((meta, tuple)) = self.table.full_tuple(rid) else {
                continue;
            };
            let key_tuple = tuple.project_with_schema(self.key_schema.clone())?;
            if tuple_within_bounds(&key_tuple, &self.request) {
                return Ok(Some((rid, meta, tuple)));
            }
        }
        Ok(None)
    }
}

fn encode_row(meta: TupleMeta, tuple: &Tuple) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(meta.insert_txn_id.to_be_bytes());
    out.extend(meta.insert_cid.to_be_bytes());
    out.extend(meta.delete_txn_id.to_be_bytes());
    out.extend(meta.delete_cid.to_be_bytes());
    out.push(u8::from(meta.is_deleted));
    encode_optional_rid(&mut out, meta.next_version);
    encode_optional_rid(&mut out, meta.prev_version);
    out.extend(TupleCodec::encode(tuple));
    out
}

fn decode_row(bytes: &[u8], schema: SchemaRef) -> QuillSQLResult<(TupleMeta, Tuple)> {
    let (meta, offset) = decode_tuple_meta(bytes)?;
    let (tuple, _) = TupleCodec::decode(&bytes[offset..], schema)?;
    Ok((meta, tuple))
}

fn encode_optional_rid(out: &mut Vec<u8>, rid: Option<RecordId>) {
    match rid {
        Some(rid) => {
            out.push(1);
            out.extend(encode_rid(rid));
        }
        None => out.push(0),
    }
}

fn decode_tuple_meta(bytes: &[u8]) -> QuillSQLResult<(TupleMeta, usize)> {
    let mut offset = 0usize;
    let insert_txn_id = read_u64(bytes, &mut offset)?;
    let insert_cid = read_u32(bytes, &mut offset)?;
    let delete_txn_id = read_u64(bytes, &mut offset)?;
    let delete_cid = read_u32(bytes, &mut offset)?;
    let is_deleted = read_u8(bytes, &mut offset)? != 0;
    let next_version = read_optional_rid(bytes, &mut offset)?;
    let prev_version = read_optional_rid(bytes, &mut offset)?;
    Ok((
        TupleMeta {
            insert_txn_id,
            insert_cid,
            delete_txn_id,
            delete_cid,
            is_deleted,
            next_version,
            prev_version,
        },
        offset,
    ))
}

fn read_optional_rid(bytes: &[u8], offset: &mut usize) -> QuillSQLResult<Option<RecordId>> {
    let present = read_u8(bytes, offset)?;
    if present == 0 {
        return Ok(None);
    }
    let rid_bytes = take(bytes, offset, 8)?;
    decode_rid(rid_bytes).map(Some)
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> QuillSQLResult<u8> {
    Ok(take(bytes, offset, 1)?[0])
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> QuillSQLResult<u32> {
    let raw = take(bytes, offset, 4)?;
    Ok(u32::from_be_bytes(raw.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> QuillSQLResult<u64> {
    let raw = take(bytes, offset, 8)?;
    Ok(u64::from_be_bytes(raw.try_into().unwrap()))
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> QuillSQLResult<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| QuillSQLError::Storage("Holt row offset overflow".to_string()))?;
    if end > bytes.len() {
        return Err(QuillSQLError::Storage(format!(
            "Holt row is truncated: need {} bytes at offset {}, len {}",
            len,
            *offset,
            bytes.len()
        )));
    }
    let out = &bytes[*offset..end];
    *offset = end;
    Ok(out)
}

pub fn encode_index_key(tuple: &Tuple, rid: RecordId) -> QuillSQLResult<Vec<u8>> {
    let mut out = Vec::new();
    for (value, col) in tuple.data.iter().zip(tuple.schema.columns.iter()) {
        encode_ordered_scalar(&mut out, value, col.data_type)?;
    }
    out.push(0xff);
    out.extend(encode_rid(rid));
    Ok(out)
}

fn encode_ordered_scalar(
    out: &mut Vec<u8>,
    value: &ScalarValue,
    _data_type: DataType,
) -> QuillSQLResult<()> {
    if value.is_null() {
        out.push(0);
        return Ok(());
    }
    out.push(1);
    match value {
        ScalarValue::Boolean(Some(v)) => out.push(u8::from(*v)),
        ScalarValue::Int8(Some(v)) => out.push((*v as u8) ^ 0x80),
        ScalarValue::Int16(Some(v)) => out.extend(((*v as u16) ^ 0x8000).to_be_bytes()),
        ScalarValue::Int32(Some(v)) => out.extend(((*v as u32) ^ 0x8000_0000).to_be_bytes()),
        ScalarValue::Int64(Some(v)) => {
            out.extend(((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes())
        }
        ScalarValue::UInt8(Some(v)) => out.push(*v),
        ScalarValue::UInt16(Some(v)) => out.extend(v.to_be_bytes()),
        ScalarValue::UInt32(Some(v)) => out.extend(v.to_be_bytes()),
        ScalarValue::UInt64(Some(v)) => out.extend(v.to_be_bytes()),
        ScalarValue::Float32(Some(v)) => out.extend(ordered_f32_bits(*v).to_be_bytes()),
        ScalarValue::Float64(Some(v)) => out.extend(ordered_f64_bits(*v).to_be_bytes()),
        ScalarValue::Varchar(Some(v)) => encode_ordered_bytes(out, v.as_bytes()),
        ScalarValue::Boolean(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Varchar(None) => unreachable!("null handled above"),
    }
    Ok(())
}

fn ordered_f32_bits(value: f32) -> u32 {
    let bits = value.to_bits();
    if bits & 0x8000_0000 == 0 {
        bits ^ 0x8000_0000
    } else {
        !bits
    }
}

fn ordered_f64_bits(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits & 0x8000_0000_0000_0000 == 0 {
        bits ^ 0x8000_0000_0000_0000
    } else {
        !bits
    }
}

fn encode_ordered_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    for byte in bytes {
        if *byte == 0 {
            out.extend([0, 0xff]);
        } else {
            out.push(*byte);
        }
    }
    out.extend([0, 0]);
}

fn tuple_within_bounds(tuple: &Tuple, request: &IndexScanRequest) -> bool {
    let lower = match &request.start {
        Bound::Included(start) => tuple.cmp(start) != Ordering::Less,
        Bound::Excluded(start) => tuple.cmp(start) == Ordering::Greater,
        Bound::Unbounded => true,
    };
    if !lower {
        return false;
    }
    match &request.end {
        Bound::Included(end) => tuple.cmp(end) != Ordering::Greater,
        Bound::Excluded(end) => tuple.cmp(end) == Ordering::Less,
        Bound::Unbounded => true,
    }
}

fn read_counter(db: &holt::DB, key: &[u8], default_value: u64) -> QuillSQLResult<u64> {
    let catalog = db.open_tree(CATALOG_TREE).map_err(map_holt_err)?;
    match catalog.get(key).map_err(map_holt_err)? {
        Some(raw) => decode_u64(&raw),
        None => {
            catalog
                .put(key, &encode_u64(default_value))
                .map_err(map_holt_err)?;
            Ok(default_value)
        }
    }
}

pub fn encode_rid(rid: RecordId) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[..4].copy_from_slice(&rid.page_id.to_be_bytes());
    out[4..].copy_from_slice(&rid.slot_num.to_be_bytes());
    out
}

pub fn decode_rid(bytes: &[u8]) -> QuillSQLResult<RecordId> {
    if bytes.len() != 8 {
        return Err(QuillSQLError::Storage(format!(
            "invalid Holt RID length {}",
            bytes.len()
        )));
    }
    let page_id = u32::from_be_bytes(bytes[..4].try_into().unwrap());
    let slot_num = u32::from_be_bytes(bytes[4..].try_into().unwrap());
    Ok(RecordId::new(page_id, slot_num))
}

fn table_key(table: &str) -> Vec<u8> {
    format!("{TABLE_PREFIX}/{table}").into_bytes()
}

fn table_schema_key(table: &str) -> Vec<u8> {
    format!("{TABLE_SCHEMA_PREFIX}/{table}").into_bytes()
}

fn index_key(table: &str, index_name: &str) -> Vec<u8> {
    format!("{INDEX_PREFIX}/{table}/{index_name}").into_bytes()
}

fn index_schema_key(table: &str, index_name: &str) -> Vec<u8> {
    format!("{INDEX_SCHEMA_PREFIX}/{table}/{index_name}").into_bytes()
}

fn parse_canonical_table_name(canonical: &str) -> QuillSQLResult<TableReference> {
    let parts = canonical.split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        [catalog, schema, table] => Ok(TableReference::Full {
            catalog: (*catalog).to_string(),
            schema: (*schema).to_string(),
            table: (*table).to_string(),
        }),
        _ => Err(QuillSQLError::Storage(format!(
            "invalid Holt canonical table name {canonical}"
        ))),
    }
}

fn encode_schema(schema: &Schema) -> QuillSQLResult<Vec<u8>> {
    let mut out = Vec::new();
    put_u32(&mut out, schema.columns.len() as u32);
    for column in &schema.columns {
        put_string(&mut out, &column.name)?;
        let data_type = column.data_type.to_string();
        put_string(&mut out, &data_type)?;
        out.push(u8::from(column.nullable));
        let default = column.default.to_string();
        put_string(&mut out, &default)?;
    }
    Ok(out)
}

fn decode_schema(bytes: &[u8]) -> QuillSQLResult<Schema> {
    let mut offset = 0usize;
    let column_count = read_u32(bytes, &mut offset)?;
    let mut columns = Vec::with_capacity(column_count as usize);
    for _ in 0..column_count {
        let name = read_string(bytes, &mut offset)?;
        let data_type_str = read_string(bytes, &mut offset)?;
        let data_type = DataType::try_from(data_type_str.as_str())?;
        let nullable = read_u8(bytes, &mut offset)? != 0;
        let default_str = read_string(bytes, &mut offset)?;
        let default = ScalarValue::from_string(&default_str, data_type)?;
        columns.push(Column::new(name, data_type, nullable).with_default(default));
    }
    Ok(Schema::new(columns))
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend(value.to_be_bytes());
}

fn put_string(out: &mut Vec<u8>, value: &str) -> QuillSQLResult<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| QuillSQLError::Storage("Holt schema string is too large".to_string()))?;
    put_u32(out, len);
    out.extend(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> QuillSQLResult<String> {
    let len = read_u32(bytes, offset)? as usize;
    let raw = take(bytes, offset, len)?;
    String::from_utf8(raw.to_vec())
        .map_err(|err| QuillSQLError::Storage(format!("invalid Holt schema string: {err}")))
}

fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn decode_u64(bytes: &[u8]) -> QuillSQLResult<u64> {
    if bytes.len() != 8 {
        return Err(QuillSQLError::Storage(format!(
            "invalid Holt u64 length {}",
            bytes.len()
        )));
    }
    Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
}

fn committed(committed: bool) -> QuillSQLResult<()> {
    if committed {
        Ok(())
    } else {
        Err(QuillSQLError::Storage(
            "Holt metadata compare-and-set failed".to_string(),
        ))
    }
}

fn encode_txn_status(status: TransactionStatus) -> u8 {
    match status {
        TransactionStatus::InProgress => 1,
        TransactionStatus::Committed => 2,
        TransactionStatus::Aborted => 3,
        TransactionStatus::Unknown => 4,
    }
}

fn decode_txn_status(raw: u8) -> QuillSQLResult<TransactionStatus> {
    match raw {
        1 => Ok(TransactionStatus::InProgress),
        2 => Ok(TransactionStatus::Committed),
        3 => Ok(TransactionStatus::Aborted),
        4 => Ok(TransactionStatus::Unknown),
        other => Err(QuillSQLError::Storage(format!(
            "invalid Holt txn status {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::catalog::{Column, DataType, Schema};
    use crate::storage::holt::encode_index_key;
    use crate::storage::page::RecordId;
    use crate::storage::tuple::Tuple;
    use crate::utils::scalar::ScalarValue;

    fn schema(column_types: &[DataType]) -> Arc<Schema> {
        Arc::new(Schema::new(
            column_types
                .iter()
                .enumerate()
                .map(|(idx, data_type)| Column::new(format!("c{idx}"), *data_type, true))
                .collect(),
        ))
    }

    fn assert_order(column_types: &[DataType], rows: Vec<Vec<ScalarValue>>) {
        let schema = schema(column_types);
        let tuples = rows
            .into_iter()
            .map(|row| Tuple::new(schema.clone(), row))
            .collect::<Vec<_>>();
        for pair in tuples.windows(2) {
            let left = &pair[0];
            let right = &pair[1];
            assert!(
                left < right,
                "test rows must be in Tuple order: {left:?} !< {right:?}"
            );
            let left_key = encode_index_key(left, RecordId::new(0, 1)).unwrap();
            let right_key = encode_index_key(right, RecordId::new(0, 1)).unwrap();
            assert!(
                left_key < right_key,
                "encoded key order differs from Tuple order for {left:?} and {right:?}"
            );
        }
    }

    #[test]
    fn ordered_index_key_matches_tuple_order_for_scalars() {
        assert_order(
            &[DataType::Int32],
            vec![
                vec![ScalarValue::Int32(None)],
                vec![ScalarValue::Int32(Some(-10))],
                vec![ScalarValue::Int32(Some(0))],
                vec![ScalarValue::Int32(Some(10))],
            ],
        );
        assert_order(
            &[DataType::UInt64],
            vec![
                vec![ScalarValue::UInt64(None)],
                vec![ScalarValue::UInt64(Some(0))],
                vec![ScalarValue::UInt64(Some(10))],
                vec![ScalarValue::UInt64(Some(u64::MAX))],
            ],
        );
        assert_order(
            &[DataType::Float64],
            vec![
                vec![ScalarValue::Float64(None)],
                vec![ScalarValue::Float64(Some(f64::NEG_INFINITY))],
                vec![ScalarValue::Float64(Some(-1.0))],
                vec![ScalarValue::Float64(Some(-0.0))],
                vec![ScalarValue::Float64(Some(0.0))],
                vec![ScalarValue::Float64(Some(1.0))],
                vec![ScalarValue::Float64(Some(f64::INFINITY))],
            ],
        );
        assert_order(
            &[DataType::Varchar(None)],
            vec![
                vec![ScalarValue::Varchar(None)],
                vec![ScalarValue::Varchar(Some(String::new()))],
                vec![ScalarValue::Varchar(Some("a".to_string()))],
                vec![ScalarValue::Varchar(Some("aa".to_string()))],
                vec![ScalarValue::Varchar(Some("b".to_string()))],
            ],
        );
    }

    #[test]
    fn ordered_index_key_handles_composite_and_duplicate_rids() {
        assert_order(
            &[DataType::Int32, DataType::Varchar(None)],
            vec![
                vec![
                    ScalarValue::Int32(Some(1)),
                    ScalarValue::Varchar(Some("a".to_string())),
                ],
                vec![
                    ScalarValue::Int32(Some(1)),
                    ScalarValue::Varchar(Some("b".to_string())),
                ],
                vec![
                    ScalarValue::Int32(Some(2)),
                    ScalarValue::Varchar(Some("a".to_string())),
                ],
            ],
        );

        let schema = schema(&[DataType::Int32]);
        let tuple = Tuple::new(schema, vec![ScalarValue::Int32(Some(1))]);
        let low = encode_index_key(&tuple, RecordId::new(0, 1)).unwrap();
        let high = encode_index_key(&tuple, RecordId::new(0, 2)).unwrap();
        assert!(low < high);
    }
}

pub(crate) fn map_holt_err(err: holt::Error) -> QuillSQLError {
    QuillSQLError::Storage(format!("Holt error: {err}"))
}
