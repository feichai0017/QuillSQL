use std::fmt;
use std::ops::Bound;
use std::sync::Arc;

use crate::catalog::{Catalog, IndexBackend, SchemaRef, TableBackend};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::holt::{HoltIndexHandle, HoltStore, HoltTableHandle};
use crate::storage::record::{RecordId, TupleMeta};
use crate::storage::tuple::Tuple;
use crate::transaction::{TransactionId, TxnContext};
use crate::utils::table_ref::TableReference;

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
}

pub trait IndexHandle: Send + Sync {
    fn name(&self) -> &str;
    fn key_schema(&self) -> SchemaRef;
    fn holt_index_id(&self) -> Option<u64>;
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

pub struct HoltStorageEngine {
    holt_store: Arc<HoltStore>,
}

impl HoltStorageEngine {
    pub fn new(holt_store: Arc<HoltStore>) -> Self {
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
            .ok_or_else(|| QuillSQLError::Execution(format!("index {} not found", name)))?;
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

impl StorageEngine for HoltStorageEngine {
    fn table(&self, catalog: &Catalog, table: &TableReference) -> QuillSQLResult<TableBinding> {
        let table_id = match catalog.table_backend(table)? {
            TableBackend::Holt { table_id } => table_id,
        };
        let schema = catalog.table_schema(table)?;
        let handle: Arc<dyn TableHandle> = Arc::new(HoltTableHandle::new(
            table.clone(),
            schema,
            table_id,
            self.holt_store.clone(),
        ));
        let indexes = catalog.table_indexes(table)?;
        let index_handles = indexes
            .into_iter()
            .map(|index| match index.backend {
                IndexBackend::Holt { index_id } => Ok(Arc::new(HoltIndexHandle::new(
                    index.name,
                    index.key_schema,
                    index_id,
                    self.holt_store.clone(),
                )) as Arc<dyn IndexHandle>),
            })
            .collect::<QuillSQLResult<Vec<_>>>()?;
        Ok(TableBinding::new(handle, index_handles))
    }
}
