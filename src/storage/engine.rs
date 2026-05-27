use std::fmt;
use std::ops::Bound;
use std::sync::Arc;

use crate::catalog::{Catalog, SchemaRef};
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::storage::holt::{HoltIndexHandle, HoltStore, HoltTableHandle};
use crate::storage::record::{RecordId, TupleMeta, EMPTY_TUPLE_META};
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
    fn index_id(&self) -> u64;
    fn insert(&self, key: &Tuple, rid: RecordId, txn_id: TransactionId) -> QuillSQLResult<()>;
    fn delete(&self, key: &Tuple, rid: RecordId, txn_id: TransactionId) -> QuillSQLResult<()>;
    fn range_scan(
        &self,
        table: Arc<dyn TableHandle>,
        request: IndexScanRequest,
    ) -> QuillSQLResult<Box<dyn TupleStream>>;
}

pub struct HoltStorage {
    holt_store: Arc<HoltStore>,
}

impl HoltStorage {
    pub fn new(holt_store: Arc<HoltStore>) -> Self {
        Self { holt_store }
    }

    pub fn table(&self, catalog: &Catalog, table: &TableReference) -> QuillSQLResult<TableBinding> {
        if let Some(rows) = catalog.virtual_table_rows(table)? {
            let schema = catalog.table_schema(table)?;
            let handle: Arc<dyn TableHandle> =
                Arc::new(VirtualTableHandle::new(table.clone(), schema, rows));
            return Ok(TableBinding::new(handle, Vec::new()));
        }

        let table_id = catalog.table_id(table)?;
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
            .map(|index| {
                Ok(Arc::new(HoltIndexHandle::new(
                    index.name,
                    index.key_schema,
                    index.index_id,
                    self.holt_store.clone(),
                )) as Arc<dyn IndexHandle>)
            })
            .collect::<QuillSQLResult<Vec<_>>>()?;
        Ok(TableBinding::new(handle, index_handles))
    }
}

#[derive(Clone)]
struct VirtualTableHandle {
    table_ref: TableReference,
    schema: SchemaRef,
    rows: Arc<Vec<Tuple>>,
}

impl VirtualTableHandle {
    fn new(table_ref: TableReference, schema: SchemaRef, rows: Vec<Tuple>) -> Self {
        Self {
            table_ref,
            schema,
            rows: Arc::new(rows),
        }
    }

    fn read_only_error(&self, operation: &str) -> QuillSQLError {
        QuillSQLError::Execution(format!(
            "{operation} is not supported on virtual table {}",
            self.table_ref.to_log_string()
        ))
    }
}

impl TableHandle for VirtualTableHandle {
    fn table_ref(&self) -> &TableReference {
        &self.table_ref
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn full_scan(&self) -> QuillSQLResult<Box<dyn TupleStream>> {
        Ok(Box::new(VirtualTupleStream {
            rows: self.rows.clone(),
            offset: 0,
        }))
    }

    fn full_tuple(&self, rid: RecordId) -> QuillSQLResult<(TupleMeta, Tuple)> {
        let Some(tuple) = self.rows.get(rid.slot_num as usize) else {
            return Err(QuillSQLError::Storage(format!(
                "virtual tuple {} not found in {}",
                rid,
                self.table_ref.to_log_string()
            )));
        };
        Ok((EMPTY_TUPLE_META, tuple.clone()))
    }

    fn insert(
        &self,
        _txn: &mut TxnContext<'_>,
        _tuple: &Tuple,
        _indexes: &[Arc<dyn IndexHandle>],
    ) -> QuillSQLResult<()> {
        Err(self.read_only_error("INSERT"))
    }

    fn delete(
        &self,
        _txn: &mut TxnContext<'_>,
        _rid: RecordId,
        _prev_meta: TupleMeta,
        _prev_tuple: Tuple,
        _indexes: &[Arc<dyn IndexHandle>],
    ) -> QuillSQLResult<()> {
        Err(self.read_only_error("DELETE"))
    }

    fn update(
        &self,
        _txn: &mut TxnContext<'_>,
        _rid: RecordId,
        _new_tuple: Tuple,
        _prev_meta: TupleMeta,
        _prev_tuple: Tuple,
        _indexes: &[Arc<dyn IndexHandle>],
    ) -> QuillSQLResult<RecordId> {
        Err(self.read_only_error("UPDATE"))
    }

    fn prepare_row_for_write(
        &self,
        _txn: &mut TxnContext<'_>,
        _rid: RecordId,
        _observed_meta: &TupleMeta,
    ) -> QuillSQLResult<Option<(TupleMeta, Tuple)>> {
        Err(self.read_only_error("WRITE"))
    }

    fn undo_insert(&self, _rid: RecordId, _txn_id: TransactionId) -> QuillSQLResult<()> {
        Err(self.read_only_error("UNDO"))
    }

    fn undo_delete(
        &self,
        _rid: RecordId,
        _prev_meta: TupleMeta,
        _prev_tuple: Tuple,
    ) -> QuillSQLResult<()> {
        Err(self.read_only_error("UNDO"))
    }
}

struct VirtualTupleStream {
    rows: Arc<Vec<Tuple>>,
    offset: usize,
}

impl TupleStream for VirtualTupleStream {
    fn next(&mut self) -> QuillSQLResult<Option<(RecordId, TupleMeta, Tuple)>> {
        let Some(tuple) = self.rows.get(self.offset) else {
            return Ok(None);
        };
        let rid = RecordId::new(0, self.offset as u32);
        self.offset += 1;
        Ok(Some((rid, EMPTY_TUPLE_META, tuple.clone())))
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
        handle.range_scan(self.table.clone(), request)
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
