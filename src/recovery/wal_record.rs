use crate::recovery::wal::codec;
use crate::recovery::Lsn;

pub use crate::recovery::wal::codec::{
    decode_checkpoint, decode_clr, decode_frame, decode_page_write,
    decode_payload as decode_wal_payload, decode_transaction, encode_frame,
    heap_record_kind_to_info, CheckpointPayload, ClrPayload, PageWritePayload, ResourceManagerId,
    TransactionPayload, TransactionRecordKind, WalFrame, WAL_CRC_LEN, WAL_HEADER_LEN, WAL_MAGIC,
    WAL_VERSION, WAL_VERSION_V1,
};

pub use crate::storage::heap::wal_codec::{
    decode_heap_record as decode_heap, encode_heap_record as encode_heap, HeapDeletePayload,
    HeapInsertPayload, HeapRecordKind, HeapRecordPayload, RelationIdent, TupleMetaRepr,
};
pub use crate::storage::index::wal_codec::{
    decode_index_record as decode_index, encode_index_record as encode_index,
    IndexInternalEntryPayload, IndexInternalMergePayload, IndexInternalRedistributePayload,
    IndexInternalSplitPayload, IndexLeafDeletePayload, IndexLeafInsertPayload,
    IndexLeafMergePayload, IndexLeafRedistributePayload, IndexLeafSplitEntryPayload,
    IndexLeafSplitPayload, IndexParentDeletePayload, IndexParentInsertPayload,
    IndexParentUpdatePayload, IndexRecordPayload, IndexRelationIdent, IndexRootAdoptPayload,
    IndexRootInstallInternalPayload, IndexRootInstallLeafPayload, IndexRootResetPayload,
};

#[derive(Debug, Clone)]
pub enum WalRecordPayload {
    PageWrite(PageWritePayload),
    Transaction(TransactionPayload),
    Heap(HeapRecordPayload),
    Index(IndexRecordPayload),
    Checkpoint(CheckpointPayload),
    /// Compensation log record: documents an UNDO action; redo is a no-op.
    Clr(ClrPayload),
}

impl WalRecordPayload {
    pub fn encode(&self, lsn: Lsn, prev_lsn: Lsn) -> Vec<u8> {
        codec::encode_frame(lsn, prev_lsn, self)
    }
}
