use crate::storage::page::TupleMeta;
use crate::transaction::{CommandId, TransactionId, INVALID_COMMAND_ID};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TransactionStatus {
    InProgress,
    Committed,
    Aborted,
    Unknown,
}

impl TransactionStatus {
    fn is_committed(self) -> bool {
        matches!(self, TransactionStatus::Committed)
    }

    fn is_aborted(self) -> bool {
        matches!(self, TransactionStatus::Aborted)
    }
}

#[derive(Debug, Clone)]
pub struct TransactionSnapshot {
    txn_id: TransactionId,
    xmin: TransactionId,
    xmax: TransactionId,
    active_txns: Vec<TransactionId>,
}

impl TransactionSnapshot {
    pub fn new(
        txn_id: TransactionId,
        xmin: TransactionId,
        xmax: TransactionId,
        mut active_txns: Vec<TransactionId>,
    ) -> Self {
        active_txns.sort_unstable();
        Self {
            txn_id,
            xmin,
            xmax,
            active_txns,
        }
    }

    pub fn txn_id(&self) -> TransactionId {
        self.txn_id
    }

    pub fn xmin(&self) -> TransactionId {
        self.xmin
    }

    pub fn xmax(&self) -> TransactionId {
        self.xmax
    }

    pub fn active_txns(&self) -> &[TransactionId] {
        &self.active_txns
    }

    fn active_contains(&self, txn_id: TransactionId) -> bool {
        self.active_txns.binary_search(&txn_id).is_ok()
    }

    pub fn is_visible<F>(&self, meta: &TupleMeta, command_id: CommandId, mut status_of: F) -> bool
    where
        F: FnMut(TransactionId) -> TransactionStatus,
    {
        let inserter = meta.insert_txn_id;
        if inserter == 0 {
            return self.other_txn_delete_invisible(meta, command_id, &mut status_of);
        }

        if inserter == self.txn_id {
            if meta.insert_cid > command_id {
                return false;
            }
            return self.visible_after_local_delete(meta, command_id, &mut status_of);
        }

        if !self.other_txn_insert_visible(inserter, &mut status_of) {
            return false;
        }

        if !meta.is_deleted {
            return true;
        }

        self.other_txn_delete_invisible(meta, command_id, &mut status_of)
    }

    fn visible_after_local_delete<F>(
        &self,
        meta: &TupleMeta,
        command_id: CommandId,
        status_of: &mut F,
    ) -> bool
    where
        F: FnMut(TransactionId) -> TransactionStatus,
    {
        if !meta.is_deleted {
            return true;
        }
        let deleter = meta.delete_txn_id;
        if deleter == self.txn_id {
            if meta.delete_cid == INVALID_COMMAND_ID {
                return true;
            }
            return meta.delete_cid > command_id;
        }
        self.other_txn_delete_invisible(meta, command_id, status_of)
    }

    fn other_txn_insert_visible<F>(&self, inserter: TransactionId, status_of: &mut F) -> bool
    where
        F: FnMut(TransactionId) -> TransactionStatus,
    {
        if inserter >= self.xmax {
            return false;
        }

        let status = status_of(inserter);
        if status.is_aborted() {
            return false;
        }

        if inserter < self.xmin {
            return status.is_committed() || matches!(status, TransactionStatus::Unknown);
        }

        if self.active_contains(inserter) {
            return false;
        }

        matches!(
            status,
            TransactionStatus::Committed | TransactionStatus::Unknown
        )
    }

    fn other_txn_delete_invisible<F>(
        &self,
        meta: &TupleMeta,
        command_id: CommandId,
        status_of: &mut F,
    ) -> bool
    where
        F: FnMut(TransactionId) -> TransactionStatus,
    {
        if !meta.is_deleted {
            return true;
        }

        let deleter = meta.delete_txn_id;
        if deleter == 0 {
            return true;
        }

        if deleter == self.txn_id {
            if meta.delete_cid == INVALID_COMMAND_ID {
                return true;
            }
            return meta.delete_cid > command_id;
        }

        if deleter >= self.xmax {
            return true;
        }

        let status = status_of(deleter);
        if deleter < self.xmin {
            return !status.is_committed();
        }

        if self.active_contains(deleter) {
            return true;
        }

        match status {
            TransactionStatus::Committed => false,
            TransactionStatus::Aborted => true,
            TransactionStatus::InProgress => true,
            TransactionStatus::Unknown => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::TupleMeta;

    #[test]
    fn snapshot_filters_uncommitted() {
        let snapshot = TransactionSnapshot::new(2, 1, 5, vec![1, 3]);
        let meta = TupleMeta {
            insert_txn_id: 1,
            insert_cid: 0,
            delete_txn_id: 0,
            delete_cid: INVALID_COMMAND_ID,
            is_deleted: false,
            next_version: None,
            prev_version: None,
        };
        assert!(!snapshot.is_visible(&meta, 0, |_| TransactionStatus::InProgress));
    }

    #[test]
    fn snapshot_allows_committed_visible() {
        let snapshot = TransactionSnapshot::new(3, 2, 10, vec![4, 6]);
        let meta = TupleMeta {
            insert_txn_id: 1,
            insert_cid: 0,
            delete_txn_id: 0,
            delete_cid: INVALID_COMMAND_ID,
            is_deleted: false,
            next_version: None,
            prev_version: None,
        };
        assert!(snapshot.is_visible(&meta, 0, |_| TransactionStatus::Committed));
    }

    #[test]
    fn snapshot_hides_committed_delete() {
        let snapshot = TransactionSnapshot::new(3, 2, 10, vec![4, 6]);
        let meta = TupleMeta {
            insert_txn_id: 1,
            insert_cid: 0,
            delete_txn_id: 5,
            delete_cid: 0,
            is_deleted: true,
            next_version: None,
            prev_version: None,
        };
        assert!(!snapshot.is_visible(&meta, 0, |txn| {
            if txn == 5 {
                TransactionStatus::Committed
            } else {
                TransactionStatus::Committed
            }
        }));
    }

    #[test]
    fn snapshot_retains_tuple_when_delete_aborted() {
        let snapshot = TransactionSnapshot::new(3, 2, 10, vec![4, 6]);
        let meta = TupleMeta {
            insert_txn_id: 1,
            insert_cid: 0,
            delete_txn_id: 5,
            delete_cid: 0,
            is_deleted: true,
            next_version: None,
            prev_version: None,
        };
        assert!(snapshot.is_visible(&meta, 0, |txn| {
            if txn == 5 {
                TransactionStatus::Aborted
            } else {
                TransactionStatus::Committed
            }
        }));
    }
}
