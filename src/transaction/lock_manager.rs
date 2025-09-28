use crate::storage::page::RecordId;
use crate::transaction::{Transaction, TransactionId};
use crate::utils::table_ref::TableReference;
use parking_lot::{Condvar, Mutex};
use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
    IntentionShared,
    IntentionExclusive,
    SharedIntentionExclusive,
}

#[derive(Debug)]
struct LockRequest {
    id: u64,
    txn_id: TransactionId,
    mode: LockMode,
    table_ref: TableReference,
    rid: Option<RecordId>,
    granted: bool,
}

#[derive(Debug, Default)]
struct LockQueue {
    requests: VecDeque<LockRequest>,
}

#[derive(Debug, Default)]
struct ResourceLock {
    state: Mutex<LockQueue>,
    condvar: Condvar,
}

impl ResourceLock {
    /// Create a new empty lock queue for a resource.
    fn new() -> Self {
        Self {
            state: Mutex::new(LockQueue {
                requests: VecDeque::new(),
            }),
            condvar: Condvar::new(),
        }
    }
}

type RowKey = (TableReference, RecordId);

#[derive(Debug)]
pub struct LockManager {
    table_lock_map: Mutex<HashMap<TableReference, Arc<ResourceLock>>>,
    row_lock_map: Mutex<HashMap<RowKey, Arc<ResourceLock>>>,
    request_id: AtomicU64,
}

impl LockManager {
    /// Create a new lock manager.
    pub fn new() -> Self {
        Self {
            table_lock_map: Mutex::new(HashMap::new()),
            row_lock_map: Mutex::new(HashMap::new()),
            request_id: AtomicU64::new(1),
        }
    }

    /// Acquire a table level lock for the given transaction.
    pub fn lock_table(&self, txn: &Transaction, mode: LockMode, table_ref: TableReference) -> bool {
        let resource = self.get_table_resource(table_ref.clone());
        self.lock_resource(
            resource,
            LockRequest {
                id: self.next_request_id(),
                txn_id: txn.id(),
                mode,
                table_ref,
                rid: None,
                granted: false,
            },
        )
    }

    /// Release a table level lock held by the transaction.
    pub fn unlock_table(&self, txn: &Transaction, table_ref: TableReference) -> bool {
        self.unlock_table_raw(txn.id(), table_ref)
    }

    /// Acquire a row level lock for the given transaction.
    pub fn lock_row(
        &self,
        txn: &Transaction,
        mode: LockMode,
        table_ref: TableReference,
        rid: RecordId,
    ) -> bool {
        let resource = self.get_row_resource(table_ref.clone(), rid);
        self.lock_resource(
            resource,
            LockRequest {
                id: self.next_request_id(),
                txn_id: txn.id(),
                mode,
                table_ref,
                rid: Some(rid),
                granted: false,
            },
        )
    }

    /// Release a row level lock held by the transaction.
    pub fn unlock_row(
        &self,
        txn: &Transaction,
        table_ref: TableReference,
        rid: RecordId,
        _force: bool,
    ) -> bool {
        self.unlock_row_raw(txn.id(), table_ref, rid)
    }

    pub fn unlock_table_raw(&self, txn_id: TransactionId, table_ref: TableReference) -> bool {
        self.unlock_table_internal(txn_id, table_ref)
    }

    pub fn unlock_row_raw(
        &self,
        txn_id: TransactionId,
        table_ref: TableReference,
        rid: RecordId,
    ) -> bool {
        self.unlock_row_internal(txn_id, table_ref, rid)
    }

    /// Force release of all locks (used during shutdown/testing).
    pub fn unlock_all(&self) {
        {
            let mut map = self.table_lock_map.lock();
            for resource in map.values() {
                let mut state = resource.state.lock();
                state.requests.clear();
                resource.condvar.notify_all();
            }
            map.clear();
        }
        {
            let mut map = self.row_lock_map.lock();
            for resource in map.values() {
                let mut state = resource.state.lock();
                state.requests.clear();
                resource.condvar.notify_all();
            }
            map.clear();
        }
    }

    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }

    fn get_table_resource(&self, table_ref: TableReference) -> Arc<ResourceLock> {
        let mut map = self.table_lock_map.lock();
        map.entry(table_ref)
            .or_insert_with(|| Arc::new(ResourceLock::new()))
            .clone()
    }

    fn get_row_resource(&self, table_ref: TableReference, rid: RecordId) -> Arc<ResourceLock> {
        let key = (table_ref, rid);
        let mut map = self.row_lock_map.lock();
        map.entry(key)
            .or_insert_with(|| Arc::new(ResourceLock::new()))
            .clone()
    }

    fn lock_resource(&self, resource: Arc<ResourceLock>, request: LockRequest) -> bool {
        let mut queue_guard = resource.state.lock();

        if let Some(existing) = queue_guard.requests.iter_mut().find(|req| {
            req.txn_id == request.txn_id
                && req.rid == request.rid
                && req.table_ref == request.table_ref
        }) {
            if existing.mode == request.mode || dominates(existing.mode, request.mode) {
                return true;
            }

            let req_id = existing.id;
            existing.mode = request.mode;
            existing.granted = false;

            loop {
                if can_grant(&queue_guard.requests, req_id) {
                    if let Some(target) =
                        queue_guard.requests.iter_mut().find(|req| req.id == req_id)
                    {
                        target.granted = true;
                    }
                    resource.condvar.notify_all();
                    return true;
                }
                resource.condvar.wait(&mut queue_guard);
            }
        }

        let request_id = request.id;
        queue_guard.requests.push_back(request);
        loop {
            if can_grant(&queue_guard.requests, request_id) {
                if let Some(req) = queue_guard
                    .requests
                    .iter_mut()
                    .find(|req| req.id == request_id)
                {
                    req.granted = true;
                }
                return true;
            }
            resource.condvar.wait(&mut queue_guard);
        }
    }

    fn unlock_table_internal(&self, txn_id: TransactionId, table_ref: TableReference) -> bool {
        let resource = {
            let map = self.table_lock_map.lock();
            map.get(&table_ref).cloned()
        };
        let Some(resource) = resource else {
            return false;
        };

        let mut queue_guard = resource.state.lock();
        let original_len = queue_guard.requests.len();
        queue_guard
            .requests
            .retain(|req| !(req.txn_id == txn_id && req.rid.is_none()));
        let removed = queue_guard.requests.len() != original_len;
        if removed {
            resource.condvar.notify_all();
        }
        let empty = queue_guard.requests.is_empty();
        drop(queue_guard);

        if empty {
            let mut map = self.table_lock_map.lock();
            if let Entry::Occupied(entry) = map.entry(table_ref) {
                if Arc::ptr_eq(entry.get(), &resource) {
                    entry.remove();
                }
            }
        }

        removed
    }

    fn unlock_row_internal(
        &self,
        txn_id: TransactionId,
        table_ref: TableReference,
        rid: RecordId,
    ) -> bool {
        let key = (table_ref.clone(), rid);
        let resource = {
            let map = self.row_lock_map.lock();
            map.get(&key).cloned()
        };
        let Some(resource) = resource else {
            return false;
        };

        let mut queue_guard = resource.state.lock();
        let original_len = queue_guard.requests.len();
        queue_guard.requests.retain(|req| {
            if req.txn_id != txn_id {
                return true;
            }
            if let Some(lock_rid) = req.rid {
                return !(lock_rid == rid && req.table_ref == table_ref);
            }
            true
        });
        let removed = queue_guard.requests.len() != original_len;
        if removed {
            resource.condvar.notify_all();
        }
        let empty = queue_guard.requests.is_empty();
        drop(queue_guard);

        if empty {
            let mut map = self.row_lock_map.lock();
            if let Entry::Occupied(entry) = map.entry(key) {
                if Arc::ptr_eq(entry.get(), &resource) {
                    entry.remove();
                }
            }
        }

        removed
    }
}

fn can_grant(queue: &VecDeque<LockRequest>, request_id: u64) -> bool {
    let (position, request) = match queue
        .iter()
        .enumerate()
        .find(|(_, req)| req.id == request_id)
    {
        Some(pair) => pair,
        None => return false,
    };

    for pending in queue.iter().take(position) {
        if !pending.granted {
            return false;
        }
    }

    for granted in queue.iter().filter(|req| req.granted) {
        if granted.id == request_id {
            continue;
        }
        if granted.txn_id == request.txn_id {
            continue;
        }
        if !modes_compatible(request.mode, granted.mode) {
            return false;
        }
    }
    true
}

fn modes_compatible(requested: LockMode, held: LockMode) -> bool {
    match requested {
        LockMode::Shared => matches!(held, LockMode::Shared | LockMode::IntentionShared),
        LockMode::Exclusive => false,
        LockMode::IntentionShared => matches!(
            held,
            LockMode::Shared
                | LockMode::IntentionShared
                | LockMode::IntentionExclusive
                | LockMode::SharedIntentionExclusive
        ),
        LockMode::IntentionExclusive => matches!(
            held,
            LockMode::IntentionShared | LockMode::IntentionExclusive
        ),
        LockMode::SharedIntentionExclusive => {
            matches!(held, LockMode::IntentionShared | LockMode::Shared)
        }
    }
}

fn dominates(held: LockMode, requested: LockMode) -> bool {
    match held {
        LockMode::Exclusive => true,
        LockMode::SharedIntentionExclusive => matches!(
            requested,
            LockMode::Shared
                | LockMode::IntentionShared
                | LockMode::IntentionExclusive
                | LockMode::SharedIntentionExclusive
        ),
        LockMode::Shared => matches!(requested, LockMode::Shared | LockMode::IntentionShared),
        LockMode::IntentionExclusive => matches!(
            requested,
            LockMode::IntentionExclusive | LockMode::IntentionShared
        ),
        LockMode::IntentionShared => matches!(requested, LockMode::IntentionShared),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::{IsolationLevel, Transaction};
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn new_txn(id: TransactionId) -> Transaction {
        Transaction::new(id, IsolationLevel::ReadCommitted, true)
    }

    #[test]
    fn shared_locks_are_compatible() {
        let manager = LockManager::new();
        let table = TableReference::bare("t_shared");
        let txn1 = new_txn(1);
        let txn2 = new_txn(2);

        assert!(manager.lock_table(&txn1, LockMode::Shared, table.clone()));
        assert!(manager.lock_table(&txn2, LockMode::Shared, table.clone()));

        assert!(manager.unlock_table(&txn1, table.clone()));
        assert!(manager.unlock_table(&txn2, table));
    }

    #[test]
    fn exclusive_waits_for_shared() {
        let manager = Arc::new(LockManager::new());
        let table = TableReference::bare("t_block");
        let txn1 = new_txn(10);
        let txn2 = new_txn(20);

        assert!(manager.lock_table(&txn1, LockMode::Shared, table.clone()));

        let acquired = Arc::new(AtomicBool::new(false));
        let acquired_clone = acquired.clone();
        let manager_clone = manager.clone();
        let table_clone = table.clone();

        let handle = thread::spawn(move || {
            let ok = manager_clone.lock_table(&txn2, LockMode::Exclusive, table_clone.clone());
            acquired_clone.store(ok, AtomicOrdering::SeqCst);
            if ok {
                manager_clone.unlock_table(&txn2, table_clone);
            }
        });

        thread::sleep(Duration::from_millis(20));
        assert!(!acquired.load(AtomicOrdering::SeqCst));

        assert!(manager.unlock_table(&txn1, table.clone()));
        handle.join().unwrap();
        assert!(acquired.load(AtomicOrdering::SeqCst));
    }

    #[test]
    fn row_lock_conflict_blocks() {
        let manager = Arc::new(LockManager::new());
        let table = TableReference::bare("t_row");
        let rid = RecordId::new(1, 1);
        let writer = new_txn(100);
        let reader = new_txn(200);

        assert!(manager.lock_row(&writer, LockMode::Exclusive, table.clone(), rid));

        let proceed = Arc::new(AtomicBool::new(false));
        let proceed_clone = proceed.clone();
        let manager_clone = manager.clone();
        let table_clone = table.clone();

        let handle = thread::spawn(move || {
            let ok = manager_clone.lock_row(&reader, LockMode::Shared, table_clone.clone(), rid);
            proceed_clone.store(ok, AtomicOrdering::SeqCst);
            if ok {
                manager_clone.unlock_row(&reader, table_clone, rid, false);
            }
        });

        thread::sleep(Duration::from_millis(20));
        assert!(!proceed.load(AtomicOrdering::SeqCst));

        assert!(manager.unlock_row(&writer, table.clone(), rid, false));
        handle.join().unwrap();
        assert!(proceed.load(AtomicOrdering::SeqCst));
    }
}
