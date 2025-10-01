use crate::storage::page::RecordId;
use crate::transaction::{Transaction, TransactionId};
use crate::utils::table_ref::TableReference;
use log::{debug, trace, warn};
use parking_lot::{Condvar, Mutex};
use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};
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

#[derive(Debug, Clone)]
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
    wait_for: Mutex<HashMap<TransactionId, HashSet<TransactionId>>>,
}

impl LockManager {
    /// Create a new lock manager.
    pub fn new() -> Self {
        Self {
            table_lock_map: Mutex::new(HashMap::new()),
            row_lock_map: Mutex::new(HashMap::new()),
            request_id: AtomicU64::new(1),
            wait_for: Mutex::new(HashMap::new()),
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

        let mut prev_mode: Option<LockMode> = None;
        let mut txn_id = request.txn_id;
        let request_id = if let Some(existing) = queue_guard.requests.iter_mut().find(|req| {
            req.txn_id == request.txn_id
                && req.rid == request.rid
                && req.table_ref == request.table_ref
        }) {
            if existing.mode == request.mode {
                return true;
            }

            if !can_upgrade(existing.mode, request.mode) {
                return false;
            }

            prev_mode = Some(existing.mode);
            txn_id = existing.txn_id;
            existing.mode = request.mode;
            existing.granted = false;
            existing.id
        } else {
            queue_guard.requests.push_back(request);
            queue_guard.requests.back().map(|req| req.id).unwrap_or(0)
        };

        loop {
            if can_grant(&queue_guard.requests, request_id) {
                if let Some(req) = queue_guard
                    .requests
                    .iter_mut()
                    .find(|req| req.id == request_id)
                {
                    req.granted = true;
                    trace!(
                        "lock granted: txn={} resource={:?} mode={:?}",
                        req.txn_id,
                        (req.table_ref.clone(), req.rid),
                        req.mode
                    );
                }
                self.clear_wait_edges(txn_id);
                return true;
            }

            let blockers = blockers_for(&queue_guard.requests, request_id);
            if !blockers.is_empty() {
                trace!("wait edge: txn={} blocking_on={:?}", txn_id, blockers);
                if self.record_wait(txn_id, &blockers) {
                    warn!("deadlock detected: txn={}", txn_id);
                    return false;
                }
            }
            resource.condvar.wait(&mut queue_guard);
            self.clear_wait_edges(txn_id);
        }
    }

    fn record_wait(&self, txn_id: TransactionId, blockers: &[TransactionId]) -> bool {
        let mut wait_for = self.wait_for.lock();
        let entry = wait_for.entry(txn_id).or_default();
        entry.clear();
        entry.extend(blockers.iter().copied());
        self.has_cycle(&wait_for, txn_id)
    }

    fn clear_wait_edges(&self, txn: TransactionId) {
        let mut wait_for = self.wait_for.lock();
        wait_for.remove(&txn);
        for edges in wait_for.values_mut() {
            edges.remove(&txn);
        }
    }

    fn has_cycle(
        &self,
        wait_for: &HashMap<TransactionId, HashSet<TransactionId>>,
        start: TransactionId,
    ) -> bool {
        fn dfs(
            graph: &HashMap<TransactionId, HashSet<TransactionId>>,
            node: TransactionId,
            start: TransactionId,
            visited: &mut HashSet<TransactionId>,
        ) -> bool {
            if !visited.insert(node) {
                return false;
            }
            if let Some(edges) = graph.get(&node) {
                for &next in edges {
                    if next == start || dfs(graph, next, start, visited) {
                        return true;
                    }
                }
            }
            visited.remove(&node);
            false
        }
        dfs(wait_for, start, start, &mut HashSet::new())
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

fn blockers_for(queue: &VecDeque<LockRequest>, request_id: u64) -> Vec<TransactionId> {
    let Some((position, target)) = queue
        .iter()
        .enumerate()
        .find(|(_, req)| req.id == request_id)
    else {
        return Vec::new();
    };
    queue
        .iter()
        .take(position)
        .filter(|req| req.granted && req.txn_id != target.txn_id)
        .map(|req| req.txn_id)
        .collect()
}

fn can_upgrade(held: LockMode, requested: LockMode) -> bool {
    matches!(
        (held, requested),
        (LockMode::Shared, LockMode::Exclusive)
            | (LockMode::Shared, LockMode::SharedIntentionExclusive)
            | (LockMode::IntentionShared, LockMode::IntentionExclusive)
            | (
                LockMode::IntentionShared,
                LockMode::SharedIntentionExclusive
            )
            | (
                LockMode::IntentionExclusive,
                LockMode::SharedIntentionExclusive
            )
    )
}

fn modes_compatible(requested: LockMode, held: LockMode) -> bool {
    match requested {
        LockMode::Shared => matches!(
            held,
            LockMode::Shared | LockMode::IntentionShared | LockMode::SharedIntentionExclusive
        ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::{IsolationLevel, Transaction};
    use crate::utils::table_ref::TableReference;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn new_txn(id: TransactionId) -> Transaction {
        Transaction::new(
            id,
            IsolationLevel::ReadCommitted,
            sqlparser::ast::TransactionAccessMode::ReadWrite,
            true,
        )
    }

    #[test]
    fn shared_locks_are_compatible() {
        let manager = LockManager::new();
        let table = TableReference::Bare {
            table: "t_shared".to_string(),
        };
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
        let table = TableReference::Bare {
            table: "t_block".to_string(),
        };
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
        let table = TableReference::Bare {
            table: "t_row".to_string(),
        };
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
