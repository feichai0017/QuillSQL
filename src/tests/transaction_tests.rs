#![cfg(test)]

use crate::session::SessionContext;
use crate::transaction::{IsolationLevel, TransactionManager, TransactionState};
use sqlparser::ast::TransactionAccessMode;
use std::sync::Arc;
use tempfile::TempDir;

fn create_manager(temp: &TempDir) -> TransactionManager {
    use crate::config::WalConfig;
    use crate::recovery::WalManager;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::disk_scheduler::DiskScheduler;

    let wal_path = temp.path().join("wal");
    let db_path = temp.path().join("test.db");

    let mut wal_config = WalConfig::default();
    wal_config.directory = wal_path;
    wal_config.sync_on_flush = false;

    let disk_manager = Arc::new(DiskManager::try_new(db_path.to_str().unwrap()).unwrap());
    let scheduler = Arc::new(DiskScheduler::new(disk_manager));
    let wal = Arc::new(WalManager::new(wal_config, scheduler, None, None).unwrap());

    TransactionManager::new(wal, true)
}

#[test]
fn begin_commit_abort() {
    let temp = TempDir::new().unwrap();
    let manager = create_manager(&temp);

    let mut txn = manager
        .begin(
            IsolationLevel::ReadCommitted,
            TransactionAccessMode::ReadWrite,
        )
        .unwrap();
    assert_eq!(txn.state(), crate::transaction::TransactionState::Running);

    manager.commit(&mut txn).unwrap();
    assert_eq!(txn.state(), crate::transaction::TransactionState::Committed);

    let mut txn2 = manager
        .begin(
            IsolationLevel::ReadCommitted,
            TransactionAccessMode::ReadWrite,
        )
        .unwrap();
    manager.abort(&mut txn2).unwrap();
    assert_eq!(txn2.state(), crate::transaction::TransactionState::Aborted);
}

#[test]
fn session_apply_set_transaction() {
    let mut session = SessionContext::new(IsolationLevel::ReadUncommitted);
    session.set_autocommit(false);

    let mut modes_txn = crate::plan::logical_plan::TransactionModes::default();
    modes_txn.isolation_level = Some(IsolationLevel::Serializable);
    modes_txn.access_mode = Some(TransactionAccessMode::ReadOnly);

    session.apply_session_modes(&modes_txn);
    assert_eq!(session.default_isolation(), IsolationLevel::Serializable);

    let temp = TempDir::new().unwrap();
    let manager = create_manager(&temp);
    let txn = manager
        .begin(
            session.default_isolation(),
            TransactionAccessMode::ReadWrite,
        )
        .unwrap();
    session.set_active_transaction(txn).unwrap();

    let mut txn_modes = crate::plan::logical_plan::TransactionModes::default();
    txn_modes.access_mode = Some(TransactionAccessMode::ReadOnly);
    session.apply_transaction_modes(&txn_modes);
    assert_eq!(
        session.active_txn().unwrap().access_mode(),
        TransactionAccessMode::ReadOnly
    );
}
