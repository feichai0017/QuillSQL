use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::database::Database;
use crate::session::SessionContext;
use crate::storage::record::RecordId;
use crate::transaction::{IsolationLevel, LockMode, TransactionManager};
use crate::utils::scalar::ScalarValue;
use crate::utils::table_ref::TableReference;
use sqlparser::ast::TransactionAccessMode;

fn create_manager() -> TransactionManager {
    TransactionManager::new()
}

fn value_as_i32(value: &ScalarValue) -> i32 {
    match value {
        ScalarValue::Int32(Some(v)) => *v,
        other => panic!("expected Int32(Some(_)), got {:?}", other),
    }
}

#[test]
fn begin_commit_abort() {
    let manager = create_manager();

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

    let modes_txn = crate::plan::logical_plan::TransactionModes {
        isolation_level: Some(IsolationLevel::Serializable),
        access_mode: Some(TransactionAccessMode::ReadOnly),
    };

    session.apply_session_modes(&modes_txn);
    assert_eq!(session.default_isolation(), IsolationLevel::Serializable);

    let manager = create_manager();
    let txn = manager
        .begin(
            session.default_isolation(),
            TransactionAccessMode::ReadWrite,
        )
        .unwrap();
    session.set_active_transaction(txn).unwrap();

    let txn_modes = crate::plan::logical_plan::TransactionModes {
        access_mode: Some(TransactionAccessMode::ReadOnly),
        ..Default::default()
    };
    session.apply_transaction_modes(&txn_modes);
    assert_eq!(
        session.active_txn().unwrap().access_mode(),
        TransactionAccessMode::ReadOnly
    );
}

#[test]
fn read_only_transaction_rejects_dml() {
    let mut db = Database::new_temp().expect("database");
    db.run("create table accounts(id int primary key, balance int)")
        .expect("create table");

    let mut session = SessionContext::new(IsolationLevel::ReadCommitted);
    session.set_autocommit(false);
    db.run_with_session(&mut session, "start transaction read only")
        .expect("start txn");

    let err = db
        .run_with_session(&mut session, "insert into accounts values (1, 100)")
        .expect_err("dml should fail in read only txn");
    assert!(
        matches!(err, crate::error::QuillSQLError::Execution(msg) if msg.contains("READ ONLY"))
    );

    db.run_with_session(&mut session, "rollback")
        .expect("rollback");
}

#[test]
fn read_committed_allows_update_after_select() {
    let mut db = Database::new_temp().expect("database");
    db.run("create table kv(id int primary key, val int)")
        .expect("create table");
    db.run("insert into kv values (1, 10)")
        .expect("insert seed");

    let rows = db
        .run("select val from kv where id = 1")
        .expect("first read");
    assert_eq!(value_as_i32(&rows[0].data[0]), 10);

    db.run("update kv set val = 20 where id = 1")
        .expect("update value");

    let rows = db
        .run("select val from kv where id = 1")
        .expect("second read");
    assert_eq!(value_as_i32(&rows[0].data[0]), 20);
}

#[test]
fn repeatable_read_blocks_update_until_commit() {
    let manager = Arc::new(create_manager());
    let table = TableReference::Bare {
        table: "kv".to_string(),
    };
    let rid = RecordId {
        page_id: 1,
        slot_num: 0,
    };

    let mut reader = manager
        .begin(
            IsolationLevel::RepeatableRead,
            TransactionAccessMode::ReadWrite,
        )
        .expect("begin rr txn");
    manager
        .acquire_table_lock(&reader, table.clone(), LockMode::IntentionShared)
        .expect("reader table lock");
    assert!(manager
        .try_acquire_row_lock(&reader, table.clone(), rid, LockMode::Shared)
        .expect("reader row lock"));

    let proceed = Arc::new(AtomicBool::new(false));
    let manager_clone = manager.clone();
    let table_clone = table.clone();
    let proceed_clone = proceed.clone();

    let handle = thread::spawn(move || {
        let mut writer = manager_clone
            .begin(
                IsolationLevel::ReadCommitted,
                TransactionAccessMode::ReadWrite,
            )
            .expect("begin writer txn");
        manager_clone
            .acquire_table_lock(&writer, table_clone.clone(), LockMode::IntentionExclusive)
            .expect("writer table lock");
        let ok = manager_clone
            .try_acquire_row_lock(&writer, table_clone.clone(), rid, LockMode::Exclusive)
            .expect("writer try lock should succeed eventually");
        proceed_clone.store(ok, AtomicOrdering::SeqCst);
        if ok {
            manager_clone.commit(&mut writer).expect("commit writer");
        } else {
            manager_clone.abort(&mut writer).expect("abort writer");
        }
    });

    thread::sleep(Duration::from_millis(50));
    assert!(
        !proceed.load(AtomicOrdering::SeqCst),
        "writer should still be blocked"
    );

    manager.commit(&mut reader).expect("commit reader");
    handle.join().expect("writer thread");
    assert!(
        proceed.load(AtomicOrdering::SeqCst),
        "writer should acquire lock after reader commit"
    );
}

#[test]
fn repeatable_read_sees_consistent_snapshot_after_update() {
    let mut db = Database::new_temp().expect("database");
    db.run("create table mvcc_t(id int primary key, val int)")
        .expect("create table");
    db.run("insert into mvcc_t values (1, 10)")
        .expect("seed row");

    let mut rr_session = SessionContext::new(IsolationLevel::RepeatableRead);
    rr_session.set_autocommit(false);
    db.run_with_session(&mut rr_session, "start transaction")
        .expect("start rr txn");

    let rows = db
        .run_with_session(&mut rr_session, "select val from mvcc_t where id = 1")
        .expect("rr initial read");
    assert_eq!(value_as_i32(&rows[0].data[0]), 10);

    let mut rc_session = SessionContext::new(IsolationLevel::ReadCommitted);
    rc_session.set_autocommit(false);
    db.run_with_session(&mut rc_session, "start transaction")
        .expect("start rc txn");
    db.run_with_session(&mut rc_session, "update mvcc_t set val = 20 where id = 1")
        .expect("perform update");
    db.run_with_session(&mut rc_session, "commit")
        .expect("commit updater");

    let rows = db
        .run_with_session(&mut rr_session, "select val from mvcc_t where id = 1")
        .expect("rr snapshot read");
    assert_eq!(value_as_i32(&rows[0].data[0]), 10);

    db.run_with_session(&mut rr_session, "commit")
        .expect("commit rr");

    let rows = db
        .run("select val from mvcc_t where id = 1")
        .expect("post commit read");
    assert_eq!(value_as_i32(&rows[0].data[0]), 20);
}
