use crate::transaction::Transaction;

pub enum IsolationLevel {
    ReadUncommitted,
    SnapshotIsolation,
    Serializable,
}

pub struct TransactionManager {}

impl TransactionManager {
    pub fn begin(&self, _isolation_level: IsolationLevel) -> Transaction {
        todo!()
    }

    pub fn commit(&self, _txn: Transaction) -> bool {
        todo!()
    }

    pub fn abort(&self, _txn: Transaction) {
        todo!()
    }
}
