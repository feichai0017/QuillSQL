use crate::transaction::{Transaction, TransactionId, TransactionState};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use crate::error::{QuillSQLError, QuillSQLResult};

/// 隔离级别枚举
#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted,
    SnapshotIsolation,
    Serializable,
}

/// 事务管理器
pub struct TransactionManager {
    /// 全局事务ID计数器
    next_txn_id: AtomicU64,
    /// 活跃事务表
    active_transactions: Arc<Mutex<HashMap<TransactionId, Transaction>>>,
}

impl TransactionManager {
    /// 创建新的事务管理器
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 开始一个新事务
    pub fn begin(&self, _isolation_level: IsolationLevel) -> QuillSQLResult<Transaction> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let txn = Transaction::new(txn_id);
        
        // 将事务添加到活跃事务表
        let mut active_txns = self.active_transactions.lock()
            .map_err(|_| QuillSQLError::Internal("Failed to acquire lock".to_string()))?;
        active_txns.insert(txn_id, Transaction::new(txn_id));
        
        Ok(txn)
    }

    /// 提交事务
    pub fn commit(&self, mut txn: Transaction) -> QuillSQLResult<bool> {
        if !txn.is_running() {
            return Err(QuillSQLError::Transaction(
                format!("Cannot commit transaction {} in state {:?}", txn.txn_id, txn.state)
            ));
        }

        // 标记事务为已提交
        txn.commit();
        
        // 从活跃事务表中移除
        let mut active_txns = self.active_transactions.lock()
            .map_err(|_| QuillSQLError::Internal("Failed to acquire lock".to_string()))?;
        active_txns.remove(&txn.txn_id);
        
        Ok(true)
    }

    /// 中止事务
    pub fn abort(&self, mut txn: Transaction) -> QuillSQLResult<()> {
        // 标记事务为已中止
        txn.abort();
        
        // 从活跃事务表中移除
        let mut active_txns = self.active_transactions.lock()
            .map_err(|_| QuillSQLError::Internal("Failed to acquire lock".to_string()))?;
        active_txns.remove(&txn.txn_id);
        
        Ok(())
    }
    
    /// 获取活跃事务数量
    pub fn active_transaction_count(&self) -> QuillSQLResult<usize> {
        let active_txns = self.active_transactions.lock()
            .map_err(|_| QuillSQLError::Internal("Failed to acquire lock".to_string()))?;
        Ok(active_txns.len())
    }
    
    /// 检查事务是否活跃
    pub fn is_transaction_active(&self, txn_id: TransactionId) -> QuillSQLResult<bool> {
        let active_txns = self.active_transactions.lock()
            .map_err(|_| QuillSQLError::Internal("Failed to acquire lock".to_string()))?;
        Ok(active_txns.contains_key(&txn_id))
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}
