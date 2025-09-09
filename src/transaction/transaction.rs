use crate::storage::tuple::Tuple;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub type TransactionId = u64;
pub const INVALID_TRANSACTION_ID: TransactionId = 0;

/// 事务状态枚举
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Running,
    Tainted,
    Committed,
    Aborted,
}

/// 事务结构体，包含事务的基本信息
#[derive(Debug)]
pub struct Transaction {
    pub txn_id: TransactionId,
    pub state: TransactionState,
    pub start_timestamp: u64,
}

impl Transaction {
    /// 创建新事务
    pub fn new(txn_id: TransactionId) -> Self {
        let start_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        
        Self {
            txn_id,
            state: TransactionState::Running,
            start_timestamp,
        }
    }
    
    /// 检查事务是否处于运行状态
    pub fn is_running(&self) -> bool {
        self.state == TransactionState::Running
    }
    
    /// 标记事务为已提交
    pub fn commit(&mut self) {
        self.state = TransactionState::Committed;
    }
    
    /// 标记事务为已中止
    pub fn abort(&mut self) {
        self.state = TransactionState::Aborted;
    }
    
    /// 标记事务为有问题的（需要回滚）
    pub fn set_tainted(&mut self) {
        self.state = TransactionState::Tainted;
    }
}

/// Represents a link to a previous version of this tuple
pub struct UndoLink {
    prev_txn: TransactionId,
    prev_log_idx: u32,
}

impl UndoLink {
    pub fn is_valid(&self) -> bool {
        self.prev_txn != INVALID_TRANSACTION_ID
    }
}

pub struct UndoLog {
    is_deleted: bool,
    modified_fields: Vec<bool>,
    tuple: Tuple,
    timestamp: u64,
    prev_version: UndoLink,
}
