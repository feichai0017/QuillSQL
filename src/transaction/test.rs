use crate::transaction::{Transaction, TransactionManager, IsolationLevel};
use crate::error::QuillSQLResult;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_manager_basic() -> QuillSQLResult<()> {
        let txn_manager = TransactionManager::new();
        
        // 测试开始事务
        let txn1 = txn_manager.begin(IsolationLevel::ReadUncommitted)?;
        assert!(txn1.is_running());
        assert_eq!(txn_manager.active_transaction_count()?, 1);
        
        // 测试第二个事务
        let txn2 = txn_manager.begin(IsolationLevel::SnapshotIsolation)?;
        assert_eq!(txn_manager.active_transaction_count()?, 2);
        
        // 测试提交第一个事务
        txn_manager.commit(txn1)?;
        assert_eq!(txn_manager.active_transaction_count()?, 1);
        
        // 测试中止第二个事务
        txn_manager.abort(txn2)?;
        assert_eq!(txn_manager.active_transaction_count()?, 0);
        
        Ok(())
    }

    #[test]
    fn test_transaction_state_transitions() -> QuillSQLResult<()> {
        let txn_manager = TransactionManager::new();
        let mut txn = txn_manager.begin(IsolationLevel::Serializable)?;
        
        // 初始状态应该是 Running
        assert!(txn.is_running());
        
        // 测试设置为 Tainted
        txn.set_tainted();
        assert!(!txn.is_running());
        
        // 创建新事务测试提交
        let mut txn2 = txn_manager.begin(IsolationLevel::ReadUncommitted)?;
        txn2.commit();
        assert!(!txn2.is_running());
        
        // 创建新事务测试中止
        let mut txn3 = txn_manager.begin(IsolationLevel::ReadUncommitted)?;
        txn3.abort();
        assert!(!txn3.is_running());
        
        Ok(())
    }

    #[test]
    fn test_transaction_id_uniqueness() -> QuillSQLResult<()> {
        let txn_manager = TransactionManager::new();
        
        let txn1 = txn_manager.begin(IsolationLevel::ReadUncommitted)?;
        let txn2 = txn_manager.begin(IsolationLevel::ReadUncommitted)?;
        let txn3 = txn_manager.begin(IsolationLevel::ReadUncommitted)?;
        
        // 确保事务ID是唯一的
        assert_ne!(txn1.txn_id, txn2.txn_id);
        assert_ne!(txn2.txn_id, txn3.txn_id);
        assert_ne!(txn1.txn_id, txn3.txn_id);
        
        // 确保事务ID是递增的
        assert!(txn1.txn_id < txn2.txn_id);
        assert!(txn2.txn_id < txn3.txn_id);
        
        Ok(())
    }
}