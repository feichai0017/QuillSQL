use crate::error::{QuillSQLError, QuillSQLResult};
use crate::transaction::{IsolationLevel, Transaction};
use sqlparser::ast::TransactionAccessMode;
/// Session-level state used to manage transactions and defaults for a client connection.
pub struct SessionContext {
    default_isolation: IsolationLevel,
    default_access_mode: TransactionAccessMode,
    autocommit: bool,
    active_txn: Option<Transaction>,
    pending_session_isolation: Option<IsolationLevel>,
    pending_session_access: Option<TransactionAccessMode>,
}

impl SessionContext {
    pub fn new(default_isolation: IsolationLevel) -> Self {
        Self {
            default_isolation,
            default_access_mode: TransactionAccessMode::ReadWrite,
            autocommit: true,
            active_txn: None,
            pending_session_isolation: None,
            pending_session_access: None,
        }
    }

    pub fn default_isolation(&self) -> IsolationLevel {
        self.default_isolation
    }

    pub fn set_default_isolation(&mut self, isolation: IsolationLevel) {
        self.default_isolation = isolation;
    }

    pub fn default_access_mode(&self) -> TransactionAccessMode {
        self.default_access_mode
    }

    pub fn set_default_access_mode(&mut self, mode: TransactionAccessMode) {
        self.default_access_mode = mode;
    }

    /// Apply a new isolation level to the currently active transaction, if one exists.
    pub fn set_active_isolation(&mut self, isolation: IsolationLevel) {
        if let Some(txn) = self.active_txn.as_mut() {
            txn.set_isolation_level(isolation);
        }
    }

    pub fn autocommit(&self) -> bool {
        self.autocommit
    }

    pub fn set_autocommit(&mut self, enabled: bool) {
        self.autocommit = enabled;
    }

    /// Retrieve the pending session isolation override that will apply to the next transaction.
    pub fn pending_session_isolation(&self) -> Option<IsolationLevel> {
        self.pending_session_isolation
    }

    /// Record a pending session isolation override that should apply to the next transaction.
    pub fn set_pending_session_isolation(&mut self, isolation: Option<IsolationLevel>) {
        self.pending_session_isolation = isolation;
    }

    pub fn pending_session_access(&self) -> Option<TransactionAccessMode> {
        self.pending_session_access
    }

    pub fn set_pending_session_access(&mut self, mode: Option<TransactionAccessMode>) {
        self.pending_session_access = mode;
    }

    pub fn has_active_transaction(&self) -> bool {
        self.active_txn.is_some()
    }

    pub fn active_txn(&self) -> Option<&Transaction> {
        self.active_txn.as_ref()
    }

    pub fn active_txn_mut(&mut self) -> Option<&mut Transaction> {
        self.active_txn.as_mut()
    }

    pub fn set_active_transaction(&mut self, mut txn: Transaction) -> QuillSQLResult<()> {
        if self.active_txn.is_some() {
            return Err(QuillSQLError::Execution(
                "transaction already active in session".to_string(),
            ));
        }
        if let Some(isolation) = self.pending_session_isolation.take() {
            self.default_isolation = isolation;
            txn.set_isolation_level(isolation);
        } else {
            self.default_isolation = txn.isolation_level();
        }

        if let Some(mode) = self.pending_session_access.take() {
            self.default_access_mode = mode;
            txn.update_access_mode(mode);
        } else {
            self.default_access_mode = txn.access_mode();
        }

        self.active_txn = Some(txn);
        Ok(())
    }

    pub fn take_active_transaction(&mut self) -> Option<Transaction> {
        self.active_txn.take()
    }

    pub fn clear_active_transaction(&mut self) {
        self.active_txn = None;
    }

    /// Apply isolation override when a transaction-scoped SET TRANSACTION is issued.
    pub fn apply_transaction_modes(&mut self, modes: &crate::plan::logical_plan::TransactionModes) {
        if let Some(level) = modes.isolation_level {
            if let Some(txn) = self.active_txn.as_mut() {
                txn.set_isolation_level(level);
            }
        }
        if let Some(mode) = modes.access_mode {
            if let Some(txn) = self.active_txn.as_mut() {
                txn.update_access_mode(mode);
            }
        }
    }

    /// Merge `SET SESSION TRANSACTION` modes into session defaults.
    pub fn apply_session_modes(&mut self, modes: &crate::plan::logical_plan::TransactionModes) {
        if let Some(level) = modes.isolation_level {
            self.default_isolation = level;
            self.pending_session_isolation = Some(level);
        }
        if let Some(mode) = modes.access_mode {
            self.default_access_mode = mode;
            self.pending_session_access = Some(mode);
        }
    }
}
