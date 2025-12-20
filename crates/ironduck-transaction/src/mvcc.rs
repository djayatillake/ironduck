//! MVCC (Multi-Version Concurrency Control) implementation

use ironduck_common::{Error, Result};
use std::sync::atomic::{AtomicU64, Ordering};

pub type TransactionId = u64;
pub type Timestamp = u64;

/// State of a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committing,
    Committed,
    Aborted,
}

/// A transaction in the MVCC system
pub struct Transaction {
    /// Unique transaction ID
    pub id: TransactionId,
    /// Start timestamp
    pub start_ts: Timestamp,
    /// Commit timestamp (0 if not committed)
    pub commit_ts: AtomicU64,
    /// Current state
    pub state: TransactionState,
}

impl Transaction {
    pub fn new(id: TransactionId, start_ts: Timestamp) -> Self {
        Transaction {
            id,
            start_ts,
            commit_ts: AtomicU64::new(0),
            state: TransactionState::Active,
        }
    }

    /// Check if this transaction can see a version with the given timestamps
    pub fn is_visible(&self, begin_ts: Timestamp, end_ts: Timestamp) -> bool {
        // Version must have been committed before we started
        if begin_ts == 0 || begin_ts > self.start_ts {
            return false;
        }

        // Version must not have been deleted before we started
        if end_ts != u64::MAX && end_ts <= self.start_ts {
            return false;
        }

        true
    }

    /// Commit this transaction
    pub fn commit(&mut self) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(Error::TransactionAborted(
                "Cannot commit non-active transaction".to_string(),
            ));
        }

        self.state = TransactionState::Committing;
        let commit_ts = super::next_timestamp();
        self.commit_ts.store(commit_ts, Ordering::Release);
        self.state = TransactionState::Committed;

        Ok(())
    }

    /// Abort this transaction
    pub fn abort(&mut self) {
        self.state = TransactionState::Aborted;
    }

    /// Check if transaction is active
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    /// Check if transaction is committed
    pub fn is_committed(&self) -> bool {
        self.state == TransactionState::Committed
    }

    /// Get commit timestamp (0 if not committed)
    pub fn get_commit_ts(&self) -> Timestamp {
        self.commit_ts.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_new() {
        let txn = Transaction::new(1, 100);
        assert_eq!(txn.id, 1);
        assert_eq!(txn.start_ts, 100);
        assert!(txn.is_active());
    }

    #[test]
    fn test_transaction_commit() {
        let mut txn = Transaction::new(1, 100);
        assert!(txn.commit().is_ok());
        assert!(txn.is_committed());
        assert!(txn.get_commit_ts() > 0);
    }

    #[test]
    fn test_transaction_abort() {
        let mut txn = Transaction::new(1, 100);
        txn.abort();
        assert_eq!(txn.state, TransactionState::Aborted);
    }

    #[test]
    fn test_visibility() {
        let txn = Transaction::new(1, 100);

        // Version created before txn started, not deleted
        assert!(txn.is_visible(50, u64::MAX));

        // Version created after txn started
        assert!(!txn.is_visible(150, u64::MAX));

        // Version deleted before txn started
        assert!(!txn.is_visible(50, 80));

        // Version deleted after txn started
        assert!(txn.is_visible(50, 150));
    }
}
