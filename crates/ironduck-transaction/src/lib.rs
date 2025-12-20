//! IronDuck Transaction - MVCC transaction management

use ironduck_common::Result;
use std::sync::atomic::{AtomicU64, Ordering};

pub mod mvcc;

pub use mvcc::{Transaction, TransactionId, TransactionState};

/// Global timestamp counter
static GLOBAL_TIMESTAMP: AtomicU64 = AtomicU64::new(1);

/// Get the next global timestamp
pub fn next_timestamp() -> u64 {
    GLOBAL_TIMESTAMP.fetch_add(1, Ordering::SeqCst)
}

/// Transaction manager
pub struct TransactionManager {
    next_txn_id: AtomicU64,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            next_txn_id: AtomicU64::new(1),
        }
    }

    /// Begin a new transaction
    pub fn begin(&self) -> Transaction {
        let id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let start_ts = next_timestamp();
        Transaction::new(id, start_ts)
    }

    /// Commit a transaction
    pub fn commit(&self, txn: &mut Transaction) -> Result<()> {
        txn.commit()
    }

    /// Abort a transaction
    pub fn abort(&self, txn: &mut Transaction) {
        txn.abort()
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}
