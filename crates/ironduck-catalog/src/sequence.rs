//! Sequence management

use super::CatalogId;
use std::sync::atomic::{AtomicI64, Ordering};

pub type SequenceId = CatalogId;

/// A sequence generates sequential numeric values
pub struct Sequence {
    /// Unique identifier
    pub id: SequenceId,
    /// Sequence name
    pub name: String,
    /// Current value (atomically updated)
    current_value: AtomicI64,
    /// Increment per call
    pub increment: i64,
    /// Minimum value
    pub min_value: i64,
    /// Maximum value
    pub max_value: i64,
    /// Whether to cycle when reaching bounds
    pub cycle: bool,
}

impl Sequence {
    pub fn new(
        id: SequenceId,
        name: String,
        start: i64,
        increment: i64,
        min_value: i64,
        max_value: i64,
        cycle: bool,
    ) -> Self {
        Sequence {
            id,
            name,
            // Start at start - increment so first nextval returns start
            current_value: AtomicI64::new(start - increment),
            increment,
            min_value,
            max_value,
            cycle,
        }
    }

    /// Get the next value from the sequence
    pub fn nextval(&self) -> i64 {
        let prev = self.current_value.fetch_add(self.increment, Ordering::SeqCst);
        let new_val = prev + self.increment;

        // Handle bounds checking with cycling
        if self.increment > 0 && new_val > self.max_value {
            if self.cycle {
                self.current_value.store(self.min_value, Ordering::SeqCst);
                return self.min_value;
            }
            // Could error here but for simplicity just return max
        } else if self.increment < 0 && new_val < self.min_value {
            if self.cycle {
                self.current_value.store(self.max_value, Ordering::SeqCst);
                return self.max_value;
            }
        }

        new_val
    }

    /// Get the current value without incrementing
    pub fn currval(&self) -> i64 {
        self.current_value.load(Ordering::SeqCst) + self.increment
    }

    /// Set the current value
    pub fn setval(&self, value: i64) {
        self.current_value.store(value - self.increment, Ordering::SeqCst);
    }
}
