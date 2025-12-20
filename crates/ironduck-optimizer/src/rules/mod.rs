//! Optimization rules

pub mod filter_pushdown;
pub mod constant_folding;

pub use filter_pushdown::FilterPushdown;
pub use constant_folding::ConstantFolding;
