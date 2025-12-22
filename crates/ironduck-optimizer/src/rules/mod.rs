//! Optimization rules

pub mod constant_folding;
pub mod filter_pushdown;
pub mod projection_pushdown;

pub use constant_folding::ConstantFolding;
pub use filter_pushdown::FilterPushdown;
pub use projection_pushdown::ProjectionPushdown;
