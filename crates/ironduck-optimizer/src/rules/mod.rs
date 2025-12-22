//! Optimization rules

pub mod constant_folding;
pub mod filter_pushdown;
pub mod predicate_simplification;
pub mod projection_pushdown;

pub use constant_folding::ConstantFolding;
pub use filter_pushdown::FilterPushdown;
pub use predicate_simplification::PredicateSimplification;
pub use projection_pushdown::ProjectionPushdown;
