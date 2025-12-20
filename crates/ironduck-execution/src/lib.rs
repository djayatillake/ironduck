//! IronDuck Execution - Vectorized query execution engine
//!
//! Implements push-based pipeline execution with vectorized operators.

pub mod chunk;
pub mod executor;
pub mod expression;
pub mod operator;
pub mod pipeline;
pub mod vector;
pub mod vectorized_expr;

pub use chunk::DataChunk;
pub use executor::{Executor, QueryResult};
pub use expression::evaluate;
pub use vector::Vector;
pub use vectorized_expr::evaluate_vectorized;
