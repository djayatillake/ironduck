//! IronDuck Planner - Logical query planning
//!
//! Converts bound statements into logical query plans.

pub mod logical_operator;
mod plan_builder;

pub use logical_operator::*;
pub use plan_builder::*;

use ironduck_binder::BoundStatement;
use ironduck_common::Result;

/// Create a logical plan from a bound statement
pub fn create_logical_plan(statement: &BoundStatement) -> Result<LogicalPlan> {
    plan_builder::build_plan(statement)
}

/// A logical query plan
#[derive(Debug, Clone)]
pub struct LogicalPlan {
    pub root: LogicalOperator,
    /// Output column names
    pub output_names: Vec<String>,
}

impl LogicalPlan {
    pub fn new(root: LogicalOperator, output_names: Vec<String>) -> Self {
        LogicalPlan { root, output_names }
    }

    /// Get the output types of this plan
    pub fn output_types(&self) -> Vec<ironduck_common::LogicalType> {
        self.root.output_types()
    }
}
