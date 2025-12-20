//! IronDuck Optimizer - Query optimization
//!
//! Applies optimization rules to logical plans.

use ironduck_common::Result;
use ironduck_planner::LogicalPlan;

pub mod rules;

/// Optimize a logical plan
pub fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
    // TODO: Apply optimization rules
    Ok(plan)
}

/// An optimization rule
pub trait OptimizationRule: Send + Sync {
    /// Name of this rule
    fn name(&self) -> &str;

    /// Apply this rule to a logical plan
    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan>;
}
