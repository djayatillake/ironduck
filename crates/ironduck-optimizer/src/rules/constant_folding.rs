//! Constant folding optimization rule

use crate::OptimizationRule;
use ironduck_planner::LogicalPlan;

/// Evaluate constant expressions at compile time
pub struct ConstantFolding;

impl OptimizationRule for ConstantFolding {
    fn name(&self) -> &str {
        "constant_folding"
    }

    fn apply(&self, _plan: &LogicalPlan) -> Option<LogicalPlan> {
        // TODO: Implement constant folding
        None
    }
}
