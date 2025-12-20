//! Filter pushdown optimization rule

use crate::OptimizationRule;
use ironduck_planner::LogicalPlan;

/// Push filters closer to the data source
pub struct FilterPushdown;

impl OptimizationRule for FilterPushdown {
    fn name(&self) -> &str {
        "filter_pushdown"
    }

    fn apply(&self, _plan: &LogicalPlan) -> Option<LogicalPlan> {
        // TODO: Implement filter pushdown
        None
    }
}
