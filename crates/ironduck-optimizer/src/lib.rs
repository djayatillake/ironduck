//! IronDuck Optimizer - Query optimization
//!
//! Applies optimization rules to logical plans.

use ironduck_common::Result;
use ironduck_planner::LogicalPlan;

pub mod rules;

use rules::{ConstantFolding, FilterPushdown, PredicateSimplification, ProjectionPushdown};

/// All optimization rules in order of application
fn get_rules() -> Vec<Box<dyn OptimizationRule>> {
    vec![
        Box::new(ConstantFolding),
        Box::new(PredicateSimplification),
        Box::new(FilterPushdown),
        Box::new(ProjectionPushdown),
    ]
}

/// Optimize a logical plan by applying all rules repeatedly until no changes
pub fn optimize(mut plan: LogicalPlan) -> Result<LogicalPlan> {
    let rules = get_rules();
    let max_iterations = 10; // Prevent infinite loops

    for _ in 0..max_iterations {
        let mut changed = false;

        for rule in &rules {
            if let Some(new_plan) = rule.apply(&plan) {
                plan = new_plan;
                changed = true;
            }
        }

        if !changed {
            break;
        }
    }

    Ok(plan)
}

/// An optimization rule
pub trait OptimizationRule: Send + Sync {
    /// Name of this rule
    fn name(&self) -> &str;

    /// Apply this rule to a logical plan
    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan>;
}
