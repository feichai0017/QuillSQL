use crate::error::QuillSQLResult;
use crate::optimizer::rule::{
    EliminateLimit, MergeLimit, PushDownFilterToScan, PushDownLimit, PushLimitIntoScan,
};
use crate::plan::logical_plan::LogicalPlan;
use std::sync::Arc;

/// `LogicalOptimizerRule` transforms one [`LogicalPlan`] into another which
/// computes the same results, but in a potentially more efficient
/// way. If there are no suitable transformations for the input plan,
/// the optimizer can simply return it as is.
pub trait LogicalOptimizerRule {
    /// Try and rewrite `plan` to an optimized form, returning None if the plan cannot be
    /// optimized by this rule.
    fn try_optimize(&self, plan: &LogicalPlan) -> QuillSQLResult<Option<LogicalPlan>>;

    /// A human-readable name for this optimizer rule
    fn name(&self) -> &str;

    /// How should the rule be applied by the optimizer
    ///
    /// If a rule use default None, it should traverse recursively plan inside itself
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

#[derive(Clone)]
pub struct LogicalOptimizer {
    /// All optimizer rules to apply
    pub rules: Vec<Arc<dyn LogicalOptimizerRule + Send + Sync>>,
    pub max_passes: usize,
}

impl LogicalOptimizer {
    pub fn new() -> Self {
        let rules: Vec<Arc<dyn LogicalOptimizerRule + Sync + Send>> = vec![
            Arc::new(PushDownFilterToScan),
            Arc::new(PushLimitIntoScan),
            Arc::new(EliminateLimit {}),
            Arc::new(MergeLimit {}),
            Arc::new(PushDownLimit {}),
        ];

        Self {
            rules,
            max_passes: 3,
        }
    }

    pub fn with_rules(rules: Vec<Arc<dyn LogicalOptimizerRule + Send + Sync>>) -> Self {
        Self {
            rules,
            max_passes: 3,
        }
    }

    pub fn optimize(&self, plan: &LogicalPlan) -> QuillSQLResult<LogicalPlan> {
        let mut new_plan = plan.clone();
        let mut i = 0;
        while i < self.max_passes {
            for rule in &self.rules {
                if let Some(optimized_plan) = self.optimize_recursively(rule, &new_plan)? {
                    new_plan = optimized_plan;
                }
            }

            i += 1;
        }
        Ok(new_plan)
    }

    pub fn optimize_recursively(
        &self,
        rule: &Arc<dyn LogicalOptimizerRule + Send + Sync>,
        plan: &LogicalPlan,
    ) -> QuillSQLResult<Option<LogicalPlan>> {
        match rule.apply_order() {
            Some(order) => match order {
                ApplyOrder::TopDown => {
                    let optimize_self_opt = rule.try_optimize(plan)?;
                    let optimize_inputs_opt = match &optimize_self_opt {
                        Some(optimized_plan) => self.optimize_inputs(rule, optimized_plan)?,
                        _ => self.optimize_inputs(rule, plan)?,
                    };
                    Ok(optimize_inputs_opt.or(optimize_self_opt))
                }
                ApplyOrder::BottomUp => {
                    let optimize_inputs_opt = self.optimize_inputs(rule, plan)?;
                    let optimize_self_opt = match &optimize_inputs_opt {
                        Some(optimized_plan) => rule.try_optimize(optimized_plan)?,
                        _ => rule.try_optimize(plan)?,
                    };
                    Ok(optimize_self_opt.or(optimize_inputs_opt))
                }
            },
            _ => rule.try_optimize(plan),
        }
    }

    fn optimize_inputs(
        &self,
        rule: &Arc<dyn LogicalOptimizerRule + Send + Sync>,
        plan: &LogicalPlan,
    ) -> QuillSQLResult<Option<LogicalPlan>> {
        let inputs = plan.inputs();
        let result = inputs
            .iter()
            .map(|sub_plan| self.optimize_recursively(rule, sub_plan))
            .collect::<QuillSQLResult<Vec<_>>>()?;
        if result.is_empty() || result.iter().all(|o| o.is_none()) {
            return Ok(None);
        }

        let new_inputs = result
            .into_iter()
            .zip(inputs)
            .map(|(new_plan, old_plan)| match new_plan {
                Some(plan) => plan,
                None => old_plan.clone(),
            })
            .collect::<Vec<_>>();
        plan.with_new_inputs(&new_inputs).map(Some)
    }
}
