use crate::optimizer::logical_optimizer::ApplyOrder;
use crate::optimizer::LogicalOptimizerRule;
use crate::plan::logical_plan::{Limit, LogicalPlan};
use crate::error::QuillSQLResult;
use std::cmp::min;
use std::sync::Arc;

pub struct MergeLimit;

impl LogicalOptimizerRule for MergeLimit {
    fn try_optimize(&self, plan: &LogicalPlan) -> QuillSQLResult<Option<LogicalPlan>> {
        let LogicalPlan::Limit(parent) = plan else {
            return Ok(None);
        };

        if let LogicalPlan::Limit(child) = &*parent.input {
            let new_limit = match (parent.limit, child.limit) {
                (Some(parent_limit), Some(child_limit)) => {
                    Some(min(parent_limit, child_limit.saturating_sub(parent.offset)))
                }
                (Some(parent_limit), None) => Some(parent_limit),
                (None, Some(child_limit)) => Some(child_limit.saturating_sub(parent.offset)),
                (None, None) => None,
            };
            let plan = LogicalPlan::Limit(Limit {
                limit: new_limit,
                offset: child.offset + parent.offset,
                input: Arc::new((*child.input).clone()),
            });
            self.try_optimize(&plan)
                .map(|opt_plan| opt_plan.or_else(|| Some(plan)))
        } else {
            Ok(None)
        }
    }

    fn name(&self) -> &str {
        "MergeLimit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::EMPTY_SCHEMA_REF;
    use crate::optimizer::rule::MergeLimit;
    use crate::optimizer::LogicalOptimizer;
    use crate::plan::logical_plan::{EmptyRelation, Limit, LogicalPlan};
    use std::sync::Arc;

    fn build_optimizer() -> LogicalOptimizer {
        LogicalOptimizer::with_rules(vec![Arc::new(MergeLimit)])
    }

    #[test]
    fn merge_limit() {
        let plan = LogicalPlan::Limit(Limit {
            limit: Some(10),
            offset: 0,
            input: Arc::new(LogicalPlan::Limit(Limit {
                limit: Some(1000),
                offset: 0,
                input: Arc::new(LogicalPlan::Limit(Limit {
                    limit: None,
                    offset: 10,
                    input: Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: EMPTY_SCHEMA_REF.clone(),
                    })),
                })),
            })),
        });
        let optimized_plan = build_optimizer().optimize(&plan).unwrap();

        if let LogicalPlan::Limit(Limit {
            limit,
            offset,
            input,
        }) = optimized_plan
        {
            assert_eq!(limit, Some(10));
            assert_eq!(offset, 10);
            assert!(matches!(input.as_ref(), LogicalPlan::EmptyRelation(_)));
        } else {
            panic!("the first node should be limit");
        }
    }
}
