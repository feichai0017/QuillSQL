use crate::error::QuillSQLResult;
use crate::optimizer::logical_optimizer::ApplyOrder;
use crate::optimizer::LogicalOptimizerRule;
use crate::plan::logical_plan::{LogicalPlan, Sort};

pub struct PushDownLimit;

impl LogicalOptimizerRule for PushDownLimit {
    fn try_optimize(&self, plan: &LogicalPlan) -> QuillSQLResult<Option<LogicalPlan>> {
        let LogicalPlan::Limit(limit) = plan else {
            return Ok(None);
        };

        let Some(limit_value) = limit.limit else {
            return Ok(None);
        };

        match limit.input.as_ref() {
            LogicalPlan::Sort(sort) => {
                let new_limit = {
                    let sort_limit = limit.offset + limit_value;
                    Some(sort.limit.map(|f| f.min(sort_limit)).unwrap_or(sort_limit))
                };
                if new_limit == sort.limit {
                    Ok(None)
                } else {
                    let new_sort = LogicalPlan::Sort(Sort {
                        order_by: sort.order_by.clone(),
                        input: sort.input.clone(),
                        limit: new_limit,
                    });
                    plan.with_new_inputs(&[new_sort]).map(Some)
                }
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "PushDownLimit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use crate::database::Database;
    use crate::optimizer::rule::PushDownLimit;
    use crate::optimizer::LogicalOptimizer;
    use crate::plan::logical_plan::{LogicalPlan, Sort};
    use std::sync::Arc;

    fn build_optimizer() -> LogicalOptimizer {
        LogicalOptimizer::with_rules(vec![Arc::new(PushDownLimit)])
    }

    #[test]
    fn push_down_limit() {
        let mut db = Database::new_temp().unwrap();
        db.run("create table t1 (a int)").unwrap();

        let plan = db
            .create_logical_plan("select a from t1 order by a limit 10")
            .unwrap();
        let optimized_plan = build_optimizer().optimize(&plan).unwrap();

        if let LogicalPlan::Limit(limit) = optimized_plan {
            if let LogicalPlan::Sort(Sort { limit, .. }) = limit.input.as_ref() {
                assert_eq!(limit, &Some(10));
            } else {
                panic!("the second node should be limit");
            }
        } else {
            panic!("the first node should be limit");
        }
    }
}
