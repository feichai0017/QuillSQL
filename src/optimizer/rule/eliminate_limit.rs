use crate::optimizer::logical_optimizer::ApplyOrder;
use crate::optimizer::LogicalOptimizerRule;
use crate::plan::logical_plan::{EmptyRelation, LogicalPlan};
use crate::error::QuillSQLResult;

pub struct EliminateLimit;

impl LogicalOptimizerRule for EliminateLimit {
    fn try_optimize(&self, plan: &LogicalPlan) -> QuillSQLResult<Option<LogicalPlan>> {
        if let LogicalPlan::Limit(limit) = plan {
            match limit.limit {
                Some(fetch) => {
                    if fetch == 0 {
                        return Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: limit.input.schema().clone(),
                        })));
                    }
                }
                None => {
                    if limit.offset == 0 {
                        let input = limit.input.as_ref();
                        // input also can be Limit, so we should apply again.
                        return Ok(Some(
                            self.try_optimize(input)?.unwrap_or_else(|| input.clone()),
                        ));
                    }
                }
            }
        }
        Ok(None)
    }

    fn name(&self) -> &str {
        "EliminateLimit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

#[cfg(test)]
mod tests {
    use crate::optimizer::rule::EliminateLimit;
    use crate::optimizer::LogicalOptimizer;
    use crate::plan::logical_plan::LogicalPlan;
    use crate::database::Database;
    use std::sync::Arc;

    fn build_optimizer() -> LogicalOptimizer {
        LogicalOptimizer::with_rules(vec![Arc::new(EliminateLimit)])
    }

    #[test]
    fn eliminate_limit() {
        let mut db = Database::new_temp().unwrap();
        db.run("create table t1 (a int)").unwrap();

        let plan = db.create_logical_plan("select a from t1 limit 0").unwrap();
        let optimized_plan = build_optimizer().optimize(&plan).unwrap();
        assert!(matches!(optimized_plan, LogicalPlan::EmptyRelation(_)));

        let plan = db.create_logical_plan("select a from t1 offset 0").unwrap();
        let optimized_plan = build_optimizer().optimize(&plan).unwrap();
        if let LogicalPlan::Project(p) = optimized_plan {
            assert!(matches!(p.input.as_ref(), LogicalPlan::TableScan(_)));
        } else {
            panic!("the first node should be project");
        }
    }
}
