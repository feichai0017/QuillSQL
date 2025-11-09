use crate::error::QuillSQLResult;
use crate::optimizer::logical_optimizer::ApplyOrder;
use crate::optimizer::LogicalOptimizerRule;
use crate::plan::logical_plan::LogicalPlan;

/// Attach `Filter` predicates directly to the underlying `TableScan`.
/// This lets the physical planner decide whether the scan itself can honor
/// the predicate (e.g. via index) while keeping the logical tree shallower.
pub struct PushDownFilterToScan;

impl LogicalOptimizerRule for PushDownFilterToScan {
    fn try_optimize(&self, plan: &LogicalPlan) -> QuillSQLResult<Option<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(None);
        };

        match filter.input.as_ref() {
            LogicalPlan::TableScan(scan) => {
                let mut new_scan = scan.clone();
                new_scan.filters.push(filter.predicate.clone());
                Ok(Some(LogicalPlan::TableScan(new_scan)))
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "PushDownFilterToScan"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use crate::database::Database;
    use crate::optimizer::rule::PushDownFilterToScan;
    use crate::optimizer::LogicalOptimizer;
    use crate::plan::logical_plan::LogicalPlan;
    use std::sync::Arc;

    fn build_optimizer() -> LogicalOptimizer {
        LogicalOptimizer::with_rules(vec![Arc::new(PushDownFilterToScan)])
    }

    #[test]
    fn pushes_filter_into_scan() {
        let mut db = Database::new_temp().unwrap();
        db.run("create table t1 (a int)").unwrap();

        let plan = db
            .create_logical_plan("select * from t1 where a > 10")
            .unwrap();
        let optimized_plan = build_optimizer().optimize(&plan).unwrap();

        match optimized_plan {
            LogicalPlan::Project(project) => match project.input.as_ref() {
                LogicalPlan::TableScan(scan) => assert_eq!(scan.filters.len(), 1),
                other => panic!("expected TableScan under project, got {other:?}"),
            },
            other => panic!("expected Project after pushdown, got {other:?}"),
        }
    }
}
