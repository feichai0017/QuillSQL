use crate::error::QuillSQLResult;
use crate::optimizer::logical_optimizer::ApplyOrder;
use crate::optimizer::LogicalOptimizerRule;
use crate::plan::logical_plan::{Limit, LogicalPlan, Project, TableScan};
use std::sync::Arc;

/// Propagate LIMIT/OFFSET requirements into the underlying table scan so that
/// execution can stop scanning early.
pub struct PushLimitIntoScan;

impl LogicalOptimizerRule for PushLimitIntoScan {
    fn try_optimize(&self, plan: &LogicalPlan) -> QuillSQLResult<Option<LogicalPlan>> {
        let LogicalPlan::Limit(limit) = plan else {
            return Ok(None);
        };

        let Some(limit_value) = limit.limit else {
            return Ok(None);
        };

        let required_rows = limit.offset.saturating_add(limit_value);
        match limit.input.as_ref() {
            LogicalPlan::TableScan(scan) => {
                maybe_attach_limit(scan, required_rows).map_or(Ok(None), |new_scan| {
                    Ok(Some(LogicalPlan::Limit(Limit {
                        limit: limit.limit,
                        offset: limit.offset,
                        input: Arc::new(LogicalPlan::TableScan(new_scan)),
                    })))
                })
            }
            LogicalPlan::Project(project) => {
                if let LogicalPlan::TableScan(scan) = project.input.as_ref() {
                    maybe_attach_limit(scan, required_rows).map_or(Ok(None), |new_scan| {
                        let new_project = LogicalPlan::Project(Project {
                            exprs: project.exprs.clone(),
                            input: Arc::new(LogicalPlan::TableScan(new_scan)),
                            schema: project.schema.clone(),
                        });
                        Ok(Some(LogicalPlan::Limit(Limit {
                            limit: limit.limit,
                            offset: limit.offset,
                            input: Arc::new(new_project),
                        })))
                    })
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "PushLimitIntoScan"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

fn maybe_attach_limit(scan: &TableScan, required_rows: usize) -> Option<TableScan> {
    let mut new_scan = scan.clone();
    let new_limit = match new_scan.limit {
        Some(existing) => existing.min(required_rows),
        None => required_rows,
    };

    if new_scan.limit == Some(new_limit) {
        return None;
    }

    new_scan.limit = Some(new_limit);
    Some(new_scan)
}

#[cfg(test)]
mod tests {
    use crate::database::Database;
    use crate::optimizer::rule::PushLimitIntoScan;
    use crate::optimizer::LogicalOptimizer;
    use crate::plan::logical_plan::{Limit, LogicalPlan};
    use std::sync::Arc;

    fn build_optimizer() -> LogicalOptimizer {
        LogicalOptimizer::with_rules(vec![Arc::new(PushLimitIntoScan)])
    }

    #[test]
    fn pushes_limit_into_scan() {
        let mut db = Database::new_temp().unwrap();
        db.run("create table t1 (a int)").unwrap();

        let plan = db
            .create_logical_plan("select * from t1 limit 5 offset 2")
            .unwrap();
        let optimized_plan = build_optimizer().optimize(&plan).unwrap();

        match optimized_plan {
            LogicalPlan::Limit(Limit { input, .. }) => match input.as_ref() {
                LogicalPlan::Project(project) => match project.input.as_ref() {
                    LogicalPlan::TableScan(scan) => assert_eq!(scan.limit, Some(7)),
                    other => panic!("expected TableScan under project, got {other:?}"),
                },
                other => panic!("expected Project inside limit, got {other:?}"),
            },
            other => panic!("expected Limit after optimization, got {other:?}"),
        }
    }
}
