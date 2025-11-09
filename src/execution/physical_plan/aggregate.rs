use crate::catalog::SchemaRef;
use crate::error::QuillSQLError;
use crate::execution::physical_plan::PhysicalPlan;
use crate::execution::{ExecutionContext, VolcanoExecutor};
use crate::expression::Expr;
use crate::function::Accumulator;
use crate::utils::scalar::ScalarValue;
use crate::{error::QuillSQLResult, storage::tuple::Tuple};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct PhysicalAggregate {
    /// The incoming physical plan
    pub input: Arc<PhysicalPlan>,
    /// Grouping expressions
    pub group_exprs: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_exprs: Vec<Expr>,
    /// The schema description of the aggregate output
    pub schema: SchemaRef,

    pub output_rows: Mutex<Vec<Tuple>>,
    pub cursor: AtomicUsize,
}

impl PhysicalAggregate {
    pub fn new(
        input: Arc<PhysicalPlan>,
        group_exprs: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            group_exprs,
            aggr_exprs,
            schema,
            output_rows: Mutex::new(vec![]),
            cursor: AtomicUsize::new(0),
        }
    }
}

impl PhysicalAggregate {
    fn build_accumulators(&self) -> QuillSQLResult<Vec<Box<dyn Accumulator>>> {
        self.aggr_exprs
            .iter()
            .map(|expr| {
                if let Expr::AggregateFunction(aggr) = expr {
                    Ok(aggr.func_kind.create_accumulator())
                } else {
                    Err(QuillSQLError::Execution(format!(
                        "aggr expr is not AggregateFunction instead of {}",
                        expr
                    )))
                }
            })
            .collect::<QuillSQLResult<Vec<Box<dyn Accumulator>>>>()
    }
}

impl VolcanoExecutor for PhysicalAggregate {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        self.input.init(context)?;
        self.cursor.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        let output_rows_len = self.output_rows.lock().unwrap().len();
        // build output rows
        if output_rows_len == 0 {
            let mut groups: HashMap<Vec<ScalarValue>, Vec<Box<dyn Accumulator>>> = HashMap::new();
            while let Some(tuple) = self.input.next(context)? {
                let group_key = self
                    .group_exprs
                    .iter()
                    .map(|e| context.eval_expr(e, &tuple))
                    .collect::<QuillSQLResult<Vec<ScalarValue>>>()?;
                let group_accumulators = if let Some(acc) = groups.get_mut(&group_key) {
                    acc
                } else {
                    let accumulators = self.build_accumulators()?;
                    groups.insert(group_key.clone(), accumulators);
                    groups.get_mut(&group_key).unwrap()
                };
                for (idx, acc) in group_accumulators.iter_mut().enumerate() {
                    acc.update_value(&context.eval_expr(&self.aggr_exprs[idx], &tuple)?)?;
                }
            }

            for (group_key, accumulators) in groups.into_iter() {
                let mut values = accumulators
                    .iter()
                    .map(|acc| acc.evaluate())
                    .collect::<QuillSQLResult<Vec<ScalarValue>>>()?;
                values.extend(group_key);
                self.output_rows
                    .lock()
                    .unwrap()
                    .push(Tuple::new(self.schema.clone(), values));
            }
        }

        let cursor = self.cursor.fetch_add(1, Ordering::SeqCst);
        Ok(self.output_rows.lock().unwrap().get(cursor).cloned())
    }

    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl std::fmt::Display for PhysicalAggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aggregate")
    }
}
