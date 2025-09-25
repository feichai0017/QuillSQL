use std::cmp::Ordering as CmpOrdering;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::catalog::SchemaRef;
use crate::error::QuillSQLError;
use crate::expression::ExprTrait;
use crate::plan::logical_plan::OrderByExpr;
use crate::{
    error::QuillSQLResult,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
};

use super::PhysicalPlan;

#[derive(Debug)]
pub struct PhysicalSort {
    pub order_bys: Vec<OrderByExpr>,
    pub input: Arc<PhysicalPlan>,

    all_tuples: Mutex<Vec<Tuple>>,
    cursor: AtomicUsize,
}
impl PhysicalSort {
    pub fn new(order_bys: Vec<OrderByExpr>, input: Arc<PhysicalPlan>) -> Self {
        PhysicalSort {
            order_bys,
            input,
            all_tuples: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
        }
    }

    fn sort_tuples(&self, a: &Tuple, b: &Tuple) -> QuillSQLResult<CmpOrdering> {
        let mut ordering = CmpOrdering::Equal;
        let mut index = 0;
        while ordering == CmpOrdering::Equal && index < self.order_bys.len() {
            let a_value = self.order_bys[index].expr.evaluate(a)?;
            let b_value = self.order_bys[index].expr.evaluate(b)?;
            ordering = if self.order_bys[index].asc {
                a_value.partial_cmp(&b_value)
            } else {
                b_value.partial_cmp(&a_value)
            }
            .ok_or(QuillSQLError::Execution(format!(
                "Can not compare {:?} and {:?}",
                a_value, b_value
            )))?;
            index += 1;
        }
        Ok(ordering)
    }
}

impl VolcanoExecutor for PhysicalSort {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        self.input.init(context)?;
        *self.all_tuples.lock().unwrap() = vec![];
        self.cursor.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        let all_tuples_len = self.all_tuples.lock().unwrap().len();
        if all_tuples_len == 0 {
            // load all tuples from input
            let mut all_tuples = Vec::new();
            while let Some(tuple) = self.input.next(context)? {
                all_tuples.push(tuple);
            }

            // sort all tuples
            let mut error = None;
            all_tuples.sort_by(|a, b| {
                let ordering = self.sort_tuples(a, b);
                if let Ok(ordering) = ordering {
                    ordering
                } else {
                    error = Some(ordering.unwrap_err());
                    CmpOrdering::Equal
                }
            });
            if let Some(error) = error {
                return Err(error);
            }
            *self.all_tuples.lock().unwrap() = all_tuples;
        }

        let cursor = self.cursor.fetch_add(1, Ordering::SeqCst);
        if cursor >= self.all_tuples.lock().unwrap().len() {
            Ok(None)
        } else {
            Ok(self.all_tuples.lock().unwrap().get(cursor).cloned())
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.input.output_schema()
    }
}

impl std::fmt::Display for PhysicalSort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Sort: {}",
            self.order_bys
                .iter()
                .map(|e| format!("{e}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}
