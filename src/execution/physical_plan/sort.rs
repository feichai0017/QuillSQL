//! Full in-memory sort operator (teaching-friendly implementation).

use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::error::QuillSQLError;
use crate::plan::logical_plan::OrderByExpr;
use crate::utils::scalar::ScalarValue;
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

    all_tuples: RefCell<Vec<Tuple>>,
    cursor: AtomicUsize,
}
impl PhysicalSort {
    pub fn new(order_bys: Vec<OrderByExpr>, input: Arc<PhysicalPlan>) -> Self {
        PhysicalSort {
            order_bys,
            input,
            all_tuples: RefCell::new(Vec::new()),
            cursor: AtomicUsize::new(0),
        }
    }

    fn compare_keys(
        &self,
        left: &[ScalarValue],
        right: &[ScalarValue],
    ) -> QuillSQLResult<CmpOrdering> {
        for (idx, order) in self.order_bys.iter().enumerate() {
            let ordering = if order.asc {
                left[idx].partial_cmp(&right[idx])
            } else {
                right[idx].partial_cmp(&left[idx])
            }
            .ok_or_else(|| {
                QuillSQLError::Execution(format!(
                    "Can not compare {:?} and {:?}",
                    left[idx], right[idx]
                ))
            })?;
            if ordering != CmpOrdering::Equal {
                return Ok(ordering);
            }
        }
        Ok(CmpOrdering::Equal)
    }
}

impl VolcanoExecutor for PhysicalSort {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        self.input.init(context)?;
        self.all_tuples.borrow_mut().clear();
        self.cursor.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        if self.all_tuples.borrow().is_empty() {
            let mut keyed_rows: Vec<(Tuple, Vec<ScalarValue>)> = Vec::new();
            while let Some(tuple) = self.input.next(context)? {
                let mut keys = Vec::with_capacity(self.order_bys.len());
                for order in &self.order_bys {
                    keys.push(context.eval_expr(&order.expr, &tuple)?);
                }
                keyed_rows.push((tuple, keys));
            }

            let mut error = None;
            keyed_rows.sort_by(|(_, left_keys), (_, right_keys)| {
                match self.compare_keys(left_keys, right_keys) {
                    Ok(ord) => ord,
                    Err(e) => {
                        error = Some(e);
                        CmpOrdering::Equal
                    }
                }
            });
            if let Some(err) = error {
                return Err(err);
            }

            let tuples = keyed_rows
                .into_iter()
                .map(|(tuple, _)| tuple)
                .collect::<Vec<_>>();
            *self.all_tuples.borrow_mut() = tuples;
        }

        let cursor = self.cursor.fetch_add(1, Ordering::SeqCst);
        let tuples_ref = self.all_tuples.borrow();
        if cursor >= tuples_ref.len() {
            Ok(None)
        } else {
            Ok(tuples_ref.get(cursor).cloned())
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
