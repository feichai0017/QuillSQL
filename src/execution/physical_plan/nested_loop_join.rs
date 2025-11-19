//! Teaching-first nested loop join with optional predicate evaluation.

use log::debug;
use std::cell::RefCell;
use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::expression::Expr;
use crate::{
    error::QuillSQLResult,
    execution::{ExecutionContext, VolcanoExecutor},
    plan::logical_plan::JoinType,
    storage::tuple::Tuple,
};

use super::PhysicalPlan;

#[derive(Debug)]
pub struct PhysicalNestedLoopJoin {
    pub join_type: JoinType,
    pub condition: Option<Expr>,
    pub left_input: Arc<PhysicalPlan>,
    pub right_input: Arc<PhysicalPlan>,
    pub schema: SchemaRef,

    left_tuple: RefCell<Option<Tuple>>,
}
impl PhysicalNestedLoopJoin {
    pub fn new(
        join_type: JoinType,
        condition: Option<Expr>,
        left_input: Arc<PhysicalPlan>,
        right_input: Arc<PhysicalPlan>,
        schema: SchemaRef,
    ) -> Self {
        PhysicalNestedLoopJoin {
            join_type,
            condition,
            left_input,
            right_input,
            schema,
            left_tuple: RefCell::new(None),
        }
    }
}
impl VolcanoExecutor for PhysicalNestedLoopJoin {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        debug!("init nested loop join executor");
        self.left_input.init(context)?;
        self.right_input.init(context)?;
        self.left_tuple.borrow_mut().take();
        Ok(())
    }
    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        let mut left_next_tuple = if self.left_tuple.borrow().is_none() {
            self.left_input.next(context)?
        } else {
            self.left_tuple.borrow().clone()
        };

        while left_next_tuple.is_some() {
            let left_tuple = left_next_tuple.clone().unwrap();

            let mut right_next_tuple = self.right_input.next(context)?;
            while right_next_tuple.is_some() {
                let right_tuple = right_next_tuple.unwrap();

                // TODO judge if matches
                if let Some(condition) = &self.condition {
                    let merged_tuple =
                        Tuple::try_merge(vec![left_tuple.clone(), right_tuple.clone()])?;
                    if context.eval_predicate(condition, &merged_tuple)? {
                        self.left_tuple.borrow_mut().replace(left_tuple.clone());
                        return Ok(Some(Tuple::try_merge(vec![left_tuple, right_tuple])?));
                    }
                } else {
                    // save latest left_next_result before return
                    self.left_tuple.borrow_mut().replace(left_tuple.clone());

                    return Ok(Some(Tuple::try_merge(vec![left_tuple, right_tuple])?));
                }

                right_next_tuple = self.right_input.next(context)?;
            }

            // reset right executor
            self.right_input.init(context)?;
            left_next_tuple = self.left_input.next(context)?;
        }
        Ok(None)
    }

    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl std::fmt::Display for PhysicalNestedLoopJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NestedLoopJoin")
    }
}
