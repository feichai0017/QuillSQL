use log::debug;
use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::error::QuillSQLError;
use crate::expression::{Expr, ExprTrait};
use crate::utils::scalar::ScalarValue;
use crate::{
    error::QuillSQLResult,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
};

use super::PhysicalPlan;

#[derive(derive_new::new, Debug)]
pub struct PhysicalFilter {
    pub predicate: Expr,
    pub input: Arc<PhysicalPlan>,
}

impl VolcanoExecutor for PhysicalFilter {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        debug!("init filter executor");
        self.input.init(context)
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        loop {
            if let Some(tuple) = self.input.next(context)? {
                let compare_res = self.predicate.evaluate(&tuple)?;
                if let ScalarValue::Boolean(Some(v)) = compare_res {
                    if v {
                        return Ok(Some(tuple));
                    }
                } else {
                    return Err(QuillSQLError::Execution(
                        "filter predicate value should be boolean".to_string(),
                    ));
                }
            } else {
                return Ok(None);
            }
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.input.output_schema()
    }
}

impl std::fmt::Display for PhysicalFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter: {}", self.predicate)
    }
}
