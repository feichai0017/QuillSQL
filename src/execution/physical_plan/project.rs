use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::expression::Expr;
use crate::{
    error::QuillSQLResult,
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
};

use super::PhysicalPlan;

#[derive(derive_new::new, Debug)]
pub struct PhysicalProject {
    pub exprs: Vec<Expr>,
    pub schema: SchemaRef,
    pub input: Arc<PhysicalPlan>,
}

impl VolcanoExecutor for PhysicalProject {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        self.input.init(context)
    }

    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        if let Some(tuple) = self.input.next(context)? {
            let mut new_values = Vec::with_capacity(self.exprs.len());
            for expr in &self.exprs {
                new_values.push(context.eval_expr(expr, &tuple)?);
            }
            Ok(Some(Tuple::new(self.output_schema(), new_values)))
        } else {
            Ok(None)
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl std::fmt::Display for PhysicalProject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Project")
    }
}
