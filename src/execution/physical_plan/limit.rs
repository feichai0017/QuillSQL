use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::catalog::SchemaRef;
use crate::{
    execution::{ExecutionContext, VolcanoExecutor},
    storage::tuple::Tuple,
    error::QuillSQLResult,
};

use super::PhysicalPlan;

#[derive(Debug)]
pub struct PhysicalLimit {
    pub limit: Option<usize>,
    pub offset: usize,
    pub input: Arc<PhysicalPlan>,

    cursor: AtomicUsize,
}
impl PhysicalLimit {
    pub fn new(limit: Option<usize>, offset: usize, input: Arc<PhysicalPlan>) -> Self {
        PhysicalLimit {
            limit,
            offset,
            input,
            cursor: AtomicUsize::new(0),
        }
    }
}
impl VolcanoExecutor for PhysicalLimit {
    fn init(&self, context: &mut ExecutionContext) -> QuillSQLResult<()> {
        self.input.init(context)?;
        self.cursor.store(0, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn next(&self, context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        loop {
            let next_tuple = self.input.next(context)?;
            if next_tuple.is_none() {
                return Ok(None);
            }
            let cursor = self
                .cursor
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if cursor < self.offset {
                continue;
            }
            return if let Some(limit) = self.limit {
                if cursor < self.offset + limit {
                    Ok(next_tuple)
                } else {
                    Ok(None)
                }
            } else {
                Ok(next_tuple)
            };
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.input.output_schema()
    }
}

impl std::fmt::Display for PhysicalLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Limit")
    }
}
