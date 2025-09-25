use std::sync::atomic::{AtomicU32, Ordering};

use crate::catalog::SchemaRef;
use crate::expression::{Expr, ExprTrait};
use crate::storage::tuple::{Tuple, EMPTY_TUPLE};
use crate::utils::scalar::ScalarValue;
use crate::{
    error::QuillSQLResult,
    execution::{ExecutionContext, VolcanoExecutor},
};

#[derive(Debug)]
pub struct PhysicalValues {
    pub schema: SchemaRef,
    pub rows: Vec<Vec<Expr>>,

    cursor: AtomicU32,
}
impl PhysicalValues {
    pub fn new(schema: SchemaRef, rows: Vec<Vec<Expr>>) -> Self {
        PhysicalValues {
            schema,
            rows,
            cursor: AtomicU32::new(0),
        }
    }
}
impl VolcanoExecutor for PhysicalValues {
    fn next(&self, _context: &mut ExecutionContext) -> QuillSQLResult<Option<Tuple>> {
        let cursor = self.cursor.fetch_add(1, Ordering::SeqCst) as usize;
        if cursor < self.rows.len() {
            let values = self.rows[cursor]
                .iter()
                .map(|e| e.evaluate(&EMPTY_TUPLE))
                .collect::<QuillSQLResult<Vec<ScalarValue>>>()?;
            debug_assert_eq!(self.schema.column_count(), values.len());

            let casted_values = values
                .iter()
                .zip(self.schema.columns.iter())
                .map(|(val, col)| val.cast_to(&col.data_type))
                .collect::<QuillSQLResult<Vec<ScalarValue>>>()?;

            Ok(Some(Tuple::new(self.output_schema(), casted_values)))
        } else {
            Ok(None)
        }
    }

    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl std::fmt::Display for PhysicalValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Values: rows={}", self.rows.len())
    }
}
