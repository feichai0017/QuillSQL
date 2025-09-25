use crate::error::QuillSQLResult;
use crate::function::aggregate::Accumulator;
use crate::utils::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub struct CountAccumulator {
    count: i64,
}

impl CountAccumulator {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn update_value(&mut self, value: &ScalarValue) -> QuillSQLResult<()> {
        if !value.is_null() {
            self.count += 1;
        }
        Ok(())
    }

    fn evaluate(&self) -> QuillSQLResult<ScalarValue> {
        Ok(self.count.into())
    }
}
