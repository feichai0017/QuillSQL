use crate::catalog::DataType;
use crate::utils::scalar::ScalarValue;
use crate::function::aggregate::Accumulator;
use crate::error::{QuillSQLError, QuillSQLResult};

#[derive(Debug, Clone)]
pub struct AvgAccumulator {
    sum: Option<f64>,
    count: u64,
}

impl AvgAccumulator {
    pub fn new() -> Self {
        Self {
            sum: None,
            count: 0,
        }
    }
}

impl Accumulator for AvgAccumulator {
    fn update_value(&mut self, value: &ScalarValue) -> QuillSQLResult<()> {
        if !value.is_null() {
            let value = match value.cast_to(&DataType::Float64)? {
                ScalarValue::Float64(Some(v)) => v,
                _ => {
                    return Err(QuillSQLError::Internal(format!(
                        "Failed to cast value {} to float64",
                        value
                    )))
                }
            };

            match self.sum {
                Some(sum) => self.sum = Some(sum + value),
                None => self.sum = Some(value),
            }
            self.count += 1;
        }
        Ok(())
    }

    fn evaluate(&self) -> QuillSQLResult<ScalarValue> {
        Ok(ScalarValue::Float64(
            self.sum.map(|f| f / self.count as f64),
        ))
    }
}
