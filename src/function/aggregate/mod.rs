mod avg;
mod count;

pub use avg::AvgAccumulator;
pub use count::CountAccumulator;
use std::fmt::Debug;

use crate::error::QuillSQLResult;
use crate::utils::scalar::ScalarValue;
use strum::{EnumIter, IntoEnumIterator};

#[derive(Clone, PartialEq, Eq, Debug, EnumIter)]
pub enum AggregateFunctionKind {
    Count,
    Avg,
}

impl AggregateFunctionKind {
    pub fn create_accumulator(&self) -> Box<dyn Accumulator> {
        match self {
            AggregateFunctionKind::Count => Box::new(CountAccumulator::new()),
            AggregateFunctionKind::Avg => Box::new(AvgAccumulator::new()),
        }
    }

    pub fn find(name: &str) -> Option<Self> {
        AggregateFunctionKind::iter().find(|kind| kind.to_string().eq_ignore_ascii_case(name))
    }
}

impl std::fmt::Display for AggregateFunctionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

pub trait Accumulator: Send + Sync + Debug {
    /// Updates the accumulator's state from its input.
    fn update_value(&mut self, value: &ScalarValue) -> QuillSQLResult<()>;

    /// Returns the final aggregate value, consuming the internal state.
    fn evaluate(&self) -> QuillSQLResult<ScalarValue>;
}
