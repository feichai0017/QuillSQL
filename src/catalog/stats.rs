use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::catalog::Schema;
use crate::error::QuillSQLResult;
use crate::expression::BinaryOp;
use crate::storage::table_heap::{TableHeap, TableIterator};
use crate::storage::tuple::Tuple;
use crate::utils::scalar::ScalarValue;

const DISTINCT_SAMPLE_LIMIT: usize = 1024;
const MIN_SELECTIVITY: f64 = 0.01;

/// Basic per-table statistics derived from a table scan.
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Number of visible (non-deleted) tuples discovered.
    pub row_count: u64,
    /// Per-column stats keyed by column name.
    pub column_stats: HashMap<String, ColumnStatistics>,
}

impl TableStatistics {
    pub fn empty(schema: &Schema) -> Self {
        let column_stats = schema
            .columns
            .iter()
            .map(|col| (col.name.clone(), ColumnStatistics::default()))
            .collect();
        Self {
            row_count: 0,
            column_stats,
        }
    }

    pub fn record_tuple(&mut self, tuple: &Tuple) {
        self.row_count += 1;
        for (column, value) in tuple.schema.columns.iter().zip(tuple.data.iter()) {
            if let Some(stats) = self.column_stats.get_mut(&column.name) {
                stats.record_value(value);
            }
        }
    }

    pub fn analyze(table_heap: Arc<TableHeap>) -> QuillSQLResult<Self> {
        let mut stats = TableStatistics::empty(table_heap.schema.as_ref());
        let mut iterator = TableIterator::new(table_heap.clone(), ..);
        while let Some((_rid, meta, tuple)) = iterator.next()? {
            if meta.is_deleted {
                continue;
            }
            stats.record_tuple(&tuple);
        }
        Ok(stats)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    pub null_count: u64,
    pub non_null_count: u64,
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    distinct_sample: HashSet<ScalarValue>,
    sampled_non_null_count: u64,
    distinct_estimate: f64,
}

impl ColumnStatistics {
    pub fn record_value(&mut self, value: &ScalarValue) {
        if value.is_null() {
            self.null_count += 1;
            return;
        }

        self.non_null_count += 1;
        let is_new_min = self.min.as_ref().map_or(true, |min| {
            matches!(value.partial_cmp(min), Some(Ordering::Less))
        });
        if is_new_min {
            self.min = Some(value.clone());
        }

        let is_new_max = self.max.as_ref().map_or(true, |max| {
            matches!(value.partial_cmp(max), Some(Ordering::Greater))
        });
        if is_new_max {
            self.max = Some(value.clone());
        }

        if self.sampled_non_null_count < DISTINCT_SAMPLE_LIMIT as u64 {
            self.sampled_non_null_count += 1;
            self.distinct_sample.insert(value.clone());
            self.distinct_estimate = self.distinct_sample.len() as f64;
        } else if self.sampled_non_null_count > 0 {
            let sample_unique = self.distinct_sample.len() as f64;
            let sample_size = self.sampled_non_null_count as f64;
            self.distinct_estimate = (sample_unique / sample_size) * self.non_null_count as f64;
        } else {
            self.distinct_estimate = self.non_null_count as f64;
        }
    }

    pub fn total_count(&self) -> u64 {
        self.null_count + self.non_null_count
    }

    pub fn estimated_distinct_count(&self) -> Option<f64> {
        if self.non_null_count == 0 {
            return None;
        }
        let estimate = if self.distinct_estimate > 0.0 {
            self.distinct_estimate
        } else {
            self.non_null_count as f64
        };
        Some(estimate.max(1.0))
    }

    pub fn selectivity_for_op(&self, op: BinaryOp, literal: &ScalarValue) -> Option<f64> {
        match op {
            BinaryOp::Eq => self
                .estimated_distinct_count()
                .map(|distinct| (1.0 / distinct).max(MIN_SELECTIVITY)),
            BinaryOp::NotEq => self
                .estimated_distinct_count()
                .map(|distinct| (1.0 - 1.0 / distinct).clamp(MIN_SELECTIVITY, 1.0)),
            BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq => {
                self.range_selectivity(op, literal)
            }
            _ => None,
        }
    }

    fn range_selectivity(&self, op: BinaryOp, literal: &ScalarValue) -> Option<f64> {
        let min = self.min.as_ref()?;
        let max = self.max.as_ref()?;
        let literal_cast = literal.cast_to(&min.data_type()).ok()?;

        let min_f = scalar_to_f64(min)?;
        let max_f = scalar_to_f64(max)?;
        let literal_f = scalar_to_f64(&literal_cast)?;

        if max_f <= min_f {
            return Some(0.5);
        }
        let fraction = ((literal_f - min_f) / (max_f - min_f)).clamp(0.0, 1.0);
        let sel = match op {
            BinaryOp::Gt => 1.0 - fraction,
            BinaryOp::GtEq => 1.0 - fraction,
            BinaryOp::Lt => fraction,
            BinaryOp::LtEq => fraction,
            _ => return None,
        };
        Some(sel.clamp(MIN_SELECTIVITY, 1.0))
    }
}

fn scalar_to_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(*v as f64),
        ScalarValue::Int16(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::UInt8(Some(v)) => Some(*v as f64),
        ScalarValue::UInt16(Some(v)) => Some(*v as f64),
        ScalarValue::UInt32(Some(v)) => Some(*v as f64),
        ScalarValue::UInt64(Some(v)) => Some(*v as f64),
        ScalarValue::Float32(Some(v)) => Some(*v as f64),
        ScalarValue::Float64(Some(v)) => Some(*v),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq_selectivity_reflects_distinct_count() {
        let mut stats = ColumnStatistics::default();
        stats.record_value(&ScalarValue::Int32(Some(1)));
        stats.record_value(&ScalarValue::Int32(Some(2)));
        stats.record_value(&ScalarValue::Int32(Some(1)));

        let sel = stats
            .selectivity_for_op(BinaryOp::Eq, &ScalarValue::Int32(Some(1)))
            .unwrap();
        assert!(sel > 0.4 && sel < 0.6);
    }

    #[test]
    fn range_selectivity_uses_min_max() {
        let mut stats = ColumnStatistics::default();
        for v in 0..=10 {
            stats.record_value(&ScalarValue::Int32(Some(v)));
        }
        let gt_sel = stats
            .selectivity_for_op(BinaryOp::Gt, &ScalarValue::Int32(Some(5)))
            .unwrap();
        assert!(gt_sel > 0.4 && gt_sel < 0.6);
    }
}
