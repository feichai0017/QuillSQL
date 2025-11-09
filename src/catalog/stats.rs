use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::Schema;
use crate::error::QuillSQLResult;
use crate::storage::table_heap::{TableHeap, TableIterator};
use crate::storage::tuple::Tuple;
use crate::utils::scalar::ScalarValue;

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
    }

    pub fn total_count(&self) -> u64 {
        self.null_count + self.non_null_count
    }
}
