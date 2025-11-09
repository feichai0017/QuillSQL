use crate::catalog::{Catalog, TableStatistics};
use crate::plan::logical_plan::TableScan;
use crate::utils::table_ref::TableReference;

/// Lightweight cost model shared by planner components.
#[derive(Debug, Clone)]
pub struct CostEstimator<'a> {
    catalog: &'a Catalog,
    default_row_estimate: f64,
    seq_scan_per_row_cost: f64,
    index_probe_per_row_cost: f64,
}

impl<'a> CostEstimator<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            default_row_estimate: 1000.0,
            seq_scan_per_row_cost: 1.0,
            index_probe_per_row_cost: 4.0,
        }
    }

    pub fn estimate_table_scan_rows(&self, scan: &TableScan) -> f64 {
        scan.estimated_row_count
            .map(|c| c as f64)
            .or_else(|| {
                self.catalog
                    .table_statistics(&scan.table_ref)
                    .map(|stats| stats.row_count as f64)
            })
            .filter(|count| *count > 0.0)
            .unwrap_or(self.default_row_estimate)
    }

    pub fn seq_scan_cost(&self, scan: &TableScan) -> f64 {
        self.estimate_table_scan_rows(scan) * self.seq_scan_per_row_cost
    }

    pub fn index_scan_cost(&self, scan: &TableScan, filtered_rows: Option<f64>) -> f64 {
        let rows = filtered_rows.unwrap_or_else(|| self.estimate_table_scan_rows(scan));
        rows * self.index_probe_per_row_cost
    }

    pub fn table_statistics(&self, table: &TableReference) -> Option<&TableStatistics> {
        self.catalog.table_statistics(table)
    }
}
