use crate::catalog::{Catalog, TableStatistics};
use crate::expression::{BinaryExpr, BinaryOp, ColumnExpr, Expr};
use crate::plan::logical_plan::TableScan;
use crate::utils::scalar::ScalarValue;
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

    pub fn estimate_filter_selectivity(&self, scan: &TableScan) -> Option<f64> {
        let stats = self
            .catalog
            .table_statistics(&scan.table_ref)
            .filter(|stats| stats.row_count > 0)?;

        let mut combined = 1.0;
        let mut any = false;
        for expr in &scan.filters {
            if let Some(sel) = self.estimate_expr_selectivity(stats, expr) {
                combined *= sel;
                any = true;
            }
        }

        if any {
            Some(combined.clamp(MIN_SELECTIVITY, 1.0))
        } else {
            None
        }
    }

    pub fn index_scan_cost(&self, filtered_rows: f64) -> f64 {
        filtered_rows.max(1.0) * self.index_probe_per_row_cost
    }

    pub fn table_statistics(&self, table: &TableReference) -> Option<&TableStatistics> {
        self.catalog.table_statistics(table)
    }

    fn estimate_expr_selectivity(&self, stats: &TableStatistics, expr: &Expr) -> Option<f64> {
        match expr {
            Expr::Binary(binary) => self.estimate_binary_selectivity(stats, binary),
            _ => None,
        }
    }

    fn estimate_binary_selectivity(
        &self,
        stats: &TableStatistics,
        binary: &BinaryExpr,
    ) -> Option<f64> {
        let (column, literal, op) = normalize_column_predicate(binary)?;
        let column_stats = stats.column_stats.get(&column.name)?;
        column_stats.selectivity_for_op(op, &literal)
    }
}

const MIN_SELECTIVITY: f64 = 0.01;

fn normalize_column_predicate<'a>(
    binary: &'a BinaryExpr,
) -> Option<(&'a ColumnExpr, ScalarValue, BinaryOp)> {
    match (&*binary.left, &*binary.right) {
        (Expr::Column(column), Expr::Literal(lit)) => Some((column, lit.value.clone(), binary.op)),
        (Expr::Literal(lit), Expr::Column(column)) => {
            let flipped = flip_op(binary.op)?;
            Some((column, lit.value.clone(), flipped))
        }
        _ => None,
    }
}

fn flip_op(op: BinaryOp) -> Option<BinaryOp> {
    match op {
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::GtEq => Some(BinaryOp::LtEq),
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::LtEq => Some(BinaryOp::GtEq),
        BinaryOp::Eq => Some(BinaryOp::Eq),
        BinaryOp::NotEq => Some(BinaryOp::NotEq),
        _ => None,
    }
}
