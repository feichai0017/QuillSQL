use crate::buffer::PAGE_SIZE;
use crate::error::{QuillSQLError, QuillSQLResult};
use crate::execution::physical_plan::PhysicalPlan;
use crate::plan::logical_plan::LogicalPlan;
use comfy_table::Cell;

use crate::storage::tuple::Tuple;

pub fn pretty_format_tuples(tuples: &Vec<Tuple>) -> comfy_table::Table {
    let mut table = comfy_table::Table::new();
    table.load_preset("||--+-++|    ++++++");

    if tuples.is_empty() {
        return table;
    }

    let schema = &tuples[0].schema;

    let mut header = Vec::new();
    for column in schema.columns.iter() {
        header.push(Cell::new(column.name.clone()));
    }
    table.set_header(header);

    for tuple in tuples {
        let mut cells = Vec::new();
        for value in tuple.data.iter() {
            cells.push(Cell::new(format!("{value}")));
        }
        table.add_row(cells);
    }

    table
}

pub fn pretty_format_logical_plan(plan: &LogicalPlan) -> String {
    pretty_format_logical_plan_recursively(plan, 0)
}

fn pretty_format_logical_plan_recursively(plan: &LogicalPlan, indent: usize) -> String {
    let mut result = format!("{:indent$}{}", "", plan);

    for input in plan.inputs() {
        result.push('\n');
        result.push_str(&pretty_format_logical_plan_recursively(input, indent + 2));
    }
    result
}

pub fn pretty_format_physical_plan(plan: &PhysicalPlan) -> String {
    pretty_format_physical_plan_recursively(plan, 0)
}

fn pretty_format_physical_plan_recursively(plan: &PhysicalPlan, indent: usize) -> String {
    let mut result = format!("{:indent$}{}", "", plan);

    for input in plan.inputs() {
        result.push('\n');
        result.push_str(&pretty_format_physical_plan_recursively(input, indent + 2));
    }
    result
}

pub fn page_bytes_to_array(bytes: &[u8]) -> [u8; PAGE_SIZE] {
    let mut data = [0u8; PAGE_SIZE];
    data.copy_from_slice(bytes);
    data
}

pub fn time() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// Compute the minimal contiguous diff window between two equal-length slices.
/// Returns Some((start, end)) where [start, end) is the changed window; None if identical.
pub fn find_contiguous_diff(a: &[u8], b: &[u8]) -> Option<(usize, usize)> {
    if a.len() != b.len() {
        return None;
    }
    let mut start = 0usize;
    while start < a.len() && a[start] == b[start] {
        start += 1;
    }
    if start == a.len() {
        return None;
    }
    let mut end = a.len();
    while end > start && a[end - 1] == b[end - 1] {
        end -= 1;
    }
    Some((start, end))
}

/// Apply a delta to dst at given offset, returns false if OOB.
pub fn apply_delta_checked(dst: &mut [u8], offset: usize, data: &[u8]) -> bool {
    if offset >= dst.len() {
        return false;
    }
    match offset.checked_add(data.len()) {
        Some(end) if end <= dst.len() => {
            dst[offset..end].copy_from_slice(data);
            true
        }
        _ => false,
    }
}

pub fn extract_id_from_filename(entry: &std::path::Path) -> QuillSQLResult<u128> {
    entry
        .extension()
        .ok_or_else(|| {
            QuillSQLError::Internal("Missing extension (ie. not in format: data.<id>)".to_string())
        })?
        .to_str()
        .ok_or_else(|| {
            QuillSQLError::Internal("Failed to convert extension to string".to_string())
        })?
        .parse::<u128>()
        .map_err(|e| QuillSQLError::Internal(format!("Failed to parse id: {}", e)))
}
