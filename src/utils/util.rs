use crate::buffer::PAGE_SIZE;
use crate::execution::physical_plan::PhysicalPlan;
use crate::plan::logical_plan::LogicalPlan;
use crate::storage::index::BPlusTreeIndex;
use crate::error::{QuillSQLError, QuillSQLResult};
use comfy_table::Cell;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;

use crate::storage::page::BPlusTreePage;
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

pub(crate) fn pretty_format_index_tree(index: &BPlusTreeIndex) -> QuillSQLResult<String> {
    let mut display = String::new();

    if index.is_empty() {
        display.push_str("Empty tree.");
        return Ok(display);
    }
    // 层序遍历
    let mut curr_queue = VecDeque::new();
    curr_queue.push_back(index.root_page_id.load(Ordering::SeqCst));

    let mut level_index = 1;
    loop {
        if curr_queue.is_empty() {
            return Ok(display);
        }
        let mut next_queue = VecDeque::new();

        // 打印当前层
        display.push_str(&format!("B+ Tree Level No.{}:\n", level_index));

        let mut level_table = comfy_table::Table::new();
        level_table.load_preset("||--+-++|    ++++++");
        let mut level_header = vec![];
        let mut level_row = vec![];

        while let Some(page_id) = curr_queue.pop_front() {
            let (_, curr_page) = index
                .buffer_pool
                .fetch_tree_page(page_id, index.key_schema.clone())?;

            match curr_page {
                BPlusTreePage::Internal(internal_page) => {
                    // build page table
                    let mut page_table = comfy_table::Table::new();
                    page_table.load_preset("||--+-++|    ++++++");
                    let mut page_header = Vec::new();
                    let mut page_row = Vec::new();
                    for (tuple, page_id) in internal_page.array.iter() {
                        page_header.push(Cell::new(
                            tuple
                                .data
                                .iter()
                                .map(|v| format!("{v}"))
                                .collect::<Vec<_>>()
                                .join(", "),
                        ));
                        page_row.push(Cell::new(page_id));
                    }
                    page_table.set_header(page_header);
                    page_table.add_row(page_row);

                    level_header.push(Cell::new(format!(
                        "page_id={}, size: {}/{}",
                        page_id, internal_page.header.current_size, internal_page.header.max_size
                    )));
                    level_row.push(Cell::new(page_table));

                    next_queue.extend(internal_page.values());
                }
                BPlusTreePage::Leaf(leaf_page) => {
                    let mut page_table = comfy_table::Table::new();
                    page_table.load_preset("||--+-++|    ++++++");
                    let mut page_header = Vec::new();
                    let mut page_row = Vec::new();
                    for (tuple, rid) in leaf_page.array.iter() {
                        page_header.push(Cell::new(
                            tuple
                                .data
                                .iter()
                                .map(|v| format!("{v}"))
                                .collect::<Vec<_>>()
                                .join(", "),
                        ));
                        page_row.push(Cell::new(format!("{}-{}", rid.page_id, rid.slot_num)));
                    }
                    page_table.set_header(page_header);
                    page_table.add_row(page_row);

                    level_header.push(Cell::new(format!(
                        "page_id={}, size: {}/{}, next_page_id={}",
                        page_id,
                        leaf_page.header.current_size,
                        leaf_page.header.max_size,
                        leaf_page.header.next_page_id
                    )));
                    level_row.push(Cell::new(page_table));
                }
            }
        }
        level_table.set_header(level_header);
        level_table.add_row(level_row);
        display.push_str(&format!("{level_table}\n"));

        level_index += 1;
        curr_queue = next_queue;
    }
}


pub fn time() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

pub fn extract_id_from_filename(
    entry: &std::path::PathBuf,
) -> QuillSQLResult<u128> {
    entry
        .extension()
        .ok_or_else(|| QuillSQLError::Internal(format!("Missing extension (ie. not in format: data.<id>)")))?
        .to_str()
        .ok_or_else(|| QuillSQLError::Internal("Failed to convert extension to string".to_string()))?
        .parse::<u128>()
        .map_err(|e| QuillSQLError::Internal(format!("Failed to parse id: {}", e)))
}