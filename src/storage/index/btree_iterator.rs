use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::buffer::{ReadPageGuard, INVALID_PAGE_ID};
use crate::error::QuillSQLResult;
use crate::storage::codec::BPlusTreeLeafPageCodec;
use crate::storage::page::RecordId;
use crate::storage::tuple::Tuple;

use super::btree_index::BPlusTreeIndex;

#[derive(Debug)]
pub struct TreeIndexIterator {
    index: Arc<BPlusTreeIndex>,
    start_bound: Bound<Tuple>,
    end_bound: Bound<Tuple>,
    current_guard: Option<ReadPageGuard>,
    cursor: usize,
    started: bool,
}

impl TreeIndexIterator {
    /// Create a new iterator over a range
    pub fn new<R: RangeBounds<Tuple>>(index: Arc<BPlusTreeIndex>, range: R) -> Self {
        Self {
            index,
            start_bound: range.start_bound().cloned(),
            end_bound: range.end_bound().cloned(),
            current_guard: None,
            cursor: 0,
            started: false,
        }
    }

    /// Iterate to the next RID in order
    pub fn next(&mut self) -> QuillSQLResult<Option<RecordId>> {
        if !self.started {
            let root_page_id = self.index.get_root_page_id()?;
            if root_page_id == INVALID_PAGE_ID {
                return Ok(None);
            }

            match &self.start_bound {
                Bound::Included(k) | Bound::Excluded(k) => {
                    if self.current_guard.is_none() {
                        let guard = self.index.find_leaf_page_for_iterator(k, root_page_id)?;
                        let (leaf, _) = BPlusTreeLeafPageCodec::decode(
                            &guard.data,
                            self.index.key_schema.clone(),
                        )?;
                        self.cursor = leaf
                            .next_closest(k, matches!(self.start_bound, Bound::Included(_)))
                            .unwrap_or(leaf.header.current_size as usize);
                        self.current_guard = Some(guard);
                    }
                }
                Bound::Unbounded => {
                    let guard = self.index.find_first_leaf_page()?;
                    self.current_guard = Some(guard);
                    self.cursor = 0;
                }
            };
            self.started = true;
        }

        if let Some(guard) = self.current_guard.as_ref() {
            // lightweight OLC on iterator read
            let (leaf, _) =
                BPlusTreeLeafPageCodec::decode(&guard.data, self.index.key_schema.clone())?;
            let v1 = leaf.header.version;
            if self.cursor >= leaf.header.current_size as usize {
                let next_page_id = leaf.header.next_page_id;
                if next_page_id == INVALID_PAGE_ID {
                    self.current_guard = None;
                    return Ok(None);
                }
                // prefetch next-next leaf to warm cache (best-effort)
                if let Ok((next_g, next_leaf)) = self
                    .index
                    .buffer_pool
                    .fetch_tree_leaf_page(next_page_id, self.index.key_schema.clone())
                {
                    if next_leaf.header.next_page_id != INVALID_PAGE_ID {
                        let _ = self
                            .index
                            .buffer_pool
                            .prefetch_page(next_leaf.header.next_page_id);
                    }
                    self.current_guard = Some(next_g);
                } else {
                    self.current_guard =
                        Some(self.index.buffer_pool.fetch_page_read(next_page_id)?);
                }
                self.cursor = 0;
                return self.next();
            }

            let (key, rid) = leaf.kv_at(self.cursor);

            let in_range = match &self.end_bound {
                Bound::Included(end_key) => key <= end_key,
                Bound::Excluded(end_key) => key < end_key,
                Bound::Unbounded => true,
            };

            if in_range {
                self.cursor += 1;
                // verify version unchanged; otherwise restart iterator lazily
                let v2 = leaf.header.version;
                if v1 == v2 {
                    return Ok(Some(*rid));
                } else {
                    // restart from current key to avoid duplicates
                    let restart_key = key.clone();
                    let root = self.index.get_root_page_id()?;
                    let guard = self.index.find_leaf_page_for_iterator(&restart_key, root)?;
                    self.current_guard = Some(guard);
                    let (leaf2, _) = BPlusTreeLeafPageCodec::decode(
                        &self.current_guard.as_ref().unwrap().data,
                        self.index.key_schema.clone(),
                    )?;
                    self.cursor = leaf2
                        .next_closest(&restart_key, true)
                        .unwrap_or(leaf2.header.current_size as usize);
                    return self.next();
                }
            }
        }

        Ok(None)
    }
}
