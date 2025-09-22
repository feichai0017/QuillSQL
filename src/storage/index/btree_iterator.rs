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
    // Byte offset to current cursor's KV within the leaf page
    kv_offset: usize,
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
            kv_offset: 0,
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
                        let (header, hdr_off) =
                            BPlusTreeLeafPageCodec::decode_header_only(&guard.data)?;
                        // find initial cursor position
                        let (full_leaf, _) = BPlusTreeLeafPageCodec::decode(
                            &guard.data,
                            self.index.key_schema.clone(),
                        )?;
                        self.cursor = full_leaf
                            .next_closest(k, matches!(self.start_bound, Bound::Included(_)))
                            .unwrap_or(header.current_size as usize);
                        // compute initial kv_offset by advancing from header once
                        let mut off = hdr_off;
                        for _ in 0..self.cursor {
                            let (_, new_off) = BPlusTreeLeafPageCodec::decode_kv_at_offset(
                                &guard.data,
                                self.index.key_schema.clone(),
                                off,
                            )?;
                            off = new_off;
                        }
                        self.kv_offset = off;
                        self.current_guard = Some(guard);
                    }
                }
                Bound::Unbounded => {
                    let guard = self.index.find_first_leaf_page()?;
                    self.current_guard = Some(guard);
                    self.cursor = 0;
                    let (_, hdr_off) = BPlusTreeLeafPageCodec::decode_header_only(
                        &self.current_guard.as_ref().unwrap().data,
                    )?;
                    self.kv_offset = hdr_off;
                }
            };
            self.started = true;
        }

        if let Some(guard) = self.current_guard.as_ref() {
            // lightweight OLC on iterator read
            // Fast path: header-only double-read for OLC
            let (h1, _) = BPlusTreeLeafPageCodec::decode_header_only(&guard.data)?;
            let v1 = h1.version;
            if self.cursor >= h1.current_size as usize {
                let next_page_id = h1.next_page_id;
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
                // reset kv_offset to the start of next leaf
                let (_nh, nh_off) = BPlusTreeLeafPageCodec::decode_header_only(
                    &self.current_guard.as_ref().unwrap().data,
                )?;
                self.kv_offset = nh_off;
                return self.next();
            }

            // decode key/rid at current cursor using cached offset
            let ((key, rid), new_off) = BPlusTreeLeafPageCodec::decode_kv_at_offset(
                &guard.data,
                self.index.key_schema.clone(),
                self.kv_offset,
            )?;

            let in_range = match &self.end_bound {
                Bound::Included(end_key) => &key <= end_key,
                Bound::Excluded(end_key) => &key < end_key,
                Bound::Unbounded => true,
            };

            if in_range {
                self.cursor += 1;
                // advance cached offset to next KV
                self.kv_offset = new_off;
                // verify version unchanged; otherwise restart iterator lazily
                let (h2, _) = BPlusTreeLeafPageCodec::decode_header_only(&guard.data)?;
                let v2 = h2.version;
                if v1 == v2 {
                    return Ok(Some(rid));
                } else {
                    // restart from current key to avoid duplicates
                    let restart_key = key.clone();
                    let root = self.index.get_root_page_id()?;
                    let guard = self.index.find_leaf_page_for_iterator(&restart_key, root)?;
                    self.current_guard = Some(guard);
                    // recompute cursor cheaply using header + linear seek
                    let (hdr2, hdr2_off) = BPlusTreeLeafPageCodec::decode_header_only(
                        &self.current_guard.as_ref().unwrap().data,
                    )?;
                    let mut off2 = hdr2_off;
                    let mut pos2 = 0usize;
                    while pos2 < hdr2.current_size as usize {
                        let ((k2, _), new_off2) = BPlusTreeLeafPageCodec::decode_kv_at_offset(
                            &self.current_guard.as_ref().unwrap().data,
                            self.index.key_schema.clone(),
                            off2,
                        )?;
                        if &k2 >= &restart_key {
                            break;
                        }
                        off2 = new_off2;
                        pos2 += 1;
                    }
                    self.cursor = pos2;
                    // set cached offset for the new cursor
                    self.kv_offset = off2;
                    return self.next();
                }
            }
        }

        Ok(None)
    }
}
