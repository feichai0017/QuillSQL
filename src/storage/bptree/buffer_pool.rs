use crate::error::{Error, Result};
use crate::storage::bptree::page::Page;
use crate::storage::bptree::page_layout::PAGE_SIZE;
// Remove Pager import
// use crate::storage::bptree::pager::Pager;
use crate::storage::bptree::disk_manager::DiskManager; // Import DiskManager
use crate::utils::cache::{FrameId, PageId};
use crate::utils::cache::lru_k::LruKReplacer;
use std::collections::HashMap;

// --- FrameGuard ---
// RAII guard for pages fetched from the buffer pool.
// Holds a reference/lock to the page data and automatically unpins on Drop.
// We need to decide on the locking strategy for the pool first.
// Let's assume we return a MutexGuard for now.

// Simple Frame structure for metadata
#[derive(Debug, Clone)] // Make metadata cloneable if needed
struct FrameMeta {
    page_id: Option<PageId>,
    pin_count: u32,
    is_dirty: bool,
}

/// Manages buffer pool pages, interfacing with DiskManager and LRU-K Replacer.
#[derive(Debug)] // Allow debugging buffer pool state
pub struct BufferPoolManager {
    pool: Vec<Page>, // Store Pages directly for simpler access with &mut self
    page_table: HashMap<PageId, FrameId>,
    frame_meta: Vec<FrameMeta>, // No Mutex needed with &mut self
    replacer: LruKReplacer,
    pub disk_manager: DiskManager,
    pool_size: usize,
    next_page_id: PageId, // Removed AtomicUsize, use simple usize with &mut self
    free_list: Vec<FrameId>, // Track free frames
}

impl BufferPoolManager {
    pub fn new(pool_size: usize, k: usize, disk_manager: DiskManager) -> Result<Self> {
        let mut pool = Vec::with_capacity(pool_size);
        let mut frame_meta = Vec::with_capacity(pool_size);
        let mut free_list = Vec::with_capacity(pool_size);

        for i in 0..pool_size {
            pool.push(Page::new([0u8; PAGE_SIZE]));
            frame_meta.push(FrameMeta {
                page_id: None,
                pin_count: 0,
                is_dirty: false,
            });
            free_list.push(i); // All frames are initially free
        }

        let next_page_id = disk_manager.get_num_pages();

        Ok(BufferPoolManager {
            pool,
            page_table: HashMap::new(),
            frame_meta,
            replacer: LruKReplacer::new(k),
            disk_manager,
            pool_size,
            next_page_id,
            free_list,
        })
    }

    /// Helper to find a free frame or evict one.
    fn get_victim_frame(&mut self) -> Option<FrameId> {
        if let Some(frame_id) = self.free_list.pop() {
            Some(frame_id)
        } else {
            self.replacer.evict()
        }
    }

    /// Internal helper to flush a specific frame if dirty.
    fn flush_frame_if_dirty(&mut self, frame_id: FrameId) -> Result<()> {
        let meta = &mut self.frame_meta[frame_id];
        if meta.is_dirty {
            if let Some(page_id) = meta.page_id {
                println!(
                    "DEBUG: Flushing dirty page {} from frame {}",
                    page_id, frame_id
                );
                let page_data = self.pool[frame_id].get_data();
                self.disk_manager.write_page(page_id, &page_data)?;
                meta.is_dirty = false;
            } else {
                // Should not happen: Dirty frame without page_id?
                eprintln!("WARN: Dirty frame {} has no associated page_id!", frame_id);
            }
        }
        Ok(())
    }

    /// Fetches the requested page from the buffer pool.
    /// Returns a mutable reference to the Page within the pool.
    /// The page is pinned and must be unpinned later using `unpin_page`.
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<Option<&mut Page>> {
        // 1. Check page table
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            println!("DEBUG: fetch_page {} found in frame {}", page_id, frame_id);
            // Page found in pool
            let meta = &mut self.frame_meta[frame_id];
            meta.pin_count += 1;
            // If it was unpinned (pin_count just became > 0), remove from replacer evictable set
            if meta.pin_count == 1 {
                self.replacer.set_evictable(frame_id, false);
            }
            self.replacer.record_access(frame_id);
            // Safety: We have &mut self, so no other thread can access self.pool concurrently.
            // We return a mutable reference tied to the lifetime of &mut self.
            Ok(Some(&mut self.pool[frame_id]))
        } else {
            println!("DEBUG: fetch_page {} not in pool. Finding victim.", page_id);
            // 2. Page not in pool, find victim frame
            let frame_id = match self.get_victim_frame() {
                Some(id) => id,
                None => return Ok(None), // No free/evictable frame found
            };
            println!("DEBUG: Victim frame for page {} is {}", page_id, frame_id);

            // 3. Prepare victim frame
            self.flush_frame_if_dirty(frame_id)?;

            // Remove old page mapping from page table if the victim frame held a page
            if let Some(old_page_id) = self.frame_meta[frame_id].page_id.take() {
                println!(
                    "DEBUG: Evicting old page {} from frame {}",
                    old_page_id, frame_id
                );
                self.page_table.remove(&old_page_id);
            }

            // 4. Load new page data
            let page_data = self.disk_manager.read_page(page_id)?;
            self.pool[frame_id] = Page::new(page_data); // Replace frame content

            // 5. Update metadata and page table
            let meta = &mut self.frame_meta[frame_id];
            meta.page_id = Some(page_id);
            meta.pin_count = 1;
            meta.is_dirty = false;
            self.page_table.insert(page_id, frame_id);

            // 6. Update replacer
            self.replacer.set_evictable(frame_id, false); // Newly fetched page is pinned
            self.replacer.record_access(frame_id);

            Ok(Some(&mut self.pool[frame_id]))
        }
    }

    /// Unpins the target page from the buffer pool.
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> Result<bool> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let meta = &mut self.frame_meta[frame_id];
            if meta.pin_count == 0 {
                println!("WARN: Unpin called on page {} with pin_count 0", page_id);
                return Ok(false); // Or Err?
            }
            meta.pin_count -= 1;
            if is_dirty {
                meta.is_dirty = true;
            }
            if meta.pin_count == 0 {
                println!(
                    "DEBUG: Page {} (frame {}) is now evictable.",
                    page_id, frame_id
                );
                self.replacer.set_evictable(frame_id, true);
            }
            Ok(true)
        } else {
            println!("WARN: Unpin called on page {} not in buffer pool", page_id);
            Ok(false) // Page not found
        }
    }

    /// Flushes the target page to disk.
    pub fn flush_page(&mut self, page_id: PageId) -> Result<bool> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let meta = &mut self.frame_meta[frame_id]; // Need mutable access to set is_dirty
            if let Some(current_page_id) = meta.page_id {
                // Double check page_id matches
                if current_page_id == page_id {
                    if meta.is_dirty {
                        println!(
                            "DEBUG: Flushing dirty page {} from frame {}",
                            page_id, frame_id
                        );
                        let page_data = self.pool[frame_id].get_data();
                        self.disk_manager.write_page(page_id, &page_data)?;
                        meta.is_dirty = false;
                    }
                    return Ok(true);
                }
            }
        }
        println!(
            "WARN: Flush called on page {} not found in buffer pool",
            page_id
        );
        Ok(false) // Page not found or mismatch
    }

    /// Creates a new page in the buffer pool.
    pub fn new_page(&mut self) -> Result<Option<(PageId, &mut Page)>> {
        let frame_id = match self.get_victim_frame() {
            Some(id) => id,
            None => return Ok(None), // Pool is full and all pages pinned
        };
        println!("DEBUG: Using victim frame {} for new page", frame_id);

        self.flush_frame_if_dirty(frame_id)?;

        // Remove old mapping if exists
        if let Some(old_page_id) = self.frame_meta[frame_id].page_id.take() {
            self.page_table.remove(&old_page_id);
        }

        // Allocate new page ID from DiskManager (increments its counter)
        let new_page_id = self.disk_manager.allocate_page();
        println!("DEBUG: Allocated new page_id {}", new_page_id);

        // Update metadata and page table for the new page
        let meta = &mut self.frame_meta[frame_id];
        meta.page_id = Some(new_page_id);
        meta.pin_count = 1;
        meta.is_dirty = true; // New page is considered dirty
        self.page_table.insert(new_page_id, frame_id);

        // Initialize the page data (e.g., zero it out)
        // Assuming Page::new creates a zeroed page initially
        self.pool[frame_id] = Page::new([0u8; PAGE_SIZE]);

        // Update replacer
        self.replacer.set_evictable(frame_id, false); // New page is pinned
        self.replacer.record_access(frame_id);

        Ok(Some((new_page_id, &mut self.pool[frame_id])))
    }

    /// Deletes a page. Tries to remove from pool. Doesn't handle disk deallocation.
    pub fn delete_page(&mut self, page_id: PageId) -> Result<bool> {
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let meta = &mut self.frame_meta[frame_id];
            if meta.pin_count > 0 {
                // Cannot delete a pinned page
                eprintln!(
                    "ERROR: Attempted to delete pinned page {} (pin_count={})",
                    page_id, meta.pin_count
                );
                return Err(Error::Internal(format!(
                    "Cannot delete pinned page {}",
                    page_id
                )));
            }

            // Reset metadata
            meta.page_id = None;
            meta.is_dirty = false;
            meta.pin_count = 0; // Should already be 0

            // Remove from page table and replacer
            self.page_table.remove(&page_id);
            self.replacer.remove(frame_id);

            // Add frame back to free list
            self.free_list.push(frame_id);

            // TODO: Optionally call disk_manager to deallocate on disk?
            // Requires DiskManager to have such a method.
            println!(
                "DEBUG: Deleted page {} from buffer pool (frame {})",
                page_id, frame_id
            );
            Ok(true)
        } else {
            println!(
                "DEBUG: Page {} not in buffer pool, delete is no-op.",
                page_id
            );
            Ok(true) // Page not in pool, considered successfully deleted
        }
    }

    /// Flushes all dirty pages in the buffer pool to disk.
    pub fn flush_all_pages(&mut self) -> Result<()> {
        println!("DEBUG: Flushing all pages.");
        for frame_id in 0..self.pool_size {
            self.flush_frame_if_dirty(frame_id)?; // Use helper
        }
        Ok(())
    }
}

// Destructor to ensure all pages are flushed on drop
impl Drop for BufferPoolManager {
    fn drop(&mut self) {
        println!("DEBUG: Dropping BufferPoolManager, flushing all pages.");
        if let Err(e) = self.flush_all_pages() {
            eprintln!("ERROR: Failed to flush all pages on drop: {:?}", e);
        }
    }
}
