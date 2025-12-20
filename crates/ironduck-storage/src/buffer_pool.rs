//! Buffer Pool Manager
//!
//! Manages a fixed-size pool of pages in memory with LRU eviction.
//! Uses RAII guards to ensure pages are properly unpinned.

use super::page::{Page, PageId, PAGE_SIZE};
use ironduck_common::{Error, Result};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Frame identifier in the buffer pool
pub type FrameId = usize;

/// A frame in the buffer pool
struct Frame {
    /// The page data
    page: RwLock<Page>,
    /// Current page ID (0 if empty)
    page_id: AtomicU64,
    /// Pin count
    pin_count: AtomicU32,
}

impl Frame {
    fn new() -> Self {
        Frame {
            page: RwLock::new(Page::new(0)),
            page_id: AtomicU64::new(0),
            pin_count: AtomicU32::new(0),
        }
    }
}

/// LRU Replacer for eviction
struct LruReplacer {
    /// Frames eligible for eviction (unpinned)
    lru_list: Vec<FrameId>,
}

impl LruReplacer {
    fn new(_capacity: usize) -> Self {
        LruReplacer {
            lru_list: Vec::new(),
        }
    }

    /// Record that a frame was accessed
    fn access(&mut self, frame_id: FrameId) {
        // Remove from list if present, will be re-added when unpinned
        self.lru_list.retain(|&id| id != frame_id);
    }

    /// Add a frame to the eviction list
    fn unpin(&mut self, frame_id: FrameId) {
        if !self.lru_list.contains(&frame_id) {
            self.lru_list.push(frame_id);
        }
    }

    /// Remove a frame from the eviction list (it was pinned)
    fn pin(&mut self, frame_id: FrameId) {
        self.lru_list.retain(|&id| id != frame_id);
    }

    /// Evict the least recently used frame
    fn evict(&mut self) -> Option<FrameId> {
        if self.lru_list.is_empty() {
            None
        } else {
            Some(self.lru_list.remove(0))
        }
    }
}

/// The buffer pool manager
pub struct BufferPool {
    /// All frames in the pool
    frames: Vec<Frame>,
    /// Page ID to Frame ID mapping
    page_table: RwLock<HashMap<PageId, FrameId>>,
    /// Free frame list
    free_list: Mutex<Vec<FrameId>>,
    /// LRU replacer
    replacer: Mutex<LruReplacer>,
    /// Pool size (number of frames)
    pool_size: usize,
}

impl BufferPool {
    /// Create a new buffer pool with the given number of frames
    pub fn new(pool_size: usize) -> Self {
        let frames: Vec<Frame> = (0..pool_size).map(|_| Frame::new()).collect();
        let free_list: Vec<FrameId> = (0..pool_size).collect();

        BufferPool {
            frames,
            page_table: RwLock::new(HashMap::new()),
            free_list: Mutex::new(free_list),
            replacer: Mutex::new(LruReplacer::new(pool_size)),
            pool_size,
        }
    }

    /// Fetch a page into the buffer pool
    pub fn fetch_page(&self, page_id: PageId) -> Result<PageGuard<'_>> {
        // Check if page is already in pool
        {
            let page_table = self.page_table.read();
            if let Some(&frame_id) = page_table.get(&page_id) {
                self.pin_frame(frame_id);
                return Ok(PageGuard::new(self, frame_id, page_id));
            }
        }

        // Need to load page
        let frame_id = self.get_free_frame()?;

        // Initialize the frame with this page
        self.frames[frame_id]
            .page_id
            .store(page_id, Ordering::Release);
        self.pin_frame(frame_id);

        // Update page table
        {
            let mut page_table = self.page_table.write();
            page_table.insert(page_id, frame_id);
        }

        // TODO: Actually read from disk

        Ok(PageGuard::new(self, frame_id, page_id))
    }

    /// Allocate a new page
    pub fn new_page(&self) -> Result<PageGuard<'_>> {
        // TODO: Track next page ID properly
        static NEXT_PAGE_ID: AtomicU64 = AtomicU64::new(1);
        let page_id = NEXT_PAGE_ID.fetch_add(1, Ordering::SeqCst);
        self.fetch_page(page_id)
    }

    /// Pin a frame
    fn pin_frame(&self, frame_id: FrameId) {
        self.frames[frame_id].pin_count.fetch_add(1, Ordering::SeqCst);
        self.replacer.lock().pin(frame_id);
    }

    /// Unpin a frame (called by PageGuard drop)
    fn unpin_frame(&self, frame_id: FrameId) {
        let old_count = self.frames[frame_id].pin_count.fetch_sub(1, Ordering::SeqCst);
        if old_count == 1 {
            // Pin count is now 0, frame is eligible for eviction
            self.replacer.lock().unpin(frame_id);
        }
    }

    /// Get a free frame (evicting if necessary)
    fn get_free_frame(&self) -> Result<FrameId> {
        // Try free list first
        {
            let mut free_list = self.free_list.lock();
            if let Some(frame_id) = free_list.pop() {
                return Ok(frame_id);
            }
        }

        // Need to evict
        let mut replacer = self.replacer.lock();
        if let Some(frame_id) = replacer.evict() {
            // Write back if dirty
            let page = self.frames[frame_id].page.read();
            if page.dirty {
                // TODO: Actually write to disk
            }

            // Remove old mapping
            let old_page_id = self.frames[frame_id].page_id.load(Ordering::Acquire);
            {
                let mut page_table = self.page_table.write();
                page_table.remove(&old_page_id);
            }

            drop(page);
            drop(replacer);

            // Reset the frame
            let mut page = self.frames[frame_id].page.write();
            page.dirty = false;

            return Ok(frame_id);
        }

        Err(Error::BufferPoolFull)
    }

    /// Get pool size
    pub fn size(&self) -> usize {
        self.pool_size
    }
}

/// RAII guard for a pinned page
/// Automatically unpins the page when dropped - prevents memory leaks!
pub struct PageGuard<'a> {
    pool: &'a BufferPool,
    frame_id: FrameId,
    page_id: PageId,
}

impl<'a> PageGuard<'a> {
    fn new(pool: &'a BufferPool, frame_id: FrameId, page_id: PageId) -> Self {
        PageGuard {
            pool,
            frame_id,
            page_id,
        }
    }

    /// Get the page ID
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Read the page data
    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, Page> {
        self.pool.frames[self.frame_id].page.read()
    }

    /// Write to the page data
    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, Page> {
        let mut page = self.pool.frames[self.frame_id].page.write();
        page.set_dirty();
        page
    }
}

impl Drop for PageGuard<'_> {
    fn drop(&mut self) {
        // This is the key to memory safety - always unpins!
        self.pool.unpin_frame(self.frame_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_new_page() {
        let pool = BufferPool::new(10);
        let page = pool.new_page();
        assert!(page.is_ok());
    }

    #[test]
    fn test_buffer_pool_fetch_page() {
        let pool = BufferPool::new(10);
        let page1 = pool.fetch_page(1).unwrap();
        let page2 = pool.fetch_page(1).unwrap();
        assert_eq!(page1.page_id(), page2.page_id());
    }

    #[test]
    fn test_page_guard_drop() {
        let pool = BufferPool::new(2);
        {
            let _page1 = pool.fetch_page(1).unwrap();
            let _page2 = pool.fetch_page(2).unwrap();
        }
        // After guards are dropped, frames should be available
        let page3 = pool.fetch_page(3);
        assert!(page3.is_ok());
    }
}
