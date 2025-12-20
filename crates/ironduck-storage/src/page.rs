//! Page management

/// Page size in bytes (256KB like DuckDB)
pub const PAGE_SIZE: usize = 262144;

/// Page identifier
pub type PageId = u64;

/// A page in the buffer pool
pub struct Page {
    /// The page data
    pub data: Box<[u8; PAGE_SIZE]>,
    /// Page ID
    pub id: PageId,
    /// Whether this page has been modified
    pub dirty: bool,
}

impl Page {
    /// Create a new empty page
    pub fn new(id: PageId) -> Self {
        Page {
            data: Box::new([0u8; PAGE_SIZE]),
            id,
            dirty: false,
        }
    }

    /// Mark this page as dirty
    pub fn set_dirty(&mut self) {
        self.dirty = true;
    }

    /// Clear the dirty flag
    pub fn clear_dirty(&mut self) {
        self.dirty = false;
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new(0)
    }
}
