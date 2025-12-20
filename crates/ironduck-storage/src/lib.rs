//! IronDuck Storage - Buffer pool and columnar storage

pub mod buffer_pool;
pub mod page;
pub mod table_storage;

pub use buffer_pool::BufferPool;
pub use page::{Page, PageId};
pub use table_storage::{TableData, TableStorage};
