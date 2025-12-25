//! IronDuck Storage - Buffer pool and columnar storage

pub mod buffer_pool;
pub mod index_storage;
pub mod page;
pub mod persistence;
pub mod table_storage;

pub use buffer_pool::BufferPool;
pub use index_storage::{BTreeIndex, IndexKey, IndexStorage};
pub use page::{Page, PageId};
pub use persistence::{
    save_metadata, load_metadata, save_table_data, load_table_data,
    database_exists, DatabaseMetadata, SchemaMetadata, TableMetadata,
    ColumnMetadata, ViewMetadata, SequenceMetadata, IndexMetadata,
    TableData as PersistedTableData, PersistenceError, PersistenceResult, STORAGE_VERSION,
};
pub use table_storage::{TableData, TableStorage};
