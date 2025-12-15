#![allow(dead_code)]
pub mod storage;
pub mod util;

pub use storage::loader::{LoadMetadata, LoadResult, LoadSpec, Loadable, Loader};
pub use storage::{Record, Storage, StorageError, StorageIterator, StorageRead, StorageResult};
pub use util::BytesRange;
