pub mod ducklake;
pub mod schema;
pub mod transaction;

use std::sync::Arc;

pub use ducklake::DuckLakeWriter;

/// Storage interface for query-time access to the durable DuckLake writer.
pub trait TieredStorage: Send + Sync {
    fn writer(&self) -> Arc<DuckLakeWriter>;
    /// Monotonic counter bumped after each successful DuckLake table mutation; query workers use it
    /// to reattach so catalog metadata matches the writer connection.
    fn catalog_write_generation(&self) -> u64;
}

/// Storage components for flush-through DuckLake durable commit.
#[derive(Clone)]
pub struct Storage {
    pub writer: Arc<DuckLakeWriter>,
}

impl Storage {
    pub fn new(writer: Arc<DuckLakeWriter>) -> Self {
        Self { writer }
    }
}

impl TieredStorage for Storage {
    fn writer(&self) -> Arc<DuckLakeWriter> {
        self.writer.clone()
    }

    fn catalog_write_generation(&self) -> u64 {
        self.writer.catalog_write_generation()
    }
}

#[cfg(test)]
mod tests {
    use super::TieredStorage;

    #[tokio::test]
    async fn storage_exposes_writer() {
        let (storage, _t) = crate::test_support::sample_storage()
            .await
            .expect("storage");
        let _ = storage.writer();
        let _ = storage.catalog_write_generation();
    }
}
