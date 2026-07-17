pub mod ducklake;
pub mod schema;
pub mod transaction;

use std::sync::Arc;

pub use ducklake::DuckLakeWriter;

/// Shared ingest↔query handle to the durable DuckLake writer.
///
/// Name is historical (buffer/staged tiers are gone). Concurrency and catalog visibility are
/// handled by DuckLake + catalog choice (`postgres` / `sqlite`), not Softprobe reattach hacks.
pub trait TieredStorage: Send + Sync {
    fn writer(&self) -> Arc<DuckLakeWriter>;
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
    }
}
