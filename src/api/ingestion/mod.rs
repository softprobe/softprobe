pub mod traces;
pub mod logs;
pub mod metrics;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestResponse {
    pub success: bool,
    pub ingested_count: usize,
    pub message: String,
}
