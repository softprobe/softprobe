//! Shared helpers to locate sibling `sp-llm/manifests/*.yaml` promotion contracts.
//!
//! `promotion_llm_v1` loads `llm-v1.yaml` from the sibling `sp-llm` checkout instead of
//! duplicating manifest content in thelake.

use std::path::PathBuf;

/// Resolve the path to one `sp-llm/manifests/<name>` file.
///
/// `SP_LLM_MANIFEST_DIR` overrides the sibling-checkout default.
pub fn sp_llm_manifest_path(name: &str) -> PathBuf {
    if let Ok(dir) = std::env::var("SP_LLM_MANIFEST_DIR") {
        return PathBuf::from(dir).join(name);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../sp-llm/manifests")
        .join(name)
}
