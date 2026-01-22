use anyhow::Result;
use std::path::{Path, PathBuf};

/// Recursively collect Parquet files under `dir` into `out`.
pub fn collect_parquet_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files(&path, out)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    Ok(())
}

/// Return a list of Parquet files under `dir`.
pub fn list_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_parquet_files(dir, &mut files)?;
    Ok(files)
}
