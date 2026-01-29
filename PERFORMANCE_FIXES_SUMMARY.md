# Performance Fixes Summary

## Issues Found

### 1. **iceberg_scan with Local Paths Fails**
- **Problem**: `iceberg_scan()` doesn't support local file paths, only S3/HTTP
- **Symptom**: Warnings "Unimplemented option mode" when using pinned metadata with local paths
- **Fix**: Check if metadata path is local before calling `iceberg_scan`, fall back to catalog/S3

### 2. **Blocking Downloads During Metadata Refresh**
- **Problem**: `update_metadata_pointer()` was downloading all Parquet files synchronously, blocking queries
- **Symptom**: Queries taking 60-100 seconds on EC2
- **Fix**: Changed to only check if files exist locally, don't download during metadata refresh

### 3. **No Background File Downloads**
- **Problem**: Files weren't being downloaded to local cache after commit
- **Symptom**: Queries always reading from S3 (slow)
- **Fix**: Added background parallel downloads after commit for all table types

### 4. **data_files.json Not Updated After Downloads**
- **Problem**: After background downloads, `data_files.json` still had S3 paths
- **Symptom**: Queries still using S3 paths even after files cached
- **Fix**: Update `data_files.json` with local paths after downloads complete

## Performance Results

### Local Test (with fixes):
- **Avg latency**: 121ms ✅
- **P95 latency**: 163ms ✅
- **Query errors**: 0 ✅
- **All query types executing**: ✅

### EC2 Test (before fixes):
- **Avg latency**: 83-89 seconds ❌
- **P95 latency**: 95-103 seconds ❌
- **Only 3 queries in 180 seconds** ❌

## Root Cause Analysis

The main issue on EC2 was:
1. Queries falling back to S3 reads (no local cache)
2. S3 network latency on EC2 is high (60-100 seconds per query)
3. Background downloads weren't happening or weren't fast enough
4. `data_files.json` wasn't being updated with local paths

## Files Changed

1. `src/query/duckdb.rs`:
   - Fixed `iceberg_scan` fallback for local paths
   - Improved local file filtering in query engine

2. `src/storage/iceberg/mod.rs`:
   - Added `check_local_cache()` - fast check without download
   - Added `download_files_in_background()` - parallel downloads after commit
   - Added `download_single_file()` - single file download
   - Updated `update_metadata_pointer()` - no longer blocks on downloads
   - Added background downloads for spans, logs, and metrics
   - Added `data_files.json` update after downloads

## Next Steps

1. **Test on EC2** with these fixes - should see much better performance
2. **Monitor background downloads** - ensure they complete quickly
3. **Consider** downloading only recent files (last N snapshots) to reduce download time
4. **Investigate** why S3 reads are so slow on EC2 (network, file size, DuckDB config)

## Testing Locally

```bash
# Use existing Lakekeeper and AWS S3 bucket
CONFIG_FILE=config-local-s3-test.yaml ./target/release/perf_stress \
  --duration 120 \
  --span-qps 10 \
  --log-qps 10 \
  --metric-qps 10 \
  --query-concurrency 1 \
  --query-interval-ms 1000 \
  --warmup-secs 15
```

Expected: ~100-200ms latency, 0 errors
