# Performance Analysis: Why Queries Are Slow

## Design Goals vs. Reality

**Design Goal**: Sub-second read freshness with local WAL/cache
**Actual Performance**: 2.6s average, 3.6s p95 latency

## Root Causes

### 1. **Iceberg Data Read from S3 on Every Query** ❌

**Problem**: The `iceberg_{kind}` views use `iceberg_scan('s3://...')` which reads metadata and data files directly from S3, bypassing local cache.

**Code Location**: `src/query/duckdb.rs:742, 772`
```rust
// Creates view that reads from S3
"CREATE OR REPLACE TEMP VIEW iceberg_{kind} AS SELECT * FROM iceberg_scan('{uri}', ...)"
```

**Impact**: 
- First read: ~2-3s (S3 metadata fetch + data download)
- Subsequent reads: Still slow if cache_httpfs isn't working or files are large
- Network latency dominates query time

**Design Violation**: 
- Spec says: "Local Cache: The system SHALL maintain a local NVMe cache for hot Parquet"
- Reality: No mechanism downloads Iceberg Parquet files to local cache

### 2. **No Proactive Local Caching of Committed Data** ❌

**Problem**: There's no background process that:
- Downloads Iceberg Parquet files to local cache after commit
- Keeps local cache in sync with Iceberg snapshots
- Uses local file paths instead of S3 paths when files are cached

**Code Location**: Missing implementation

**Impact**: Every query must read from S3, even for "hot" data that was just committed.

**Design Violation**:
- Spec says: "Cache read-through: WHEN a query targets recently flushed data, THEN the system serves data from the local cache when available"
- Reality: No local cache for Iceberg data exists

### 3. **Union View Combines Fast (Local) + Slow (S3) Sources** ⚠️

**Problem**: The union view does:
```sql
SELECT * FROM iceberg_spans      -- SLOW: Reads from S3
UNION ALL SELECT * FROM staged_spans  -- FAST: Local files
UNION ALL SELECT * FROM wal_spans     -- FAST: Local files
```

**Code Location**: `src/query/duckdb.rs:540-546`

**Impact**: Even if WAL/staged data is fast, the Iceberg portion adds 2-3s latency.

### 4. **cache_httpfs May Not Be Working Effectively** ⚠️

**Problem**: 
- Earlier tests showed HTTP 403 errors (now fixed)
- cache_httpfs caches HTTP requests, but may not cache all S3 reads
- Cache might not be warm on first query
- Large files may not fit in cache

**Code Location**: `src/query/cache.rs:33-54`

**Impact**: Even with cache_httpfs configured, queries still hit S3.

### 5. **Pinned Metadata Not Used in Normal Query Path** ❌

**Problem**: There's a "pinned metadata" mechanism that can use local files (`read_parquet([local_files])`), but:
- It's only used if `DUCKDB_TEST_ICEBERG_FALLBACK_PATH` is set
- Normal queries use `IcebergSource::Catalog` or `IcebergSource::ScanUri` which read from S3

**Code Location**: `src/query/duckdb.rs:632-667`

**Impact**: Fast local file path exists but isn't used in production.

## Recommended Fixes

### Priority 1: Implement Local Cache for Iceberg Data

1. **Download Parquet files to local cache after commit**
   - When optimizer commits data, download Parquet files to `cache_dir/iceberg/{table}/{snapshot}/`
   - Store mapping: `{snapshot_id} -> [local_file_paths]`

2. **Use local paths in iceberg views when available**
   - Check if files are cached locally
   - Use `read_parquet([local_files])` instead of `iceberg_scan('s3://...')`
   - Fall back to S3 if cache miss

3. **Background cache warming**
   - Pre-download recent snapshots' Parquet files
   - Keep cache in sync with current snapshot

### Priority 2: Optimize cache_httpfs Usage

1. **Verify cache_httpfs is working**
   - Add diagnostics to measure cache hit rate
   - Ensure S3 filesystem is properly wrapped

2. **Increase cache size if needed**
   - Default cache might be too small for Parquet files

### Priority 3: Reduce S3 Metadata Fetches

1. **Cache Iceberg metadata files locally**
   - Download manifest-list and manifest files to local cache
   - Use local paths when available

2. **Use pinned metadata in production**
   - Generate pinned metadata with local file paths after commit
   - Use pinned metadata as default source instead of catalog/scan_uri

## Expected Performance After Fixes

- **Warm queries (cached data)**: <500ms (meets design goal)
- **Cold queries (cache miss)**: ~2-3s (first read, then cached)
- **Mixed queries**: ~1s average (some cached, some not)

## Design Compliance

The current implementation violates these design requirements:

1. ❌ **Local Cache requirement**: No local cache for Iceberg Parquet files
2. ❌ **Cache read-through scenario**: Queries don't use local cache when available
3. ⚠️ **Near Real-Time Query Performance**: 2.6s average vs <1s target
4. ⚠️ **Concurrent Query Throughput**: p95 latency 3.6s vs <1s target
