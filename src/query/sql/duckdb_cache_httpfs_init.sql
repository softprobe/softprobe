-- cache_httpfs initialization.
--
-- IMPORTANT: INSTALL must run before LOAD. Some DuckDB builds require the
-- community registry for this extension.
-- https://duckdb.org/community_extensions/extensions/cache_httpfs#installing-and-loading
INSTALL cache_httpfs FROM community;
LOAD cache_httpfs;
SET cache_httpfs_cache_block_size = 8388608;
SET cache_httpfs_disk_cache_reader_enable_memory_cache = 0;
SET cache_httpfs_type = 'on_disk';

-- Reserve 10GB of disk space (value in bytes)
SET cache_httpfs_min_disk_bytes_for_cache = 10737418240;

-- Use LRU eviction for better cache efficiency
SET cache_httpfs_evict_policy = 'lru_single_proc';

-- Set the directory where the cache should live
-- SET cache_httpfs_cache_directory = ?;