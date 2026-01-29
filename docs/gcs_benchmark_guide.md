# GCS Server Benchmark Guide

This guide explains how to run a full stress performance benchmark on a server with Google Cloud Storage (GCS).

## Prerequisites

### 1. GCS Setup

1. **Create a GCS bucket**:
   ```bash
   gsutil mb -p YOUR_PROJECT_ID -l us-central1 gs://YOUR-BUCKET-NAME
   ```

2. **Create a service account** with Storage Admin role:
   ```bash
   gcloud iam service-accounts create datalake-benchmark \
       --display-name="Datalake Benchmark Service Account"
   
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
       --member="serviceAccount:datalake-benchmark@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
       --role="roles/storage.admin"
   ```

3. **Create and download service account key**:
   ```bash
   gcloud iam service-accounts keys create ~/gcs-benchmark-key.json \
       --iam-account=datalake-benchmark@YOUR_PROJECT_ID.iam.gserviceaccount.com
   ```

### 2. Iceberg Catalog Setup

If using REST catalog, ensure your catalog endpoint is accessible:
- Catalog URI: `https://your-catalog-endpoint/catalog`
- Authentication token (if required)

### 3. Server Requirements

- **CPU**: 8+ cores recommended for high concurrency
- **Memory**: 16GB+ RAM (4GB per query worker)
- **Disk**: Fast SSD for cache directory (100GB+ recommended)
- **Network**: High bandwidth to GCS (10Gbps+ for production)

## Configuration

### Step 1: Create Config File

Copy the template and update with your values:

```bash
cp config-gcs-benchmark.yaml config-gcs-benchmark.yaml.local
```

Edit `config-gcs-benchmark.yaml.local`:
- `wal_bucket`: Your GCS bucket name
- `warehouse`: `gs://YOUR-BUCKET-NAME/warehouse`
- `catalog_uri`: Your Iceberg REST catalog endpoint
- `cache_dir`: Server-local cache directory (fast SSD)
- `spill_directory`: DuckDB spill directory

### Step 2: Set Environment Variables

```bash
export GOOGLE_APPLICATION_CREDENTIALS=~/gcs-benchmark-key.json
export CONFIG_FILE=config-gcs-benchmark.yaml.local
export GCS_BUCKET=YOUR-BUCKET-NAME
export CATALOG_URI=https://your-catalog-endpoint/catalog
```

### Step 3: GCS Authentication

DuckDB supports two authentication methods:

#### Option A: Application Default Credentials (Recommended)

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

In config file, leave `s3.endpoint` as `null` - DuckDB will use ADC automatically.

#### Option B: HMAC Keys (S3-Compatible API)

1. Create HMAC key in GCP Console:
   - Cloud Storage → Settings → Interoperability
   - Create HMAC key for service account

2. Update config:
   ```yaml
   s3:
     endpoint: "storage.googleapis.com"
     access_key_id: "YOUR_HMAC_ACCESS_KEY"
     secret_access_key: "YOUR_HMAC_SECRET"
   ```

## Running the Benchmark

### Quick Start

```bash
# Make script executable
chmod +x scripts/benchmark_gcs_server.sh

# Run with defaults (5 min test, 1 min warmup)
./scripts/benchmark_gcs_server.sh
```

### Custom Parameters

```bash
# High-intensity benchmark
BENCHMARK_DURATION=600 \
BENCHMARK_WARMUP=120 \
SPAN_QPS=500 \
LOG_QPS=500 \
METRIC_QPS=500 \
QUERY_CONCURRENCY=32 \
QUERY_INTERVAL_MS=300 \
./scripts/benchmark_gcs_server.sh
```

### Manual Run

```bash
# Build release binary
cargo build --release --bin perf_stress

# Run benchmark
RUST_LOG=info \
CONFIG_FILE=config-gcs-benchmark.yaml.local \
cargo run --release --bin perf_stress -- \
    --duration 300 \
    --warmup-secs 60 \
    --span-qps 200 \
    --log-qps 200 \
    --metric-qps 200 \
    --query-concurrency 16 \
    --query-interval-ms 500
```

## Benchmark Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--duration` | 300s | Total test duration |
| `--warmup-secs` | 60s | Warmup period (not counted in stats) |
| `--span-qps` | 200/s | Spans per second to ingest |
| `--log-qps` | 200/s | Logs per second to ingest |
| `--metric-qps` | 200/s | Metrics per second to ingest |
| `--query-concurrency` | 16 | Concurrent query workers |
| `--query-interval-ms` | 500ms | Interval between queries per worker |

## Understanding Results

The benchmark reports:

1. **Ingestion Stats**:
   - Records produced per type (spans/logs/metrics)
   - Ingestion errors

2. **Query Performance**:
   - Total queries executed
   - Query errors
   - **Steady-state latency** (post-warmup):
     - Average latency
     - P95 latency
   - **Per-query-type breakdown**:
     - Execution count
     - Error count
     - Average latency
     - P95 latency

### Target Metrics

Based on design goals:
- **Business attribute lookups**: <500ms p95
- **Error rate queries**: <1s p95
- **Latency analysis**: <2s p95
- **Session drilldowns**: <1s p95

## Monitoring

### During Benchmark

1. **GCS Console**: Monitor egress costs and bandwidth
2. **Server Metrics**: CPU, memory, disk I/O, network
3. **Application Logs**: Set `RUST_LOG=debug` for detailed logs

### Cache Performance

Enable cache profiling:
```bash
PERF_CACHE_PROFILE=1 ./scripts/benchmark_gcs_server.sh
```

Check cache directory size:
```bash
du -sh /var/tmp/datalake/cache/duckdb_http_cache
```

### GCS Costs

Monitor in GCP Console:
- **Storage**: Data stored in bucket
- **Egress**: Data transferred out (queries)
- **Operations**: Read/write API calls

Typical costs (us-central1):
- Storage: $0.020/GB/month
- Egress: $0.12/GB (first 10TB)
- Operations: $0.05 per 10K Class A operations

## Troubleshooting

### Authentication Errors

```
Error: Failed to access GCS bucket
```

**Solution**: Verify service account key and permissions:
```bash
gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS"
gsutil ls gs://YOUR-BUCKET-NAME
```

### High Query Latency

If queries are slow:
1. **Check cache hit rate**: Enable `PERF_CACHE_PROFILE=1`
2. **Increase cache size**: Larger `cache_dir` disk
3. **Reduce query concurrency**: Lower `--query-concurrency`
4. **Check network**: Monitor GCS egress bandwidth

### Out of Memory

If workers crash:
1. **Reduce concurrency**: Lower `--query-concurrency`
2. **Reduce memory per query**: Edit `duckdb.max_memory_per_query` in config
3. **Enable spill to disk**: Ensure `enable_spill_to_disk: true`

### High Egress Costs

To reduce GCS egress:
1. **Enable cache_httpfs**: Already enabled by default
2. **Increase cache size**: More data cached locally
3. **Run in same region**: Ensure server is in same GCP region as bucket
4. **Use VPC**: Connect via private Google access

## Advanced Configuration

### Optimize for High Throughput

```yaml
duckdb:
  max_connections: 32  # More workers
  max_memory_per_query: "8GB"  # More memory per query
  max_query_duration_seconds: 120  # Longer timeouts
```

### Optimize for Low Latency

```yaml
ingest_engine:
  cache_dir: "/mnt/nvme/cache"  # Fast NVMe SSD

duckdb:
  max_connections: 8  # Fewer workers, less contention
  max_memory_per_query: "2GB"
```

### Production Settings

```yaml
span_buffering:
  max_buffer_bytes: 268435456  # 256MB
  flush_interval_seconds: 30  # More frequent flushes

compaction:
  target_file_size_bytes: 134217728  # 128MB files
  compaction_interval_seconds: 1800  # 30 min
```

## Next Steps

After benchmarking:
1. **Analyze results**: Compare against design goals
2. **Optimize bottlenecks**: Identify slow query types
3. **Tune configuration**: Adjust based on workload
4. **Scale testing**: Test with higher QPS/concurrency
5. **Cost analysis**: Review GCS egress costs
