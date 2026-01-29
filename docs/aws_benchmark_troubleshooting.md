# AWS Benchmark Troubleshooting Guide

## Instance Becomes Unresponsive During Benchmark

### Problem
The EC2 instance loses SSH connectivity and becomes unresponsive during benchmark execution, often showing "Out of Memory" (OOM) errors in console logs.

### Root Cause
The default benchmark parameters are too aggressive for the `c5.2xlarge` instance type (16GB RAM):
- 16 concurrent DuckDB connections × 4GB max memory per query = 64GB theoretical memory usage
- High QPS (200 spans/logs/metrics per second)
- Multiple query workers competing for resources

### Solution

#### 1. Reduced Memory Configuration
The following changes have been made to prevent OOM:

**config-aws-benchmark.yaml:**
```yaml
duckdb:
  max_connections: 4  # Reduced from 16
  max_memory_per_query: "1GB"  # Reduced from 4GB
```

**Default Benchmark Parameters:**
- `--span-qps 100` (reduced from 200)
- `--log-qps 100` (reduced from 200)
- `--metric-qps 100` (reduced from 200)
- `--query-concurrency 4` (reduced from 16)

#### 2. Swap Space
The deployment script now automatically creates a 16GB swap file to provide additional memory buffer.

#### 3. If Instance is Already Unresponsive

**Option A: Reboot the Instance**
```bash
APP_ID=$(python3 -c "import json; print(json.load(open('aws_benchmark_state.json'))['app_instance_id'])")
aws ec2 reboot-instances --instance-ids $APP_ID --region us-east-1
# Wait 2-3 minutes, then SSH should work again
```

**Option B: Recreate the Stack**
```bash
./scripts/aws_benchmark.sh cleanup
./scripts/aws_benchmark.sh setup
```

#### 4. For Higher Performance (Optional)
If you need higher throughput, consider:
- Upgrading to `c5.4xlarge` (32GB RAM) or `c5.9xlarge` (72GB RAM)
- Set `INSTANCE_TYPE=c5.4xlarge` before running setup
- Then increase parameters accordingly

### Monitoring Memory Usage
```bash
# SSH to instance and monitor memory
watch -n 1 free -h
# Or check for OOM kills
dmesg | grep -i "out of memory\|killed process"
```

### Recommended Settings by Instance Type

| Instance Type | RAM | Max Connections | Max Memory/Query | QPS (each) | Query Concurrency |
|--------------|-----|----------------|------------------|------------|-------------------|
| c5.2xlarge   | 16GB | 4 | 1GB | 100 | 4 |
| c5.4xlarge   | 32GB | 8 | 2GB | 200 | 8 |
| c5.9xlarge   | 72GB | 16 | 4GB | 400 | 16 |
