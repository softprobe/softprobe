#!/bin/bash
set -euo pipefail

# Safe AWS Performance Test Script with Safeguards
# This script gradually increases load and monitors for SSH timeouts

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KEY_FILE="$HOME/.ssh/datalake-benchmark-key.pem"

log_info() {
    echo -e "\033[0;32m[INFO]\033[0m $1"
}

log_warn() {
    echo -e "\033[0;33m[WARN]\033[0m $1"
}

log_error() {
    echo -e "\033[0;31m[ERROR]\033[0m $1"
}

# Check SSH connectivity with timeout
check_ssh() {
    local ip=$1
    local timeout=${2:-5}
    
    if ssh -i "$KEY_FILE" -o ConnectTimeout=$timeout -o StrictHostKeyChecking=no ubuntu@"$ip" "echo 'OK'" 2>/dev/null | grep -q "OK"; then
        return 0
    else
        return 1
    fi
}

# Cleanup old WAL files on remote instance
cleanup_wal_on_instance() {
    local ip=$1
    log_info "Cleaning up old WAL files on $ip..."
    
    ssh -i "$KEY_FILE" -o ConnectTimeout=10 -o StrictHostKeyChecking=no ubuntu@"$ip" bash << 'CLEANUP'
        set -x
        # Stop any running perf_stress processes
        pkill -9 perf_stress || true
        pkill -9 softprobe || true
        
        # Clean up cache directories
        sudo rm -rf /var/tmp/datalake/cache/wal/* || true
        sudo rm -rf /var/tmp/datalake/cache/spans/* || true
        sudo rm -rf /var/tmp/datalake/cache/logs/* || true
        sudo rm -rf /var/tmp/datalake/cache/metrics/* || true
        sudo rm -rf /var/tmp/datalake/cache/wal_watermarks/* || true
        sudo rm -rf /var/tmp/datalake/cache/staged_watermarks/* || true
        sudo rm -rf /var/tmp/datalake/cache/wal_manifest/* || true
        sudo rm -rf /var/tmp/datalake/cache/iceberg_metadata/* || true
        
        # Recreate directories
        sudo mkdir -p /var/tmp/datalake/cache/{wal,spans,logs,metrics,wal_watermarks,staged_watermarks,wal_manifest,iceberg_metadata}
        sudo chown -R ubuntu:ubuntu /var/tmp/datalake/cache
        
        echo "Cleanup complete"
CLEANUP
}

# Reset catalog (drop and recreate warehouse)
reset_catalog() {
    local catalog_ip=$1
    log_info "Resetting catalog on $catalog_ip..."
    
    ssh -i "$KEY_FILE" -o ConnectTimeout=10 -o StrictHostKeyChecking=no ubuntu@"$catalog_ip" bash << 'RESET'
        cd lakekeeper
        docker-compose down -v || true
        docker-compose up -d
        sleep 10
        
        # Wait for lakekeeper to be ready
        for i in {1..30}; do
            if curl -s http://localhost:8181/catalog/v1/config >/dev/null 2>&1; then
                echo "Lakekeeper is ready"
                break
            fi
            sleep 2
        done
RESET
}

# Copy optimized code to instance
deploy_optimized_code() {
    local app_ip=$1
    log_info "Deploying optimized code to $app_ip..."
    
    cd "$PROJECT_ROOT"
    
    # Create tarball of updated files
    tar czf /tmp/datalake-optimized.tar.gz \
        src/ingest_engine/mod.rs \
        src/query/duckdb.rs \
        Cargo.toml \
        Cargo.lock
    
    # Copy and extract
    scp -i "$KEY_FILE" -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
        /tmp/datalake-optimized.tar.gz ubuntu@"$app_ip":~/
    
    ssh -i "$KEY_FILE" -o ConnectTimeout=10 -o StrictHostKeyChecking=no ubuntu@"$app_ip" bash << 'DEPLOY'
        cd ~/datalake
        tar xzf ../datalake-optimized.tar.gz
        
        # Rebuild with optimizations
        source ~/.cargo/env
        cargo build --release --bin perf_stress 2>&1 | tail -20
        
        if [ -f target/release/perf_stress ]; then
            echo "Build successful"
        else
            echo "Build failed!"
            exit 1
        fi
DEPLOY
}

# Run test with monitoring and timeout
run_monitored_test() {
    local app_ip=$1
    local duration=$2
    local span_qps=$3
    local log_qps=$4
    local metric_qps=$5
    local query_concurrency=$6
    
    log_info "Running test: duration=${duration}s, span_qps=$span_qps, log_qps=$log_qps, metric_qps=$metric_qps, query_concurrency=$query_concurrency"
    
    # Start test in background
    ssh -i "$KEY_FILE" -o ConnectTimeout=10 -o StrictHostKeyChecking=no ubuntu@"$app_ip" bash << TESTCMD &
        cd ~/datalake
        CONFIG_FILE=config-aws-benchmark.yaml ./target/release/perf_stress \
          --duration $duration \
          --warmup-secs 10 \
          --span-qps $span_qps \
          --log-qps $log_qps \
          --metric-qps $metric_qps \
          --query-concurrency $query_concurrency \
          --query-interval-ms 500 2>&1 | tee /tmp/test-\$(date +%s).log
TESTCMD
    
    local ssh_pid=$!
    local max_wait=$((duration + 60))
    local elapsed=0
    
    # Monitor SSH connectivity while test runs
    while kill -0 $ssh_pid 2>/dev/null; do
        sleep 5
        elapsed=$((elapsed + 5))
        
        if [ $elapsed -ge $max_wait ]; then
            log_warn "Test exceeded maximum time, terminating..."
            kill $ssh_pid 2>/dev/null || true
            return 1
        fi
        
        # Check SSH every 10 seconds
        if [ $((elapsed % 10)) -eq 0 ]; then
            if ! check_ssh "$app_ip" 3; then
                log_error "Lost SSH connection to $app_ip!"
                kill $ssh_pid 2>/dev/null || true
                return 1
            fi
        fi
    done
    
    wait $ssh_pid
    return $?
}

# Get test results
get_test_results() {
    local app_ip=$1
    log_info "Fetching test results from $app_ip..."
    
    ssh -i "$KEY_FILE" -o ConnectTimeout=10 -o StrictHostKeyChecking=no ubuntu@"$app_ip" bash << 'RESULTS'
        # Get latest test log
        LATEST_LOG=$(ls -t /tmp/test-*.log 2>/dev/null | head -1)
        if [ -n "$LATEST_LOG" ]; then
            echo "=== Test Results ==="
            cat "$LATEST_LOG"
        fi
        
        echo ""
        echo "=== WAL File Counts ==="
        for dir in /var/tmp/datalake/cache/wal/*; do
            if [ -d "$dir" ]; then
                count=$(find "$dir" -name "*.parquet" 2>/dev/null | wc -l)
                echo "$(basename $dir): $count files"
            fi
        done
        
        echo ""
        echo "=== Cache Disk Usage ==="
        du -sh /var/tmp/datalake/cache/* 2>/dev/null || true
RESULTS
}

# Main execution
main() {
    local catalog_ip="44.211.62.162"
    local app_ip="34.205.20.255"
    local bucket="datalake-benchmark-1769290134"
    
    log_info "Starting safe AWS performance test..."
    log_info "Catalog: $catalog_ip"
    log_info "App: $app_ip"
    log_info "Bucket: $bucket"
    
    # Step 1: Verify connectivity
    log_info "Step 1: Verifying connectivity..."
    if ! check_ssh "$app_ip"; then
        log_error "Cannot connect to app instance"
        exit 1
    fi
    if ! check_ssh "$catalog_ip"; then
        log_error "Cannot connect to catalog instance"
        exit 1
    fi
    log_info "Connectivity OK"
    
    # Step 2: Deploy optimized code
    log_info "Step 2: Deploying optimized code..."
    if ! deploy_optimized_code "$app_ip"; then
        log_error "Failed to deploy code"
        exit 1
    fi
    
    # Step 3: Clean up and reset
    log_info "Step 3: Cleaning up old data..."
    cleanup_wal_on_instance "$app_ip"
    reset_catalog "$catalog_ip"
    
    # Step 4: Run progressive tests
    log_info "Step 4: Running progressive load tests..."
    
    # Test 1: Light load (baseline)
    log_info "Test 1: Light load (10 QPS each, 30s)"
    if ! run_monitored_test "$app_ip" 30 10 10 10 2; then
        log_error "Test 1 failed or instance became unresponsive"
        exit 1
    fi
    get_test_results "$app_ip"
    sleep 5
    
    # Test 2: Medium load
    log_info "Test 2: Medium load (25 QPS each, 60s)"
    if ! run_monitored_test "$app_ip" 60 25 25 25 2; then
        log_error "Test 2 failed or instance became unresponsive"
        exit 1
    fi
    get_test_results "$app_ip"
    sleep 5
    
    # Test 3: Design target load
    log_info "Test 3: Target load (50 QPS each, 120s)"
    if ! run_monitored_test "$app_ip" 120 50 70 70 4; then
        log_error "Test 3 failed or instance became unresponsive"
        exit 1
    fi
    get_test_results "$app_ip"
    
    log_info "All tests completed successfully!"
}

main "$@"
