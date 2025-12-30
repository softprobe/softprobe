# Testing Guide - SoftProbe OTLP Backend

This guide explains how to reliably run integration tests in any environment.

## Quick Start

### Local Development (Recommended)

```bash
# 1. Start test infrastructure (MinIO + Iceberg REST)
make setup-local

# 2. Run all tests
make test-local

# 3. Stop infrastructure when done
make teardown-local
```

### Using Test Script

```bash
# Start infrastructure
./scripts/test-setup.sh setup

# Verify it's running
./scripts/test-setup.sh verify

# Run tests
cd softprobe-otlp-backend
cargo test

# View logs if needed
./scripts/test-setup.sh logs

# Stop infrastructure
./scripts/test-setup.sh teardown
```

## Test Types

### 1. Unit Tests (Fast, No Dependencies)

```bash
# Run only unit tests (no infrastructure needed)
cargo test --lib

# Or using Makefile
make test-quick
```

**When to use:** During development for quick feedback on code changes.

### 2. Local Integration Tests (Requires MinIO)

```bash
# Start infrastructure first
make setup-local

# Run integration tests with local MinIO
make test-local

# Equivalent to:
ICEBERG_TEST_TYPE=local cargo test --test iceberg_integration_test -- --test-threads=1
```

**When to use:** Before committing changes, to verify end-to-end functionality.

### 3. Cloudflare R2 Integration Tests (Cloud)

```bash
# Run integration tests with Cloudflare R2
make test-r2

# Equivalent to:
ICEBERG_TEST_TYPE=r2 cargo test --test iceberg_integration_test -- --test-threads=1
```

**Requirements:**
- Valid R2 credentials in `tests/config/test-r2.yaml`
- Internet connection
- Auto-detects sandboxed environments and enables TLS bypass if needed

**When to use:** To test against production-like cloud infrastructure.

## Environment Detection

The test infrastructure automatically detects your environment:

### Normal Environment
- Direct internet access
- Standard TLS validation
- No special configuration needed

### Sandboxed/Restricted Environment
- Behind corporate proxy or MITM proxy
- TLS interception (e.g., Anthropic sandbox, corporate network)
- **Automatically enables TLS bypass** when running `make test-r2`

### CI/CD Environment
- Uses GitHub Actions service containers
- Automatically starts MinIO and Iceberg REST
- Uses `make test-ci` or run workflow

## Configuration Files

### Local MinIO Tests
**File:** `tests/config/test.yaml`

```yaml
iceberg:
  catalog_type: rest
  catalog_uri: http://localhost:8181
  warehouse: s3://warehouse/iceberg
  table_name: test_spans

s3:
  endpoint: http://localhost:9002
  access_key_id: minioadmin
  secret_access_key: minioadmin
```

### Cloudflare R2 Tests
**File:** `tests/config/test-r2.yaml`

```yaml
iceberg:
  catalog_type: rest
  catalog_uri: https://catalog.cloudflarestorage.com/...
  catalog_token: "your-token-here"
  warehouse: "your-warehouse-id"
  table_name: test_spans

s3:
  endpoint: https://<account>.r2.cloudflarestorage.com
  access_key_id: "your-access-key"
  secret_access_key: "your-secret-key"
```

**⚠️ Security Note:** The test-r2.yaml file contains real credentials. Never commit real credentials to git. Rotate tokens after use in shared environments.

## Environment Variables

### Test Selection
- `ICEBERG_TEST_TYPE` - Which backend to use
  - `local` (default) - Use local MinIO + Iceberg REST
  - `r2` - Use Cloudflare R2

### TLS Configuration
- `ICEBERG_DISABLE_TLS_VALIDATION` - Disable TLS validation
  - Not set (default) - Normal TLS validation
  - Set to any value - Disable validation (for sandboxed environments)
  - **Auto-detected by `make test-r2`**

### Debugging
- `RUST_BACKTRACE=1` - Show full stack traces on errors
- `RUST_LOG=debug` - Enable debug logging

## Troubleshooting

### Problem: "Failed to connect to catalog"

**Symptoms:**
```
Error: failed to lookup address information: Temporary failure in name resolution
```

**Solution:**
```bash
# Verify infrastructure is running
make check-local

# If not running, start it
make setup-local

# Check service health
curl http://localhost:9002/minio/health/live
curl http://localhost:8181/v1/config
```

### Problem: "Invalid peer certificate: UnknownIssuer"

**Symptoms:**
```
Error: invalid peer certificate: UnknownIssuer
```

**Cause:** You're in a sandboxed environment with TLS interception

**Solution:**
```bash
# Use make test-r2 (auto-detects and fixes)
make test-r2

# Or manually set the environment variable
ICEBERG_DISABLE_TLS_VALIDATION=1 ICEBERG_TEST_TYPE=r2 cargo test --test iceberg_integration_test
```

### Problem: "Tests fail when run in parallel"

**Symptoms:**
```
Error: table already exists / table not found
```

**Cause:** Tests share the same table and conflict

**Solution:**
```bash
# Always run integration tests with --test-threads=1
cargo test --test iceberg_integration_test -- --test-threads=1

# Or use make targets (already configured)
make test-local
make test-r2
```

### Problem: "MinIO not ready"

**Symptoms:**
```
❌ MinIO not ready
```

**Solution:**
```bash
# Check Docker is running
docker ps

# Restart infrastructure
make teardown-local
make setup-local

# Check logs for errors
./scripts/test-setup.sh logs
```

### Problem: "Namespace does not exist"

**Symptoms:**
```
Error: Tried to create a table under a namespace that does not exist
```

**Solution for R2:**
```bash
# Create the 'default' namespace in R2
curl -X POST "https://catalog.cloudflarestorage.com/<account>/<catalog>/v1/namespaces" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"namespace": ["default"], "properties": {}}'
```

**Solution for local:**
```bash
# Restart infrastructure (recreates namespaces)
make setup-local
```

## CI/CD Integration

### GitHub Actions

The CI workflow automatically:
1. Starts MinIO and Iceberg REST services
2. Waits for services to be healthy
3. Runs unit tests
4. Runs integration tests with local backend
5. Cleans up services

**Configuration:** `.github/workflows/ci.yml`

### Running CI Locally

```bash
# Simulate CI environment
make ci-full

# Or step by step
make check-fmt
make lint
make build
make test-ci
```

## Test Infrastructure Details

### Local Services

**MinIO (S3-compatible storage)**
- Console: http://localhost:9001
- API: http://localhost:9002
- Credentials: minioadmin / minioadmin

**Iceberg REST Catalog**
- REST API: http://localhost:8181
- Warehouse: s3://warehouse/

### Docker Compose

Services are defined in `/docker-compose.yml`:
- `minio` - Object storage
- `iceberg-rest` - Catalog service

```bash
# Manual control
docker-compose up -d minio iceberg-rest    # Start
docker-compose logs -f minio iceberg-rest  # View logs
docker-compose down -v                     # Stop and remove volumes
```

## Best Practices

### Before Committing
```bash
# 1. Format code
make fmt

# 2. Run lints
make lint

# 3. Run all local tests
make test-all

# 4. Check CI will pass
make ci-full
```

### Writing Tests

**Unit tests** (fast, no dependencies):
- Test individual functions and logic
- Mock external dependencies
- Place in `src/` with `#[cfg(test)]`

**Integration tests** (slower, require infrastructure):
- Test end-to-end workflows
- Use real catalog and storage
- Place in `tests/` directory
- Always use `--test-threads=1` for Iceberg tests

### Test Isolation

Integration tests share infrastructure, so:
- Use unique session IDs (UUID) for test data
- Use `--test-threads=1` to avoid conflicts
- Clean up is handled by table recreation

## Performance Tips

### Faster Test Runs

```bash
# 1. Run only unit tests during development
make test-quick

# 2. Cache Rust dependencies
# CI automatically caches ~/.cargo

# 3. Use release builds for performance testing
cargo test --release

# 4. Run specific tests
cargo test test_iceberg_writer_initialization
```

### Faster Infrastructure Startup

```bash
# Keep infrastructure running between test runs
make setup-local
# ... run tests multiple times ...
make teardown-local  # Only when done
```

## Advanced Usage

### Custom Test Configuration

```bash
# Use custom config file
CONFIG_FILE=my-config.yaml cargo test

# Override specific settings
S3_ENDPOINT=http://my-minio:9000 cargo test
```

### Debugging Tests

```bash
# Show all output (don't capture)
cargo test -- --nocapture

# Show debug logs
RUST_LOG=debug cargo test -- --nocapture

# Run with full backtrace
RUST_BACKTRACE=full cargo test
```

### Testing Against Different Backends

```bash
# Local MinIO
ICEBERG_TEST_TYPE=local make test-local

# Cloudflare R2
ICEBERG_TEST_TYPE=r2 make test-r2

# AWS S3 (if configured)
ICEBERG_TEST_TYPE=aws cargo test  # (requires setup)
```

## Summary

**For everyday development:**
```bash
make test-quick          # Fast unit tests
```

**Before committing:**
```bash
make test-all            # Full local test suite
```

**For cloud verification:**
```bash
make test-r2             # Test with Cloudflare R2
```

**In CI/CD:**
```bash
make test-ci             # Automated CI testing
```

All test commands are designed to be reliable across different environments with automatic detection and configuration.
