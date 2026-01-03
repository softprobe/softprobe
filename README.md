# SoftProbe OTLP Backend

OpenTelemetry-compatible backend for recording storage/retrieval with Apache Iceberg and S3.

## Overview

This Rust service provides:
- **OTLP-compatible ingestion API** for receiving recordings from Java agents
- **Iceberg metadata storage** for queryable recording metadata
- **S3 payload storage** for request/response bodies in batched Parquet files
- **DuckDB query engine** for fast analytics queries
- **Automatic compaction** to maintain query performance

## Architecture

See `docs/design.md` for detailed architecture documentation.

## Configuration

Configuration can be provided via:
1. **Config file**: `config.yaml` (default)
2. **Environment variables**: Override specific settings
3. **Defaults**: Sensible defaults for development

### Environment Variables

- `PORT`: Server port (default: 8090)
- `S3_BUCKET`: S3 bucket name (default: softprobe-recordings)
- `S3_REGION`: AWS region (default: us-east-1)
- `CONFIG_FILE`: Path to config file (default: config.yaml)

## Development

### Prerequisites

- Rust 1.70+
- AWS credentials configured (for S3 access)
- Docker (for local testing with MinIO)

### Build

```bash
cargo build --release
```

### Run

```bash
cargo run
```

### Tests

**Quick Start:**
```bash
# Start test infrastructure
make setup-local

# Run all tests
make test-local

# Stop infrastructure
make teardown-local
```

**For detailed testing instructions, see [TESTING.md](TESTING.md)**

**Common test commands:**
```bash
make test-quick      # Unit tests only (fast, no infrastructure)
make test-local      # Integration tests with local MinIO
make test-r2         # Integration tests with Cloudflare R2
make test-all        # All tests
```

## Make Commands (Holistic View)

### Core Development
```bash
make build
make lint
make fmt
make check-fmt
make clean
```

### Infrastructure
```bash
make setup-local
make teardown-local
make check-local
```

### Tests
```bash
make test-quick
make test-local
make test-r2
make test-ci
make test-all
```

### Data & Verification
```bash
make generate-telemetry
make verify-e2e
make demo-session
make duckdb-shell
make drop-tables
```

### Discoverability
```bash
make help
make help-scripts
```

## API Endpoints

### Health Check

```bash
curl http://localhost:8090/health
```

### Ingestion

```bash
POST /v1/recordings/ingest
Content-Type: application/json

{
  "recordings": [
    {
      "record_id": "rec-123",
      "app_id": "app-1",
      "operation_name": "POST /api/payment",
      ...
    }
  ]
}
```

### Query

```bash
POST /v1/recordings/query
Content-Type: application/json

{
  "app_id": "app-1",
  "start_time": "2025-01-01T00:00:00Z",
  "end_time": "2025-01-02T00:00:00Z"
}
```

### Retrieve Payloads

```bash
POST /v1/recordings/retrieve
Content-Type: application/json

{
  "record_ids": ["rec-123", "rec-456"]
}
```

## Design Document

All implementation follows the design document:
- **Location**: `../docs/migration-to-iceberg-design.md`
- **Version**: 1.7 (latest)
- **Status**: Reviewed and approved

## License

Apache 2.0
