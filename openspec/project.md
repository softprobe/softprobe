# Project Context

## Purpose
This is a standalone OpenTelemetry-compatible collector with Apache Iceberg storage backend. The project provides:

- **OTLP Ingestion**: Standard OpenTelemetry Protocol endpoints for traces, logs, and metrics
- **Iceberg Storage**: Scalable data lake storage for telemetry data with efficient querying
- **Session Management**: Session-aware buffering and batching for optimal storage efficiency
- **Query API**: Fast retrieval of telemetry data with metadata filtering and payload access
- **Analytics**: DuckDB-powered analytical queries on Iceberg data

## Tech Stack

### OTLP Collector (Rust)
- **Rust 1.70+** with Axum web framework for high-performance data ingestion
- **Apache Iceberg 0.7** for scalable data lake storage with built-in metadata
- **DuckDB** for analytical queries on telemetry data
- **Parquet** format with Zstd compression for efficient storage
- **AWS S3/MinIO/Cloudflare R2** for object storage backend

## Project Conventions

### Code Style
- **Rust**: Standard rustfmt formatting with cargo clippy for linting
- **Naming**: snake_case for Rust modules and functions, PascalCase for types
- **Error Handling**: anyhow for application errors, thiserror for library errors
- **Async**: Tokio runtime for all async operations

### Architecture Patterns
- **OTLP Standard Compliance**: Full OpenTelemetry Protocol specification support
- **Session-aware Buffering**: Intelligent batching based on session boundaries for traces/logs
- **Data Lake Architecture**: Iceberg tables with built-in metadata (manifests, partition stats)
- **Multi-app Row Groups**: Efficient Parquet organization for cross-application queries
- **RESTful Query API**: Standard HTTP endpoints for data retrieval

### Testing Strategy
- **Unit Tests**: Cargo test for component-level testing
- **Integration Tests**: Docker-based testing with MinIO/Iceberg/Trino
- **Test Targets**: `make test-quick` (unit), `make test-local` (integration)
- **CI/CD**: Automated testing on pull requests

### Git Workflow
- **Main Branch**: `main` for production releases
- **Feature Branches**: Descriptive names with context
- **Cargo Workspace**: Single-crate project with optional features
- **Docker Support**: Containerized deployment with docker-compose

## Domain Context

### Core Concepts
- **OTLP Trace**: OpenTelemetry trace data with spans, events, and attributes
- **OTLP Log**: OpenTelemetry log records with severity, body, and attributes
- **OTLP Metric**: OpenTelemetry metric data points (gauge, sum, histogram) - aggregations
- **Session**: Logical grouping of related traces/logs (NOT metrics - metrics are aggregations)
- **Session ID**: Unique identifier for correlating traces and logs within a session
- **Raw Storage**: Iceberg tables storing complete OTLP payloads in Parquet
- **Iceberg Metadata**: Built-in manifests, partition stats, and row group statistics for query optimization

### Ingestion Flow
1. **OTLP Endpoint** receives traces/logs/metrics via POST `/v1/traces`, `/v1/logs`, `/v1/metrics`
2. **Session Buffering** groups traces and logs by session_id; metrics buffered separately
3. **Batch Writing** flushes sessions to Iceberg: coordinated writes for traces/logs, separate for metrics
4. **Iceberg Commit** creates snapshots with manifest files containing partition/row-group statistics
5. **Compaction** background process optimizes Parquet files for query performance

### Query Flow
1. **Direct Iceberg Query** with predicates (session_id, time range, attributes)
2. **Manifest Pruning** Iceberg eliminates irrelevant files using partition and row-group statistics
3. **Payload Access** reads Parquet row groups with efficient predicate pushdown
4. **Result Assembly** returns traces/logs/metrics in OTLP format or JSON

## Important Constraints

### Technical Constraints
- **OTLP Compliance**: Must implement OpenTelemetry Protocol v1.0+ specification
- **Data Scale**: Handle high-throughput telemetry ingestion (10k+ spans/sec per instance)
- **Storage Efficiency**: Target >85% storage cost reduction vs. traditional TSDB
- **Memory Efficiency**: Efficient buffering and batching without excessive memory usage

### Operational Constraints
- **Horizontal Scalability**: Support multiple collector instances with consistent hashing
- **Data Retention**: Configurable TTL policies for Iceberg table lifecycle
- **Cost Optimization**: Efficient Parquet compression and partition strategies

## External Dependencies

### Storage Infrastructure
- **Object Storage**: S3-compatible API (AWS S3, MinIO, Cloudflare R2)
- **Iceberg Catalog**: REST catalog or Hive Metastore for table management

### Development Dependencies
- **Cargo**: Rust dependency resolution and build tool
- **Docker**: Container runtime for local testing and deployment
- **GitHub**: Source code repository and CI/CD integration
