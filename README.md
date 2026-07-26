# Softprobe Runtime

Rust runtime for authenticated OTLP ingestion, tenant-scoped DuckLake storage,
DuckDB queries, telemetry search, and capture/replay control sessions.

## Architecture

DuckLake is the only durable telemetry backend:

```text
OTLP HTTP/gRPC
  -> tenant-bound runtime
  -> Arrow + temporary Parquet
  -> DuckLake transaction
  -> PostgreSQL/SQLite metadata
  -> inlined rows or Parquet under data_path
```

Ingest is flush-through: one OTLP request becomes one DuckLake commit. The
OpenTelemetry collector owns batching. There is no application ingest buffer,
staged storage tier, application WAL, Apache Iceberg, or Lakekeeper path.

See [`docs/design.md`](docs/design.md) for the current architecture and
[`docs/legacy/`](docs/legacy/README.md) for superseded designs.

## Local development

Prerequisites:

- Rust toolchain
- Docker and Docker Compose
- a dynamic DuckDB library (`DUCKDB_DOWNLOAD_LIB=1` lets the build fetch it)

Start MinIO, DuckLake PostgreSQL, and Redis:

```bash
make setup-local
```

Build and run checks:

```bash
DUCKDB_DOWNLOAD_LIB=1 make build
make test
make lint
make check-fmt
```

Stop local infrastructure:

```bash
make teardown-local
```

`make test` is the pre-merge test target. It runs unit tests and isolated
integration tests against MinIO, PostgreSQL, and Redis.

Local Redis is published on host port **6380** by default (`REDIS_PORT`) so it
does not collide with workspace demo Redis on **6379**. Override if needed:
`REDIS_PORT=6390 make setup-local test`.

## Run

The main binary requires control-plane Redis wiring:

```bash
export REDIS_HOST=127.0.0.1
export CONFIG_FILE=config.yaml
cargo run --bin softprobe-runtime
```

Defaults:

- HTTP: `0.0.0.0:8090`
- OTLP/gRPC traces: `0.0.0.0:4317`
- config file: `config.yaml`

Set `SOFTPROBE_GRPC_DISABLE=1` to disable the gRPC listener.

## Configuration

The canonical example is [`config.yaml`](config.yaml). The active storage
section is `ducklake`:

```yaml
ducklake:
  catalog_type: "postgres" # postgres (production) or sqlite (local)
  metadata_path: "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake"
  data_path: "./warehouse/ducklake/data/"
  catalog_alias: "softprobe"
  metadata_schema: "softprobe"
  data_inlining_row_limit: 10000
  writer_pool_size: 4
```

YAML holds non-secret settings only. The top-level sections are `server`,
`object_store` (`region` / optional `endpoint`), `query`, `maintenance`,
`ducklake`, and `dropdown_catalog`. Unknown or legacy keys are rejected. Object
storage credentials are never stored in YAML; resolve them from the
environment:

- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` [/ `AWS_SESSION_TOKEN`]: `s3://`
  paths (MinIO, R2, AWS)
- `GCS_HMAC_ACCESS_KEY_ID` / `GCS_HMAC_SECRET` (or `GCP_HMAC_*`): `gs://` paths

Supported direct environment overrides are:

- `CONFIG_FILE`
- `PORT`
- `S3_REGION`
- `SOFTPROBE_MAX_HTTP_BODY_BYTES`

The runtime also uses deployment variables such as `REDIS_HOST`,
`REDIS_PORT`, `REDIS_PASSWORD`, `SOFTPROBE_AUTH_URL`, `SOFTPROBE_LISTEN_ADDR`,
and `OTEL_GRPC_PORT`.

For `gs://` DuckLake paths, DuckDB uses GCS HMAC interoperability credentials:
`GCS_HMAC_ACCESS_KEY_ID` and `GCS_HMAC_SECRET` (or their `GCP_HMAC_*`
aliases).

## Main HTTP endpoints

Health and discovery:

- `GET /health`
- `GET /ready`
- `GET /openapi.json`
- `GET /swagger`

OTLP ingestion:

- `POST /v1/traces`
- `POST /v1/logs`
- `POST /v1/metrics`

LLM evaluation:

- `POST /v1/llm/scores`

Query and telemetry:

- `POST /v1/query/sql` (internal/debug SQL surface)
- `POST /v1/telemetry/search`
- `POST /v1/telemetry/details`
- `GET /v1/telemetry/fields`
- `GET /v1/telemetry/fields/{field}/values`
- `GET /v1/telemetry/sessions/{session_id}`
- `GET /v1/telemetry/traces/{trace_id}`
- `GET /v1/data/ducklake-connection`

Control-plane routes also cover tenant provisioning, sessions, injection,
captures, promotions, and dropdown catalog lookups. `/v1/*` operational routes
require bearer authentication; tenant provisioning validates its admin bearer
inside the handler.

The focused ingestion and promotion HTTP contract is
[`docs/ingestion-openapi.yaml`](docs/ingestion-openapi.yaml). Schema promotion
semantics are in [`docs/promotion.md`](docs/promotion.md).

## Query DuckLake locally

```bash
make duckdb-shell
```

This renders the configured DuckLake ATTACH statement, performs a `SELECT 1`
smoke, and starts DuckDB. See
[`docs/adhoc-duckdb-ducklake.md`](docs/adhoc-duckdb-ducklake.md).

## Instrumentation and promotion

HTTP bodies are captured from `http.request` and `http.response` span events.
When those event fields are absent, the runtime accepts equivalent span
attributes, including OBI `.content` body keys. Business identifiers are
explicit searchable `sp.*` span attributes set by the application — Softprobe
does not invent them.

- Instrumentation: [`docs/instrumentation_guide.md`](docs/instrumentation_guide.md)
- Schema promotion (explicit manifests for declared `sp.*` and other sources):
  [`docs/promotion.md`](docs/promotion.md)

## Maintenance

The runtime schedules DuckLake-native maintenance for every configured tenant
scope:

- merge adjacent data files;
- expire old snapshots;
- clean old files;
- prune optional dropdown-catalog values.

Settings are under `maintenance` and `dropdown_catalog` in `config.yaml`.

## License

Apache-2.0
