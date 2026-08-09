# Softprobe Runtime

> **The durable evidence foundation for production AI.** Softprobe preserves
> AI traces and recordings as customer-controlled data assets, directly
> queryable with standard SQL and reusable across investigation, evaluation,
> regression, governance, and continuous-improvement workflows.

Rust runtime for authenticated OTLP ingestion, tenant-scoped DuckLake storage,
DuckDB queries, and telemetry search.

Traditional software telemetry is often retained only for a short incident
window. AI traces have lasting value: today's production recording can become
tomorrow's evaluation case, regression test, audit evidence, or improvement
dataset. Softprobe keeps that evidence open and durable, while Parquet VARIANT
shredding and tenant-controlled column promotion provide workload-specific
query paths without discarding the original context.

See the
[product and competitive positioning](docs/positioning.md) for the strategy
and technical rationale.

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

Start MinIO and DuckLake PostgreSQL:

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
integration tests against MinIO and PostgreSQL (performance is separate).

GitHub Actions (self-hosted Linux; Make-only):

- `.github/workflows/ci.yml` — on push/PR: `make setup-local` then `make ci-full`
  (`check-fmt`, `lint`, `build-release` → `dist/`, `test-ci`). Warm SLO ≤ 15m.
- `.github/workflows/performance.yml` — **manual** only: `make test-perf`
  (`PERF_SUITE=all|latency|concurrency|stability`, `PERF_TARGET_MS=1000`). Warm SLO ≤ 8m.
- `.github/workflows/release.yml` — on GitHub Release: `make release`
  (`ci-full` + `test-perf` + `publish-docker`). Warm SLO ≤ 25m.

## Run

```bash
export CONFIG_FILE=config.yaml
export SOFTPROBE_AUTH_URL=http://127.0.0.1:8091/validate
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

The runtime also uses deployment variables such as `SOFTPROBE_AUTH_URL`,
`SOFTPROBE_LISTEN_ADDR`, and `OTEL_GRPC_PORT`.

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

Control-plane routes also cover tenant provisioning, promotions, and dropdown
catalog lookups. `/v1/*` operational routes require bearer authentication;
tenant provisioning validates its admin bearer inside the handler.

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

## Publish Docker image

Product bits are built **once on the host** (`make build-release` → cargo-chef +
`cargo build --release --locked` → `dist/`). The Dockerfile is packaging-only
(`COPY dist/…`); it never runs cargo.

Official path: GitHub Release → `.github/workflows/release.yml` → `make release`
(same `ci-full` + `test-perf` + `publish-docker` as local).

Local/emergency image push: `make build-release && make publish-docker TAG=vX.Y.Z`
(on Mac, `build-release` uses a linux/amd64 builder container running the same
script). `build.sh` only tags/pushes; it refuses to run without a complete
`dist/`. Optional BuildKit registry cache (`…/splake:buildcache`) speeds base
layers only — do not deploy `:buildcache` as a runtime image.

## License

Apache-2.0
