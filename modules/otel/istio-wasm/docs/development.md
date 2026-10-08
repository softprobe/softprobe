# Developing the OpenTelemetry Istio Wasm module

The module captures HTTP traffic in Envoy sidecars, creates OpenTelemetry trace
spans, propagates W3C trace context, and exports OTLP data. Its Rust crate and
all module-specific build, test, and deployment files are under
`modules/otel/istio-wasm/`.

## Prerequisites

- Rust stable and Cargo
- The `wasm32-unknown-unknown` target
- Protocol Buffers compiler
- Docker for the Envoy integration harness
- Kubernetes, Kind, and Istio for cluster workflows

Install the Rust target with:

```bash
rustup target add wasm32-unknown-unknown
```

## Build

From the repository root:

```bash
make build
```

Or from this module directory, run the same target. The binary and SHA256 file
are written under the repository-root `target/wasm32-unknown-unknown/release/`.

The crate's `build.rs` compiles the vendored files in
`opentelemetry/proto/`; keep those definitions alongside the crate when
reorganizing it.

## Local Envoy integration

```bash
make integration-test
```

This builds the Wasm binary and starts the Envoy and Go harness defined in
`test/docker-compose.yml`. Use `make test-logs` to inspect the harness logs.

## Istio development workflow

The root Makefile forwards module targets. For example:

```bash
make dev-quickstart
make forward
make status
make dev-reload
make cluster-down
```

The quickstart creates a Kind cluster, deploys the local Wasm server and demo,
and configures Istio to load the built module. The deployment files are under
`deploy/`; the cluster setup is under `scripts/`.

## Repository layout

- `src/`: HTTP interception, filtering, trace context, and OTLP span handling
- `opentelemetry/proto/`: protobuf definitions used by the crate build
- `test/`: local Envoy and Go integration harness
- `deploy/`: Istio and OpenTelemetry Kubernetes resources
- `examples/`: sample workload configuration
- `scripts/`: cluster and module development helpers
