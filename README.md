# Softprobe

Softprobe is being organized around agent observability and evaluation. The
primary telemetry backend is
[`softprobe/thelake`](https://github.com/softprobe/thelake); this repository
currently contains the Istio tracing integration, with agent and evaluation
components to be added as they are developed.

## OpenTelemetry modules

The retained Istio WebAssembly integration lives in
[`modules/otel/istio-wasm`](modules/otel/istio-wasm). It captures HTTP traffic
from Istio workloads and exports trace data through the OpenTelemetry protocol.
It remains a standalone Rust crate and can be developed from the repository root:

```bash
make build
make integration-test
```

The module's [development guide](modules/otel/istio-wasm/docs/development.md)
and [deployment guide](modules/otel/istio-wasm/docs/deployment.md) cover local
setup and Istio installation. The root Cargo workspace currently contains this
module.
