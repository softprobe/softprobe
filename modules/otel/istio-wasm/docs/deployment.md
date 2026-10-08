# Deploying the OpenTelemetry Istio Wasm module

This module instruments HTTP traffic at the Istio sidecar and exports
OpenTelemetry traces to the configured Softprobe endpoint. It is an optional
telemetry source; `thelake` remains the primary telemetry backend.

## Requirements

- Kubernetes with Istio 1.18 or later
- An image registry accessible to the Envoy sidecars
- Network access from the mesh to the configured OTLP endpoint

## Quick start

The minimal manifest installs client and server WasmPlugin resources and the
service entry used for the default Softprobe endpoint:

```bash
kubectl apply -f modules/otel/istio-wasm/deploy/minimal.yaml
```

The manifest currently matches all HTTP traffic. Narrow `collectionRules` or
use the scoped Bookinfo manifest before enabling collection in a workload with
traffic that should not be captured.

For a mesh-wide install, use the production manifest:

```bash
kubectl apply -f modules/otel/istio-wasm/deploy/production/sp-istio-agent.yaml
```

For a scoped Bookinfo example:

```bash
kubectl apply -f modules/otel/istio-wasm/deploy/production/test-bookinfo.yaml
```

## Configure the endpoint and collection rules

Set `sp_backend_url` to the OTLP HTTP endpoint and configure the supported
`collectionRules` in the WasmPlugin manifest. The default endpoint is
`https://o.softprobe.ai`. The bundled manifests include an Istio ServiceEntry
and DestinationRule for that host.

The filter captures HTTP request and response data. Review the allowed hosts
and paths before deployment, and keep collected payload data within your
organization's telemetry policy.

## Install a pinned module image

The checked-in manifests retain the previously published image references so
existing installs continue to resolve during the move. New module releases
publish `softprobe/otel-istio-wasm`; release assets include manifests pinned to
that module version and its matching SHA256. Module releases use tags of the
form `otel-istio-wasm-vX.Y.Z`.

## Verify and remove

```bash
kubectl get wasmplugin -A
kubectl logs -n istio-system deploy/istiod | grep sp-istio
```

Remove the selected manifest with the matching `kubectl delete -f` command.
