# Deploy the OpenTelemetry Istio Wasm module

Run the following commands from the repository root. This module captures
configured HTTP traffic and exports OpenTelemetry traces.

## Minimal install

```bash
kubectl apply -f modules/otel/istio-wasm/deploy/minimal.yaml
```

## Mesh-wide install

```bash
kubectl apply -f modules/otel/istio-wasm/deploy/production/sp-istio-agent.yaml
```

## Scoped Bookinfo example

```bash
kubectl label namespace default istio-injection=enabled --overwrite
kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.22/samples/bookinfo/platform/kube/bookinfo.yaml
kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.22/samples/bookinfo/networking/bookinfo-gateway.yaml
kubectl apply -f modules/otel/istio-wasm/deploy/production/test-bookinfo.yaml
```

Review each manifest's `collectionRules` before deployment. The quickstart
rules can match broad traffic, and the filter captures request and response
data.

## Verify or remove

```bash
kubectl get wasmplugin -A
kubectl delete -f modules/otel/istio-wasm/deploy/minimal.yaml
```
