# Quick Start - Full Stack Local Deployment

This directory contains deployment manifests for running the full stack locally in Kind/K8s without requiring Softprobe account or API keys.

## Components

1. **ClickHouse** - Database for storing spans
2. **Otel Backend** - Spring Boot backend service
3. **Context Viewer** - Next.js frontend for visualizing traces

## Prerequisites

- Kind cluster with Istio installed (use `scripts/cluster-setup.sh`)
- kubectl configured

## Deployment Steps

These steps integrate with the existing quick start flow:

```bash
# 1. Set up Kind cluster, Istio, and OpenTelemetry Operator
curl -L https://raw.githubusercontent.com/softprobe/softprobe/refs/heads/main/scripts/cluster-setup.sh | sh

# 2. Deploy backend infrastructure (ClickHouse)
kubectl apply -f deploy/quickstart/clickhouse.yaml

# 3. Build backend image and load into Kind
cd backend
docker build -t otel-backend:latest .
kind load docker-image otel-backend:latest --name sp-demo-cluster
cd ..

# 4. Deploy backend service
kubectl apply -f deploy/quickstart/backend.yaml

# 5. Build context-viewer image and load into Kind
cd context-viewer
docker build -t context-viewer:latest .
kind load docker-image context-viewer:latest --name sp-demo-cluster
cd ..

# 6. Deploy context viewer
kubectl apply -f deploy/quickstart/context-viewer.yaml

# 7. Deploy WASM plugin pointing to local backend
kubectl apply -f deploy/quickstart/wasm-plugin.yaml

# 8. Deploy demo app
kubectl apply -f examples/travel/apps.yaml

# 9. Port forward to access services
kubectl port-forward -n otel-backend svc/otel-backend 8080:8080 &
kubectl port-forward -n otel-backend svc/context-viewer 3000:3000 &
kubectl port-forward -n istio-system svc/istio-ingressgateway 8081:80
```

## Accessing the Services

- **Demo App**: http://localhost:8081
  - Use the travel demo to generate traffic

- **Backend API**: http://localhost:8080
  - Health: http://localhost:8080/health
  - API Docs: http://localhost:8080/swagger-ui.html
  - Sessions: http://localhost:8080/api/sessions
  - Traces: http://localhost:8080/api/sessions/{sessionId}

- **Context Viewer**: http://localhost:3000
  - View trace visualizations
  - Filter and search sessions
  - No Softprobe account required!

## Configuration

### Backend Configuration

Edit `backend.yaml` ConfigMap to customize:
- ClickHouse connection settings
- Storage provider (S3/GCS)
- Cache TTL

### Context Viewer Configuration

Edit `context-viewer.yaml` ConfigMap to set:
- `NEXT_PUBLIC_API_BASE_URL` - Backend API URL

### WASM Plugin Configuration

Edit `wasm-plugin.yaml` to customize:
- `sp_backend_url` - Backend service URL
- Collection rules
- Traffic direction

## Troubleshooting

### Check Backend Logs

```bash
kubectl logs -n otel-backend deployment/otel-backend
```

### Check ClickHouse

```bash
kubectl exec -it -n otel-backend clickhouse-0 -- clickhouse-client
```

### Verify Services

```bash
# Health check
curl http://localhost:8080/health

# List sessions
curl http://localhost:8080/api/sessions
```

### Check WASM Plugin

```bash
kubectl get wasmplugin -A
kubectl logs <pod-name> -c istio-proxy | grep SP
```

