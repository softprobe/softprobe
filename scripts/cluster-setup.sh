#!/bin/bash

# SP Istio WASM - Kubernetes cluster and Istio setup script
# This script creates and configures the entire environment from scratch, including Kind cluster, Istio service mesh, and OpenTelemetry

set -e

CLUSTER_NAME="sp-demo-cluster"

# 1. Create Kind cluster
echo "📦 Creating Kind cluster..."
if kind get clusters | grep -q "$CLUSTER_NAME"; then
    echo "⚠️  Cluster $CLUSTER_NAME already exists, skipping creation"
else
    kind create cluster --name $CLUSTER_NAME
    echo "✅ Kind cluster creation completed"
fi

# Check cluster connection
echo "🔍 Checking cluster connection..."
if ! kubectl cluster-info &> /dev/null; then
    echo "❌ Unable to connect to Kubernetes cluster"
    exit 1
fi
echo "✅ Cluster connection is normal"

# 2. Install Istio
echo "🌐 Installing Istio..."
if kubectl get namespace istio-system &> /dev/null; then
    echo "⚠️  Istio is already installed, skipping installation step"
else
    istioctl install --set values.defaultRevision=default -y
    echo "✅ Istio installation completed"
fi

# 3. Enable Istio injection
echo "💉 Enabling Istio injection for default namespace..."
kubectl label namespace default istio-injection=enabled --overwrite
echo "✅ Istio injection enabled"

# 4. Install cert-manager (dependency for OpenTelemetry Operator)
echo "🔐 Installing cert-manager..."
if kubectl get deployment cert-manager -n cert-manager &> /dev/null; then
    echo "⚠️  cert-manager is already installed, skipping installation step"
else
    kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.0/cert-manager.yaml
    
    # Wait for cert-manager to be ready
    echo "⏳ Waiting for cert-manager to be ready..."
    kubectl wait --for=condition=available --timeout=300s deployment/cert-manager -n cert-manager
    kubectl wait --for=condition=available --timeout=300s deployment/cert-manager-cainjector -n cert-manager
    kubectl wait --for=condition=available --timeout=300s deployment/cert-manager-webhook -n cert-manager
    echo "✅ cert-manager installation completed"
fi

# 5. Install OpenTelemetry Operator
echo "📊 Installing OpenTelemetry Operator..."
if kubectl get deployment opentelemetry-operator-controller-manager -n opentelemetry-operator-system &> /dev/null; then
    echo "⚠️  OpenTelemetry Operator is already installed, skipping installation step"
else
    kubectl apply -f https://github.com/open-telemetry/opentelemetry-operator/releases/latest/download/opentelemetry-operator.yaml
    # Wait for OpenTelemetry Operator to be ready
    echo "⏳ Waiting for OpenTelemetry Operator to be ready..."
    kubectl wait --for=condition=available --timeout=300s deployment/opentelemetry-operator-controller-manager -n opentelemetry-operator-system
    
    # Wait for webhook service to be ready
    echo "⏳ Waiting for OpenTelemetry Operator webhook service to be ready..."
    kubectl wait --for=condition=ready --timeout=300s pod -l app.kubernetes.io/name=opentelemetry-operator -n opentelemetry-operator-system
    
    # Additional wait time to ensure webhook is fully started
    echo "⏳ Waiting for webhook service to fully start..."
    sleep 30
    
    echo "✅ OpenTelemetry Operator installation completed"
fi

# 6. Apply OpenTelemetry auto-instrumentation configuration
echo "📊 Applying OpenTelemetry auto-instrumentation configuration..."
kubectl apply -f https://raw.githubusercontent.com/softprobe/softprobe/refs/heads/main/deploy/otel-auto-instrumentation.yaml

# Wait for configuration processing
echo "⏳ Waiting for configuration processing..."
sleep 10
echo "✅ OpenTelemetry auto-instrumentation configuration applied"

# 7. Deploy MinIO
echo "📦 Deploying MinIO..."
kubectl apply -f deploy/quickstart/minio.yaml
echo "⏳ Waiting for MinIO to be ready..."
kubectl wait --for=condition=ready pod -l app=minio -n otel-backend --timeout=120s
echo "⏳ Waiting for MinIO bucket setup..."
kubectl wait --for=condition=complete job/minio-setup -n otel-backend --timeout=60s 2>/dev/null || echo "⚠️  MinIO setup job may still be running"
echo "✅ MinIO deployed"

# 8. Deploy ClickHouse
echo "📦 Deploying ClickHouse..."
kubectl apply -f deploy/quickstart/clickhouse.yaml
echo "⏳ Waiting for ClickHouse to be ready..."
# Wait for StatefulSet to be ready, or fallback to pod wait
if kubectl wait --for=condition=ready statefulset/clickhouse -n otel-backend --timeout=120s 2>/dev/null; then
    echo "✅ ClickHouse StatefulSet ready"
elif kubectl wait --for=condition=ready pod -l app=clickhouse -n otel-backend --timeout=120s 2>/dev/null; then
    echo "✅ ClickHouse pod ready"
else
    echo "⚠️  ClickHouse wait timeout, but continuing..."
    sleep 5
fi
echo "✅ ClickHouse deployed"

# 9. Build and load backend image
echo "🔨 Building backend image..."
cd backend
if ! docker build -t otel-backend:latest .; then
    echo "❌ Backend build failed"
    exit 1
fi
kind load docker-image otel-backend:latest --name $CLUSTER_NAME
cd ..
echo "✅ Backend image built and loaded"

# 10. Deploy backend
echo "🚀 Deploying backend..."
kubectl apply -f deploy/quickstart/backend.yaml
echo "⏳ Waiting for backend to be ready..."
kubectl wait --for=condition=available --timeout=180s deployment/otel-backend -n otel-backend
echo "✅ Backend deployed"

# 11. Build and load context-viewer image
echo "🔨 Building context-viewer image..."
cd context-viewer
if ! docker build -t context-viewer:latest .; then
    echo "❌ Context-viewer build failed"
    exit 1
fi
kind load docker-image context-viewer:latest --name $CLUSTER_NAME
cd ..
echo "✅ Context-viewer image built and loaded"

# 12. Deploy context-viewer
echo "🚀 Deploying context-viewer..."
kubectl apply -f deploy/quickstart/context-viewer.yaml
echo "⏳ Waiting for context-viewer to be ready..."
kubectl wait --for=condition=available --timeout=180s deployment/context-viewer -n otel-backend
echo "✅ Context-viewer deployed"

# 13. Deploy WASM plugin pointing to local backend
echo "🔌 Deploying WASM plugin..."
kubectl apply -f deploy/quickstart/wasm-plugin.yaml
echo "✅ WASM plugin deployed"

echo ""
echo "🎉 Full stack environment setup completed!"
echo ""
echo "Next steps:"
echo "1. Deploy demo app: kubectl apply -f examples/travel/apps.yaml"
echo "2. Port forward services:"
echo "   - Backend: kubectl port-forward -n otel-backend svc/otel-backend 8080:8080"
echo "   - Context-viewer: kubectl port-forward -n otel-backend svc/context-viewer 3000:3000"
echo "   - Demo app: kubectl port-forward -n istio-system svc/istio-ingressgateway 8081:80"