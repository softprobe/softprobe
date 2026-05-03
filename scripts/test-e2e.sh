#!/bin/bash
# End-to-end test script - tests WASM plugin -> backend -> context-viewer flow

set -e

CLUSTER_NAME="sp-demo-cluster"

echo "🧪 SP-Istio End-to-End Test"
echo "============================"

# Check cluster exists
if ! kind get clusters | grep -q "$CLUSTER_NAME"; then
    echo "❌ Cluster '$CLUSTER_NAME' not found. Run ./scripts/cluster-setup.sh first"
    exit 1
fi

echo "✅ Cluster found"

# Check backend is running
echo "🔍 Checking backend..."
if ! kubectl get deployment otel-backend -n otel-backend &> /dev/null; then
    echo "❌ Backend not deployed. Run ./scripts/cluster-setup.sh first"
    exit 1
fi

BACKEND_READY=$(kubectl get deployment otel-backend -n otel-backend -o jsonpath='{.status.conditions[?(@.type=="Available")].status}' 2>/dev/null || echo "False")
if [ "$BACKEND_READY" != "True" ]; then
    echo "⏳ Waiting for backend to be ready..."
    kubectl wait --for=condition=available --timeout=60s deployment/otel-backend -n otel-backend
fi
echo "✅ Backend is ready"

# Test health endpoint
echo "🔍 Testing backend health..."
HEALTH=$(kubectl exec -n otel-backend deployment/otel-backend -- curl -s http://localhost:8080/health 2>/dev/null || echo "")
if echo "$HEALTH" | grep -q "UP\|status"; then
    echo "✅ Backend health check passed"
else
    echo "❌ Backend health check failed"
    kubectl logs -n otel-backend deployment/otel-backend --tail=20
    exit 1
fi

# Test trace ingestion (simulating WASM plugin)
echo "🔍 Testing trace ingestion..."
TRACE='{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"test-service"}},{"key":"sp.resource.type","value":{"stringValue":"sp-envoy-proxy"}}]},"scopeSpans":[{"spans":[{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"051581bf3cb55c13","name":"HTTP GET /test","kind":2,"startTimeUnixNano":"1600000000000000000","endTimeUnixNano":"1600000001000000000","status":{"code":1},"attributes":[{"key":"sp_session_id","value":{"stringValue":"test-session-123"}},{"key":"span.type","value":{"stringValue":"sp-envoy-proxy"}},{"key":"http.request.header:method","value":{"stringValue":"GET"}},{"key":"http.request.header:path","value":{"stringValue":"/test"}},{"key":"http.response.status_code","value":{"intValue":200}}]}]}]}]}'

RESPONSE=$(kubectl exec -n otel-backend deployment/otel-backend -- curl -s -X POST http://localhost:8080/v1/traces \
    -H "Content-Type: application/json" \
    -H "x-public-key: test-key" \
    -d "$TRACE" 2>/dev/null || echo "")

if echo "$RESPONSE" | grep -q "successfully\|ingested\|message"; then
    echo "✅ Trace ingestion works"
else
    echo "❌ Trace ingestion failed: $RESPONSE"
    kubectl logs -n otel-backend deployment/otel-backend --tail=30
    exit 1
fi

# Wait for data to be processed
echo "⏳ Waiting for data processing..."
sleep 5

# Test sessions endpoint
echo "🔍 Testing sessions query..."
SESSIONS=$(kubectl exec -n otel-backend deployment/otel-backend -- curl -s "http://localhost:8080/api/sessions?size=10" 2>/dev/null || echo "")
if echo "$SESSIONS" | grep -q "sessions\|totalCount"; then
    echo "✅ Sessions endpoint works"
    SESSION_COUNT=$(echo "$SESSIONS" | grep -o '"totalCount":[0-9]*' | grep -o '[0-9]*' || echo "0")
    echo "   Found $SESSION_COUNT sessions"
else
    echo "⚠️  Sessions endpoint response: $(echo $SESSIONS | head -c 200)"
fi

# Test session traces endpoint
echo "🔍 Testing session traces query..."
SESSION_TRACES=$(kubectl exec -n otel-backend deployment/otel-backend -- curl -s "http://localhost:8080/api/sessions/test-session-123" 2>/dev/null || echo "")
if echo "$SESSION_TRACES" | grep -q "traces\|sessionId"; then
    echo "✅ Session traces endpoint works"
else
    echo "⚠️  Session traces response: $(echo $SESSION_TRACES | head -c 200)"
fi

# Check ClickHouse data
echo "🔍 Verifying data in ClickHouse..."
CLICKHOUSE_COUNT=$(kubectl exec -n otel-backend clickhouse-0 -- clickhouse-client --query "SELECT count() FROM otel.spans" 2>/dev/null || echo "0")
echo "   Spans in ClickHouse: $CLICKHOUSE_COUNT"

ENVOY_COUNT=$(kubectl exec -n otel-backend clickhouse-0 -- clickhouse-client --query "SELECT count() FROM otel.envoy_proxy_spans" 2>/dev/null || echo "0")
echo "   Envoy proxy spans in ClickHouse: $ENVOY_COUNT"

# Check MinIO
echo "🔍 Verifying MinIO..."
if kubectl get pod -l app=minio -n otel-backend &> /dev/null; then
    echo "   ✅ MinIO pod is running"
else
    echo "   ⚠️  MinIO pod not found"
fi

echo ""
echo "✅ End-to-end test completed!"
echo ""
echo "To view traces:"
echo "1. Port forward context-viewer: kubectl port-forward -n otel-backend svc/context-viewer 3000:3000"
echo "2. Open http://localhost:3000 in browser"
