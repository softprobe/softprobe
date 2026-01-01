# OpenTelemetry Instrumentation Guide

This guide shows how to instrument your application to capture HTTP bodies and business attributes for storage in the Softprobe OTLP backend.

## Design Philosophy

We use two complementary OpenTelemetry features:

1. **Span Events** - For capturing large payloads (HTTP request/response bodies)
2. **Span Attributes** - For capturing searchable business metadata (user IDs, order IDs, etc.)

This separation provides:
- **Columnar efficiency**: Parquet doesn't read body columns when querying by attributes
- **No attribute limits**: Bodies stored as events avoid the 128 span attribute limit
- **Semantic clarity**: Events represent "notable moments", attributes represent "searchable metadata"

See [ADR-006](decision_log.md#adr-006-http-bodies-via-span-events-not-span-attributes) for the full decision rationale.

## HTTP Body Instrumentation Pattern

### JavaScript/TypeScript

```javascript
import { trace, context } from '@opentelemetry/api';

const tracer = trace.getTracer('my-service');

async function handleRequest(req, res) {
  const span = tracer.startActiveSpan('POST /api/orders', async (span) => {
    try {
      // Capture HTTP request data as span event
      span.addEvent('http.request', {
        'http.request.headers': JSON.stringify(req.headers),
        'http.request.body': JSON.stringify(req.body)
      });

      // Standard HTTP attributes (searchable)
      span.setAttribute('http.request.method', 'POST');
      span.setAttribute('http.request.path', '/api/orders');

      // Business attributes for search (sp.* convention)
      span.setAttribute('sp.user.id', req.body.userId);
      span.setAttribute('sp.order.id', orderResult.orderId);
      span.setAttribute('sp.tenant.id', req.headers['x-tenant-id']);

      // Process request
      const orderResult = await createOrder(req.body);

      // Capture HTTP response data as span event
      span.addEvent('http.response', {
        'http.response.headers': JSON.stringify(res.getHeaders()),
        'http.response.body': JSON.stringify(orderResult)
      });

      // Response status (searchable)
      span.setAttribute('http.response.status_code', 201);

      span.setStatus({ code: 1 }); // OK
      span.end();

      return orderResult;
    } catch (error) {
      span.setStatus({ code: 2, message: error.message }); // ERROR
      span.end();
      throw error;
    }
  });
}
```

### Python

```python
from opentelemetry import trace
import json

tracer = trace.get_tracer(__name__)

def handle_request(request):
    with tracer.start_as_current_span("POST /api/orders") as span:
        # Capture HTTP request data as span event
        span.add_event("http.request", {
            "http.request.headers": json.dumps(dict(request.headers)),
            "http.request.body": json.dumps(request.json)
        })

        # Standard HTTP attributes (searchable)
        span.set_attribute("http.request.method", "POST")
        span.set_attribute("http.request.path", "/api/orders")

        # Business attributes for search (sp.* convention)
        span.set_attribute("sp.user.id", request.json["userId"])
        span.set_attribute("sp.tenant.id", request.headers.get("X-Tenant-Id"))

        try:
            # Process request
            order_result = create_order(request.json)

            # Business attribute from result
            span.set_attribute("sp.order.id", order_result["orderId"])

            # Capture HTTP response data as span event
            span.add_event("http.response", {
                "http.response.headers": json.dumps({"content-type": "application/json"}),
                "http.response.body": json.dumps(order_result)
            })

            # Response status (searchable)
            span.set_attribute("http.response.status_code", 201)

            return order_result

        except Exception as e:
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            raise
```

### Java

```java
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.context.Scope;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OrderController {
    private final Tracer tracer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderResponse handleRequest(OrderRequest request, HttpHeaders headers) {
        Span span = tracer.spanBuilder("POST /api/orders").startSpan();
        try (Scope scope = span.makeCurrent()) {
            // Capture HTTP request data as span event
            span.addEvent("http.request", Attributes.builder()
                .put("http.request.headers", objectMapper.writeValueAsString(headers))
                .put("http.request.body", objectMapper.writeValueAsString(request))
                .build());

            // Standard HTTP attributes (searchable)
            span.setAttribute("http.request.method", "POST");
            span.setAttribute("http.request.path", "/api/orders");

            // Business attributes for search (sp.* convention)
            span.setAttribute("sp.user.id", request.getUserId());
            span.setAttribute("sp.tenant.id", headers.getFirst("X-Tenant-Id"));

            // Process request
            OrderResponse orderResult = orderService.createOrder(request);

            // Business attribute from result
            span.setAttribute("sp.order.id", orderResult.getOrderId());

            // Capture HTTP response data as span event
            Map<String, String> responseHeaders = Map.of("Content-Type", "application/json");
            span.addEvent("http.response", Attributes.builder()
                .put("http.response.headers", objectMapper.writeValueAsString(responseHeaders))
                .put("http.response.body", objectMapper.writeValueAsString(orderResult))
                .build());

            // Response status (searchable)
            span.setAttribute("http.response.status_code", 201);

            return orderResult;

        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
}
```

## Business Attribute Convention: `sp.*` Namespace

Use the `sp.*` prefix for all business-specific attributes that you want to query on:

### Common Patterns

```javascript
// User identification
span.setAttribute('sp.user.id', 'user-123');
span.setAttribute('sp.user.email', 'user@example.com');

// Order/transaction tracking
span.setAttribute('sp.order.id', 'ORD-456');
span.setAttribute('sp.transaction.id', 'TXN-789');

// Multi-tenancy
span.setAttribute('sp.tenant.id', 'tenant-abc');
span.setAttribute('sp.organization.id', 'org-xyz');

// Session tracking
span.setAttribute('sp.session.id', 'sess-12345');

// Travel industry
span.setAttribute('sp.pnr', 'ABC123');
span.setAttribute('sp.booking.reference', 'BK-456');

// E-commerce
span.setAttribute('sp.cart.id', 'cart-789');
span.setAttribute('sp.product.sku', 'PROD-001');

// SaaS applications
span.setAttribute('sp.workspace.id', 'ws-123');
span.setAttribute('sp.project.id', 'proj-456');
```

### Why `sp.*`?

The `sp.` prefix stands for "Softprobe" and ensures:
- No conflicts with OpenTelemetry semantic conventions
- Clear separation from standard attributes (`http.*`, `db.*`, etc.)
- Easy identification of business-specific searchable fields

## Standard HTTP Attributes

In addition to business attributes, always include standard HTTP semantic conventions:

```javascript
// Request attributes
span.setAttribute('http.request.method', 'POST');
span.setAttribute('http.request.path', '/api/orders');
span.setAttribute('http.target', '/api/orders?filter=active'); // If query params exist

// Response attributes
span.setAttribute('http.response.status_code', 201);
```

These are automatically extracted and stored in dedicated columns for efficient querying.

## Storage Schema

Your instrumentation data is stored in the `traces` Iceberg table with this structure:

### Columns Populated from Span Events

| Column | Source | Example |
|--------|--------|---------|
| `http_request_headers` | `http.request` event attribute `http.request.headers` | `{"Content-Type":"application/json"}` |
| `http_request_body` | `http.request` event attribute `http.request.body` | `{"userId":"user-123","items":[...]}` |
| `http_response_headers` | `http.response` event attribute `http.response.headers` | `{"Content-Type":"application/json"}` |
| `http_response_body` | `http.response` event attribute `http.response.body` | `{"orderId":"ORD-456","status":"created"}` |

### Columns Populated from Span Attributes

| Column | Source | Example |
|--------|--------|---------|
| `http_request_method` | Span attribute `http.request.method` | `POST` |
| `http_request_path` | Span attribute `http.request.path` or `http.target` | `/api/orders` |
| `http_response_status_code` | Span attribute `http.response.status_code` or `http.status_code` | `201` |
| `attributes` | All span attributes (MAP) | `{"sp.user.id":"user-123","sp.order.id":"ORD-456"}` |

## Querying Your Data

### Find Sessions by User ID

```sql
SELECT session_id, trace_id, span_id, timestamp, http_request_path
FROM traces
WHERE attributes['sp.user.id'] = 'user-123'
ORDER BY timestamp DESC;
```

### Find Orders with Request Bodies

```sql
SELECT
  attributes['sp.order.id'] AS order_id,
  http_request_body,
  http_response_body,
  timestamp
FROM traces
WHERE
  attributes['sp.order.id'] IS NOT NULL
  AND http_response_status_code = 201
ORDER BY timestamp DESC;
```

### AI Agent Context Retrieval

```sql
-- Get all HTTP interactions for a user session
SELECT
  span_id,
  message_type,
  http_request_method,
  http_request_path,
  http_request_body,
  http_response_body,
  timestamp
FROM traces
WHERE
  attributes['sp.session.id'] = 'sess-12345'
ORDER BY timestamp ASC;
```

## Performance Considerations

### Columnar Separation Benefits

When you query by business attributes, Parquet only reads the `attributes` MAP column, **NOT** the body columns:

```sql
-- This query reads ONLY: session_id, trace_id, attributes
-- Body columns (http_request_body, http_response_body) are NOT read
SELECT session_id, trace_id
FROM traces
WHERE attributes['sp.user.id'] = 'user-123';
```

This provides significant I/O savings when bodies are large (1KB-100KB+).

### Compression

HTTP bodies are stored as STRING columns compressed with ZSTD level 3, providing:
- 5-10x compression ratio for JSON payloads
- Fast decompression for reads
- Low CPU overhead during ingestion

## Best Practices

### 1. Always Use Both Events and Attributes

```javascript
// ✅ GOOD: Bodies in events, metadata in attributes
span.addEvent('http.request', {
  'http.request.body': JSON.stringify(body)
});
span.setAttribute('sp.user.id', body.userId);

// ❌ BAD: Bodies in attributes (hits 128 attribute limit)
span.setAttribute('http.request.body', JSON.stringify(body));
```

### 2. Use Consistent Business Attribute Names

```javascript
// ✅ GOOD: Consistent naming across services
span.setAttribute('sp.user.id', userId);
span.setAttribute('sp.order.id', orderId);

// ❌ BAD: Inconsistent naming
span.setAttribute('sp.userId', userId); // different service
span.setAttribute('sp.user_id', userId); // another service
```

### 3. Avoid Storing Secrets

```javascript
// ❌ BAD: Storing secrets
span.addEvent('http.request', {
  'http.request.headers': JSON.stringify(headers) // May contain Authorization header
});

// ✅ GOOD: Redact secrets
const sanitizedHeaders = { ...headers };
delete sanitizedHeaders.authorization;
span.addEvent('http.request', {
  'http.request.headers': JSON.stringify(sanitizedHeaders)
});
```

### 4. Use Session IDs for Grouping

```javascript
// ✅ GOOD: Explicit session tracking
span.setAttribute('sp.session.id', sessionId);

// If no sp.session.id is set, trace_id is used as grouping key
```

## Next Steps

1. **Validate Instrumentation**: Use the OTLP endpoint at `http://localhost:8080/v1/traces` to test your instrumentation
2. **Query Your Data**: Use DuckDB or Iceberg-compatible query engines to explore stored traces
3. **Create Custom Views**: Build industry-specific wide tables for your use case (see [Storage Design](storage_design.md))

## Support

For questions or issues:
- Review [ADR-006](decision_log.md#adr-006-http-bodies-via-span-events-not-span-attributes) for design rationale
- Check [Design Doc](design.md) for architecture overview
- See [Storage Design](storage_design.md) for advanced querying patterns
