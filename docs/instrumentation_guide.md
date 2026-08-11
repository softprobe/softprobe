# Softprobe Runtime OpenTelemetry Instrumentation Guide

This guide shows how to instrument your application to capture HTTP bodies and business attributes for storage in the Softprobe OTLP backend.

## Session wording (read this first)

- **`sp.session.id` on spans** is the **correlation id** stored in the DuckLake
  `session_id` column (it defaults to `trace_id` if unset). Typical production
  use: **end-user session**, checkout flow, or any stable id you want to query
  by.

If documentation mentions “session”, it means **`sp.session.id` / `session_id`
column** (telemetry correlation), not SoftProbe capture/replay control sessions
(removed from this runtime).

## Design Philosophy

We use two complementary OpenTelemetry features:

1. **Span Events** - For capturing large payloads (HTTP request/response bodies)
2. **Span Attributes** - For capturing searchable business metadata (user IDs, order IDs, etc.)

This separation provides:
- **Query efficiency**: metadata queries do not need to select body columns
- **No attribute limits**: Bodies stored as events avoid the 128 span attribute limit
- **Semantic clarity**: Events represent "notable moments", attributes represent "searchable metadata"

The original rationale is preserved in the
[legacy decision log](legacy/decision-log-iceberg-era.md#adr-006-http-bodies-via-span-events-not-span-attributes).

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

      // Explicit business attributes for search (sp.* convention; not automatic)
      span.setAttribute('sp.user.id', req.body.user.id);
      span.setAttribute('sp.tenant.id', req.headers['x-tenant-id']);

      // Process request
      const orderResult = await createOrder(req.body);
      span.setAttribute('sp.order.id', orderResult.orderId);

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

        # Explicit business attributes for search (sp.* convention; not automatic)
        span.set_attribute("sp.user.id", request.json["user.id"])
        span.set_attribute("sp.tenant.id", request.headers.get("X-Tenant-Id"))

        try:
            # Process request
            order_result = create_order(request.json)

            # Explicit business attribute from result
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

            // Explicit business attributes for search (sp.* convention; not automatic)
            span.setAttribute("sp.user.id", request.getUserId());
            span.setAttribute("sp.tenant.id", headers.getFirst("X-Tenant-Id"));

            // Process request
            OrderResponse orderResult = orderService.createOrder(request);

            // Explicit business attribute from result
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

A **business attribute** is application-domain metadata you want to search or
correlate on later (user id, order id, booking reference, workflow name). Softprobe
does **not** invent these fields. Your application must set them explicitly on
the span (or log/metric attributes).

Use the `sp.*` prefix for Softprobe-specific business keys:

```javascript
// Explicit application instrumentation — nothing here is automatic
span.setAttribute('sp.user.id', 'user-123');
span.setAttribute('sp.user.email', 'user@example.com');
span.setAttribute('sp.order.id', 'ORD-456');
span.setAttribute('sp.transaction.id', 'TXN-789');
span.setAttribute('sp.tenant.id', 'tenant-abc');
span.setAttribute('sp.organization.id', 'org-xyz');
span.setAttribute('sp.session.id', 'sess-12345');
span.setAttribute('sp.pnr', 'ABC123');
span.setAttribute('sp.booking.reference', 'BK-456');
span.setAttribute('sp.cart.id', 'cart-789');
span.setAttribute('sp.product.sku', 'PROD-001');
span.setAttribute('sp.workspace.id', 'ws-123');
span.setAttribute('sp.project.id', 'proj-456');
```

### Why `sp.*`?

- Avoids collisions with OpenTelemetry semantic conventions (`http.*`, `db.*`, …)
- Makes Softprobe business keys easy to spot in the `attributes` VARIANT
- Gives promotion manifests a stable key to extract into typed columns

`sp.*` is only a naming convention. Softprobe stores these keys in
`attributes` like any other attribute. Promoting them to dedicated columns is a
separate, explicit step — see [`promotion.md`](promotion.md).

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

Your instrumentation data is stored in the DuckLake `traces` table with this
structure:

### HTTP columns populated primarily from span events

| Column | Source | Example |
|--------|--------|---------|
| `http_request_headers` | `http.request` event attribute `http.request.headers` | `{"Content-Type":"application/json"}` |
| `http_request_body` | `http.request` event attribute `http.request.body` | `{"user.id":"user-123","items":[...]}` |
| `http_response_headers` | `http.response` event attribute `http.response.headers` | `{"Content-Type":"application/json"}` |
| `http_response_body` | `http.response` event attribute `http.response.body` | `{"orderId":"ORD-456","status":"created"}` |

### Columns Populated from Span Attributes

| Column | Source | Example |
|--------|--------|---------|
| `http_request_method` | Span attribute `http.request.method` | `POST` |
| `http_request_path` | Span attribute `http.request.path` or `http.target` | `/api/orders` |
| `http_response_status_code` | Span attribute `http.response.status_code` or `http.status_code` | `201` |
| `attributes` | All span attributes (VARIANT, shredded) | `{"sp.user.id":"user-123","sp.order.id":"ORD-456"}` |

### HTTP payload fallback

Events are preferred. For compatibility with SDKs and OBI/eBPF telemetry, the
runtime falls back to span attributes when an event did not provide a value:

- headers: `http.request.headers`, `http.response.headers`
- bodies: `http.request.body` / `http.response.body`
- OBI body aliases: `http.request.body.content`,
  `http.response.body.content`

When both event and span-attribute values exist, the event value wins. For
body keys within the same source, the shorter key without `.content` wins.

## Column promotion (runtime-scoped)

After you emit business attributes, you can promote selected keys to dedicated
typed SQL columns for that tenant. Promotion is **not** configured in
process-global `config.yaml`.

Canonical contract (manifests, apply API, lifecycle, failure modes):
[`promotion.md`](promotion.md).

Summary:

1. Instrument attributes such as `sp.user.id`.
2. Apply a `softprobe.promotion.v1` manifest with
   `POST /v1/promotions/apply`.
3. New ingest writes the promoted column; historical rows stay `NULL`.
4. Query either `CAST(attributes['sp.user.id'] AS VARCHAR)` or the promoted column name.

## Querying Your Data

### Find Sessions by User ID

**Using VARIANT lookup** (works with or without promotion):
```sql
SELECT session_id, trace_id, span_id, timestamp, http_request_path
FROM traces
WHERE CAST(attributes['sp.user.id'] AS VARCHAR) = 'user-123'
ORDER BY timestamp DESC;
```

**Using a promoted column** only after an explicit telemetry promotion creates
that column (example SQL and manifests in [`promotion.md`](promotion.md)).

### Find Orders with Request Bodies

```sql
SELECT
  CAST(attributes['sp.order.id'] AS VARCHAR) AS order_id,
  http_request_body,
  http_response_body,
  timestamp
FROM traces
WHERE
  CAST(attributes['sp.order.id'] AS VARCHAR) IS NOT NULL
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
  CAST(attributes['sp.session.id'] AS VARCHAR) = 'sess-12345'
ORDER BY timestamp ASC;
```

## Performance Considerations

### Column selection

When data is in Parquet, a query by business attributes can read shredded
VARIANT subfields without selecting body columns:

```sql
-- This query can prune to shredded attribute paths
-- Body columns (http_request_body, http_response_body) are NOT read
SELECT session_id, trace_id
FROM traces
WHERE CAST(attributes['sp.user.id'] AS VARCHAR) = 'user-123';
```

This reduces object-store I/O when bodies are large. DuckLake may instead keep
small committed batches inline in its metadata catalog, depending on
`ducklake.data_inlining_row_limit`. For file-level VARIANT shredding stats,
set `DATA_INLINING_ROW_LIMIT 0` so data lands in Parquet files.

## Best Practices

### 1. Always Use Both Events and Attributes

```javascript
// ✅ GOOD: Bodies in events, metadata in attributes
span.addEvent('http.request', {
  'http.request.body': JSON.stringify(body)
});
span.setAttribute('sp.user.id', body.user.id);

// ⚠️ COMPATIBILITY FALLBACK: accepted by the runtime, but events are preferred
// because large bodies consume the span attribute budget.
span.setAttribute('http.request.body', JSON.stringify(body));
```

### 2. Use Consistent Business Attribute Names

```javascript
// ✅ GOOD: Consistent naming across services
span.setAttribute('sp.user.id', user.id);
span.setAttribute('sp.order.id', orderId);

// ❌ BAD: Inconsistent naming
span.setAttribute('sp.user.id', user.id); // different service
span.setAttribute('sp.user_id', user.id); // another service
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

## Web session recording (rrweb)

Browser DOM recordings are ingested as ordinary OTLP spans (no separate write API).

| Field | Value |
|---|---|
| span name | `softprobe.web.recording` |
| attribute `sp.observation.type` | `recording` |
| attribute `sp.session.id` | correlation id (same as LLM session when linked) |
| attribute `sp.recording.batch_index` | monotonic batch counter |
| event name | `sp.recording.batch` |
| event attribute `sp.recording.events` | JSON string of rrweb events (FullSnapshots may set `isCompressed`) |

Query: `GET /v1/llm/sessions/{session_id}/recording?from=&to=&limit=` returns
ordered `batches` and flattened `events` for the player. Batches are ordered by
`sp.recording.batch_index` (then `start_time`). The response includes
`truncated: true` when the batch `limit` (default 50, max 200) was hit — long
sessions may need a narrower `from`/`to` window.

Producer SDK: [`@softprobe/web-record`](https://github.com/softprobe/softprobe-js/tree/main/packages/web-record).
Explorer UI: Softprobe LLM [web session replay](https://github.com/softprobe/sp-llm/blob/main/docs/web-session-replay.md).

Keep searchable metadata (browser, URL, visitor id) on span attributes; keep the
large rrweb payload on the span event — same pattern as HTTP bodies.

### Browser CORS note

Browser OTLP (`POST /v1/traces` with `Authorization`) triggers a CORS OPTIONS
preflight **without** a Bearer token. The runtime skips auth for `OPTIONS /v1/*`
and keeps `CorsLayer` outermost so SPA recorders (e.g. softprobe-code) can export.

## Next Steps

1. **Validate Instrumentation**: Use the OTLP endpoint at `http://localhost:8090/v1/traces`.
2. **Query Your Data**: Use the runtime telemetry APIs or DuckDB attached to the same DuckLake scope.
3. **Promote important fields**: Follow [`promotion.md`](promotion.md) when a
   business attribute needs a dedicated column.

## Support

For questions or issues:
- Check the [current design](design.md) for the runtime architecture.
- See [schema promotion](promotion.md) for `sp.*`, manifests, and apply API.
- See [ad hoc DuckDB/DuckLake queries](adhoc-duckdb-ducklake.md) for local SQL access.
- Review the [legacy ADR-006](legacy/decision-log-iceberg-era.md#adr-006-http-bodies-via-span-events-not-span-attributes) for the original event-versus-attribute rationale.
