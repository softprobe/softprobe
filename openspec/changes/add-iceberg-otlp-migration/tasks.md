## 0. Foundation (Completed)
- [x] 0.1 Implement OTLP `/v1/traces` endpoint with session buffering
- [x] 0.2 Create Iceberg table schema for traces storage (`traces`)
- [x] 0.3 Implement multi-app row-group batching and Parquet writer

## 1. OTLP Logs and Metrics APIs
- [x] 1.1 Implement OTLP `/v1/logs` endpoint accepting LogRecords
- [x] 1.2 Create separate log buffer (independent from traces buffer to avoid fragmentation)
- [x] 1.3 Create Iceberg table schema for logs (`logs`) with session-based partitioning
- [x] 1.4 Implement flush logic for logs buffer to `logs` table
- [x] 1.5 Implement OTLP `/v1/metrics` endpoint accepting Metrics
- [x] 1.6 Add separate metrics buffering (NOT session-based, organized by metric_name)
- [x] 1.7 Create Iceberg table schema for metrics with metric_name partitioning (experimental)

## 2. Query APIs and Session Management
- [ ] 2.1 Implement query API for retrieving sessions by ID using direct Iceberg scans
- [ ] 2.2 Add parallel retrieval from traces and logs tables for complete session view
- [ ] 2.3 Add time-range query support with Iceberg partition elimination
- [ ] 2.4 Implement attribute-based filtering using Iceberg predicate pushdown
- [ ] 2.5 Add pagination support for large query results
- [ ] 2.6 Implement session timeout and auto-flush mechanisms for traces/logs buffers

## 3. Production Readiness
- [ ] 3.1 Add monitoring and observability (metrics, traces for the collector itself)
- [ ] 3.2 Implement graceful shutdown and buffer persistence for all signal types
- [ ] 3.3 Add configuration for multiple storage backends (S3, MinIO, R2)
- [ ] 3.4 Performance tuning: flush triggers, batch sizes, concurrency limits
- [ ] 3.5 Optimize Iceberg manifest pruning and row group statistics usage
- [ ] 3.6 Add compaction jobs for long-term storage optimization

## 4. Validation and Testing
- [ ] 4.1 Add integration tests for all three OTLP endpoints
- [ ] 4.2 Validate OTLP protocol compliance with official test suites
- [ ] 4.3 Benchmark ingestion throughput (target: 10k+ spans/sec)
- [ ] 4.4 Benchmark query performance using direct Iceberg scans
- [ ] 4.5 Test coordinated traces+logs flush for session consistency
- [ ] 4.6 Load testing with realistic multi-app, multi-session workloads
- [ ] 4.7 Validate metrics storage pattern (acknowledge experimental status)

---

## Phase 1: HTTP Body Storage & Business Attribute Indexing

> **Decision Context**: See [Storage Design](../../../docs/storage_design.md#executive-decision--analysis) and [Design Doc Section 5.1](../../../docs/design.md#51-table-traces)

### 5. HTTP Body Storage & Business Attribute Indexing (Unified)

**Goal**: Store full HTTP bodies via span events AND enable fast session lookup via business attributes

> **Decision Context**: See [ADR-006: HTTP Bodies via Span Events](../../../docs/decision_log.md#adr-006-http-bodies-via-span-events-not-span-attributes)

**Instrumentation Pattern** (User-facing):
```javascript
// HTTP bodies stored as span events
span.addEvent('http.request', {
  'http.request.headers': JSON.stringify(headers),
  'http.request.body': JSON.stringify(requestBody)
});

span.addEvent('http.response', {
  'http.response.headers': JSON.stringify(responseHeaders),
  'http.response.body': JSON.stringify(responseBody)
});

// Business attributes for search
span.setAttribute('sp.user.id', 'user-123');
span.setAttribute('sp.order.id', 'ORD-456');

// Standard HTTP attributes
span.setAttribute('http.request.method', 'POST');
span.setAttribute('http.request.path', '/api/orders');
span.setAttribute('http.response.status_code', 201);
```

**Implementation Tasks**:

- [x] 5.1 Update `traces` table schema to extract HTTP data:
  - Extract `http.request` event → `http_request_body`, `http_request_headers` columns
  - Extract `http.response` event → `http_response_body`, `http_response_headers` columns
  - Extract standard span attributes → `http_request_method`, `http_request_path`, `http_response_status_code`
  - Keep business attributes (`sp.*`) in `attributes` MAP for search

- [x] 5.2 Implement ingestion pipeline changes:
  - Parse span events array, find `http.request` and `http.response` events
  - Extract event attributes → HTTP body columns
  - Validate bodies stored as STRING (Parquet ZSTD compression)
  - Ensure `attributes` MAP captures all `sp.*` attributes

- [x] 5.3 Update Arrow conversion to handle event extraction:
  - Event parsing logic: `events.find(e => e.name === 'http.request')`
  - Proper field IDs for HTTP body columns in Iceberg schema
  - Test Parquet ZSTD compression on realistic JSON bodies

- [x] 5.4 Create unified instrumentation documentation:
  - **JavaScript/TypeScript example**: Show both `span.addEvent()` and `span.setAttribute()`
  - **Python example**: OpenTelemetry Python SDK with events + attributes
  - **Java example**: Java SDK instrumentation pattern
  - **Convention guide**: Explain separation (events for bodies, attributes for search)
  - Document `sp.*` naming convention: `sp.user.id`, `sp.order.id`, `sp.email`, `sp.pnr`

- [x] 5.5 Validate existing ingestion code:
  - Confirm `attributes` MAP properly stores user-provided `sp.*` attributes
  - Confirm `events` ARRAY is correctly parsed and stored
  - No code changes needed if already working

### 6. Benchmarking & Validation

**Critical**: Validate design assumptions before Phase 1 launch

- [ ] 6.1 **Columnar Separation Benchmark**:
  - Query: `SELECT session_id, trace_id FROM traces WHERE attributes['sp.user.id'] = 'user-123'`
  - Measure I/O: Confirm Parquet does NOT read `http_request_body`, `http_response_body` columns
  - Use `EXPLAIN` or Parquet read stats to validate
  - **Success Criteria**: <10% I/O overhead vs. table without body columns

- [ ] 6.2 **MAP Query Performance Benchmark**:
  - Test with 10M, 50M, 100M spans
  - Query: `WHERE attributes['sp.user.id'] = 'user-123'`
  - Measure latency (target: <500ms)
  - Test with various cardinalities (10 users vs 1M users)

- [ ] 6.3 **Compression Ratio Benchmark**:
  - Measure Parquet file sizes with realistic JSON bodies (1KB, 10KB, 100KB)
  - Compare ZSTD level 3 vs level 6 vs level 9
  - Calculate storage cost at 100TB scale
  - **Target**: 5-10x compression ratio for JSON

- [ ] 6.4 **Multi-Backend Storage Validation**:
  - Test ingestion and queries on AWS S3 (baseline)
  - Test ingestion and queries on Cloudflare R2 (validate zero-egress)
  - Test ingestion and queries on MinIO (validate self-hosted)
  - Compare query latency across backends
  - Validate S3-compatible API works consistently

### 7. User ETL Templates & Documentation

**Goal**: Enable power users to create their own wide tables

- [ ] 7.1 Create DuckDB SQL templates for common industries:
  - E-commerce: Extract user_id, order_id, cart items from bodies
  - SaaS: Extract tenant_id, user_id, API endpoint
  - Travel: Extract PNR, passenger details, booking reference

- [ ] 7.2 Document how to create custom wide tables:
  - Example: `CREATE TABLE orders_wide AS SELECT json_extract(...) FROM traces`
  - Show incremental update patterns
  - Best practices for indexing wide tables

- [ ] 7.3 Create troubleshooting query examples:
  - Find sessions by user ID
  - Find sessions by order ID
  - Find failed requests with bodies
  - AI agent context retrieval pattern

### 8. Cloud-Neutral Object Storage Support

**Goal**: Support S3-compatible object storage (customer choice)

- [ ] 8.1 Add S3-compatible storage configuration:
  - Configurable endpoint (AWS S3, Cloudflare R2, GCS, MinIO, etc.)
  - Credentials management (access keys, IAM roles)
  - Validate iceberg-rust works with multiple backends

- [ ] 8.2 Test with multiple storage providers:
  - Primary: AWS S3 (industry standard baseline)
  - Secondary: Cloudflare R2 (zero-egress validation)
  - Tertiary: MinIO (self-hosted validation)

- [ ] 8.3 Create deployment guides for each backend:
  - AWS S3 setup guide
  - Cloudflare R2 setup guide
  - MinIO self-hosted setup guide
  - Cost comparison documentation

### 9. Phase 1 Launch Checklist

**Pre-Launch Validation**:

- [ ] 9.1 All benchmarks complete and meet success criteria:
  - [ ] Columnar separation: <10% I/O overhead
  - [ ] MAP query latency: <500ms for session lookups
  - [ ] Compression ratio: 5-10x for JSON bodies
  - [ ] Multi-backend storage: S3, R2, MinIO validated

- [ ] 9.2 Documentation complete:
  - [ ] `sp.*` attribute convention guide
  - [ ] OpenTelemetry instrumentation examples (3+ languages)
  - [ ] DuckDB SQL templates (3+ industries)
  - [ ] Troubleshooting query cookbook

- [ ] 9.3 Design partner program:
  - [ ] Recruit 10 design partners
  - [ ] Onboarding materials prepared
  - [ ] Feedback collection process

**Phase 1 Success Metrics** (Month 3):
- 10 design partner customers using HTTP body storage
- <500ms query latency for session lookups via MAP
- Customer feedback: "10x cheaper than Datadog"
- Storage cost validation: matches R2 projections

---

## Phase 2: Managed ETL (Conditional)

> **Trigger Criteria** (See [Storage Design](../../../docs/storage_design.md#phase-2-triggers-month-4)):
> - MAP query performance >500ms at customer scale
> - Customers explicitly request managed extraction
> - Revenue validates investment (>$500K ARR)

### 11. Business Index Table (If Phase 2 Triggered)

- [ ] 11.1 Design `business_index` table schema
- [ ] 11.2 Build extraction engine with configurable rules
- [ ] 11.3 Create ETL job scheduler (DuckDB or Spark-based)
- [ ] 11.4 Implement incremental extraction (process only new spans)
- [ ] 11.5 Pre-built extraction templates for 5+ industries

### 12. Separate Payloads Table (If Columnar Separation Fails)

- [ ] 12.1 Create `http_payloads` table schema
- [ ] 12.2 Modify ingestion to dual-write (traces + http_payloads)
- [ ] 12.3 Update query API to join tables when bodies needed
- [ ] 12.4 Migrate existing data to new schema

---

## Decision Gates

**At Month 4** - Evaluate Phase 2 Need:
1. Review MAP query latency at 100M+ spans
2. Collect customer feedback on `sp.*` adoption
3. Check revenue milestone ($500K ARR?)
4. Assess competitive pressure

**Proceed with Phase 2 ONLY IF**:
- Performance issues confirmed AND
- Customer demand validated AND
- Revenue justifies investment
