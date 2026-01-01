# Architecture Decision Log

**Project**: Softprobe Data Lake
**Purpose**: Record all major architectural decisions with context, rationale, alternatives considered, and consequences

---

## Decision Record Format

Each decision follows this structure:
- **Date**: When the decision was made
- **Status**: Proposed | Accepted | Deprecated | Superseded
- **Context**: What prompted this decision
- **Decision**: What we decided to do
- **Rationale**: Why we made this choice
- **Alternatives Considered**: What other options we evaluated
- **Consequences**: Expected outcomes (positive and negative)
- **References**: Links to related documents and discussions

---

## ADR-001: Apache Iceberg as Table Format

**Date**: 2025-12-31
**Status**: Accepted
**Deciders**: Engineering Team, Executive Leadership

### Context

We needed to choose a table format for our data lake that would:
1. Store HTTP request/response bodies efficiently
2. Support future integration with multiple query engines (Snowflake, Trino, Databricks, DuckDB)
3. Work well with our Rust-based collector
4. Enable vendor-neutral positioning as a strategic differentiator
5. Support our long-term vision of becoming the AI-ready enterprise data lake

Three major options: Apache Iceberg, Delta Lake, and Apache Hudi.

### Decision

**Use Apache Iceberg** as our table format.

### Rationale

**Ecosystem Value Over Marginal Performance**

The core insight is that **ecosystem value >> marginal performance difference**. While Delta Lake shows 1.7x better performance in TPC-DS OLAP benchmarks, this advantage is:

1. **Overstated for our use case**:
   - TPC-DS measures star-schema OLAP queries, not our session-based lookups
   - Network I/O from object storage dominates query time, not Parquet decode
   - Our queries (session lookups + batch ETL) play to Iceberg's strengths (partition pruning, row group statistics)

2. **The performance gap will close**:
   - Iceberg has high development velocity and active community
   - Performance optimizations are continuous
   - Gap is shrinking with each release

3. **Multi-engine support is strategic**:
   - Iceberg: Spark, Trino, Dremio, Flink, DuckDB, Snowflake
   - Delta Lake: Databricks-first, others catching up slowly
   - When we add Fivetran/Airbyte integrations (Phase 2+), Iceberg has broadest support

4. **Vendor neutrality = competitive positioning**:
   - "Open-source first" is a core value proposition
   - Delta Lake has Databricks influence (despite being open)
   - Iceberg is truly vendor-neutral (Apache Foundation)

5. **Rust ecosystem support**:
   - iceberg-rust 0.7+ is actively maintained
   - Delta Lake has no official Rust support
   - Critical for our Rust-based collector

### Alternatives Considered

#### Option A: Delta Lake
**Pros**:
- 1.7x faster OLAP queries (TPC-DS benchmarks)
- Mature, battle-tested
- Excellent Databricks integration

**Cons**:
- Databricks-centric ecosystem (vendor lock-in risk)
- Limited Rust support
- Would conflict with "open-source first" positioning
- Harder to integrate with Snowflake, Trino in future

**Why Rejected**: Ecosystem lock-in outweighs 1.7x performance advantage

#### Option B: Apache Hudi
**Pros**:
- Optimized for upserts and CDC
- Apache foundation

**Cons**:
- Our workload is append-heavy, not upsert-heavy
- Limited multi-engine support
- Smaller ecosystem than Iceberg or Delta

**Why Rejected**: Wrong tool for our workload

### Consequences

**Positive**:
- ✅ Vendor neutrality supports long-term strategic positioning
- ✅ Multi-engine support enables future integrations (Fivetran, Airbyte, Snowflake)
- ✅ Rust support matches our technology stack
- ✅ Active community and rapid development velocity
- ✅ Hidden partitioning, schema evolution, time travel features

**Negative**:
- ⚠️ 1.7x slower OLAP queries (but mitigated by our workload differences)
- ⚠️ Potentially slower full-table scans for ETL (but network I/O dominates)

**Mitigations**:
- Monitor query performance with SLAs
- Optimize Parquet file sizes (128MB → 256MB if needed)
- Add query result caching (Redis) for hot sessions
- Z-ordering on session_id + timestamp if needed
- Escape hatch: Migrate to Delta Lake if ALL mitigations fail

**When We'd Reconsider**:
- Session lookup latency >1s AND all optimizations fail
- Customer demand for Databricks-native integration
- iceberg-rust library becomes unmaintained

### References

- [Storage Design Analysis](storage_design.md#is-iceberg-the-right-foundation)
- [Design Document Section 4](design.md#4-storage-platform-decision)
- [Iceberg vs Delta vs Hudi: 2025 Guide](https://medium.com/@rajnishdumca/iceberg-vs-delta-lake-vs-hudi-the-2025-guide-from-a-data-architects-perspective-0e48a039b9a1)
- [Lakehouse Feature Comparison](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison)

---

## ADR-002: Cloud-Neutral Object Storage Strategy

**Date**: 2025-12-31
**Status**: Accepted
**Deciders**: Engineering Team, Executive Leadership

### Context

AI agents and ETL pipelines will repeatedly read HTTP bodies from storage, creating **high egress workload**. We need a storage strategy that:
1. Supports customer choice (bring your own cloud)
2. Works with any S3-compatible object storage
3. Optimizes for cost (especially egress charges)
4. Maintains vendor neutrality as a core principle

### Decision

**Support S3-compatible object storage with customer-owned infrastructure** (cloud-neutral approach).

Customers can choose:
- **AWS S3** (industry standard, existing infrastructure)
- **Cloudflare R2** (zero egress, cost-optimized)
- **Google Cloud Storage** (GCP customers)
- **Azure Blob Storage** (via S3-compatible endpoint)
- **MinIO** (self-hosted, full control)
- **Any S3-compatible storage**

### Rationale

**Cloud Neutrality = Strategic Flexibility**

1. **Customer Choice**:
   - Enterprise customers often have existing cloud commitments (AWS credits, GCP contracts)
   - Some prefer self-hosted infrastructure for compliance/sovereignty
   - Multi-cloud strategies require flexibility
   - "Bring your own storage" reduces lock-in concerns

2. **Cost Awareness** (but not prescription):
   - **Egress costs matter**: AWS S3 charges $0.09/GB, R2 charges $0
   - For 100TB data + 50TB monthly egress:
     - AWS S3: ~$6,800/month
     - Cloudflare R2: ~$1,500/month
   - **BUT**: Customer decides based on their total cost of ownership

3. **Implementation Simplicity**:
   - S3-compatible API is standard across all providers
   - iceberg-rust works with any S3-compatible storage
   - Single code path, multiple deployment options

4. **Competitive Positioning**:
   - "Works with your cloud" vs "locked to our infrastructure"
   - Easier enterprise adoption (no new vendor approval needed)
   - Self-hosted option for regulated industries

### Supported Storage Backends

| Provider | Type | Egress Cost | Best For |
|----------|------|-------------|----------|
| **AWS S3** | Managed | $0.09/GB | AWS customers, existing infra |
| **Cloudflare R2** | Managed | $0 (zero) | Cost optimization, high egress |
| **Google Cloud Storage** | Managed | $0.12/GB | GCP customers |
| **MinIO** | Self-hosted | $0 | Compliance, full control, on-prem |
| **Wasabi** | Managed | $0 | Cost optimization alternative |

**Recommendation** (not requirement): For high-egress workloads (AI agents + ETL), zero-egress providers (R2, Wasabi) or self-hosted (MinIO) offer significant cost advantages.

### Implementation

**Configuration**:
```rust
// Support all S3-compatible backends via standard config
storage:
  type: s3_compatible
  endpoint: <customer-choice>  // s3.amazonaws.com, r2.cloudflarestorage.com, etc.
  bucket: <customer-bucket>
  credentials: <customer-credentials>
```

**Testing Strategy**:
- Primary testing: AWS S3 (broadest compatibility)
- Secondary testing: Cloudflare R2 (validate zero-egress)
- Tertiary testing: MinIO (validate self-hosted)

### Alternatives Considered

#### Option A: AWS S3 Only
**Pros**:
- Industry standard
- Simplest to support (one code path)

**Cons**:
- High egress costs ($0.09/GB) hurt customer economics
- Vendor lock-in conflicts with open-source positioning
- No flexibility for GCP/Azure customers

**Why Rejected**: Conflicts with cloud-neutral strategy

#### Option B: Cloudflare R2 Only
**Pros**:
- Zero egress costs
- Best economics for AI/ETL workloads
- Still S3-compatible

**Cons**:
- Forces customers onto new vendor
- No option for existing AWS/GCP commitments
- Can't support self-hosted or regulated environments

**Why Rejected**: Too prescriptive, reduces customer flexibility

#### Option C: Multi-Backend with Custom Code
**Pros**:
- Native APIs for each cloud (S3, GCS, Azure Blob)

**Cons**:
- 3-5x implementation complexity
- Higher maintenance burden
- S3-compatible API is already universal

**Why Rejected**: Unnecessary complexity, S3 API is standard

### Consequences

**Positive**:
- ✅ Works with customer's existing cloud infrastructure
- ✅ Self-hosted option for compliance/sovereignty
- ✅ No vendor lock-in concerns
- ✅ Cost-conscious customers can choose zero-egress providers
- ✅ "Cloud-neutral" strengthens open-source positioning

**Negative**:
- ⚠️ Must support/test multiple storage backends
- ⚠️ Can't guarantee cost savings (depends on customer choice)
- ⚠️ Documentation must cover multiple providers

**Mitigations**:
- Clear cost comparison docs (help customers make informed choice)
- Automated compatibility tests for major providers (S3, R2, MinIO)
- Reference architectures for each backend

**Default Recommendation**:
- **Cost-sensitive + high egress**: Cloudflare R2 or Wasabi
- **AWS customers**: AWS S3 (leverage existing infrastructure)
- **GCP customers**: Google Cloud Storage
- **Compliance/on-prem**: MinIO self-hosted

### References

- [S3 Storage Cost Comparison 2025](https://www.s3compare.io/)
- [Cloudflare R2 vs AWS S3](https://themedev.net/blog/cloudflare-r2-vs-aws-s3/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [Design Document Section 4.0.1](design.md#401-object-storage-cloudflare-r2)

---

## ADR-003: Columnar Separation (Bodies in Same Table)

**Date**: 2025-12-31
**Status**: Accepted
**Deciders**: Engineering Team

### Context

Need to store HTTP bodies separately from metadata to avoid expensive I/O during routine queries. Two architectural options:
1. **Separate tables**: `traces` (metadata) + `http_payloads` (bodies)
2. **Same table, columnar separation**: Rely on Parquet's columnar format

### Decision

**Store HTTP bodies as STRING columns in the same `traces` table**, relying on Parquet's columnar format for I/O separation.

### Rationale

**Start Simple, Validate Performance**

1. **Parquet Already Provides Separation**:
   - Query: `SELECT session_id FROM traces WHERE attributes['sp.user.id'] = 'user-123'`
   - Parquet only reads columns referenced in the query
   - Body columns (`http_request_body`, `http_response_body`) are never touched

2. **Simpler Architecture**:
   - One table = one write path
   - One Iceberg transaction per write (not two)
   - No joins required to get full context
   - Easier to reason about and debug

3. **Can Migrate Later if Needed**:
   - If benchmarks show columnar separation is insufficient
   - Phase 2: Move to separate `http_payloads` table
   - But don't over-engineer before validating the problem exists

4. **Retention Flexibility**:
   - Can still implement different retention via compaction
   - Drop body columns from old Parquet files during compaction
   - Achieve same goal without dual-table complexity

### Alternatives Considered

#### Option A: Separate Tables
**Design**:
```sql
CREATE TABLE traces (
  session_id, trace_id, attributes, ...
  -- NO body columns
);

CREATE TABLE http_payloads (
  session_id, span_id,
  http_request_body, http_response_body, ...
);
```

**Pros**:
- Clear separation
- Different retention policies (metadata 90d, bodies 30d)
- Explicit guarantee bodies aren't read during metadata queries

**Cons**:
- Two Iceberg commits per write (complexity + transaction overhead)
- Requires join to get full context
- More complex write path
- Premature optimization (solving problem before confirming it exists)

**Why Rejected**: Over-engineering before validating the need

### Consequences

**Positive**:
- ✅ Simple architecture (one table, one write)
- ✅ No joins required for full context
- ✅ Parquet columnar format handles I/O separation automatically
- ✅ Can migrate to separate table in Phase 2 if needed

**Negative**:
- ⚠️ Can't have different retention policies (without compaction)
- ⚠️ Larger Parquet files (but compression mitigates)
- ⚠️ If columnar separation fails, need migration

**Validation Required**:
- **Benchmark I/O patterns**: Measure bytes read when querying metadata-only
- **Success Criteria**: <10% I/O overhead vs table without body columns
- **Tools**: DuckDB `EXPLAIN`, Parquet read statistics

**Escape Hatch**:
- If columnar separation proves insufficient (>10% I/O overhead)
- Migrate to separate `http_payloads` table (Phase 2, Task 12)

### References

- [Storage Design Section on Separation](storage_design.md#storage-separation-table-level-vs-column-level)
- [Design Document Section 5.1](design.md#51-table-traces)

---

## ADR-004: User-Provided Business Attributes (`sp.*` Convention)

**Date**: 2025-12-31
**Status**: Accepted
**Deciders**: Engineering Team

### Context

Need to enable fast session lookup by business identifiers (user ID, order ID, PNR, etc.). Multiple approaches:
1. User-provided attributes in code
2. OTLP Collector filter (pre-ingestion extraction)
3. Post-ingestion ETL pipeline
4. User's own ETL (we just store raw data)

**Key Constraint**: Extraction at high-volume ingestion is compute-expensive, and we can't handle all user schemas.

### Decision

**Hybrid Strategy: User-Provided `sp.*` Attributes (Option 1) + User ETL Templates (Option 4)**

Phase 1: Ship both options, let users choose
Phase 2: Add managed ETL (Option 3) only if customers demand it

### Rationale

**Time to Market + Validation Before Investment**

**Decision Framework** (weighted scoring):

| Criterion | Weight | Option 1 | Option 2 | Option 3 | Option 4 |
|-----------|--------|----------|----------|----------|----------|
| Time to Market | 40% | 95% | 60% | 40% | 100% |
| Scalability | 30% | 70% | 70% | 95% | 50% |
| Flexibility | 20% | 60% | 80% | 95% | 100% |
| Ops Complexity | 10% | 95% | 50% | 30% | 100% |
| **Total** | | **81%** | **66%** | **64%** | **85%** |

**Why Option 1 (User-Provided)**:
1. **Fast to market**: 3-4 weeks (just documentation)
2. **Zero ingestion overhead**: No parsing at write time
3. **Users control schema**: They know their business attributes best
4. **Validates market fit**: See if customers actually want this

**Why Option 4 (User ETL)**:
1. **Ultimate flexibility**: Power users get full control
2. **Zero work for us**: No extraction engine to build
3. **Immediate availability**: DuckDB can query JSON bodies today
4. **Natural fit**: Users already doing ETL will prefer this

**Why NOT Option 3 (Managed ETL) in Phase 1**:
1. **3+ months to build**: Extraction engine, scheduler, templates
2. **Unvalidated need**: Don't know if customers want this
3. **High complexity**: ETL jobs, configs, monitoring
4. **Can add later**: If Phase 1 succeeds and customers demand it

### Alternatives Considered

#### Option 2: OTLP Collector Filter
**Design**: Run OTLP Collector with extraction processor before our collector

**Pros**:
- Reuse OTLP ecosystem
- Users configure extraction rules
- No ingestion overhead in our collector

**Cons**:
- Additional infrastructure (OTLP Collector deployment)
- Two configs to manage (OTLP Collector + our collector)
- Extraction is still compute-expensive (just moved to different process)
- Harder to debug (two-hop architecture)

**Why Rejected**: Adds complexity without solving core problems

#### Option 3: Post-Ingestion ETL (Observe-Style)
**Design**: Store raw, run SQL transforms to extract attributes into `business_index` table

**Pros**:
- Change extraction rules without re-ingestion
- Backfill historical data
- Observe-proven approach

**Cons**:
- 3+ months to build (extraction engine, scheduler, templates)
- Extraction lag (minutes to hours)
- More storage (raw + extracted)
- ETL job management overhead
- **Unvalidated market need**

**Why Rejected**: Over-investment before validating customer demand

### Consequences

**Phase 1 Implementation**:

**Option 1: User-Provided `sp.*`**:
- Document convention: `sp.user.id`, `sp.order.id`, `sp.email`, etc.
- OpenTelemetry SDK examples (JavaScript, Python, Java)
- Store in `attributes` MAP
- Query: `WHERE attributes['sp.user.id'] = 'user-123'`

**Option 4: User ETL Templates**:
- DuckDB SQL templates for common industries (e-commerce, SaaS, travel)
- Example: `CREATE TABLE orders_wide AS SELECT json_extract(http_request_body, '$.orderId') AS order_id ...`
- Document incremental update patterns

**Positive**:
- ✅ 3-4 weeks to ship (vs 3 months for managed ETL)
- ✅ Validates market fit before over-investing
- ✅ Zero ingestion overhead
- ✅ Users who care about search will instrument properly
- ✅ Power users get full control immediately

**Negative**:
- ⚠️ Integration burden on users (must modify instrumentation)
- ⚠️ If users refuse to instrument, we need managed ETL (Phase 2)
- ⚠️ MAP query performance unknown (needs benchmarking)

**Success Metrics** (Phase 1, Month 3):
- 10 design partner customers
- <500ms query latency for session lookups via MAP
- Customer feedback on `sp.*` adoption rate

**Phase 2 Triggers** (Month 4+):
- MAP query performance >500ms at customer scale
- Customers explicitly request managed extraction
- Revenue validates investment (>$500K ARR)

**If Phase 2 Triggered**:
- Build Observe-style ETL framework
- Pre-built templates (e-commerce, SaaS, travel)
- `business_index` table with scheduled extraction

### References

- [Storage Design Options 1-4](storage_design.md#design-options-for-our-system)
- [Storage Design Decision](storage_design.md#business-attribute-indexing-final-decision)
- [OpenSpec Tasks Section 6](../openspec/changes/add-iceberg-otlp-migration/tasks.md#6-business-attribute-indexing-sp-convention)

---

## ADR-005: Store Bodies as STRING (Not BINARY)

**Date**: 2025-12-31
**Status**: Accepted
**Deciders**: Engineering Team

### Context

HTTP request/response bodies need to be stored efficiently. Two options:
1. Store as STRING, let Parquet compress
2. Pre-compress to BINARY, store compressed bytes

### Decision

**Store bodies as STRING columns, rely on Parquet ZSTD compression**.

### Rationale

**Simplicity + Queryability > Marginal Compression Gains**

1. **Parquet ZSTD is Excellent**:
   - Compression ratios of 5-10x for JSON are common
   - ZSTD level 3 (default) balances speed and compression
   - No need for application-level compression

2. **Query-Friendly**:
   - DuckDB can query JSON directly: `json_extract(http_request_body, '$.orderId')`
   - No decompression step before parsing
   - Users can create wide tables with SQL

3. **Simpler Pipeline**:
   - No compression/decompression code
   - No codec management (`compression_codec` field)
   - Fewer failure modes

4. **Proven at Scale**:
   - Many companies store TB+ of JSON in Parquet
   - No evidence we need application-level compression

### Alternatives Considered

#### Option A: Pre-compress to BINARY
**Design**:
```sql
CREATE TABLE traces (
  request_body BINARY,  -- Compressed with zstd
  request_body_uncompressed_size BIGINT,
  compression_codec STRING,
  ...
);
```

**Pros**:
- Guaranteed compression (can tune per-body)
- Smaller individual values (better Parquet efficiency?)
- Can handle non-text bodies (images, binary)

**Cons**:
- Double compression? (app-level + Parquet)
- Must decompress before querying
- More complex ingestion/query path
- Can't use DuckDB `json_extract` directly

**Why Rejected**: Over-engineering without evidence of need

### Consequences

**Positive**:
- ✅ Simple ingestion (no compression code)
- ✅ Query-friendly (DuckDB `json_extract` works)
- ✅ Parquet ZSTD handles compression well
- ✅ Standard approach (proven at scale)

**Negative**:
- ⚠️ If compression ratio is insufficient, need to add BINARY option
- ⚠️ Can't handle binary bodies (images, PDFs) without base64

**Validation Required**:
- Benchmark compression ratios (JSON bodies 1KB, 10KB, 100KB)
- Compare ZSTD level 3 vs 6 vs 9
- Target: 5-10x compression ratio

**When We'd Reconsider**:
- Compression ratios <3x (insufficient)
- Storage costs exceed projections
- Need to store binary bodies (images, PDFs)

### References

- [Storage Design Analysis](storage_design.md#storage-schema-considerations)
- [Design Document Section 5.1](design.md#51-table-traces)

---

## Summary: Decision Cascade

These decisions form a coherent strategy:

```
Strategic Goal (ADR-000: Implicit)
  "Build AI-ready enterprise data lake"
         ↓
Storage Platform (ADR-001, ADR-002)
  Iceberg (ecosystem) + S3-compatible (cloud-neutral)
         ↓
Storage Architecture (ADR-003, ADR-005)
  Columnar separation + STRING bodies
         ↓
Data Capture (ADR-006)
  HTTP bodies via span events + business attributes via span attributes
         ↓
Business Attribute Indexing (ADR-004)
  User-provided sp.* + User ETL (Phase 1)
  Managed ETL (Phase 2, conditional)
```

**Core Philosophy**:
- **Ecosystem value > marginal performance**
- **Cloud neutrality > vendor lock-in**
- **Customer choice > prescribed infrastructure**
- **Start simple, validate, then invest**
- **User flexibility > managed convenience (initially)**

---

## ADR-006: HTTP Bodies via Span Events (Not Span Attributes)

**Date**: 2025-12-31
**Status**: Accepted
**Deciders**: Engineering Team

### Context

Need to decide how to capture HTTP request/response bodies in OTLP. Two options:
1. **Span Events**: Store bodies in `span.addEvent('http.request', { 'http.request.body': ... })`
2. **Span Attributes**: Store bodies in `span.setAttribute('sp.http.request.body', ...)`

OTLP has three attribute scopes (see [OTLP Specification](https://opentelemetry.io/docs/specs/otel/common/)):
- **Resource Attributes**: Entity producing telemetry (service, pod) - sent once per batch
- **Span Attributes**: Operation metadata (http.method, db.statement) - per span
- **Span Event Attributes**: Time-stamped events within a span (errors, exceptions, custom events)

OTLP's [HTTP Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/http/http-spans/) define standard attributes like `http.request.method`, `http.response.status_code`, but **do NOT define attributes for request/response bodies**.

### Decision

**Store HTTP bodies as Span Events** (not Span Attributes).

```javascript
// Recommended instrumentation pattern
span.addEvent('http.request', {
  'http.request.headers': JSON.stringify(headers),
  'http.request.body': JSON.stringify(requestBody)
});

span.addEvent('http.response', {
  'http.response.headers': JSON.stringify(responseHeaders),
  'http.response.body': JSON.stringify(responseBody)
});

// Business attributes remain as span attributes
span.setAttribute('sp.user.id', 'user-123');
span.setAttribute('sp.order.id', 'ORD-456');
```

### Rationale

**Semantic Correctness + Practical Benefits**

1. **OTLP-Native Semantics**:
   - Span events represent "notable moments during span lifecycle" ([Events Specification](https://opentelemetry.io/docs/specs/semconv/general/events/))
   - Request sent and response received ARE notable moments
   - Bodies are contextual data about events, not attributes of the operation

2. **Avoids Attribute Limits**:
   - Span attributes have default limit of 128 attributes per span
   - Large JSON bodies could hit this limit when combined with other attributes
   - Span events have no such limit

3. **Separation of Concerns**:
   - **Span Attributes**: Operation metadata (`sp.user.id`, `sp.order.id`) for search
   - **Span Events**: Full body content for deep analysis
   - Clear distinction between "searchable metadata" vs "raw payload"

4. **Unified Implementation**:
   - Both business attributes AND HTTP bodies follow OTLP standards
   - No need for custom `sp.http.*` convention that conflicts with OTLP semantics

5. **Storage Flexibility**:
   - Iceberg schema extracts events into columns or keeps as ARRAY<STRUCT>
   - Can optimize storage based on access patterns

### Alternatives Considered

#### Option A: Span Attributes (`sp.http.request.body`)

**Pros**:
- Simpler (everything is an attribute)
- Users already familiar with `span.setAttribute()`

**Cons**:
- Conflicts with OTLP semantics (bodies aren't "attributes")
- May hit 128 attribute limit
- Custom `sp.*` convention not standard
- Harder to separate searchable metadata from payloads

**Why Rejected**: Misaligned with OTLP event model

#### Option B: Resource Attributes

**Pros**:
- Sent once per batch (efficient)

**Cons**:
- Resource attributes describe the SERVICE, not individual requests
- Bodies change per request, resources are immutable
- Semantically incorrect

**Why Rejected**: Completely wrong scope

### Consequences

**Positive**:
- ✅ Aligns with OTLP standard event semantics
- ✅ No attribute count limits
- ✅ Clear separation: attributes for search, events for bodies
- ✅ Future-proof for other event types (database queries, message queue payloads)
- ✅ Unified implementation (no separate tasks for bodies vs business attributes)

**Negative**:
- ⚠️ Users must learn `span.addEvent()` in addition to `span.setAttribute()`
- ⚠️ Slightly more complex instrumentation (two methods vs one)

**Mitigations**:
- Provide clear documentation with examples in 3+ languages
- Create auto-instrumentation libraries that handle this automatically
- Show unified pattern: `sp.*` attributes + `http.request/response` events

**Implementation Impact**:
- Iceberg schema extracts from `events` ARRAY instead of `attributes` MAP
- Documentation combines both patterns in single guide
- Tasks 5 & 6 merged into unified implementation

### References

- [Common specification concepts | OpenTelemetry](https://opentelemetry.io/docs/specs/otel/common/)
- [Semantic conventions for HTTP spans | OpenTelemetry](https://opentelemetry.io/docs/specs/semconv/http/http-spans/)
- [Semantic conventions for events | OpenTelemetry](https://opentelemetry.io/docs/specs/semconv/general/events/)
- [OpenSpec Tasks Section 5](../openspec/changes/add-iceberg-otlp-migration/tasks.md#5-http-body-storage--business-attribute-indexing)

---

## Future Decisions (Pending)

### ADR-007: Query Engine Choice (DuckDB vs Spark)
- **Status**: To Be Decided
- **Timeline**: Phase 1 benchmarking
- **Context**: Need to choose primary query engine for Phase 1

### ADR-008: Metrics Storage Strategy
- **Status**: To Be Decided
- **Timeline**: Post Phase 1
- **Context**: Iceberg may not be ideal for time-series metrics

### ADR-009: Schema Evolution Strategy
- **Status**: To Be Decided
- **Timeline**: When first schema change needed
- **Context**: How to handle adding/removing columns safely

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-12-31 | Engineering Team | Initial decision log with ADR-001 through ADR-005 |
| 1.1 | 2025-12-31 | Engineering Team | ADR-002 updated to cloud-neutral strategy; Added ADR-006 (HTTP bodies via span events) |
