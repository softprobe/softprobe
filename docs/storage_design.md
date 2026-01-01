# HTTP Body Storage & Business Attribute Indexing Design

**Date**: 2025-12-31
**Status**: Brainstorming / Design Discussion
**Purpose**: Explore options for storing HTTP request/response bodies and enabling efficient search by business attributes

> **Strategic Context**: This design directly supports [Softprobe's business vision](goals.md) of building an AI-ready enterprise data lake. HTTP body storage is **Phase 1** of our broader strategy to capture all business data for LLM-based AI agents.

---

## Problem Statement

We need to:

1. **Store complete HTTP request/response bodies** for AI troubleshooting and ETL/analytics
2. **Enable fast search** by business-specific attributes (user ID, order ID, PNR, email, etc.)
3. **Avoid expensive read/write operations** - don't read bodies during routine queries
4. **Support diverse customer schemas** - can't hardcode all extraction patterns

**Key Constraint**: Extraction at high-volume ingestion is **compute expensive** and we **can't possibly handle all user cases** with predefined extraction rules.

---

## Research: How Modern Observability Platforms Handle This

### 1. Observe Inc (Snowflake-based)

**Architecture**: [Blog Post](https://www.observeinc.com/blog/observe-o2-architecture) | [Technical Deep Dive](https://medium.com/snowflake/how-observe-uses-snowflake-to-deliver-the-observability-cloud-part-2-f95014777173)

**Approach**: "Schema-on-Demand" with Post-Ingestion Transformation

**Data Flow**:
```
OTLP Data → Kafka Buffer → Micro-batch Load to Snowflake Data Lake
                                ↓
                         Raw Events Stored
                                ↓
                    Schema-on-Demand Transforms
                                ↓
            Incremental Views + Materialized Datasets
                                ↓
                    Token Indexes + Data Graph
```

**Key Design Decisions**:

- **When**: POST-INGESTION transformation (not at write time)
- **How**: SQL-based transforms create "datasets" from raw events
  - Customer-defined or app-supplied transformation rules
  - Transforms run as Snowflake queries (materialized views)
  - Incremental updates keep datasets fresh
- **Indexing**: Token indexes on extracted fields, semantic relationships between entities
- **Storage**: Snowflake data lake (columnar, compressed)

**Advantages**:
- ✅ Decouple ingestion from transformation (fast writes)
- ✅ Change extraction logic without re-ingestion (backfill historical data)
- ✅ Leverage SQL for transformations (familiar, powerful)
- ✅ Customers define their own extraction patterns

**Trade-offs**:
- ⚠️ Extraction lag (minutes to hours depending on batch schedule)
- ⚠️ More storage (raw + transformed)
- ⚠️ Requires ETL job orchestration

---

### 2. Honeycomb.io (Custom Column Store)

**Architecture**: [Why Column Store](https://www.honeycomb.io/blog/why-observability-requires-distributed-column-store) | [Technical Details](https://www.honeycomb.io/resources/why-we-built-our-own-distributed-column-store)

**Approach**: No Pre-defined Indexes, Query-Time Aggregation

**Data Flow**:
```
OTLP Data → Kafka → Retriever (Custom Columnar Store)
                         ↓
               Store as Wide Events (unlimited columns)
                         ↓
                    NO INDEXES
                         ↓
              Query-time Column Scans + Aggregation
```

**Key Design Decisions**:

- **When**: NO extraction - store raw, wide events
- **How**: Custom columnar datastore (Retriever)
  - Separate file per column on disk
  - No indexes at all - "you don't need to decide what is fast because everything needs to be fast"
  - Query engine computes aggregates (COUNT, AVG, P95, HEATMAP) on the fly
- **Indexing**: None - relies on columnar scan efficiency
- **Storage**: Column-oriented (disk, not memory)

**Advantages**:
- ✅ No ingestion overhead (just write columns)
- ✅ No pre-planning required (any field can be queried)
- ✅ Flexible schema (unlimited columns)

**Trade-offs**:
- ⚠️ Query performance depends on data volume and column cardinality
- ⚠️ Works best for OLAP-style aggregations, not needle-in-haystack searches
- ⚠️ Custom database (not OSS/standard)

---

### 3. Axiom (Cloud-native)

**Architecture**: [Platform Architecture](https://axiom.co/docs/platform-overview/architecture) | [High-Cardinality Metrics](https://axiom.co/blog/metrics)

**Approach**: Hyper-Cardinality Support via Columnar Optimizations

**Data Flow**:
```
OTLP Data → Distributed Edge Layer → Specialized Services
                                          ↓
                           EventDB (events) / MetricsDB (metrics)
                                          ↓
                         Proprietary Columnar Formats
                                          ↓
                      Serverless Query Workers + Caching
```

**Key Design Decisions**:

- **When**: INGESTION-time processing into optimized columnar format
- **How**: Schema-less ingestion with automatic columnar optimization
  - Predicate pushdown
  - Intelligent caching (hot data)
  - Serverless query workers on compressed data
- **Indexing**: No explicit secondary indexes - relies on columnar efficiency + caching
- **Storage**: Proprietary columnar formats (EventDB for events, MetricsDB for time-series)

**Advantages**:
- ✅ "Hyper-cardinality" queries perform well (high unique value counts)
- ✅ Sub-second query performance via caching
- ✅ No index management overhead

**Trade-offs**:
- ⚠️ Proprietary storage format (vendor lock-in)
- ⚠️ Requires significant engineering investment to build

---

## Design Options for Our System

### Option 1: User-Provided Span Attributes (Push Model)

**Approach**: Ask users to extract business attributes themselves and add to span attributes with `sp.*` convention.

**Implementation**:

```rust
// User's instrumentation code (OpenTelemetry SDK)
let span = tracer.start("checkout");
span.set_attribute("sp.user.id", "user-123");
span.set_attribute("sp.order.id", "ORD-456");
span.set_attribute("sp.email", "user@example.com");

// Still capture full body in events
span.add_event("http.request.body", json!({
    "userId": "user-123",
    "orderId": "ORD-456",
    "items": [...]
}));
```

**Our Collector**:
- Store all `sp.*` attributes in the `attributes` MAP
- Optionally allow users to configure "unwanted" attributes to filter/redact
- Bodies stored in events as usual

**Query Pattern**:
```sql
SELECT session_id, trace_id
FROM traces
WHERE attributes['sp.user.id'] = 'user-123'
  AND date >= CURRENT_DATE - INTERVAL 7 DAY
```

**Advantages**:
- ✅ Zero ingestion overhead (no parsing/extraction)
- ✅ Users control what's indexed (they know their schema)
- ✅ Works with any data format (not just JSON)
- ✅ Simple collector implementation

**Disadvantages**:
- ❌ Integration burden on users (must modify instrumentation)
- ❌ Requires user understanding of what attributes matter
- ❌ May conflict with standard OTLP semantic conventions
- ❌ Harder for users with legacy systems or third-party libraries

**When to Use**:
- Users have full control over instrumentation
- Greenfield applications
- High-volume scenarios where ingestion speed is critical

---

### Option 2: OTLP Collector Filter/Processor (Pre-Ingestion)

**Approach**: Run an OTLP Collector processor (or our own filter) that extracts attributes from bodies BEFORE sending to our collector.

**Architecture**:

```
User App → OTLP Exporter → [OTLP Collector with Filter] → Our Collector
                                     ↓
                         Extract sp.* from bodies
                         Add to span attributes
```

**Filter Configuration** (user-defined YAML):

```yaml
# otlp-collector-config.yaml
processors:
  extract_business_attributes:
    rules:
      - name: "user_id"
        source: "event.attributes[sp.http.request.body]"
        json_path: "$.userId"
        target: "sp.user.id"

      - name: "order_id"
        source: "event.attributes[sp.http.request.body]"
        json_path: "$.orderId"
        target: "sp.order.id"
```

**Our Collector**:
- Receives spans with `sp.*` attributes already populated
- Store in `attributes` MAP
- Bodies still in events

**Advantages**:
- ✅ No ingestion overhead in our collector
- ✅ Users configure extraction rules (we don't hardcode)
- ✅ Reuse standard OTLP Collector ecosystem
- ✅ Can be deployed as sidecar or central collector

**Disadvantages**:
- ❌ Additional infrastructure (OTLP Collector deployment)
- ❌ Users must manage two configs (OTLP Collector + our collector)
- ❌ Still compute-intensive extraction (just moved to different process)
- ❌ Harder to debug (two-hop architecture)

**When to Use**:
- Users already running OTLP Collector
- Want to leverage existing OTLP processor ecosystem
- Need centralized extraction logic (multi-tenant scenarios)

---

### Option 3: Post-Ingestion ETL Pipeline (Observe-Style)

**Approach**: Store raw data first, run SQL-based transforms to extract business attributes into materialized tables.

**Architecture**:

```
OTLP Data → Our Collector → Iceberg "traces" table (raw)
                                    ↓
                        [ETL Job: Hourly/Daily]
                                    ↓
            DuckDB/Spark SQL: Parse bodies, extract attributes
                                    ↓
                  Iceberg "business_index" table
```

**Implementation**:

**Step 1: Raw Storage** (no extraction)
```rust
// Just store everything as-is
SpanData {
    attributes: span.attributes,  // Original span attrs only
    events: [
        Event {
            name: "http.request.body",
            attributes: { "sp.http.request.body": "{\"userId\":\"user-123\",...}" }
        }
    ]
}
```

**Step 2: User-Defined Extraction Config**
```yaml
# config/etl/{app_id}.yaml
app_id: "checkout-service"

extraction_rules:
  - attribute_key: "user_id"
    json_path: "$.userId"
    source_event: "http.request.body"

  - attribute_key: "order_id"
    json_path: "$.orderId"
    source_event: "http.request.body"

  - attribute_key: "email"
    json_path: "$.customer.email"
    source_event: "http.request.body"
```

**Step 3: ETL Job** (DuckDB or Spark)
```sql
-- Run hourly to extract new data
CREATE TABLE business_index_staging AS
SELECT
    session_id,
    trace_id,
    span_id,
    timestamp,
    json_extract_string(
        list_extract(events, 1).attributes['sp.http.request.body'],
        '$.userId'
    ) AS user_id,
    json_extract_string(
        list_extract(events, 1).attributes['sp.http.request.body'],
        '$.orderId'
    ) AS order_id,
    date
FROM traces
WHERE timestamp >= (SELECT COALESCE(MAX(timestamp), '1970-01-01') FROM business_index)
  AND app_id = 'checkout-service';

-- Merge into index
INSERT INTO business_index SELECT * FROM business_index_staging;
```

**Step 4: Query** (two-hop lookup)
```sql
-- Step 1: Find sessions
SELECT session_id FROM business_index WHERE user_id = 'user-123';

-- Step 2: Get full context
SELECT * FROM traces WHERE session_id IN (...);
```

**Advantages**:
- ✅ Zero ingestion overhead (fast writes)
- ✅ Can change extraction rules and backfill historical data
- ✅ Users define their own extraction (SQL-based or config-driven)
- ✅ Leverage existing SQL/Iceberg ecosystem
- ✅ ETL can be run on separate compute (not in critical path)

**Disadvantages**:
- ❌ Extraction lag (minutes to hours)
- ❌ More storage (raw + extracted)
- ❌ ETL job management overhead
- ❌ Need scheduler/orchestrator (Airflow, Dagster, cron)

**When to Use**:
- Extraction lag is acceptable (not real-time troubleshooting)
- Want flexibility to change extraction logic
- High ingestion volume where compute is expensive
- Users comfortable with SQL/config-driven transformations

---

### Option 4: Leverage User's ETL Wide Tables

**Approach**: If users are already using bodies as ETL source, reuse their wide tables for business attribute lookups.

**User Workflow**:

```sql
-- User creates their own wide table from our raw data
CREATE TABLE orders_wide AS
SELECT
    session_id,
    json_extract_string(request_body, '$.orderId') AS order_id,
    json_extract_string(request_body, '$.userId') AS user_id,
    json_extract_string(request_body, '$.items') AS items,
    json_extract_string(request_body, '$.total') AS total,
    timestamp
FROM traces
WHERE http_path LIKE '/checkout%';
```

**Our Role**:
- Provide easy-to-query raw data (bodies accessible via SQL)
- Document best practices for creating wide tables
- Optionally: Provide pre-built templates for common patterns

**Advantages**:
- ✅ Zero work for us (users build their own indexes)
- ✅ Ultimate flexibility (users know their schema best)
- ✅ Reuse existing analytics infrastructure
- ✅ No extraction overhead in our system

**Disadvantages**:
- ❌ Users must do the work themselves
- ❌ No standardized query interface for troubleshooting
- ❌ Hard to provide "out-of-box" search experience
- ❌ Each user rebuilds similar patterns

**When to Use**:
- Power users / data engineers
- Users already have ETL pipelines
- We want to remain a "dumb" storage layer
- Users need maximum control

---

## Hybrid Approach: Combine Multiple Options

**Recommendation**: Support multiple paths, let users choose based on their needs.

### Tier 1: User-Provided Attributes (Lowest Latency)

For users who can modify instrumentation:

```rust
span.set_attribute("sp.user.id", "user-123");  // Immediately searchable
```

Our collector stores in `attributes` MAP → Fast queries on `traces` table.

### Tier 2: Optional ETL Templates (Medium Latency)

For users who prefer post-ingestion extraction:

```yaml
# config/etl/templates/common.yaml
templates:
  - name: "e-commerce"
    rules:
      - extract: "user_id" from "$.userId"
      - extract: "order_id" from "$.orderId"
      - extract: "cart_total" from "$.cart.total"

  - name: "travel-booking"
    rules:
      - extract: "pnr" from "$.pnr"
      - extract: "passenger_email" from "$.passenger.email"
```

We provide:
- Pre-built SQL templates for common industries
- Scheduler integration (optional)
- Users can customize or write their own

### Tier 3: User's Own ETL (Fully Custom)

For advanced users:
- We expose raw data via Iceberg
- Users build their own wide tables
- We provide documentation and examples

---

## Storage Schema Considerations

### Raw Traces Table (All Options)

```sql
CREATE TABLE traces (
  session_id STRING,
  trace_id STRING,
  span_id STRING,
  parent_span_id STRING,
  timestamp TIMESTAMPTZ,
  app_id STRING,
  message_type STRING,

  attributes MAP<STRING, STRING>,  -- Includes user-provided sp.* attrs

  events ARRAY<STRUCT<
    name STRING,
    timestamp TIMESTAMPTZ,
    attributes MAP<STRING, STRING>  -- Contains sp.http.request.body, etc.
  >>,

  date DATE  -- Partition key
)
PARTITIONED BY (date)
SORT BY (session_id, trace_id, timestamp)
```

**Question**: Should we separate bodies into a different table?

**Option A**: Bodies in Events (Current)
- ✅ Simplicity - one table
- ✅ Co-located with metadata
- ❌ Every query reads body data (even if not needed)
- ❌ Larger Parquet files

**Option B**: Separate HTTP Payloads Table
```sql
CREATE TABLE http_payloads (
  session_id STRING,
  span_id STRING,
  timestamp TIMESTAMPTZ,
  http_method STRING,
  http_path STRING,
  request_headers STRING,  -- JSON string
  request_body STRING,     -- Full body
  response_headers STRING,
  response_body STRING,
  date DATE
)
PARTITIONED BY (date, session_id)
```

- ✅ Metadata queries don't read bodies (I/O savings)
- ✅ Bodies only read when needed (AI troubleshooting, ETL)
- ✅ Can apply different compression/retention policies
- ❌ More complex writes (two tables)
- ❌ Join required to correlate metadata + bodies

**Recommendation**: **Option B (Separate Payloads Table)**
- Aligns with "separated storage" principle
- Optimizes for most common query pattern (search by metadata, rarely read bodies)
- Enables different data lifecycle (e.g., retain metadata 90 days, bodies 30 days)

### Business Index Table (Options 3 & 4)

```sql
CREATE TABLE business_index (
  attribute_key STRING,      -- "user_id", "order_id", "pnr", etc.
  attribute_value STRING,    -- "user-123", "ORD-456", etc.
  session_id STRING,
  span_id STRING,
  timestamp TIMESTAMPTZ,
  date DATE
)
PARTITIONED BY (date, attribute_key)
SORT BY (attribute_key, attribute_value, session_id)
```

**Bloom Filters**: On `attribute_key`, `attribute_value`, `session_id` (FPP: 0.001)

**Query Pattern**:
```sql
-- Fast: bloom filter + partition pruning
SELECT session_id FROM business_index
WHERE attribute_key = 'user_id'
  AND attribute_value = 'user-123'
  AND date >= CURRENT_DATE - INTERVAL 7 DAY;
```

---

## Data Flow Diagrams

### Option 1: User-Provided Attributes

```
┌──────────────────────────────────────────────┐
│ User App (with sp.* instrumentation)        │
│ span.set_attribute("sp.user.id", "user-123")│
└────────────────┬─────────────────────────────┘
                 │ OTLP
                 ▼
┌─────────────────────────────────────────────┐
│ Our Collector                               │
│ - Store attributes MAP (includes sp.*)      │
│ - Store events with bodies                  │
└────────────────┬────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────┐
│ Iceberg: traces table                       │
│ - attributes['sp.user.id'] = 'user-123'     │
│ - events[0].attributes['sp.http...'] = body │
└─────────────────────────────────────────────┘
                 │
                 ▼
         Query: WHERE attributes['sp.user.id'] = 'user-123'
```

### Option 3: Post-Ingestion ETL

```
┌──────────────────────────────────────────────┐
│ User App (standard instrumentation)         │
│ - Captures bodies in events                 │
└────────────────┬─────────────────────────────┘
                 │ OTLP
                 ▼
┌─────────────────────────────────────────────┐
│ Our Collector                               │
│ - Store raw (no extraction)                 │
└────────────────┬────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────┐
│ Iceberg: traces table (raw)                 │
│ - events[0].attributes['sp.http...'] = body │
└────────────────┬────────────────────────────┘
                 │
                 ▼
       [ETL Job: Hourly/Daily]
                 │
                 ▼
┌─────────────────────────────────────────────┐
│ DuckDB/Spark: Parse bodies, extract attrs  │
│ - json_extract(body, '$.userId')           │
└────────────────┬────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────┐
│ Iceberg: business_index table              │
│ - attribute_key = 'user_id'                │
│ - attribute_value = 'user-123'             │
│ - session_id = 'sess-abc'                  │
└─────────────────────────────────────────────┘
                 │
                 ▼
         Query: WHERE attribute_key='user_id'
                  AND attribute_value='user-123'
                 │
                 ▼
         Fetch full context from traces table
```

---

## Performance & Cost Considerations

### Ingestion Compute Cost

| Option | CPU Cost | Memory Cost | Latency |
|--------|----------|-------------|---------|
| Option 1 (User-provided) | ⭐⭐⭐⭐⭐ Zero | ⭐⭐⭐⭐⭐ Zero | ⭐⭐⭐⭐⭐ Minimal |
| Option 2 (OTLP Filter) | ⭐⭐ High (in filter) | ⭐⭐⭐ Medium | ⭐⭐⭐ Medium |
| Option 3 (ETL) | ⭐⭐⭐⭐⭐ Zero (async) | ⭐⭐⭐⭐⭐ Zero (async) | ⭐⭐ High (lag) |
| Option 4 (User ETL) | ⭐⭐⭐⭐⭐ Zero | ⭐⭐⭐⭐⭐ Zero | ⭐ Very High |

### Storage Cost

| Option | Raw Data | Extracted Data | Total |
|--------|----------|----------------|-------|
| Option 1 | traces table | In attributes MAP | 1x + small overhead |
| Option 2 | traces table | In attributes MAP | 1x + small overhead |
| Option 3 | traces table | business_index table | 1.1x - 1.5x |
| Option 4 | traces table | User-managed | 1x (for us) |

### Query Performance (Troubleshooting Lookup)

| Option | Query Pattern | Latency | Scalability |
|--------|---------------|---------|-------------|
| Option 1 | MAP lookup on traces | 100-500ms | Good (<1B spans) |
| Option 2 | MAP lookup on traces | 100-500ms | Good (<1B spans) |
| Option 3 | Index lookup → traces join | 50-200ms | Excellent (billions) |
| Option 4 | User's table | Varies | User's problem |

---

## Recommendation

### Phase 1: Start with Option 1 (User-Provided Attributes)

**Why**:
- Simplest implementation (no extraction engine needed)
- Zero ingestion overhead (critical for high volume)
- Users who care about search will instrument properly
- Validates the concept without over-engineering

**Implementation**:
1. Document `sp.*` convention in our OTLP extension guide
2. Store all span attributes in `attributes` MAP
3. Provide query examples for common patterns
4. Benchmark MAP query performance with realistic data

**Success Criteria**: If MAP queries perform well (<500ms) at expected scale, STOP HERE.

### Phase 2: If Needed, Add Option 3 (ETL Templates)

**When**: MAP queries are too slow OR users request post-ingestion extraction

**Implementation**:
1. Create `business_index` table schema
2. Build configurable ETL job (DuckDB or Spark)
3. Provide templates for common industries (e-commerce, travel, finance)
4. Document how users can customize templates

### Phase 3: Advanced Users (Option 4)

**Always Available**: Document how to use raw data for custom ETL

---

## Open Questions

1. **Retention Policy**: Should bodies have shorter retention than metadata?
   - Example: Metadata 90 days, bodies 30 days
   - Saves storage cost, still supports recent troubleshooting

2. **Privacy/PII**: How to handle sensitive data in bodies?
   - Redaction at ingestion? (defeats analytics purpose)
   - Encryption? (complicates ETL)
   - User responsibility? (document best practices)

3. **Body Size Limits**: What's the max body size we accept?
   - 10MB? 100MB?
   - Should we reject, truncate, or store externally (e.g., S3 with reference)?

4. **Compression**: Store bodies as STRING or pre-compress to BINARY?
   - Recommendation: STRING (let Parquet compress with ZSTD)
   - Rationale: Simpler, DuckDB can query JSON directly

5. **ETL Scheduler**: If we go with Option 3, what scheduler?
   - Simple: Cron in Docker
   - Advanced: Dagster, Airflow
   - Iceberg-native: Use Spark with incremental reads

---

## Executive Decision & Analysis

### Is Iceberg the Right Foundation?

**Decision**: ✅ **YES - Proceed with Iceberg**

#### Iceberg vs. Alternatives Analysis

**Performance Benchmarks** ([Source](https://medium.com/@rajnishdumca/iceberg-vs-delta-lake-vs-hudi-the-2025-guide-from-a-data-architects-perspective-0e48a039b9a1)):
- Delta Lake: 1.7x faster than Iceberg in TPC-DS OLAP queries
- Iceberg: Slowest in benchmarks, but designed for vendor neutrality
- Hudi: Optimized for upserts (not our append-heavy workload)

**Critical Insight**: TPC-DS benchmarks don't measure our actual use case (session lookups + batch ETL).

**The 1.7x "Slowness" is Overstated**:
1. **Ecosystem value >> marginal performance**: Multi-engine support, vendor neutrality, and community momentum matter far more than 1.7x in synthetic benchmarks
2. **Gap will close**: Iceberg development velocity is high, performance optimizations are continuous
3. **Wrong workload tested**: TPC-DS measures OLAP star-schema queries, not our session-based + ETL patterns
4. **Network I/O dominates**: When reading from R2/S3, network latency dwarfs Parquet decode time
5. **Our queries are different**: Session lookups benefit from Iceberg's partition pruning + row group stats more than Delta's raw scan speed

| Requirement | Iceberg | Delta Lake | Winner |
|-------------|---------|------------|--------|
| **Vendor Neutrality** | ✅ Apache, no vendor control | ⚠️ Databricks-centric | **Iceberg** |
| **Multi-Engine Support** | ✅ Spark, Trino, Dremio, Flink, DuckDB | ⚠️ Databricks-first | **Iceberg** |
| **Rust Ecosystem** | ✅ iceberg-rust 0.7+ | ❌ No official support | **Iceberg** |
| **OLAP Query Speed** | ⚠️ 1.7x slower | ✅ Fastest | **Delta** |
| **Session Lookups** | ✅ Partition pruning + row groups | ✅ Good | **Tie** |
| **Open Spec** | ✅ Fully open | ⚠️ Databricks influence | **Iceberg** |

**Business Rationale**:
1. **Vendor neutrality critical** for "open-source first" positioning
2. **Multi-engine support** enables future Snowflake/Trino/Databricks integrations
3. **Rust support** matches our technology stack
4. **Performance tax acceptable** - 1.7x slower in OLAP doesn't hurt our session-based queries
5. **Future integrations** - Fivetran/Airbyte have best Iceberg support

**When We'd Reconsider**:
- Session lookup latency >1s (AND optimization attempts fail)
- Customer demand for Databricks-native integration
- iceberg-rust library becomes unmaintained

### Storage Cost Analysis: Cloud-Neutral Strategy

**Cost Comparison** ([Source](https://themedev.net/blog/cloudflare-r2-vs-aws-s3/)):

| Provider | Storage Cost/GB | Egress Cost/GB | 100TB + 50TB egress/month |
|----------|----------------|----------------|---------------------------|
| **AWS S3** | $0.023 | $0.09 | **$6,800** |
| **Cloudflare R2** | $0.015 | **$0 (ZERO)** | **$1,500** |
| **Google Cloud Storage** | $0.020 | $0.12 | **$8,300** |
| **MinIO (self-hosted)** | Variable | $0 | Infrastructure cost |
| **Wasabi** | $0.0069 | **$0 (ZERO)** | **$690** |

**Strategic Decision**: Support S3-compatible storage, let customers choose.

**Business Impact**:
- **Cost-conscious customers**: Zero-egress providers (R2, Wasabi) save 78%+ vs AWS S3
- **Enterprise customers**: Can use existing AWS/GCP infrastructure (leverage cloud credits)
- **Regulated industries**: Self-hosted MinIO for compliance/sovereignty
- **Competitive positioning**: "Works with your cloud" vs vendor lock-in

**Key Insight**: Cloud neutrality increases adoption while customers with high egress (AI agents + ETL) naturally gravitate toward zero-egress providers.

### Storage Separation: Table-Level vs. Column-Level

**Question**: Should we separate bodies at TABLE level or COLUMN level?

#### Option A: Separate Tables
```sql
traces (metadata only)
http_payloads (bodies only)
business_index (extracted attributes)
```
**Pros**: Different retention policies, clear separation
**Cons**: Two Iceberg commits per write, join required for full context

#### Option B: Same Table, Columnar Separation
```sql
traces (
  metadata_columns: session_id, trace_id, attributes
  body_columns: request_body, response_body
)
```
**Pros**: Single write, Parquet columnar format = automatic I/O separation
**Cons**: Can't have different retention (but can drop columns during compaction)

**Decision**: ✅ **Start with Option B (Columnar Separation)**

**Rationale**:
- Parquet's columnar format already provides separation
- Query `SELECT session_id FROM traces WHERE ...` only reads metadata columns
- Simpler architecture (one table, one write path)
- Can migrate to separate tables in Phase 2 if needed

**Validation**: Benchmark to confirm Parquet skips body columns during metadata-only queries.

### Business Attribute Indexing: Final Decision

**Scoring Framework**:

| Criterion | Weight |
|-----------|--------|
| Time to Market | 40% |
| Scalability | 30% |
| Flexibility | 20% |
| Ops Complexity | 10% |

**Options Scored**:

| Option | TTM | Scale | Flex | Ops | **Total** |
|--------|-----|-------|------|-----|-----------|
| 1: User `sp.*` | 95% | 70% | 60% | 95% | **81%** |
| 2: OTLP Filter | 60% | 70% | 80% | 50% | **66%** |
| 3: Post-ETL | 40% | 95% | 95% | 30% | **64%** |
| 4: User ETL | 100% | 50% | 100% | 100% | **85%** |

**Decision**: ✅ **Hybrid Strategy (Option 1 + Option 4)**

### Phase 1 Implementation (Months 1-3)

**Ship**:
1. ✅ **User-Provided `sp.*` Attributes** (Option 1)
   - Document instrumentation best practices
   - Provide OpenTelemetry SDK examples
   - Store in `attributes` MAP
   - Query with DuckDB MAP lookups

2. ✅ **Enable Raw Body Access for User ETL** (Option 4)
   - Bodies stored as STRING columns (Parquet ZSTD compressed)
   - Document DuckDB SQL templates for creating wide tables
   - Examples: e-commerce, SaaS, travel booking

**Don't Ship** (Defer to Phase 2):
- ❌ Extraction engine (too complex, not validated)
- ❌ Separate `http_payloads` table (unless benchmark proves necessary)
- ❌ `business_index` table (wait for scale validation)

**Success Metrics**:
- 10 design partner customers
- <500ms query latency for session lookups via MAP
- Customer feedback: "10x cheaper than Datadog"

### Phase 2 Triggers (Month 4+)

**Add Managed ETL (Option 3) IF**:
1. MAP query performance >500ms at customer scale
2. Customers explicitly request managed extraction
3. Revenue validates investment (>$500K ARR)

**Implementation**:
- Observe-style ETL framework
- Pre-built templates (e-commerce, SaaS, travel)
- `business_index` table with scheduled extraction jobs

### Performance Reality Check

**Our Query Patterns** (not TPC-DS):

1. **Troubleshooting Lookup**: `WHERE session_id = 'xxx'` or `WHERE attributes['sp.user.id'] = 'xxx'`
   - Iceberg wins: Partition pruning + row group stats + bloom filters
   - Session-based row groups = single row group read
   - Expected latency: <200ms ✅

2. **ETL/Analytics**: `SELECT * FROM traces WHERE date = '2025-12-30'`
   - Delta wins in benchmarks (1.7x faster)
   - BUT: Network I/O from R2 dominates, not Parquet decode
   - 30s vs 50s for 1TB scan = doesn't matter for batch ETL ✅

3. **AI Agent Full Context**: `SELECT * FROM traces WHERE session_id IN (...)`
   - Iceberg wins: Same as #1, session-locality optimization ✅

**Conclusion**: Iceberg's "slowness" is in workloads we don't prioritize.

### Risk Mitigation

**Risk 1: Iceberg Performance Fails**
- Mitigation: Query caching (Redis), optimize Parquet file sizes, Z-ordering
- Escape hatch: Migrate to Delta Lake if ALL mitigations fail

**Risk 2: User Adoption of `sp.*`**
- Mitigation: Make instrumentation DEAD SIMPLE (SDKs, auto-instrumentation)
- Escape hatch: Build managed extraction (Option 3) in Phase 2

**Risk 3: R2 Pricing Changes**
- Mitigation: Long-term contract with Cloudflare
- Escape hatch: MinIO self-hosted

---

## Next Steps

### Immediate Actions (Next 2 Weeks)

1. **Benchmark Columnar Separation**
   ```sql
   -- Test: Does Parquet skip body columns?
   SELECT session_id, trace_id, attributes
   FROM traces
   WHERE attributes['sp.user.id'] = 'user-123'
   ```
   Measure I/O with vs. without body columns

2. **Prototype `sp.*` Convention**
   - Update documentation
   - Create OpenTelemetry instrumentation examples
   - Test MAP query performance with 10M spans

3. **Validate Multi-Backend Storage**
   - Benchmark compression ratios (JSON bodies with ZSTD)
   - Test with AWS S3, Cloudflare R2, and MinIO
   - Validate S3-compatible API consistency
   - Document cost implications for each backend

### Phase 1 Launch Checklist

- [ ] HTTP body storage as STRING columns in `traces` table
- [ ] `sp.*` attribute convention documented
- [ ] OpenTelemetry SDK instrumentation guide
- [ ] DuckDB query interface with MAP support
- [ ] SQL templates for user ETL (3+ industry templates)
- [ ] Benchmark report: <500ms session lookups
- [ ] Design partner program (10 customers)

### Phase 2 Decision Gates

**Evaluate at Month 4**:
- [ ] MAP query latency at 100M+ spans
- [ ] Customer feedback on `sp.*` adoption
- [ ] Revenue milestone ($500K ARR?)
- [ ] Competitive pressure (Databricks native?)

**If Phase 2 Needed**:
- [ ] Design `business_index` table schema
- [ ] Build ETL job scheduler (DuckDB-based)
- [ ] Create extraction rule config format
- [ ] Pre-built templates for 5+ industries

---

## References

### Internal Documentation
- **[Softprobe Strategic Goals](goals.md)** - Business vision and long-term roadmap
- **[Technical Design](design.md)** - Overall architecture and implementation

### External Research
- [Observe + Snowflake Architecture](https://www.observeinc.com/blog/observe-o2-architecture)
- [How Observe Uses Snowflake (Part 2)](https://medium.com/snowflake/how-observe-uses-snowflake-to-deliver-the-observability-cloud-part-2-f95014777173)
- [Why Honeycomb Built a Distributed Column Store](https://www.honeycomb.io/blog/why-observability-requires-distributed-column-store)
- [Honeycomb's Column Store Design](https://www.honeycomb.io/resources/why-we-built-our-own-distributed-column-store)
- [Axiom Architecture](https://axiom.co/docs/platform-overview/architecture)
- [Axiom High-Cardinality Metrics](https://axiom.co/blog/metrics)
