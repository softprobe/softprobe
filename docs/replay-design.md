# Replay Service Integration (Design)

**Status**: Draft - Pending Review  
**Scope**: Phase 2 project (separate from initial migration)

---

## Goals
- Read/find replay cases via Query Service
- Create batches for efficient execution
- Pull baseline recordings (by trace_id across selected apps)
- Store replayed recordings into Rust OTLP Collector (same raw storage path)
- Query replay results via Query Service
- Compare baseline vs replay and persist outcomes

## Common Identifiers and Attributes
- planId, planItemId, caseId, recordId (trace_id), session_id
- categoryType, operationName, recordEnvironment, recordVersion
- sendStatus, compareStatus, requestDateTime, recordTime
- Replay-specific OTLP attributes on spans:
  - `sp.record.source = "replay"`
  - `sp.replay.run_id = "<uuid>"`
  - `sp.replay.plan_id`, `sp.replay.plan_item_id`
  - `sp.replay.instance_id = "<target-instance-id>"`
  - Optional: `sp.base.record_id` when target generates a new trace

## End-to-End Flow
```
Replay Scheduler
   │
   │ (1) Discover cases (planId, filters)
   ▼
Query Service
   │
   │ (2) Create batches (by app-set/instance/run_id)
   ▼
Replay Executors
   │
   │ (3) Pull baseline by trace_id & app-set → get pointers to raw data
   ▼
Query Service → returns Iceberg pointers (file_uri, row_group_index, row_index)
   │
   │ (4) Replay to target; target app is OTLP-instrumented → emits spans
   │     Spans flow to Rust OTLP Collector with replay attributes
   ▼
Rust OTLP Collector → Iceberg raw data (replay)
   │
   │ (5) Query replay result indices by planId/planItemId/run_id/status
   ▼
Query Service → returns pointers to replay rows
   │
   │ (6) Compare baseline vs replay payloads using pointers
   ▼
Compare Writer → persist comparison outcome (summary + pointers)
```

## 1) Read/Find Replay Cases (Query Service)
- API: `GET /query/replay/cases`
- Filters: `planId` (required), `planItemId`, `contextIdentifier`, `sendStatus`, `compareStatus`, `recordTime`/`requestDateTime` range
- Response: list of cases with `recordId` (trace_id), `appId[]`, `planItemId`, execution times, status

## 2) Create Replay Batches
- Group by `(target instance, app-set)`; cap cases per batch; time-slice windows
- Assign `run_id` (uuid) per batch; propagate to all replay spans
- Concurrency controls: per-instance QPS; retries on transient errors

## 3) Pull Baseline Data Per Replay Case
- API: `POST /query/recordings/baseline`
- Request: `trace_id`, `app_ids[]`, optional time/category/operation filters
- Response: ordered list of pointers per app: `iceberg_file_uri`, `row_group_index`, `row_index_in_group`, plus message metadata
- Executors fetch payloads using pointers (stream via Query Service or direct reader)

## 4) Store Replayed Recordings (Rust OTLP Collector)
- Target app emits OTLP spans to Rust Collector (preferred), or runner emits synthetic spans
- All replay spans include replay attributes above. If target allocates new traces, include `sp.base.record_id` for linkage
- Collector: OTLP `/v1/traces` → session-aware buffering → multi-app row groups → direct Iceberg write

## 5) Query Replay Results (Query Service)
- API: `GET /query/replay/results`
- Filters: `planId`, `planItemId`, `run_id`, `compareStatus`, time range
- Response: index of results: `recordId` (baseline), `replayId` (if applicable), `recordTime`, `replayTime`, `diffResultCode` (if computed), and row pointers to baseline and replay spans

## 6) Compare Results
- For each result row, load baseline and replay payloads using row pointers
- Run structured diff using existing comparison configuration
- Persist outcome (summary + minimal artifacts): `planId`, `planItemId`, `recordId`, `replayId`, `diffResultCode`, `compareStatus`, and pointers for drill-down
- APIs:
  - `GET /query/replay/diff?id=...` → returns pointer sets (or payloads if server-side compare enabled)
  - Optional: `POST /compare` to offload diff to a service

## Error Handling & Idempotency
- `run_id` per batch ensures idempotency across replays
- Dedupe by `(trace_id, span_id, run_id)` in ingestion/ETL
- Backpressure: scheduler reduces concurrency when query or collector latency breaches thresholds

## Observability
- Replay metrics: cases discovered/executed, success/error counts, per-batch and per-case latencies
- Collector metrics: replay ingestion rate by `run_id`, buffering, flush latency
- Query metrics: baseline pointer lookup latency, replay result lookup latency
- Compare metrics: diff counts by category/operation, error reasons

## Out of Scope for Phase 1
- No DDL changes. Metadata surfaces for replay (e.g., replay_cases, replay_results) will be finalized in this project’s implementation plan.


