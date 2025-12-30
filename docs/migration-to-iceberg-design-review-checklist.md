# Design Document Review Checklist

**Document**: `docs/migration-to-iceberg-design.md`  
**Version**: 1.9  
**Date**: 2025-01-29  
**Status**: Revised - Pending Review (All Feedback Addressed)

---

## Review Process

### Step 1: Initial Draft Review
- [ ] Design document created with all required sections
- [ ] Technical accuracy verified
- [ ] Schema design validated
- [ ] API design reviewed
- [ ] Performance targets reviewed

### Step 2: Team Review
- [ ] Document shared with development team
- [ ] Team feedback collected
- [ ] Architecture concerns addressed
- [ ] Implementation concerns addressed
- [ ] Open questions answered

### Step 3: Architecture Review
- [ ] Document presented to architecture team
- [ ] Architecture concerns addressed
- [ ] Scalability concerns addressed
- [ ] Cost concerns addressed
- [ ] Risk concerns addressed

### Step 4: Stakeholder Approval
- [ ] Document shared with stakeholders
- [ ] Business concerns addressed
- [ ] Timeline approved
- [ ] Budget approved
- [ ] Final sign-off obtained

---

## Section Review Status

| Section | Reviewer | Status | Notes |
|---------|----------|--------|-------|
| Executive Summary | | Pending | |
| Current Architecture Analysis | | Pending | |
| Target Architecture Design | | Pending | |
| Metadata Schema Design | | Pending | |
| Batching & Compaction Strategy | | Pending | |
| Query Strategy | | Pending | |
| Migration Strategy | | Pending | |
| Performance Targets | | Pending | |
| Risk Assessment | | Pending | |
| Implementation Plan | | Pending | |
| API Design | | Pending | |
| Monitoring & Observability | | Pending | |
| Cost Analysis | | Pending | |

---

## Action Items from Review

### Architecture Review Feedback (2025-01-21)

**All 10 items addressed** ✅

See `docs/design-review-feedback-addressed.md` for detailed changes.

**Summary of Changes (v1.1)**:
1. ✅ Payload batching (100-1000 pairs per file)
2. ✅ Idempotent commits with reconciliation
3. ✅ Partition strategy (bucket hash reduces fan-out)
4. ✅ Iceberg syntax correction (SORTED BY)
5. ✅ Batch thresholds reconciled
6. ✅ DuckDB isolation documented
7. ✅ Payload retrieval optimized
8. ✅ Cost model corrected
9. ✅ Historical backfill tooling added
10. ✅ Security & compliance section added

**Follow-up Changes (v1.2)**:
1. ✅ S3 GET cost calculation fixed (1M GETs with 90% cache hit = $0.40/month)
2. ✅ Parquet row lookup clarified (no indexes, uses offset for direct access)
3. ✅ payload_file_offset usage documented (write: enumerate(), retrieval: read_row_by_index())

**Code Corrections (v1.3)**:
1. ✅ Batching code bug fixed (payload_batch variable now properly cloned)
2. ✅ Query predicate corrected (app_id = ? for automatic Iceberg pruning)
3. ✅ Parquet row access performance clarified (row group load required, 5-10ms realistic)

**Critical Fixes (v1.4)**:
1. ✅ flush_with_idempotent_commit payload_batch scope issue fixed (added as parameter)
2. ✅ Row group calculation fixed (added payload_row_group_index field, captured at write time from Parquet metadata)

**Background Flush Fixes (v1.5)**:
1. ✅ Background flush task tuple destructuring fixed (captures both URI and row_group_boundaries)
2. ✅ Replaced flush_with_payload_file with inline logic (matches main flush path)

**Helper Function & Transaction Safety (v1.6)**:
1. ✅ find_row_group_for_row helper function defined (binary search implementation with examples)
2. ✅ Background flush aligned with transactional flow (uses flush_with_idempotent_commit to prevent orphaned payload files)

**Double-Write Issue Fix (v1.7)**:
1. ✅ Fixed double-write problem - payload written once with transaction marker
2. ✅ Metadata derived from actual committed file (preventing offset/row group mismatches)
3. ✅ Removed flush_with_idempotent_commit helper, inlined transaction flow for clarity

**OTLP Compatibility Update (v1.8)**:
1. ✅ Documented OTLP ingestion over Protobuf, JSON, and Apache Arrow
2. ✅ Added detailed `softprobe.*` attribute mapping for resource/span data
3. ✅ Updated Iceberg schema (added `span_id`) and Phase 1 deliverables to include Arrow ingestion and attribute validation

**Session-Centric Pivot (v1.9)**:
1. ✅ Replaced Iceberg/DuckDB with session-centric ClickHouse architecture
2. ✅ Documented session-based payload storage and retention
3. ✅ Updated migration plan, risk analysis, monitoring, and cost sections

**Session Data Lake Reset (v2.0)**:
1. ✅ Simplified ingestion to raw OTLP session Parquet files (no Kafka)
2. ✅ Introduced Flink ETL for metadata and analytic tables
3. ✅ Rewrote architecture, storage, analytics, and roadmap sections accordingly

**Iceberg + ClickHouse Refinement (v2.1)**:
1. ✅ Documented Iceberg raw table schema and session-segment batching
2. ✅ Metadata now records `(file_path, row_offset, row_count)` per segment
3. ✅ Time-series stats generated via Flink and stored in ClickHouse SummingMergeTree tables

### Pending Review Items
_None - awaiting team review and final approval_

---

## Review Schedule

| Review Phase | Target Date | Actual Date | Status |
|--------------|-------------|-------------|--------|
| Initial Draft | 2025-01-20 | 2025-01-20 | ✅ Complete |
| Architecture Review (v1.0) | 2025-01-21 | 2025-01-21 | ✅ Complete |
| Feedback Addressed (v1.1) | 2025-01-21 | 2025-01-21 | ✅ Complete |
| Follow-up Review (v1.2) | 2025-01-22 | 2025-01-22 | ✅ Complete |
| Code Corrections (v1.3) | 2025-01-23 | 2025-01-23 | ✅ Complete |
| Critical Fixes (v1.4) | 2025-01-24 | 2025-01-24 | ✅ Complete |
| Background Flush Fixes (v1.5) | 2025-01-25 | 2025-01-25 | ✅ Complete |
| Helper Function & Transaction Safety (v1.6) | 2025-01-26 | 2025-01-26 | ✅ Complete |
| Double-Write Issue Fix (v1.7) | 2025-01-27 | 2025-01-27 | ✅ Complete |
| OTLP Compatibility Update (v1.8) | 2025-01-28 | 2025-01-28 | ✅ Complete |
| Session-Centric Pivot (v1.9) | 2025-01-29 | 2025-01-29 | ✅ Complete |
| Team Review | 2025-01-27 | | ⏳ Pending |
| Final Approval | 2025-01-31 | | ⏳ Pending |

---

## Approval Signatures

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Tech Lead | | | |
| Architecture Lead | | | |
| Product Owner | | | |
| Engineering Manager | | | |

---

**Note**: This checklist should be updated as the review progresses. All reviewers should add their feedback and mark sections as reviewed.
