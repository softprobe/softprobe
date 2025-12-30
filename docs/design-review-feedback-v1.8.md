# Design Review Feedback v1.8 - OTLP Compatibility Update

**Date**: 2025-01-28  
**Document**: `docs/migration-to-iceberg-design.md`  
**Version**: 1.7 → 1.8

---

## Summary

Expanded the ingestion design to specify full OpenTelemetry compatibility (Protobuf, JSON, and Apache Arrow) and documented the exact attribute mapping required to populate SoftProbe-specific metadata.

---

## Key Changes

1. **Ingestion API Documentation**  
   - Added supported OTLP encodings (JSON/Protobuf/Arrow).  
   - Defined the canonical `softprobe.*` attribute namespace and mapping rules.  
   - Clarified how request/response payloads are represented via span events.  
   - Described legacy JSON shortcut as temporary and deprecated.  
   - Added `span_id` column to Iceberg schema so OTLP span identifiers are retained for debugging.

2. **Implementation Plan Updates**  
   - Phase 1 deliverables now include Arrow ingestion support and attribute validation.

---

## Follow-up Actions

- Implement attribute validation and Arrow ingestion during Phase 1.  
- Update agent/collector configurations to emit the required `softprobe.*` attributes.  
- Add conformance tests to ensure unsupported payloads return 422.

---

**Status**: Documentation updated; engineering tasks added to plan ✅
