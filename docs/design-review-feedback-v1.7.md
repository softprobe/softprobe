# Design Review Feedback v1.7 - Double-Write Issue Fix

**Date**: 2025-01-27  
**Document**: `docs/migration-to-iceberg-design.md`  
**Version**: 1.6 → 1.7

---

## Summary

Critical double-write issue fixed. The design now ensures payload is written ONCE with transaction marker, and metadata is derived from that actual committed file, preventing offset/row group mismatches.

---

## Issue

### Double-Write Problem ✅

**Issue**:
- Payload batch was written TWICE:
  1. First write via `store_batch` to get boundaries
  2. Second write via `flush_with_idempotent_commit` → `store_batch_with_transaction`
- This caused:
  - Wasted throughput and cost (duplicate S3 PUT)
  - Potential failure (object already exists)
  - **Critical**: Metadata (offsets, row_group_index) derived from first write might not match second write
  - Second write could produce different row group boundaries due to compression differences

**Root Cause**:
- Initial write was done to get row group boundaries
- Then `flush_with_idempotent_commit` wrote the same batch again with transaction marker
- Metadata was created from first write, but actual persisted file was from second write

---

## Solution

### Single Write with Transaction Marker ✅

**New Flow**:
1. **Write payload ONCE with transaction marker**: `store_batch_with_transaction` writes file with transaction ID
2. **Derive metadata from actual file**: Extract row group boundaries from the actual written Parquet file
3. **Create metadata with correct offsets**: Use boundaries from actual file to create metadata
4. **Write metadata with same transaction ID**: Write metadata to Iceberg with same transaction ID
5. **Commit both OR rollback both**: Either both succeed (commit) or both fail (rollback payload file)

**Changes Made**:

1. **Main Flush Path** (Lines 756-805):
   - Removed initial `store_batch` call
   - Write payload ONCE via `store_batch_with_transaction` with transaction ID
   - Derive metadata from actual written file boundaries
   - Write metadata to Iceberg with same transaction ID
   - Commit or rollback atomically

2. **Background Flush Path** (Lines 826-875):
   - Removed duplicate `store_batch` call
   - Same flow as main flush: write once with transaction, derive metadata, commit/rollback

3. **Idempotent Commit Strategy** (Lines 934-945):
   - Removed helper function `flush_with_idempotent_commit`
   - Inlined transaction flow directly in flush paths
   - Clarified that metadata must be derived from actual committed file

**Key Principle**: 
> Payload is written ONCE with transaction marker. Metadata is derived from that actual written file. If metadata write fails, the payload file is deleted (rollback). This ensures no orphaned payload files.

---

## Impact

- **No Double Writes**: Payload written once, saving S3 PUT costs and throughput
- **Accurate Metadata**: Offsets and row group indices match actual persisted file
- **Transaction Safety**: Atomic commit/rollback prevents orphaned files
- **Simplified Flow**: Removed helper function, inlined transaction logic for clarity

---

## Validation Checklist

- [x] Payload written once with transaction marker
- [x] Metadata derived from actual written file boundaries
- [x] No duplicate S3 PUT operations
- [x] Transaction safety maintained (commit/rollback)
- [x] Both flush paths (main and background) follow same pattern
- [x] Code compiles and works correctly

---

## Next Steps

1. **Team Review**: Review updated design document (v1.7)
2. **Final Approval**: Obtain stakeholder sign-off
3. **Implementation**: Begin Phase 1 after approval

---

**Status**: Critical double-write issue addressed ✅

