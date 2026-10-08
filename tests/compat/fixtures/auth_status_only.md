# Auth contract notes (Phase 0)

Runtime auth middleware (`runtime_auth_middleware`) returns **status-only**
bodies for Bearer failures:

| Case | HTTP status | Body |
|------|-------------|------|
| Missing `Authorization` | 401 | empty |
| Malformed Bearer | 401 | empty |
| Unknown / rejected API key | 403 | empty |

Scope-header mismatch (`X-Scope-OrgID` ≠ authenticated tenant) is checked in
compat stubs and returns the **protocol-native** error envelope with HTTP 403.

See `docs/compat/auth.md` and `tests/compat_phase0.rs`.
