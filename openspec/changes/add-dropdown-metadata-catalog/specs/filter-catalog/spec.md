## ADDED Requirements

### Requirement: Tenant-scoped dropdown metadata store

The system SHALL persist distinct telemetry dimension values per tenant in Postgres table `ui_dropdown_catalog` with primary key `(tenant_id, entity_type, entity_value)` and `last_seen_at` for recency.

#### Scenario: Table exists beside DuckLake metadata

- **WHEN** `dropdown_catalog.enabled` is true and DuckLake `catalog_type` is `postgres`
- **THEN** the runtime creates the catalog table and index in the configured metadata schema if missing

#### Scenario: Upsert from ingest batches

- **WHEN** trace record batches are committed to DuckLake
- **THEN** distinct string/int32 dimension values are upserted from those batches before the DuckLake `INSERT` executes

### Requirement: Hosted catalog HTTP API

The system SHALL expose bounded read endpoints for the authenticated tenant when hosted routes and dropdown catalog are active.

#### Scenario: List entity types

- **WHEN** a client calls `GET /v1/catalog/entity-types` with valid bearer credentials
- **THEN** the response lists distinct `entity_type` values seen within the configured active window for that tenant only

#### Scenario: List values for one type

- **WHEN** a client calls `GET /v1/catalog/values?entityType=…&limit=…` with valid bearer credentials
- **THEN** the response lists distinct `entity_value` strings for that type, ordered by recent `last_seen_at`, capped by `limit`

### Requirement: Catalog maintenance

The system SHALL optionally delete catalog rows older than the configured day threshold during the scheduled maintenance loop.

#### Scenario: TTL prune

- **WHEN** `maintenance_prune_enabled` is true and maintenance runs
- **THEN** rows with `last_seen_at` older than `active_values_days` are deleted
