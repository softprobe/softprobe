## ADDED Requirements

### Requirement: Preserve Existing Query Parameters
The system SHALL serve existing recording query parameters (appId, beginTime/endTime, operationName, recordEnvironment, categoryType, recordId) via metadata indexes with raw retrieval from Iceberg.

#### Scenario: recordId lookup compatibility
- **WHEN** a client queries by `recordId`
- **THEN** the system resolves to `trace_id` and returns the full record through the new path without requiring API changes

### Requirement: Greenfield Rollout (No Dual-Write)
The system SHALL be deployed as a greenfield path without dual-writing; migration of legacy data is out of scope for this change.

#### Scenario: Greenfield deployment
- **WHEN** the new pipeline is deployed
- **THEN** traffic flows to the new path without requiring dual-write or rollback routing

