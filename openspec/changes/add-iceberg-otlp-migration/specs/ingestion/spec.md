## ADDED Requirements

### Requirement: OTLP Ingestion Endpoint
The system SHALL expose an OTLP HTTP endpoint `/v1/traces` accepting protobuf and JSON payloads.

#### Scenario: Accept OTLP spans over HTTP
- **WHEN** a client sends OTLP spans to `/v1/traces`
- **THEN** the service responds 200 on valid payloads and enqueues spans for processing

### Requirement: Session-Aware Buffering with Trace Contiguity
The system SHALL buffer spans by `session_id` and flush as session-scoped row groups, ensuring spans for the same `trace_id` are contiguous within the session.

#### Scenario: Group by session and keep traces contiguous
- **WHEN** spans arrive for the same `session_id`
- **THEN** they are grouped and flushed together, internally ordered by `trace_id` then timestamp so reading by trace_id requires a single contiguous read within the row group

### Requirement: SpMocker to OTLP Mapping
The system SHALL map SpMocker fields to OTLP spans and attributes.

#### Scenario: Field mapping applied
- **WHEN** SpMocker data is ingested
- **THEN** `recordId`→`trace_id`, `operationName`→`span.name`, `appId`→`resource.attributes["sp.app.id"]`, `sessionId`→`span.attributes["sp.session.id"]`, payload→`span.events[name="sp.body"]`

