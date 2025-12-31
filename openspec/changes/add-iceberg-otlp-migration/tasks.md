## 0. Foundation (Completed)
- [x] 0.1 Implement OTLP `/v1/traces` endpoint with session buffering
- [x] 0.2 Create Iceberg table schema for traces storage (`otlp_traces`)
- [x] 0.3 Implement multi-app row-group batching and Parquet writer

## 1. OTLP Logs and Metrics APIs
- [ ] 1.1 Implement OTLP `/v1/logs` endpoint accepting LogRecords
- [ ] 1.2 Extend session buffering to handle both traces and logs together
- [ ] 1.3 Create Iceberg table schema for logs (`otlp_logs`) with session-based partitioning
- [ ] 1.4 Implement coordinated flush for traces+logs to maintain session alignment
- [ ] 1.5 Implement OTLP `/v1/metrics` endpoint accepting Metrics
- [ ] 1.6 Add separate metrics buffering (NOT session-based, organized by metric_name)
- [ ] 1.7 Create Iceberg table schema for metrics with metric_name partitioning (experimental)

## 2. Query APIs and Session Management
- [ ] 2.1 Implement query API for retrieving sessions by ID using direct Iceberg scans
- [ ] 2.2 Add parallel retrieval from traces and logs tables for complete session view
- [ ] 2.3 Add time-range query support with Iceberg partition elimination
- [ ] 2.4 Implement attribute-based filtering using Iceberg predicate pushdown
- [ ] 2.5 Add pagination support for large query results
- [ ] 2.6 Implement session timeout and auto-flush mechanisms for traces/logs buffers

## 3. Production Readiness
- [ ] 3.1 Add monitoring and observability (metrics, traces for the collector itself)
- [ ] 3.2 Implement graceful shutdown and buffer persistence for all signal types
- [ ] 3.3 Add configuration for multiple storage backends (S3, MinIO, R2)
- [ ] 3.4 Performance tuning: flush triggers, batch sizes, concurrency limits
- [ ] 3.5 Optimize Iceberg manifest pruning and row group statistics usage
- [ ] 3.6 Add compaction jobs for long-term storage optimization

## 4. Validation and Testing
- [ ] 4.1 Add integration tests for all three OTLP endpoints
- [ ] 4.2 Validate OTLP protocol compliance with official test suites
- [ ] 4.3 Benchmark ingestion throughput (target: 10k+ spans/sec)
- [ ] 4.4 Benchmark query performance using direct Iceberg scans
- [ ] 4.5 Test coordinated traces+logs flush for session consistency
- [ ] 4.6 Load testing with realistic multi-app, multi-session workloads
- [ ] 4.7 Validate metrics storage pattern (acknowledge experimental status)
