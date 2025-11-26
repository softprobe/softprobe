# Test Plan for SP-Istio Open Source Stack

## Overview

This test plan covers the full-stack open-source implementation of SP-Istio, including:
- WASM Plugin (Istio extension)
- Backend Service (Spring Boot with ClickHouse)
- Context Viewer (Next.js frontend)
- Infrastructure components (ClickHouse, Redis)

## Test Environment Setup

### Prerequisites
- Kind cluster with Istio installed
- kubectl configured
- Docker Desktop running
- Backend and context-viewer images built and loaded into Kind

### Test Data
- Travel demo app deployed in Kind cluster
- Sample HTTP traffic generated through the demo app

## Test Categories

### 1. Unit Tests

#### 1.1 Backend Unit Tests
**Status**: Pending Implementation

**Test Cases**:
- [ ] `OtelClickHouseRepository.storeSpans()` - Verify span storage
- [ ] `OtelClickHouseRepository.storeLogs()` - Verify log storage
- [ ] `OtelClickHouseRepository.storeMetrics()` - Verify metric storage
- [ ] `QueryClickHouseRepository.queryTraces()` - Verify trace querying
- [ ] `TraceProcessingService.processTraces()` - Verify trace processing
- [ ] `TraceViewService.getTraceView()` - Verify trace view generation
- [ ] `TraceViewService.getTraceDetail()` - Verify trace detail retrieval
- [ ] `OtelCacheRepository.storeSpans()` - Verify cache storage
- [ ] Table creation logic in ClickHouse repository
- [ ] Timestamp conversion and formatting
- [ ] Span transformation (regular spans vs Envoy proxy spans)

**Test Framework**: JUnit 5, Mockito
**Location**: `backend/src/test/java/`

#### 1.2 Context Viewer Unit Tests
**Status**: Pending Implementation

**Test Cases**:
- [ ] Component rendering tests
- [ ] API client tests
- [ ] Trace visualization logic
- [ ] Session filtering and search

**Test Framework**: Jest, React Testing Library
**Location**: `context-viewer/src/**/*.test.tsx`

### 2. Integration Tests

#### 2.1 Backend Integration Tests
**Status**: Pending Implementation

**Test Cases**:
- [ ] OTLP trace ingestion (protobuf format)
- [ ] OTLP trace ingestion (JSON format)
- [ ] ClickHouse database connection and table creation
- [ ] Redis cache connection and operations
- [ ] End-to-end trace storage and retrieval
- [ ] Session-based trace querying
- [ ] Error handling for invalid OTLP data
- [ ] Concurrent trace ingestion

**Test Framework**: Spring Boot Test, Testcontainers (ClickHouse, Redis)
**Location**: `backend/src/test/java/ai/softprobe/otel/integration/`

#### 2.2 WASM Plugin Integration Tests
**Status**: Existing (may need updates)

**Test Cases**:
- [ ] HTTP request/response capture
- [ ] OTLP trace export to backend
- [ ] Session ID propagation
- [ ] Traffic filtering rules
- [ ] Performance under load

**Test Framework**: Go test suite
**Location**: `test/`

### 3. End-to-End Tests

#### 3.1 Full Stack E2E Tests
**Status**: Manual Testing Required

**Test Scenarios**:

**Scenario 1: Basic Trace Capture**
1. Deploy full stack (WASM plugin, backend, context-viewer, ClickHouse, Redis)
2. Generate HTTP traffic through travel demo app
3. Verify traces appear in ClickHouse
4. Verify traces visible in context-viewer
5. Verify trace details are complete

**Scenario 2: Session-Based Tracing**
1. Generate multiple HTTP requests with same session ID
2. Verify all requests appear in same session in context-viewer
3. Verify trace correlation across services

**Scenario 3: High Volume Traffic**
1. Generate 1000+ HTTP requests
2. Verify all traces are captured
3. Verify no data loss
4. Verify backend performance

**Scenario 4: Error Handling**
1. Stop ClickHouse service
2. Verify backend handles connection errors gracefully
3. Restart ClickHouse
4. Verify backend reconnects and continues processing

**Scenario 5: Context Viewer Functionality**
1. Open context-viewer in browser
2. Verify session list loads
3. Verify trace visualization renders correctly
4. Verify trace detail view shows all span information
5. Verify filtering and search works

**Test Location**: Manual testing in Kind cluster

### 4. Performance Tests

#### 4.1 Backend Performance
**Status**: Pending Implementation

**Metrics to Measure**:
- [ ] Trace ingestion throughput (traces/second)
- [ ] Query response time (p50, p95, p99)
- [ ] ClickHouse write latency
- [ ] Redis cache hit rate
- [ ] Memory usage under load
- [ ] CPU usage under load

**Test Tools**: Apache Bench, JMeter, or custom load generator

#### 4.2 WASM Plugin Performance
**Status**: Existing (may need updates)

**Metrics to Measure**:
- [ ] Request latency overhead
- [ ] Memory footprint
- [ ] CPU usage
- [ ] Throughput impact

### 5. Security Tests

#### 5.1 Backend Security
**Status**: Pending Implementation

**Test Cases**:
- [ ] Input validation for OTLP data
- [ ] SQL injection prevention in queries
- [ ] XSS prevention in API responses
- [ ] CORS configuration
- [ ] Rate limiting (if implemented)
- [ ] Authentication/authorization (if added)

#### 5.2 WASM Plugin Security
**Status**: Existing (may need updates)

**Test Cases**:
- [ ] Request/response data sanitization
- [ ] Memory safety (Rust guarantees)
- [ ] No sensitive data leakage in logs

### 6. Compatibility Tests

#### 6.1 OTLP Protocol Compatibility
**Status**: Pending Implementation

**Test Cases**:
- [ ] OTLP v1.0.0 compatibility
- [ ] Protobuf encoding/decoding
- [ ] JSON encoding/decoding
- [ ] Different span kinds (SERVER, CLIENT, etc.)
- [ ] Different attribute types
- [ ] Events and links handling

#### 6.2 ClickHouse Compatibility
**Status**: Pending Implementation

**Test Cases**:
- [ ] ClickHouse 23.x compatibility
- [ ] ClickHouse 24.x compatibility
- [ ] Table schema evolution
- [ ] Data type handling

#### 6.3 Browser Compatibility (Context Viewer)
**Status**: Pending Implementation

**Test Cases**:
- [ ] Chrome/Chromium
- [ ] Firefox
- [ ] Safari
- [ ] Edge

### 7. Deployment Tests

#### 7.1 Kubernetes Deployment
**Status**: Manual Testing Required

**Test Cases**:
- [ ] All components deploy successfully
- [ ] Health checks pass
- [ ] Services are accessible
- [ ] ConfigMaps and Secrets work correctly
- [ ] Resource limits are appropriate
- [ ] Pod restart and recovery

#### 7.2 Docker Image Builds
**Status**: Manual Testing Required

**Test Cases**:
- [ ] Backend Docker image builds successfully
- [ ] Context-viewer Docker image builds successfully
- [ ] Images are loadable into Kind
- [ ] Images start correctly in Kubernetes

## Test Execution Plan

### Phase 1: Unit Tests (Week 1)
1. Implement backend unit tests
2. Implement context-viewer unit tests
3. Achieve >80% code coverage

### Phase 2: Integration Tests (Week 2)
1. Set up testcontainers for ClickHouse and Redis
2. Implement backend integration tests
3. Update WASM plugin integration tests if needed

### Phase 3: E2E Tests (Week 3)
1. Set up Kind cluster test environment
2. Execute all E2E scenarios
3. Document results and fix issues

### Phase 4: Performance Tests (Week 4)
1. Set up load testing infrastructure
2. Execute performance tests
3. Analyze results and optimize

### Phase 5: Security & Compatibility (Week 5)
1. Execute security tests
2. Execute compatibility tests
3. Fix any issues found

## Test Data Requirements

### Sample Traces
- Simple HTTP request/response
- Multi-service distributed trace
- Trace with events and links
- Trace with various attribute types
- Large trace (many spans)

### Sample Sessions
- Single request session
- Multi-request session
- Session with errors
- Long-running session

## Success Criteria

### Unit Tests
- ✅ >80% code coverage
- ✅ All tests pass
- ✅ No flaky tests

### Integration Tests
- ✅ All integration tests pass
- ✅ Tests run in CI/CD pipeline
- ✅ Tests complete in <5 minutes

### E2E Tests
- ✅ All scenarios pass
- ✅ No data loss
- ✅ Performance meets requirements

### Performance Tests
- ✅ Backend handles 1000 traces/second
- ✅ Query response time <500ms (p95)
- ✅ WASM plugin overhead <5ms per request

## Known Issues & Limitations

### Current Limitations
1. No automated E2E tests (manual testing required)
2. No performance benchmarks established
3. No security audit completed
4. Limited error handling test coverage

### Future Improvements
1. Add automated E2E tests using Kubernetes test framework
2. Establish performance baselines
3. Add comprehensive security testing
4. Add chaos engineering tests

## Test Tools & Frameworks

### Backend
- JUnit 5
- Mockito
- Spring Boot Test
- Testcontainers (ClickHouse, Redis)
- AssertJ

### Context Viewer
- Jest
- React Testing Library
- Playwright (for E2E)

### Load Testing
- Apache Bench
- JMeter
- k6

### Infrastructure
- Kind (local Kubernetes)
- kubectl
- Docker

## Reporting

### Test Reports
- Unit test results: JUnit XML format
- Integration test results: JUnit XML format
- E2E test results: Manual documentation
- Performance test results: JSON/CSV format

### Test Metrics
- Code coverage percentage
- Test execution time
- Pass/fail rates
- Performance benchmarks

## Maintenance

### Test Maintenance
- Review and update tests monthly
- Add tests for new features
- Remove obsolete tests
- Update test data as needed

### Test Environment
- Keep test environment in sync with production
- Update dependencies regularly
- Maintain test data freshness

