# Project Context

## Purpose
SoftProbe is a record-and-replay testing platform that captures real application interactions and enables automated testing through replay mechanisms. The platform provides:

- **Recording**: Java agents instrument applications to capture live traffic, API calls, and dependencies (mock data)
- **Storage**: Centralized repository for recorded interactions with efficient querying and retrieval
- **Replay**: Automated execution of recorded interactions against target applications for regression testing
- **Comparison**: Advanced JSON comparison engine for validating actual vs expected responses
- **Analytics**: AI-powered analysis of session data and testing patterns

## Tech Stack

### Backend Services (Java)
- **Java 21** with Spring Boot 3.4.3
- **Maven** for build management and multi-module project structure
- **MongoDB** for primary data storage (with migration to Apache Iceberg planned)
- **Redis** for caching and active session management

### OTLP Backend (Rust), yet to be implemented
- **Rust** with Axum web framework for high-performance data ingestion
- **Apache Iceberg** for scalable data lake storage
- **DuckDB** for analytical queries on recorded data
- **Parquet** format for efficient storage and compression
- **AWS S3/Oracle Object Storage** for payload storage

### Agent Technology (Java)
- **Java Agent** instrumentation for bytecode manipulation
- **Dubbo**, **Spring**, **Servlet** framework instrumentation
- **SPI (Service Provider Interface)** for extensible plugin architecture

## Project Conventions

### Code Style
- **Java**: Uses Lombok for boilerplate reduction, follows standard Spring Boot conventions
- **Rust**: Standard rustfmt formatting with cargo clippy for linting
- **Python**: PEP 8 compliance with type hints
- **Naming**: kebab-case for services/modules, camelCase for Java, snake_case for Rust/Python

### Architecture Patterns
- **Microservices**: Independent services for storage, replay orchestration, and API management
- **Agent-based Recording**: Lightweight instrumentation with minimal application overhead
- **Event-driven**: Asynchronous communication via HTTP APIs
- **Data Lake Architecture**: Separation of metadata (fast queries) and payloads (batch storage)
- **Comparison Engine**: Configurable JSON diffing with exclusion/inclusion rules

### Testing Strategy
- **Unit Tests**: JUnit 5 for Java services, cargo test for Rust, pytest for Python
- **Integration Tests**: Node.js-based API testing in dedicated integration-tests module
- **Record-and-Replay**: Self-testing using the platform's own capabilities
- **Build Flag**: Use `-DskipTests` for faster builds during development

### Git Workflow
- **Main Branch**: `main` for production releases
- **Feature Branches**: Descriptive names (e.g., `bill/pingan`)
- **Multi-module Build**: Maven reactor build across all components
- **Docker Support**: Containerized services with build.sh automation

## Domain Context

### Core Concepts
- **SpMocker**: Recorded interaction data including request, response, and all dependency mocks
- **MainEntry**: Primary recorded interaction serving as test case entry point
- **MockItem**: Individual mock data for database calls, external APIs, or other dependencies
- **Record ID**: Unique identifier for original recorded interactions
- **Replay ID**: Unique identifier for replay test executions
- **Comparison Rules**: Configurable logic for validating response differences
- **Session Metadata**: User interaction tracking with visitor analytics

### Recording Process
1. **Java Agent** instruments target application at runtime
2. **Traffic Capture** intercepts HTTP requests, database calls, external API interactions
3. **Mock Generation** creates deterministic mock data for all dependencies
4. **Data Upload** sends SpMocker objects to storage service via batch APIs
5. **Indexing** organizes recorded data by application, operation, and time

### Replay Process
1. **Test Planning** configures replay parameters, target environment, and comparison rules
2. **Data Retrieval** fetches recorded interactions and loads mock data into cache
3. **Request Execution** sends original requests to target application under test
4. **Response Capture** collects actual responses from target application
5. **Comparison** validates actual vs expected using configurable diff engine
6. **Reporting** provides detailed analysis of test results and failures

## Important Constraints

### Technical Constraints
- **Agent Performance**: Must maintain <5% application overhead during recording
- **Data Scale**: Handle billions of recorded interactions efficiently
- **Real-time Requirements**: Sub-second response times for replay operations
- **Backward Compatibility**: Support Java 8+ applications for agent deployment
- **Memory Efficiency**: Optimal handling of large request/response payloads

### Business Constraints
- **Multi-tenancy**: Isolated data and configurations per organization
- **Data Retention**: Configurable TTL policies for recorded data lifecycle
- **Cost Optimization**: Efficient storage tiering (hot metadata, cold payloads)
- **Accuracy Requirements**: High-fidelity replay with deterministic results

## External Dependencies

### Cloud Infrastructure
- **AWS S3**: Alternative/backup storage provider, or MinIO for on-premis

### Development Dependencies
- **Maven Central**: Java dependency resolution
- **Docker Hub**: Container image hosting
- **GitHub**: Source code repository and CI/CD integration
