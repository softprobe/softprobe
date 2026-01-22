# SoftProbe OTLP Backend - Test & Development Makefile
#
# This Makefile provides convenient targets for running tests across different environments:
# - Local development with MinIO
# - Cloudflare R2 cloud testing
# - CI/CD environments
#
# Usage:
#   make test-local     - Run all tests with local MinIO (requires docker-compose)
#   make test-r2        - Run all tests with Cloudflare R2
#   make test-ci        - Run tests in CI environment (auto-detects sandboxed env)
#   make test-quick     - Run unit tests only (no integration tests)
#   make setup-local    - Start local test infrastructure (MinIO + Iceberg REST)
#   make teardown-local - Stop local test infrastructure
#   make clean          - Clean build artifacts

.PHONY: help test-local test-r2 test-ci test-quick test-gcp test-gcp-stress test-deployment-local test-deployment-stress stress-test setup-local teardown-local clean build lint fmt check-fmt verify-e2e verify-quick demo-session duckdb-shell generate-telemetry drop-tables

# Default target
help:
	@echo "SoftProbe OTLP Backend - Testing & Development"
	@echo ""
	@echo "Test Targets:"
	@echo "  make test-local      - Run integration tests with local MinIO"
	@echo "  make test-r2         - Run integration tests with Cloudflare R2"
	@echo "  make test-ci         - Run tests in CI environment (auto-setup)"
	@echo "  make test-quick      - Run unit tests only (no integration)"
	@echo "  make test-all        - Run all tests (unit + integration)"
	@echo ""
	@echo "Deployment Testing:"
	@echo "  make test-gcp              - Test GCP deployment (https://i.softprobe.ai)"
	@echo "  make test-gcp-stress       - Stress test GCP with 10K+ spans"
	@echo "  make test-deployment-local - Test local deployment via Python script"
	@echo "  make test-deployment-stress - Stress test local with large dataset"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make setup-local     - Start local test infrastructure"
	@echo "  make teardown-local  - Stop local test infrastructure"
	@echo "  make check-local     - Verify local infrastructure is running"
	@echo ""
	@echo "Data & Verification:"
	@echo "  make generate-telemetry - Generate demo OTLP data"
	@echo "  make verify-e2e      - End-to-end verification (services + data)"
	@echo "  make verify-quick    - Quick DuckDB verification"
	@echo "  make demo-session    - Run session query demo"
	@echo "  make duckdb-shell    - Launch DuckDB with Iceberg views"
	@echo "  make drop-tables     - Drop Iceberg tables in REST catalog"
	@echo ""
	@echo "Development:"
	@echo "  make build           - Build the project"
	@echo "  make lint            - Run clippy lints"
	@echo "  make fmt             - Format code"
	@echo "  make check-fmt       - Check code formatting"
	@echo "  make clean           - Clean build artifacts"
	@echo ""
	@echo "Script Helpers:"
	@echo "  make help-scripts    - List script-backed targets"
	@echo ""

# Build targets
build:
	@echo "🔨 Building project..."
	cargo build

build-release:
	@echo "🔨 Building release..."
	cargo build --release

publish-docker:
	@echo "🔨 Publishing Docker image..."
	docker buildx build --platform linux/amd64 --push -t gcr.io/cs-poc-sasxbttlzroculpau4u6e2l/softprobe-otlp-backend:latest .

# Code quality targets
lint:
	@echo "🔍 Running clippy..."
	cargo clippy -- -D warnings

fmt:
	@echo "✨ Formatting code..."
	cargo fmt

check-fmt:
	@echo "🔍 Checking code formatting..."
	cargo fmt -- --check

clean:
	@echo "🧹 Cleaning build artifacts..."
	cargo clean
	rm -rf target/

# Local infrastructure management
setup-local:
	@echo "🚀 Starting local test infrastructure..."
	@echo "📦 Starting MinIO and Lakekeeper REST catalog..."
	@docker-compose up -d minio db migrate lakekeeper
	@echo "⏳ Waiting for services to be healthy..."
	@sleep 5
	@echo "✅ Checking MinIO health..."
	@curl -sf http://localhost:9002/minio/health/live > /dev/null || (echo "❌ MinIO not ready" && exit 1)
	@echo "🪣 Creating MinIO bucket 'warehouse'..."
	@docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1 || true
	@docker exec minio mc mb local/warehouse > /dev/null 2>&1 || \
		(docker exec minio mc ls local/warehouse > /dev/null 2>&1 && echo "✅ Bucket 'warehouse' already exists") || \
		(echo "❌ Failed to create or verify bucket 'warehouse'" && exit 1)
	@echo "✅ Bucket 'warehouse' is ready"
	@echo "✅ Checking Lakekeeper REST health..."
	@docker exec lakekeeper /home/nonroot/lakekeeper healthcheck > /dev/null 2>&1 || (echo "❌ Lakekeeper REST not ready" && exit 1)
	@echo "🔧 Bootstrapping Lakekeeper (if needed)..."
	@bootstrap_status=$$(curl -s -o /tmp/lakekeeper_bootstrap.json -w "%{http_code}" -X POST http://localhost:8181/management/v1/bootstrap \
		-H "Content-Type: application/json" \
		-d '{"accept-terms-of-use": true}'); \
	if [ "$$bootstrap_status" = "204" ] || \
		([ "$$bootstrap_status" = "400" ] && grep -q "CatalogAlreadyBootstrapped" /tmp/lakekeeper_bootstrap.json); then \
		echo "✅ Lakekeeper bootstrapped"; \
	else \
		echo "❌ Lakekeeper bootstrap failed (HTTP $$bootstrap_status)"; \
		cat /tmp/lakekeeper_bootstrap.json; \
		exit 1; \
	fi
	@echo "🧊 Creating Lakekeeper warehouse 'default' (if needed)..."
	@warehouse_status=$$(curl -s -o /tmp/lakekeeper_warehouse.json -w "%{http_code}" -X POST http://localhost:8181/management/v1/warehouse \
		-H "Content-Type: application/json" \
		-d '{"warehouse-name":"default","project-id":"00000000-0000-0000-0000-000000000000","storage-profile":{"type":"s3","bucket":"warehouse","key-prefix":"iceberg","endpoint":"http://minio:9000","region":"us-east-1","path-style-access":true,"flavor":"minio","sts-enabled":false},"storage-credential":{"type":"s3","credential-type":"access-key","aws-access-key-id":"minioadmin","aws-secret-access-key":"minioadmin"}}'); \
	if [ "$$warehouse_status" = "201" ] || \
		([ "$$warehouse_status" = "400" ] && (grep -q "CreateWarehouseStorageProfileOverlap" /tmp/lakekeeper_warehouse.json || grep -q "WarehouseAlreadyExists" /tmp/lakekeeper_warehouse.json)); then \
		echo "✅ Warehouse 'default' is ready"; \
	else \
		echo "❌ Warehouse creation failed (HTTP $$warehouse_status)"; \
		cat /tmp/lakekeeper_warehouse.json; \
		exit 1; \
	fi
	@echo "🔐 Ensuring Lakekeeper storage credentials use static keys..."
	@warehouse_id=$$(curl -s http://localhost:8181/management/v1/warehouse | python3 -c 'import json,sys; data=json.load(sys.stdin); print(next((w.get("id") or w.get("warehouse-id") for w in data.get("warehouses", []) if w.get("name")=="default"), ""))'); \
	if [ -z "$$warehouse_id" ]; then \
		echo "❌ Unable to resolve Lakekeeper warehouse ID"; \
		exit 1; \
	fi; \
	storage_status=$$(curl -s -o /tmp/lakekeeper_storage.json -w "%{http_code}" -X POST "http://localhost:8181/management/v1/warehouse/$$warehouse_id/storage" \
		-H "Content-Type: application/json" \
		-d '{"storage-profile":{"type":"s3","bucket":"warehouse","key-prefix":"iceberg","endpoint":"http://minio:9000","region":"us-east-1","path-style-access":true,"flavor":"minio","sts-enabled":false},"storage-credential":{"type":"s3","credential-type":"access-key","aws-access-key-id":"minioadmin","aws-secret-access-key":"minioadmin"}}'); \
	if [ "$$storage_status" = "200" ]; then \
		echo "✅ Lakekeeper storage profile updated"; \
	else \
		echo "❌ Lakekeeper storage update failed (HTTP $$storage_status)"; \
		cat /tmp/lakekeeper_storage.json; \
		exit 1; \
	fi
	@echo "✅ Local test infrastructure is ready!"
	@echo ""
	@echo "Services available:"
	@echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
	@echo "  - MinIO API: http://localhost:9002"
	@echo "  - Lakekeeper REST: http://localhost:8181/catalog"

teardown-local:
	@echo "🛑 Stopping local test infrastructure..."
	@docker-compose down
	@echo "✅ Local infrastructure stopped"

check-local:
	@echo "🔍 Checking local infrastructure..."
	@curl -sf http://localhost:9002/minio/health/live > /dev/null && echo "✅ MinIO is running" || echo "❌ MinIO is not running (run 'make setup-local')"
	@docker exec lakekeeper /home/nonroot/lakekeeper healthcheck > /dev/null 2>&1 && echo "✅ Lakekeeper REST is running" || echo "❌ Lakekeeper REST is not running (run 'make setup-local')"

# Test targets
test-quick:
	@echo "🧪 Running unit tests..."
	cargo test --lib

test-local: check-local
	@echo "🧪 Running integration tests with local MinIO..."
	@echo "📝 Configuration: tests/config/test.yaml"
	@echo "🗄️  Backend: MinIO (localhost:9002) + Lakekeeper REST (localhost:8181)"
	@echo ""
	ICEBERG_TEST_TYPE=local cargo test --test iceberg_integration_test -- --test-threads=1 --nocapture

test-r2:
	@echo "🧪 Running integration tests with Cloudflare R2..."
	@echo "📝 Configuration: tests/config/test-r2.yaml"
	@echo "☁️  Backend: Cloudflare R2 Iceberg Catalog"
	@echo "⚠️  Note: Requires valid R2 credentials in test-r2.yaml"
	@echo ""
	@if [ -z "$$ICEBERG_DISABLE_TLS_VALIDATION" ]; then \
		echo "🔒 Detecting environment..."; \
		if curl -sf https://www.google.com > /dev/null 2>&1; then \
			echo "✅ Direct internet access available"; \
			ICEBERG_TEST_TYPE=r2 cargo test --test iceberg_integration_test -- --test-threads=1 --nocapture; \
		else \
			echo "⚠️  Detected restricted/sandboxed environment"; \
			echo "⚠️  Enabling TLS validation bypass for testing"; \
			ICEBERG_DISABLE_TLS_VALIDATION=1 ICEBERG_TEST_TYPE=r2 cargo test --test iceberg_integration_test -- --test-threads=1 --nocapture; \
		fi \
	else \
		echo "🔓 TLS validation bypass already enabled"; \
		ICEBERG_TEST_TYPE=r2 cargo test --test iceberg_integration_test -- --test-threads=1 --nocapture; \
	fi

test-ci:
	@echo "🧪 Running tests in CI environment..."
	@echo "🔍 Auto-detecting environment and requirements..."
	@# In CI, we expect services to be available via docker-compose or service containers
	@if curl -sf http://localhost:8181/catalog/v1/config?warehouse=default > /dev/null 2>&1; then \
		echo "✅ Lakekeeper REST catalog detected"; \
		echo "🧪 Running integration tests with local catalog..."; \
		ICEBERG_TEST_TYPE=local cargo test --test iceberg_integration_test -- --test-threads=1; \
	else \
		echo "⚠️  No local catalog found, running unit tests only"; \
		cargo test --lib; \
	fi

test-all: test-quick test-local
	@echo "✅ All tests completed!"

# Convenience targets
test: test-local

# Development workflow
dev-check: check-fmt lint test-quick
	@echo "✅ Development checks passed!"

# Continuous Integration full check
ci-full: check-fmt lint build test-ci
	@echo "✅ CI checks completed!"

# Data & verification helpers
generate-telemetry:
	@python3 scripts/generate_telemetry.py

verify-e2e:
	@./scripts/verify_e2e.sh

verify-quick:
	@./scripts/verify_quick.sh

demo-session:
	@./scripts/demo_session_queries.sh

duckdb-shell:
	@./scripts/interactive_query.sh

drop-tables:
	@./scripts/drop_all_tables.sh

help-scripts:
	@echo "Script-backed targets:"
	@echo "  make generate-telemetry"
	@echo "  make verify-e2e"
	@echo "  make verify-quick"
	@echo "  make demo-session"
	@echo "  make duckdb-shell"
	@echo "  make drop-tables"

# GCP Deployment Testing
test-gcp:
	@echo "🌐 Testing GCP deployment at https://i.softprobe.ai..."
	@echo "⚠️  This tests the production deployment"
	@echo ""
	@if ! command -v python3 >/dev/null 2>&1; then \
		echo "❌ Python 3 is required. Please install python3."; \
		exit 1; \
	fi
	@if ! python3 -c "import requests" 2>/dev/null; then \
		echo "📦 Installing requests library..."; \
		uv pip install --user requests || uv pip install requests; \
	fi
	@python test_deployment.py --env gcp

test-gcp-stress:
	@echo "🌐 Stress testing GCP deployment with 10K+ spans..."
	@echo "⚠️  This will trigger buffer flush on production"
	@echo ""
	@if ! command -v python3 >/dev/null 2>&1; then \
		echo "❌ Python 3 is required. Please install python3."; \
		exit 1; \
	fi
	@if ! python3 -c "import requests" 2>/dev/null; then \
		echo "📦 Installing requests library..."; \
		pip3 install --user requests || pip3 install requests; \
	fi
	@python test_deployment.py --env gcp --span-count 10000 --session-count 100

test-deployment-local: check-local
	@echo "🧪 Testing local deployment via Python script..."
	@if ! command -v python3 >/dev/null 2>&1; then \
		echo "❌ Python 3 is required. Please install python3."; \
		exit 1; \
	fi
	@if ! python3 -c "import requests" 2>/dev/null; then \
		echo "📦 Installing requests library..."; \
		pip3 install --user requests || pip3 install requests; \
	fi
	@python3 test_deployment.py --env local

test-deployment-stress: check-local
	@echo "🧪 Stress testing local deployment with large dataset..."
	@if ! command -v python3 >/dev/null 2>&1; then \
		echo "❌ Python 3 is required. Please install python3."; \
		exit 1; \
	fi
	@if ! python3 -c "import requests" 2>/dev/null; then \
		echo "📦 Installing requests library..."; \
		pip3 install --user requests || pip3 install requests; \
	fi
	@python3 test_deployment.py --env local --span-count 20000

stress-test: setup-local
	@echo "🧪 Stress testing local deployment via `perf_stress`..."
	@CONFIG_FILE=config.yaml \
		cargo run --bin perf_stress -- \
		--duration 60 --span-qps 50 --log-qps 70 --metric-qps 70 --query-concurrency 4 --query-interval-ms 500
	@$(MAKE) teardown-local
