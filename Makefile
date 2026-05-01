# SoftProbe OTLP Backend - Test & Development Makefile
#
# This Makefile provides convenient targets for running tests across different environments:
# - Local development with MinIO
# - Cloudflare R2 cloud testing
# - CI/CD environments
#
# Usage:
#   make test           - Unit tests + full local integration (MinIO; integration-e2e)
#   make test-quick     - Unit tests only
#   make test-smoke     - Lightweight integration tests (no MinIO)
#   make test-local     - Full integration only (MinIO + integration-e2e)
#   make test-r2        - Integration tests with Cloudflare R2
#   make test-ci        - CI: MinIO → lib + integration-e2e; else lib only
#   make setup-local    - Start MinIO (+ DuckLake Postgres for postgres-catalog dev)
#   make teardown-local - Stop local test infrastructure
#   make clean          - Clean build artifacts

.PHONY: help test test-all test-local test-smoke test-r2 test-ci test-quick test-gcp test-gcp-stress test-deployment-local test-deployment-stress stress-test stress-test-r2-ducklake stress-test-gcs-ducklake setup-local teardown-local setup-minio teardown-minio check-minio check-local check-local-postgres clean build lint fmt check-fmt verify-e2e verify-quick demo-session duckdb-shell generate-telemetry drop-tables

# Gated modules: tests/integration/mod.rs (iceberg, ingest/query, performance, …).
INTEGRATION_E2E_FLAGS = --features integration-e2e --test tests

# Ensure libduckdb is fetched when not present on host.
# Can be overridden by callers: `DUCKDB_DOWNLOAD_LIB=0 make build`
export DUCKDB_DOWNLOAD_LIB ?= 1

# Default target
help:
	@echo "SoftProbe OTLP Backend - Testing & Development"
	@echo ""
	@echo "Test Targets:"
	@echo "  make test            - Unit tests + full local integration (MinIO + integration-e2e)"
	@echo "  make test-all        - Same as make test"
	@echo "  make test-local      - Full integration only (MinIO + integration-e2e)"
	@echo "  make test-smoke      - Fast integration smoke (no MinIO; default test crate features)"
	@echo "  make test-r2         - Integration tests with Cloudflare R2 (+ integration-e2e)"
	@echo "  make test-ci         - CI: MinIO present → lib + integration-e2e; else lib only"
	@echo "  make test-quick      - Unit tests only (cargo test --lib)"
	@echo ""
	@echo "Deployment Testing:"
	@echo "  make test-gcp              - Test GCP deployment (https://i.softprobe.ai)"
	@echo "  make test-gcp-stress       - Stress test GCP with 10K+ spans"
	@echo "  make stress-test-gcs-ducklake - Local DuckLake stress against GCS bucket"
	@echo "  make stress-test-r2-ducklake - Stress test DuckLake on Cloudflare R2"
	@echo "  make test-deployment-local - Test local deployment via Python script"
	@echo "  make test-deployment-stress - Stress test local with large dataset"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make setup-local     - Start MinIO (+ DuckLake Postgres for dev)"
	@echo "  make teardown-local  - Stop docker-compose stack in this directory"
	@echo "  make check-local          - Verify MinIO (required for integration tests)"
	@echo "  make check-local-postgres - Verify DuckLake Postgres (optional dev catalog)"
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
	docker buildx build --platform linux/amd64 --push -t gcr.io/cs-poc-sasxbttlzroculpau4u6e2l/splake:latest .

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
	@echo "📦 Starting MinIO and DuckLake Postgres catalog..."
	@docker-compose up -d minio ducklake-postgres
	@echo "⏳ Waiting for services to be healthy..."
	@sleep 5
	@echo "✅ Checking MinIO health..."
	@curl -sf http://localhost:9000/minio/health/live > /dev/null || (echo "❌ MinIO not ready" && exit 1)
	@echo "🪣 Creating MinIO bucket 'warehouse'..."
	@docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1 || true
	@docker exec minio mc mb local/warehouse > /dev/null 2>&1 || \
		(docker exec minio mc ls local/warehouse > /dev/null 2>&1 && echo "✅ Bucket 'warehouse' already exists") || \
		(echo "❌ Failed to create or verify bucket 'warehouse'" && exit 1)
	@echo "✅ Bucket 'warehouse' is ready"
	@echo "🦆 Checking DuckLake Postgres health..."
	@docker exec ducklake-postgres pg_isready -U ducklake -d ducklake > /dev/null 2>&1 || (echo "❌ DuckLake Postgres not ready" && exit 1)
	@echo "✅ Local test infrastructure is ready!"
	@echo ""
	@echo "Services available:"
	@echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
	@echo "  - MinIO API: http://localhost:9000"
	@echo "  - DuckLake catalog DB: postgres://ducklake@localhost:5432/ducklake"

teardown-local:
	@echo "🛑 Stopping local test infrastructure..."
	@docker-compose down
	@echo "✅ Local infrastructure stopped"

setup-minio:
	@echo "🚀 Starting MinIO for DuckLake stress testing..."
	@docker-compose up -d minio
	@echo "⏳ Waiting for MinIO health..."
	@sleep 3
	@curl -sf http://localhost:9000/minio/health/live > /dev/null || (echo "❌ MinIO not ready" && exit 1)
	@echo "🪣 Creating MinIO bucket 'warehouse'..."
	@docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1 || true
	@docker exec minio mc mb local/warehouse > /dev/null 2>&1 || \
		(docker exec minio mc ls local/warehouse > /dev/null 2>&1 && echo "✅ Bucket 'warehouse' already exists") || \
		(echo "❌ Failed to create or verify bucket 'warehouse'" && exit 1)
	@echo "✅ MinIO is ready for stress testing"

teardown-minio:
	@echo "🛑 Stopping MinIO stress-test infrastructure..."
	@docker-compose stop minio > /dev/null 2>&1 || true
	@docker-compose rm -f minio > /dev/null 2>&1 || true
	@echo "✅ MinIO infrastructure stopped"

check-minio:
	@echo "🔍 Checking MinIO..."
	@curl -sf http://localhost:9000/minio/health/live > /dev/null && echo "✅ MinIO is running" || (echo "❌ MinIO is not running (run 'make setup-minio')" && exit 1)

check-local: check-minio
	@echo "✅ Local test prerequisites satisfied (MinIO; integration tests use file DuckLake metadata + s3://warehouse data)"

check-local-postgres:
	@echo "🔍 Checking DuckLake Postgres..."
	@docker exec ducklake-postgres pg_isready -U ducklake -d ducklake > /dev/null 2>&1 && echo "✅ DuckLake Postgres is running" || (echo "❌ DuckLake Postgres is not running (run 'make setup-local' from softprobe-runtime/)" && exit 1)

# Test targets
test-quick:
	@echo "🧪 Running unit tests..."
	cargo test --lib

test-smoke:
	@echo "🧪 Running lightweight integration tests (no integration-e2e feature)..."
	cargo test --test tests -- --test-threads=1

test-local: check-local
	@echo "🧪 Running full integration tests with local MinIO (integration-e2e)..."
	@echo "📝 Configuration: tests/config/test.yaml"
	@echo "🗄️  Backend: MinIO :9000 (DuckLake test data); metadata is per-run temp files"
	@echo ""
	SPLAKE_RESET_DUCKLAKE=1 ICEBERG_TEST_TYPE=local cargo test $(INTEGRATION_E2E_FLAGS) -- --test-threads=1 --nocapture

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
			ICEBERG_TEST_TYPE=r2 cargo test $(INTEGRATION_E2E_FLAGS) -- --test-threads=1 --nocapture; \
		else \
			echo "⚠️  Detected restricted/sandboxed environment"; \
			echo "⚠️  Enabling TLS validation bypass for testing"; \
			ICEBERG_DISABLE_TLS_VALIDATION=1 ICEBERG_TEST_TYPE=r2 cargo test $(INTEGRATION_E2E_FLAGS) -- --test-threads=1 --nocapture; \
		fi \
	else \
		echo "🔓 TLS validation bypass already enabled"; \
		ICEBERG_TEST_TYPE=r2 cargo test $(INTEGRATION_E2E_FLAGS) -- --test-threads=1 --nocapture; \
	fi

test-ci:
	@echo "🧪 Running tests in CI environment..."
	@echo "🔍 Auto-detecting environment and requirements..."
	@# In CI, we expect services to be available via docker-compose or service containers
	@if curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then \
		echo "✅ MinIO detected"; \
		echo "🧪 Running unit tests..."; \
		cargo test --lib; \
		echo "🧪 Running integration tests (integration-e2e)..."; \
		SPLAKE_RESET_DUCKLAKE=1 ICEBERG_TEST_TYPE=local cargo test $(INTEGRATION_E2E_FLAGS) -- --test-threads=1; \
	else \
		echo "⚠️  No local MinIO on :9000, running unit tests only"; \
		cargo test --lib; \
	fi

test-all: test-quick test-local
	@echo "✅ All tests completed!"

# Default pre-merge check: lib + full integration (requires MinIO).
test: test-all

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
	@python3 test_deployment.py --env gcp --span-count 10000 --session-count 100

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

stress-test: setup-minio
	@echo "🧪 Stress testing local deployment via perf_stress..."
	@set -e; \
		PORT=38090; \
		TMP_CONFIG=/tmp/splake-stress.yaml; \
		sed "s/port: 8090/port: $$PORT/" config.yaml > $$TMP_CONFIG; \
		echo "🚀 Starting splake on port $$PORT..."; \
		SPLAKE_RESET_DUCKLAKE=1 CONFIG_FILE=$$TMP_CONFIG cargo run --bin softprobe-runtime > /tmp/splake-stress.log 2>&1 & \
		SPLAKE_PID=$$!; \
		trap 'kill $$SPLAKE_PID >/dev/null 2>&1 || true; $(MAKE) teardown-minio >/dev/null 2>&1 || true' EXIT; \
		for i in 1 2 3 4 5 6 7 8 9 10; do \
			if curl -sf "http://127.0.0.1:$$PORT/health" >/dev/null 2>&1; then \
				break; \
			fi; \
			sleep 1; \
		done; \
		curl -sf "http://127.0.0.1:$$PORT/health" >/dev/null 2>&1 || (echo "❌ splake failed to start"; cat /tmp/splake-stress.log; exit 1); \
		CONFIG_FILE=$$TMP_CONFIG cargo run --bin perf_stress -- \
			--service-url "http://127.0.0.1:$$PORT" \
			--duration 60 --span-qps 50 --log-qps 70 --metric-qps 70 --query-concurrency 4 --query-interval-ms 500; \
		kill $$SPLAKE_PID >/dev/null 2>&1 || true; \
		trap - EXIT
	@$(MAKE) teardown-minio

stress-test-r2-ducklake:
	@echo "☁️  Stress testing DuckLake with Cloudflare R2 object storage..."
	@set -e; \
		R2_CONFIG=$${R2_CONFIG:-tests/config/test-r2.yaml}; \
		PORT=$${PORT:-38091}; \
		if [ ! -f "$$R2_CONFIG" ]; then \
			echo "❌ R2 config file not found: $$R2_CONFIG"; \
			exit 1; \
		fi; \
		R2_BUCKET=$${R2_BUCKET:-$$(rg "^\\s*wal_bucket:\\s*" "$$R2_CONFIG" -m 1 | sed -E 's/^[^:]*:[[:space:]]*"?([^"#]+)"?.*/\1/' | xargs)}; \
		if [ "$$R2_BUCKET" = "your-bucket-name" ]; then \
			echo "❌ R2 bucket in $$R2_CONFIG is still placeholder: ingest_engine.wal_bucket=your-bucket-name"; \
			echo "   Update $$R2_CONFIG with your real Iceberg R2 settings OR pass R2_BUCKET=<real-bucket>."; \
			exit 1; \
		fi; \
		if [ -z "$$R2_BUCKET" ]; then \
			echo "❌ Could not resolve R2 bucket from $$R2_CONFIG."; \
			echo "   Set ingest_engine.wal_bucket or pass R2_BUCKET=<real-bucket>."; \
			exit 1; \
		fi; \
		if rg -n "your-bucket-name" "$$R2_CONFIG" >/dev/null; then \
			echo "❌ $$R2_CONFIG still appears to contain placeholder/sample R2 values."; \
			echo "   Set real R2 credentials/bucket first, then rerun."; \
			exit 1; \
		fi; \
		TMP_CONFIG=/tmp/splake-r2-ducklake-stress.yaml; \
		cp $$R2_CONFIG $$TMP_CONFIG; \
		sed -i.bak "s/port: 8090/port: $$PORT/" $$TMP_CONFIG && rm -f $$TMP_CONFIG.bak; \
		printf "\nducklake:\n" >> $$TMP_CONFIG; \
		printf "  catalog_type: \"duckdb\"\n" >> $$TMP_CONFIG; \
		printf "  metadata_path: \"/tmp/splake-r2-ducklake/metadata.ducklake\"\n" >> $$TMP_CONFIG; \
		printf "  data_path: \"s3://%s/ducklake/\"\n" "$$R2_BUCKET" >> $$TMP_CONFIG; \
		printf "  catalog_alias: \"softprobe\"\n" >> $$TMP_CONFIG; \
		printf "  metadata_schema: \"main\"\n" >> $$TMP_CONFIG; \
		printf "  data_inlining_row_limit: 0\n" >> $$TMP_CONFIG; \
		echo "🚀 Starting splake with $$R2_CONFIG on port $$PORT..."; \
		SPLAKE_RESET_DUCKLAKE=1 CONFIG_FILE=$$TMP_CONFIG cargo run --bin softprobe-runtime > /tmp/splake-r2-ducklake-stress.log 2>&1 & \
		SPLAKE_PID=$$!; \
		trap 'kill $$SPLAKE_PID >/dev/null 2>&1 || true; rm -f $$TMP_CONFIG' EXIT; \
		for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do \
			if curl -sf "http://127.0.0.1:$$PORT/health" >/dev/null 2>&1; then \
				break; \
			fi; \
			sleep 1; \
		done; \
		curl -sf "http://127.0.0.1:$$PORT/health" >/dev/null 2>&1 || (echo "❌ splake failed to start"; cat /tmp/splake-r2-ducklake-stress.log; exit 1); \
		echo "🧪 Running 10s smoke check before full stress..."; \
		CONFIG_FILE=$$TMP_CONFIG cargo run --bin perf_stress -- \
			--service-url "http://127.0.0.1:$$PORT" \
			--duration 10 --span-qps 10 --log-qps 10 --metric-qps 10 --query-concurrency 1 --query-interval-ms 1000 \
			> /tmp/perf-r2-ducklake-smoke.log 2>&1; \
		if rg -n "errors:\\s*[1-9]|Total query errors:\\s*[1-9]|Steady-state query errors:\\s*[1-9]" /tmp/perf-r2-ducklake-smoke.log >/dev/null; then \
			echo "❌ R2 smoke check failed (non-zero errors)."; \
			echo "---- perf smoke output ----"; \
			cat /tmp/perf-r2-ducklake-smoke.log; \
			echo "---- splake error lines ----"; \
			rg -n "ERROR|Error|failed|Failed" /tmp/splake-r2-ducklake-stress.log || true; \
			exit 1; \
		fi; \
		echo "✅ Smoke check passed; starting full stress run..."; \
		CONFIG_FILE=$$TMP_CONFIG cargo run --bin perf_stress -- \
			--service-url "http://127.0.0.1:$$PORT" \
			--duration 60 --span-qps 50 --log-qps 70 --metric-qps 70 --query-concurrency 4 --query-interval-ms 500; \
		kill $$SPLAKE_PID >/dev/null 2>&1 || true; \
		trap - EXIT; \
		rm -f $$TMP_CONFIG

stress-test-gcs-ducklake:
	@echo "☁️  Stress testing local DuckLake against GCS bucket..."
	@set -e; \
		GCP_CONFIG=$${GCP_CONFIG:-tests/config/test-gcp.yaml}; \
		PORT=$${PORT:-38092}; \
		CACHE_ROOT=$${CACHE_ROOT:-/tmp/splake-gcs-ducklake}; \
		if [ ! -f "$$GCP_CONFIG" ]; then \
			echo "❌ GCP config file not found: $$GCP_CONFIG"; \
			exit 1; \
		fi; \
		GCS_BUCKET=$${GCS_BUCKET:-$$(rg "^\\s*wal_bucket:\\s*" "$$GCP_CONFIG" -m 1 | sed -E 's/^[^:]*:[[:space:]]*"?([^"#]+)"?.*/\1/' | xargs)}; \
		if [ -z "$$GCS_BUCKET" ]; then \
			echo "❌ Could not resolve wal_bucket from $$GCP_CONFIG."; \
			echo "   Set ingest_engine.wal_bucket or pass GCS_BUCKET=<real-bucket>."; \
			exit 1; \
		fi; \
		if [ "$$GCS_BUCKET" = "YOUR-GCS-BUCKET-NAME" ] || [ "$$GCS_BUCKET" = "your-bucket-name" ]; then \
			echo "❌ GCS bucket appears to be placeholder in $$GCP_CONFIG."; \
			exit 1; \
		fi; \
		if [ -z "$$GOOGLE_APPLICATION_CREDENTIALS" ]; then \
			echo "❌ GOOGLE_APPLICATION_CREDENTIALS is not set for GCS auth."; \
			exit 1; \
		fi; \
		TMP_CONFIG=/tmp/splake-gcs-ducklake-stress.yaml; \
		cp "$$GCP_CONFIG" "$$TMP_CONFIG"; \
		sed -i.bak "s/port: 8090/port: $$PORT/" "$$TMP_CONFIG" && rm -f "$$TMP_CONFIG.bak"; \
		sed -i.bak "s/max_buffer_spans: 10000/max_buffer_spans: 1/" "$$TMP_CONFIG" && rm -f "$$TMP_CONFIG.bak"; \
		sed -i.bak "s/flush_interval_seconds: 60/flush_interval_seconds: 1/" "$$TMP_CONFIG" && rm -f "$$TMP_CONFIG.bak"; \
		sed -i.bak "s/optimizer_interval_seconds: 300/optimizer_interval_seconds: 2/" "$$TMP_CONFIG" && rm -f "$$TMP_CONFIG.bak"; \
		rm -rf "$$CACHE_ROOT"; \
		mkdir -p "$$CACHE_ROOT/cache" "$$CACHE_ROOT/wal"; \
		sed -i.bak "s|cache_dir: .*|cache_dir: \"$$CACHE_ROOT/cache\"|" "$$TMP_CONFIG" && rm -f "$$TMP_CONFIG.bak"; \
		if rg -n "^\\s*wal_dir:\\s*" "$$TMP_CONFIG" >/dev/null; then \
			sed -i.bak "s|wal_dir: .*|wal_dir: \"$$CACHE_ROOT/wal\"|" "$$TMP_CONFIG" && rm -f "$$TMP_CONFIG.bak"; \
		else \
			awk -v wal="  wal_dir: \"$$CACHE_ROOT/wal\"" '1; /cache_dir:/ {print wal}' "$$TMP_CONFIG" > "$$TMP_CONFIG.inject" && mv "$$TMP_CONFIG.inject" "$$TMP_CONFIG"; \
		fi; \
		printf "\nducklake:\n" >> "$$TMP_CONFIG"; \
		printf "  catalog_type: \"duckdb\"\n" >> "$$TMP_CONFIG"; \
		printf "  metadata_path: \"/tmp/splake-gcs-ducklake/metadata.ducklake\"\n" >> "$$TMP_CONFIG"; \
		printf "  data_path: \"s3://%s/ducklake/\"\n" "$$GCS_BUCKET" >> "$$TMP_CONFIG"; \
		printf "  catalog_alias: \"softprobe\"\n" >> "$$TMP_CONFIG"; \
		printf "  metadata_schema: \"main\"\n" >> "$$TMP_CONFIG"; \
		printf "  data_inlining_row_limit: 0\n" >> "$$TMP_CONFIG"; \
		echo "🚀 Starting local splake on port $$PORT using GCS bucket $$GCS_BUCKET..."; \
		SPLAKE_RESET_DUCKLAKE=1 CONFIG_FILE="$$TMP_CONFIG" cargo run --bin softprobe-runtime > /tmp/splake-gcs-ducklake-stress.log 2>&1 & \
		SPLAKE_PID=$$!; \
		trap 'kill $$SPLAKE_PID >/dev/null 2>&1 || true; rm -f "$$TMP_CONFIG"' EXIT; \
		for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do \
			if curl -sf "http://127.0.0.1:$$PORT/health" >/dev/null 2>&1; then \
				break; \
			fi; \
			sleep 1; \
		done; \
		curl -sf "http://127.0.0.1:$$PORT/health" >/dev/null 2>&1 || (echo "❌ splake failed to start"; cat /tmp/splake-gcs-ducklake-stress.log; exit 1); \
		echo "♨️  Warmup: ingest-only pass to create committed DuckLake tables..."; \
		CONFIG_FILE="$$TMP_CONFIG" cargo run --bin perf_stress -- \
			--service-url "http://127.0.0.1:$$PORT" \
			--duration 12 --span-qps 10 --log-qps 10 --metric-qps 10 --query-concurrency 0 --query-interval-ms 1000 \
			> /tmp/perf-gcs-ducklake-warmup.log 2>&1; \
		echo "🧪 Running 10s smoke check before full stress..."; \
		CONFIG_FILE="$$TMP_CONFIG" cargo run --bin perf_stress -- \
			--service-url "http://127.0.0.1:$$PORT" \
			--duration 10 --span-qps 10 --log-qps 10 --metric-qps 10 --query-concurrency 1 --query-interval-ms 1000 \
			> /tmp/perf-gcs-ducklake-smoke.log 2>&1; \
		if rg -n "errors:\\s*[1-9]|Total query errors:\\s*[1-9]|Steady-state query errors:\\s*[1-9]" /tmp/perf-gcs-ducklake-smoke.log >/dev/null; then \
			echo "❌ GCS smoke check failed (non-zero errors)."; \
			echo "---- perf smoke output ----"; \
			cat /tmp/perf-gcs-ducklake-smoke.log; \
			echo "---- splake error lines ----"; \
			rg -n "ERROR|Error|failed|Failed" /tmp/splake-gcs-ducklake-stress.log || true; \
			exit 1; \
		fi; \
		echo "✅ Smoke check passed; starting full stress run..."; \
		CONFIG_FILE="$$TMP_CONFIG" cargo run --bin perf_stress -- \
			--service-url "http://127.0.0.1:$$PORT" \
			--duration 60 --span-qps 50 --log-qps 70 --metric-qps 70 --query-concurrency 4 --query-interval-ms 500; \
		kill $$SPLAKE_PID >/dev/null 2>&1 || true; \
		trap - EXIT; \
		rm -f "$$TMP_CONFIG"
