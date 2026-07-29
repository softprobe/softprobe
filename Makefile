# SoftProbe OTLP Backend - Test & Development Makefile
#
# This Makefile provides convenient targets for running tests across different environments:
# - Local development with MinIO
# - Cloudflare R2 cloud testing
# - CI/CD environments
#
# Usage:
#   make test           - Unit tests + full local integration (MinIO + Postgres; integration-e2e)
#   make test-smoke     - Alias of test-quick (library + lightweight tests/tests.rs)
#   make test-local     - Full integration only (MinIO + Postgres + integration-e2e)
#   make test-r2        - Integration tests with Cloudflare R2
#   make test-ci        - CI: MinIO+Postgres → lib + integration-e2e; else lib only
#   make setup-local    - Start MinIO + DuckLake Postgres (required for make test)
#   make teardown-local - Stop local test infrastructure
#   make clean          - Clean build artifacts

.PHONY: help test test-all test-local test-smoke test-r2 test-gcs test-ci test-gcp test-gcp-stress test-deployment-local test-deployment-stress stress-test stress-test-r2-ducklake stress-test-gcs-ducklake setup-local teardown-local setup-minio teardown-minio check-minio check-local check-local-postgres clean build lint fmt check-fmt demo-session duckdb-shell generate-telemetry drop-tables

# Gated modules: tests/integration/mod.rs (iceberg, ingest/query, …). DuckDB-heavy performance
# tests must run one cargo process per test to avoid libduckdb SIGSEGV after repeated global-state
# setup/teardown in a single test binary.
INTEGRATION_E2E_FEATURE = --features integration-e2e
INTEGRATION_E2E_TESTS = --test tests
INTEGRATION_E2E_FLAGS = $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS)
INTEGRATION_PERF_TESTS = \
	performance::perf_union_read_concurrency \
	performance::perf_union_read_latency \
	performance::perf_view_recreate_stability

# Some DuckDB-heavy integration::ingest_commit_query tests can trigger process-level instability
# when executed together in one test binary process. Run every ingest_commit_query test in an
# isolated cargo invocation.
INTEGRATION_ISOLATED_TEST_PREFIX = integration::ingest_commit_query::

# Ensure libduckdb is fetched when not present on host.
# Can be overridden by callers: `DUCKDB_DOWNLOAD_LIB=0 make build`
export DUCKDB_DOWNLOAD_LIB ?= 1

# Default target
help:
	@echo "SoftProbe OTLP Backend - Testing & Development"
	@echo ""
	@echo "Test Targets:"
	@echo "  make test            - Unit tests + full local integration (MinIO + Postgres + integration-e2e)"
	@echo "  make test-all        - Same as make test"
	@echo "  make test-local      - Full integration only (MinIO + Postgres + integration-e2e)"
	@echo "  make test-r2         - Integration tests with Cloudflare R2 (+ integration-e2e)"
	@echo "  make test-gcs        - Integration tests with GCS DuckLake data_path (+ integration-e2e)"
	@echo "  make test-ci         - CI: MinIO+Postgres present → test-quick + integration-e2e; else test-quick only"
	@echo "  make test-quick      - Library unit tests + tests/tests.rs (no integration-e2e; no Docker)"
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
	@echo "  make setup-local     - Start MinIO + DuckLake Postgres (required for make test)"
	@echo "  make teardown-local  - Stop docker-compose stack in this directory"
	@echo "  make check-local          - Verify MinIO (required for integration tests)"
	@echo "  make check-local-postgres - Verify DuckLake Postgres (required for make test / test-local)"
	@echo "  make check-local-e2e      - Verify MinIO + Postgres"
	@echo ""
	@echo "Data & Verification:"
	@echo "  make generate-telemetry - Generate demo OTLP data"
	@echo "  make test-all / test-local - Automated ingest + DuckLake coverage (see repo e2e README)"
	@echo "  make demo-session    - Run session query demo"
	@echo "  make duckdb-shell    - Launch DuckDB against local DuckLake (attach smoke runs first)"
	@echo "  make drop-tables     - Drop DuckLake telemetry tables (traces/logs/metrics)"
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

# Official images: GitHub Release vX.Y.Z → .github/workflows/release.yml
# Local/emergency: ./build.sh [vX.Y.Z]
publish-docker:
	@echo "🔨 Publishing Docker image (prefer GitHub Release; this is emergency/local)..."
	./build.sh

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
	@echo "📦 Starting MinIO and DuckLake Postgres..."
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
	@echo "✅ MinIO prerequisites satisfied"

check-local-postgres:
	@echo "🔍 Checking DuckLake Postgres..."
	@docker exec ducklake-postgres pg_isready -U ducklake -d ducklake > /dev/null 2>&1 && echo "✅ DuckLake Postgres is running" || (echo "❌ DuckLake Postgres is not running (run 'make setup-local')" && exit 1)

check-local-e2e: check-local check-local-postgres
	@echo "✅ Local e2e prerequisites satisfied (MinIO + DuckLake Postgres)"

# Test targets
# Single cargo run: #[cfg(test)] in src/ plus the default integration crate tests/tests.rs
# (modules gated behind integration-e2e are skipped unless that feature is enabled).
test-quick:
	@echo "🧪 Running library + lightweight integration tests (no integration-e2e)..."
	cargo test --lib --test tests -- --test-threads=1

test-local: check-local-e2e
	@echo "🧪 Running full integration tests with MinIO + DuckLake Postgres (integration-e2e)..."
	@echo "📝 Configuration: tests/config/test.yaml"
	@echo "🗄️  Backend: MinIO :9000 + Postgres catalog (ducklake-postgres)"
	@echo ""
	@export AWS_ACCESS_KEY_ID=$${AWS_ACCESS_KEY_ID:-minioadmin}; \
	export AWS_SECRET_ACCESS_KEY=$${AWS_SECRET_ACCESS_KEY:-minioadmin}; \
	export AWS_REGION=$${AWS_REGION:-us-east-1}; \
	for test_name in $$(SPLAKE_RESET_DUCKLAKE=1 E2E_BACKEND=local cargo test $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) -- --list 2>/dev/null | rg "^integration::" | awk '{name=$$1; sub(/:$$/, "", name); print name}'); do \
		echo "🧪 Running integration $$test_name in an isolated process..."; \
		SPLAKE_RESET_DUCKLAKE=1 E2E_BACKEND=local cargo test $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) $$test_name -- --test-threads=1 --nocapture || exit $$?; \
	done; \
	for test_name in $(INTEGRATION_PERF_TESTS); do \
		echo "🧪 Running integration_perf $$test_name in an isolated process..."; \
		SPLAKE_RESET_DUCKLAKE=1 E2E_BACKEND=local cargo test $(INTEGRATION_E2E_FEATURE) --test integration_perf $$test_name -- --test-threads=1 --nocapture || exit $$?; \
	done

test-gcs: check-local-e2e
	@echo "🧪 Running integration tests with GCS object store..."
	@echo "📝 Configuration: tests/config/test-gcs.yaml"
	@set -e; \
		: "$${GCS_HMAC_ACCESS_KEY_ID:?Set GCS_HMAC_ACCESS_KEY_ID}"; \
		: "$${GCS_HMAC_SECRET:?Set GCS_HMAC_SECRET}"; \
		GCS_BUCKET=$${GCS_BUCKET:-softprobe-datalake-ducklake}; \
		RUN_ID=$$(date +%Y%m%d-%H%M%S)-$$$$; \
		GCS_E2E_PREFIX="gs://$$GCS_BUCKET/ducklake/e2e/$$RUN_ID/"; \
		echo "☁️  Backend prefix: $$GCS_E2E_PREFIX"; \
		export GCS_BUCKET GCS_HMAC_ACCESS_KEY_ID GCS_HMAC_SECRET GCS_E2E_PREFIX; \
		export AWS_ACCESS_KEY_ID=$${AWS_ACCESS_KEY_ID:-minioadmin}; \
		export AWS_SECRET_ACCESS_KEY=$${AWS_SECRET_ACCESS_KEY:-minioadmin}; \
		export PERF_TARGET_MS=$${PERF_TARGET_MS:-3000}; \
		trap 'echo "🧹 Cleaning GCS prefix $$GCS_E2E_PREFIX"; gcloud storage rm -r "$$GCS_E2E_PREFIX"** >/dev/null 2>&1 || gcloud storage rm -r "$$GCS_E2E_PREFIX" >/dev/null 2>&1 || true' EXIT; \
		for test_name in $$(SPLAKE_RESET_DUCKLAKE=1 E2E_BACKEND=gcs cargo test $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) -- --list 2>/dev/null | rg "^integration::" | awk '{name=$$1; sub(/:$$/, "", name); print name}'); do \
			echo "🧪 Running integration $$test_name in an isolated process..."; \
			SPLAKE_RESET_DUCKLAKE=1 E2E_BACKEND=gcs cargo test $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) $$test_name -- --test-threads=1 --nocapture || exit $$?; \
		done; \
		for test_name in $(INTEGRATION_PERF_TESTS); do \
			echo "🧪 Running integration_perf $$test_name in an isolated process..."; \
			SPLAKE_RESET_DUCKLAKE=1 E2E_BACKEND=gcs cargo test $(INTEGRATION_E2E_FEATURE) --test integration_perf $$test_name -- --test-threads=1 --nocapture || exit $$?; \
		done

test-r2:
	@echo "🧪 Running integration tests with Cloudflare R2..."
	@echo "📝 Configuration: tests/config/test-r2.yaml"
	@echo "☁️  Backend: Cloudflare R2 (S3-compatible DuckLake data_path)"
	@echo "⚠️  Note: Requires AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY and a real R2 endpoint/bucket"
	@echo ""
	@if [ -z "$$E2E_DISABLE_TLS_VALIDATION" ]; then \
		echo "🔒 Detecting environment..."; \
		if curl -sf https://www.google.com > /dev/null 2>&1; then \
			echo "✅ Direct internet access available"; \
			E2E_BACKEND=r2 cargo test $(INTEGRATION_E2E_FLAGS) -- --test-threads=1 --nocapture && \
			for test_name in $(INTEGRATION_PERF_TESTS); do \
				echo "🧪 Running integration_perf $$test_name in an isolated process..."; \
				E2E_BACKEND=r2 cargo test $(INTEGRATION_E2E_FEATURE) --test integration_perf $$test_name -- --test-threads=1 --nocapture || exit $$?; \
			done; \
		else \
			echo "⚠️  Detected restricted/sandboxed environment"; \
			echo "⚠️  Enabling TLS validation bypass for testing"; \
			E2E_DISABLE_TLS_VALIDATION=1 E2E_BACKEND=r2 cargo test $(INTEGRATION_E2E_FLAGS) -- --test-threads=1 --nocapture && \
			for test_name in $(INTEGRATION_PERF_TESTS); do \
				echo "🧪 Running integration_perf $$test_name in an isolated process..."; \
				E2E_DISABLE_TLS_VALIDATION=1 E2E_BACKEND=r2 cargo test $(INTEGRATION_E2E_FEATURE) --test integration_perf $$test_name -- --test-threads=1 --nocapture || exit $$?; \
			done; \
		fi \
	else \
		echo "🔓 TLS validation bypass already enabled"; \
		E2E_BACKEND=r2 cargo test $(INTEGRATION_E2E_FLAGS) -- --test-threads=1 --nocapture && \
		for test_name in $(INTEGRATION_PERF_TESTS); do \
			echo "🧪 Running integration_perf $$test_name in an isolated process..."; \
			E2E_BACKEND=r2 cargo test $(INTEGRATION_E2E_FEATURE) --test integration_perf $$test_name -- --test-threads=1 --nocapture || exit $$?; \
		done; \
	fi

test-ci:
	@echo "🧪 Running tests in CI environment..."
	@echo "🔍 Auto-detecting environment and requirements..."
	@if curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1 \
		&& docker exec ducklake-postgres pg_isready -U ducklake -d ducklake > /dev/null 2>&1; then \
		echo "✅ MinIO + DuckLake Postgres detected"; \
		$(MAKE) test-quick; \
		$(MAKE) test-local; \
	else \
		echo "⚠️  MinIO and/or DuckLake Postgres missing; running test-quick only"; \
		echo "   (pre-merge bar is make test with both services — run make setup-local)"; \
		$(MAKE) test-quick; \
	fi

test-all: test-quick test-local
	@echo "✅ All tests completed!"

# Default pre-merge check: lib + full integration (requires MinIO + DuckLake Postgres).
test: test-all

.PHONY: check-local-e2e

# Development workflow
dev-check: check-fmt lint test-quick
	@echo "✅ Development checks passed!"

# Continuous Integration full check
ci-full: check-fmt lint build test-ci
	@echo "✅ CI checks completed!"

# Data & verification helpers
generate-telemetry:
	@python3 scripts/generate_telemetry.py

demo-session:
	@./scripts/demo_session_queries.sh

duckdb-shell:
	@./scripts/interactive_query.sh

drop-tables:
	@./scripts/drop_all_tables.sh

help-scripts:
	@echo "Script-backed targets:"
	@echo "  make generate-telemetry"
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
		if ! rg -n "^ducklake:\\s*$$" "$$R2_CONFIG" >/dev/null; then \
			echo "❌ $$R2_CONFIG is missing required ducklake: block."; \
			exit 1; \
		fi; \
		R2_BUCKET=$${R2_BUCKET:-$$(rg "^\\s*data_path:\\s*" "$$R2_CONFIG" -m 1 | sed -E 's|.*s3://([^/]+)/.*|\1|' | xargs)}; \
		if [ -z "$$R2_BUCKET" ] || [ "$$R2_BUCKET" = "YOUR-R2-BUCKET" ] || [ "$$R2_BUCKET" = "your-bucket-name" ]; then \
			echo "❌ Could not resolve a real R2 bucket from $$R2_CONFIG."; \
			echo "   Set ducklake.data_path to s3://<bucket>/ducklake/ or pass R2_BUCKET=<real-bucket>."; \
			exit 1; \
		fi; \
		TMP_CONFIG=/tmp/splake-r2-ducklake-stress.yaml; \
		cp $$R2_CONFIG $$TMP_CONFIG; \
		sed -i.bak "s/port: 8090/port: $$PORT/" $$TMP_CONFIG && rm -f $$TMP_CONFIG.bak; \
		sed -i.bak "s|data_path: .*|data_path: \"s3://$$R2_BUCKET/ducklake/\"|" $$TMP_CONFIG && rm -f $$TMP_CONFIG.bak; \
		echo "🚀 Starting splake with $$R2_CONFIG on port $$PORT (bucket $$R2_BUCKET)..."; \
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
		if ! rg -n "^ducklake:\\s*$$" "$$GCP_CONFIG" >/dev/null; then \
			echo "❌ $$GCP_CONFIG is missing required ducklake: block."; \
			exit 1; \
		fi; \
		GCS_BUCKET=$${GCS_BUCKET:-$$(rg "^\\s*data_path:\\s*" "$$GCP_CONFIG" -m 1 | sed -E 's|.*(gs|s3)://([^/]+)/.*|\2|' | xargs)}; \
		if [ -z "$$GCS_BUCKET" ] || [ "$$GCS_BUCKET" = "YOUR-GCS-BUCKET" ] || [ "$$GCS_BUCKET" = "YOUR-GCS-BUCKET-NAME" ] || [ "$$GCS_BUCKET" = "your-bucket-name" ]; then \
			echo "❌ Could not resolve a real GCS bucket from $$GCP_CONFIG."; \
			echo "   Set ducklake.data_path to gs://<bucket>/ducklake/ or pass GCS_BUCKET=<real-bucket>."; \
			exit 1; \
		fi; \
		if [ -z "$$GCS_HMAC_ACCESS_KEY_ID" ] || [ -z "$$GCS_HMAC_SECRET" ]; then \
			echo "❌ GCS_HMAC_ACCESS_KEY_ID and GCS_HMAC_SECRET are required for gs:// DuckLake I/O."; \
			exit 1; \
		fi; \
		TMP_CONFIG=/tmp/splake-gcs-ducklake-stress.yaml; \
		cp "$$GCP_CONFIG" "$$TMP_CONFIG"; \
		sed -i.bak "s/port: 8090/port: $$PORT/" "$$TMP_CONFIG" && rm -f "$$TMP_CONFIG.bak"; \
		rm -rf "$$CACHE_ROOT"; \
		mkdir -p "$$CACHE_ROOT/cache"; \
		sed -i.bak "s|cache_dir: .*|cache_dir: \"$$CACHE_ROOT/cache\"|" "$$TMP_CONFIG" && rm -f "$$TMP_CONFIG.bak"; \
		sed -i.bak "s|data_path: .*|data_path: \"gs://$$GCS_BUCKET/ducklake/\"|" "$$TMP_CONFIG" && rm -f "$$TMP_CONFIG.bak"; \
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
