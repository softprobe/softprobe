# SoftProbe OTLP Backend - Test & Development Makefile
#
# Canonical product bits: make build-release → dist/ (cargo-chef + --release --locked).
# Docker/image packaging never compiles Rust — it COPY's dist/ only.
#
# Timing SLOs (warm self-hosted Linux):
#   ci-full  ≤ 15m (900s)   workflow timeout 20m
#   test-perf ≤ 8m (480s)   workflow timeout 15m
#   release  ≤ 25m (1500s)  workflow timeout 35m
#
# Usage:
#   make setup-local && make ci-full
#   make test-perf
#   make build-release && make package-image
#   make release   # ci-full + test-perf + publish-docker

.PHONY: help test test-all test-local test-smoke test-quick test-r2 test-gcs test-ci test-perf \
	test-gcp test-gcp-stress test-deployment-local test-deployment-stress \
	stress-test stress-test-r2-ducklake stress-test-gcs-ducklake \
	setup-local teardown-local setup-minio teardown-minio check-minio check-local check-local-postgres check-local-e2e \
	clean build build-release package-image publish-docker test-publish-tags \
	lint fmt check-fmt demo-session duckdb-shell generate-telemetry drop-tables \
	ci-full release doctor-ci help-scripts ensure-python-requests

COMPOSE ?= $(shell command -v docker-compose >/dev/null 2>&1 && echo docker-compose || echo "docker compose")

INTEGRATION_E2E_FEATURE = --features integration-e2e
INTEGRATION_E2E_TESTS = --test tests
INTEGRATION_PERF_TESTS = \
	performance::perf_union_read_concurrency \
	performance::perf_union_read_latency \
	performance::perf_view_recreate_stability

PERF_SUITE ?= all
# Performance quality bar (do not raise to hide regressions).
export PERF_TARGET_MS ?= 1000
export PERF_CONCURRENCY ?= 8
export PERF_EVENTS_PER_SESSION ?= 1000

# Wall-clock SLO goals (seconds). Enforced when CI=true or ENFORCE_SLO=1.
CI_GOAL_SECS ?= 900
PERF_GOAL_SECS ?= 480
RELEASE_GOAL_SECS ?= 1500

export DUCKDB_DOWNLOAD_LIB ?= 1

# ---- timing helpers ----
# Usage: $(call phase,name,command...)
define phase
	@start=$$(date +%s); \
	echo "PHASE=$(1) start"; \
	$(2); \
	end=$$(date +%s); \
	elapsed=$$((end - start)); \
	echo "PHASE=$(1) elapsed=$${elapsed}s"
endef

define enforce_slo
	@total=$(1); goal=$(2); label=$(3); \
	echo "TOTAL=$${total}s goal=$${goal}s ($${label})"; \
	if [ "$${CI:-}" = "true" ] || [ "$${ENFORCE_SLO:-0}" = "1" ]; then \
		if [ "$${total}" -gt "$${goal}" ]; then \
			echo "❌ $${label} wall clock $${total}s exceeds $${goal}s goal"; \
			exit 1; \
		fi; \
	fi
endef

help:
	@echo "SoftProbe OTLP Backend - Testing & Development"
	@echo ""
	@echo "Build (host-first; image never runs cargo):"
	@echo "  make build            - Debug build (day-to-day)"
	@echo "  make build-release    - cargo-chef + release --locked → dist/"
	@echo "  make package-image    - docker build from dist/ (local load)"
	@echo "  make publish-docker   - build-release if needed + push (TAG= TAG_LATEST=)"
	@echo "  make release          - ci-full + test-perf + publish-docker"
	@echo ""
	@echo "Test Targets:"
	@echo "  make test / test-all  - test-quick + test-local (no perf)"
	@echo "  make test-smoke       - Alias of test-quick"
	@echo "  make test-local       - Isolated integration-e2e (MinIO + Postgres)"
	@echo "  make test-perf        - Performance suite only (PERF_SUITE=all|latency|concurrency|stability)"
	@echo "  make test-ci          - CI tests (fails if infra missing when CI=true)"
	@echo "  make ci-full          - fmt + lint + build-release + test-ci (≤15m warm SLO)"
	@echo ""
	@echo "Infrastructure: setup-local / teardown-local / doctor-ci"
	@echo "Stress: make stress-test BACKEND=local|r2|gcs  (aliases: stress-test-r2-ducklake, …)"
	@echo ""

doctor-ci:
	@set -e; \
	missing=0; \
	for c in cargo rustc docker curl rg clang; do \
		if ! command -v $$c >/dev/null 2>&1; then echo "❌ missing: $$c"; missing=1; fi; \
	done; \
	if ! command -v mold >/dev/null 2>&1; then echo "⚠️  mold not found (linux rustflags may fail)"; fi; \
	if ! docker info >/dev/null 2>&1; then echo "❌ docker not usable"; missing=1; fi; \
	if [ "$$missing" -ne 0 ]; then exit 1; fi; \
	echo "✅ doctor-ci ok"

# Build targets
build:
	@echo "🔨 Building debug..."
	cargo build --locked

build-release:
	@echo "🔨 Building release → dist/ (cargo-chef + --locked)..."
	./scripts/build-release.sh

ensure-dist:
	@test -x dist/softprobe-runtime -a -f dist/config.yaml || $(MAKE) build-release
	@if [ ! -f dist/libduckdb.so ]; then \
		echo "📦 dist/ lacks libduckdb.so — rebuilding for linux/amd64 (image packaging)..."; \
		TARGET_PLATFORM=linux/amd64 $(MAKE) build-release; \
	fi

package-image: ensure-dist
	@echo "🐳 Packaging image from dist/ (no cargo in Docker)..."
	docker build -t softprobe/splake:local .

# Official images: GitHub Release → release.yml → make release / publish-docker.
publish-docker: ensure-dist
	@echo "🔨 Publishing Docker image TAG=$(or $(TAG),latest) TAG_LATEST=$(or $(TAG_LATEST),1)..."
	TAG_LATEST=$(or $(TAG_LATEST),1) ./build.sh $(or $(TAG),latest)

test-publish-tags:
	@set -euo pipefail; \
	out=$$(PRINT_TAGS=1 TAG_LATEST=0 ./build.sh v1.2.3-rc.1); \
	echo "$$out" | grep -qx '.*:v1.2.3-rc.1'; \
	! echo "$$out" | grep -q ':latest$$'; \
	out=$$(PRINT_TAGS=1 ./build.sh v1.2.3); \
	echo "$$out" | grep -qx '.*:v1.2.3'; \
	echo "$$out" | grep -qx '.*:latest'; \
	out=$$(PRINT_TAGS=1 ./build.sh latest); \
	echo "$$out" | grep -qx '.*:latest'; \
	test "$$(echo "$$out" | wc -l | tr -d ' ')" = "1"; \
	args=$$(PRINT_BUILDX_ARGS=1 ./build.sh v1.2.3); \
	echo "$$args" | grep -Fxq -- '--builder'; \
	echo "$$args" | grep -Fxq -- 'thelake-builder'; \
	echo "$$args" | grep -Fxq -- '--cache-from'; \
	echo "$$args" | grep -Fxq -- 'type=registry,ref=us-central1-docker.pkg.dev/cs-poc-sasxbttlzroculpau4u6e2l/softprobe/splake:buildcache,ignore-error=true'; \
	echo "$$args" | grep -Fxq -- '--cache-to'; \
	echo "$$args" | grep -Fxq -- 'type=registry,ref=us-central1-docker.pkg.dev/cs-poc-sasxbttlzroculpau4u6e2l/softprobe/splake:buildcache,mode=max'; \
	echo "✅ publish tag plan ok"

lint:
	@echo "🔍 Running clippy..."
	cargo clippy --lib --bin softprobe-runtime -- -D warnings

fmt:
	cargo fmt

check-fmt:
	cargo fmt -- --check

clean:
	cargo clean
	rm -rf target/ dist/ recipe.json .cargo-linux/

# Local infrastructure
setup-local:
	@echo "🚀 Starting MinIO + DuckLake Postgres..."
	@$(COMPOSE) up -d minio ducklake-postgres
	@sleep 5
	@curl -sf http://localhost:9000/minio/health/live > /dev/null || (echo "❌ MinIO not ready" && exit 1)
	@$(MAKE) --no-print-directory _minio-bucket
	@docker exec ducklake-postgres pg_isready -U ducklake -d ducklake > /dev/null 2>&1 || (echo "❌ DuckLake Postgres not ready" && exit 1)
	@echo "✅ Local test infrastructure is ready"

teardown-local:
	@$(COMPOSE) down
	@echo "✅ Local infrastructure stopped"

_minio-bucket:
	@docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1 || true
	@docker exec minio mc mb local/warehouse > /dev/null 2>&1 || \
		(docker exec minio mc ls local/warehouse > /dev/null 2>&1 && echo "✅ Bucket 'warehouse' already exists") || \
		(echo "❌ Failed to create or verify bucket 'warehouse'" && exit 1)

setup-minio:
	@$(COMPOSE) up -d minio
	@sleep 3
	@curl -sf http://localhost:9000/minio/health/live > /dev/null || (echo "❌ MinIO not ready" && exit 1)
	@$(MAKE) --no-print-directory _minio-bucket
	@echo "✅ MinIO is ready"

teardown-minio:
	@$(COMPOSE) stop minio > /dev/null 2>&1 || true
	@$(COMPOSE) rm -f minio > /dev/null 2>&1 || true

check-minio:
	@curl -sf http://localhost:9000/minio/health/live > /dev/null && echo "✅ MinIO is running" || (echo "❌ MinIO is not running (run 'make setup-local')" && exit 1)

check-local: check-minio

check-local-postgres:
	@docker exec ducklake-postgres pg_isready -U ducklake -d ducklake > /dev/null 2>&1 && echo "✅ DuckLake Postgres is running" || (echo "❌ DuckLake Postgres is not running (run 'make setup-local')" && exit 1)

check-local-e2e: check-local check-local-postgres
	@echo "✅ Local e2e prerequisites satisfied (MinIO + DuckLake Postgres)"

# ---- tests ----
test-quick:
	@echo "🧪 Library + lightweight tests (no integration-e2e)..."
	cargo test --lib --test tests -- --test-threads=1

test-smoke: test-quick

test-local: check-local-e2e
	@echo "🧪 Isolated integration-e2e (no perf — use make test-perf)..."
	@export AWS_ACCESS_KEY_ID=$${AWS_ACCESS_KEY_ID:-minioadmin}; \
	export AWS_SECRET_ACCESS_KEY=$${AWS_SECRET_ACCESS_KEY:-minioadmin}; \
	export AWS_REGION=$${AWS_REGION:-us-east-1}; \
	export SPLAKE_RESET_DUCKLAKE=1 E2E_BACKEND=local; \
	./scripts/run-isolated-cargo-tests.sh $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) --list-prefix integration::

test-gcs: check-local-e2e
	@echo "🧪 Isolated integration-e2e on GCS..."
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
		export SPLAKE_RESET_DUCKLAKE=1 E2E_BACKEND=gcs; \
		trap 'echo "🧹 Cleaning GCS prefix $$GCS_E2E_PREFIX"; gcloud storage rm -r "$$GCS_E2E_PREFIX"** >/dev/null 2>&1 || gcloud storage rm -r "$$GCS_E2E_PREFIX" >/dev/null 2>&1 || true' EXIT; \
		./scripts/run-isolated-cargo-tests.sh $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) --list-prefix integration::

test-r2:
	@echo "🧪 Isolated integration-e2e on R2..."
	@set -e; \
	export E2E_BACKEND=r2; \
	if [ -z "$${E2E_DISABLE_TLS_VALIDATION:-}" ]; then \
		if ! curl -sf https://www.google.com > /dev/null 2>&1; then \
			export E2E_DISABLE_TLS_VALIDATION=1; \
			echo "⚠️  Enabling TLS validation bypass"; \
		fi; \
	fi; \
	./scripts/run-isolated-cargo-tests.sh $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) --list-prefix integration::

test-ci:
	@echo "🧪 Running tests (CI=$${CI:-false})..."
	@if curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1 \
		&& docker exec ducklake-postgres pg_isready -U ducklake -d ducklake > /dev/null 2>&1; then \
		echo "✅ MinIO + DuckLake Postgres detected"; \
		$(MAKE) test-quick; \
		$(MAKE) test-local; \
	else \
		if [ "$${CI:-}" = "true" ]; then \
			echo "❌ MinIO and/or DuckLake Postgres missing in CI — run make setup-local first"; \
			exit 1; \
		fi; \
		echo "⚠️  Infra missing; running test-quick only (local). Pre-merge bar: make setup-local && make test"; \
		$(MAKE) test-quick; \
	fi

test-perf: check-local-e2e
	@echo "🧪 Performance suite PERF_SUITE=$(PERF_SUITE) PERF_TARGET_MS=$${PERF_TARGET_MS}..."
	@set -e; \
	t0=$$(date +%s); \
	case "$(PERF_SUITE)" in \
		all) tests="$(INTEGRATION_PERF_TESTS)" ;; \
		latency) tests="performance::perf_union_read_latency" ;; \
		concurrency) tests="performance::perf_union_read_concurrency" ;; \
		stability) tests="performance::perf_view_recreate_stability" ;; \
		*) echo "❌ Unknown PERF_SUITE=$(PERF_SUITE)"; exit 1 ;; \
	esac; \
	export AWS_ACCESS_KEY_ID=$${AWS_ACCESS_KEY_ID:-minioadmin}; \
	export AWS_SECRET_ACCESS_KEY=$${AWS_SECRET_ACCESS_KEY:-minioadmin}; \
	export AWS_REGION=$${AWS_REGION:-us-east-1}; \
	export SPLAKE_RESET_DUCKLAKE=1 E2E_BACKEND=local; \
	./scripts/run-isolated-cargo-tests.sh $(INTEGRATION_E2E_FEATURE) --test integration_perf --tests $$tests; \
	t1=$$(date +%s); \
	total=$$((t1 - t0)); \
	echo "TOTAL=$${total}s goal=$(PERF_GOAL_SECS)s (test-perf)"; \
	if [ "$${CI:-}" = "true" ] || [ "$${ENFORCE_SLO:-0}" = "1" ]; then \
		if [ "$${total}" -gt "$(PERF_GOAL_SECS)" ]; then \
			echo "❌ test-perf wall clock $${total}s exceeds $(PERF_GOAL_SECS)s goal"; \
			exit 1; \
		fi; \
	fi; \
	echo "✅ Performance tests completed!"

test-all: test-quick test-local
	@echo "✅ All tests completed!"

test: test-all

dev-check: check-fmt lint test-quick
	@echo "✅ Development checks passed!"

# Continuous Integration: release bits + tests (no perf).
ci-full:
	@set -e; \
	t0=$$(date +%s); \
	echo "PHASE=check-fmt start"; s=$$(date +%s); $(MAKE) check-fmt; echo "PHASE=check-fmt elapsed=$$(($$(date +%s)-s))s"; \
	echo "PHASE=lint start"; s=$$(date +%s); $(MAKE) lint; echo "PHASE=lint elapsed=$$(($$(date +%s)-s))s"; \
	echo "PHASE=build-release start"; s=$$(date +%s); $(MAKE) build-release; echo "PHASE=build-release elapsed=$$(($$(date +%s)-s))s"; \
	echo "PHASE=test-ci start"; s=$$(date +%s); $(MAKE) test-ci; echo "PHASE=test-ci elapsed=$$(($$(date +%s)-s))s"; \
	total=$$(($$(date +%s) - t0)); \
	echo "TOTAL=$${total}s goal=$(CI_GOAL_SECS)s (ci-full)"; \
	if [ "$${CI:-}" = "true" ] || [ "$${ENFORCE_SLO:-0}" = "1" ]; then \
		if [ "$${total}" -gt "$(CI_GOAL_SECS)" ]; then \
			echo "❌ ci-full wall clock $${total}s exceeds $(CI_GOAL_SECS)s goal"; \
			exit 1; \
		fi; \
	fi; \
	echo "✅ CI checks completed!"

# Release gate: same Make targets as CI + perf + publish (one job, no second compile).
# Pass TAG= / TAG_LATEST= through to publish-docker.
release:
	@set -e; \
	t0=$$(date +%s); \
	$(MAKE) ci-full; \
	echo "PHASE=test-perf start"; s=$$(date +%s); $(MAKE) test-perf; echo "PHASE=test-perf elapsed=$$(($$(date +%s)-s))s"; \
	echo "PHASE=publish-docker start"; s=$$(date +%s); \
	$(MAKE) publish-docker TAG="$(or $(TAG),latest)" TAG_LATEST="$(or $(TAG_LATEST),1)"; \
	echo "PHASE=publish-docker elapsed=$$(($$(date +%s)-s))s"; \
	total=$$(($$(date +%s) - t0)); \
	echo "TOTAL=$${total}s goal=$(RELEASE_GOAL_SECS)s (release)"; \
	if [ "$${CI:-}" = "true" ] || [ "$${ENFORCE_SLO:-0}" = "1" ]; then \
		if [ "$${total}" -gt "$(RELEASE_GOAL_SECS)" ]; then \
			echo "❌ release wall clock $${total}s exceeds $(RELEASE_GOAL_SECS)s goal"; \
			exit 1; \
		fi; \
	fi; \
	echo "✅ release completed!"

generate-telemetry:
	@python3 scripts/generate_telemetry.py

demo-session:
	@./scripts/demo_session_queries.sh

duckdb-shell:
	@./scripts/interactive_query.sh

drop-tables:
	@./scripts/drop_all_tables.sh

help-scripts:
	@echo "Script-backed: generate-telemetry demo-session duckdb-shell drop-tables stress-test build-release"

ensure-python-requests:
	@command -v python3 >/dev/null 2>&1 || (echo "❌ Python 3 required" && exit 1)
	@python3 -c "import requests" 2>/dev/null || pip3 install --user requests || pip3 install requests

test-gcp: ensure-python-requests
	@python3 test_deployment.py --env gcp

test-gcp-stress: ensure-python-requests
	@python3 test_deployment.py --env gcp --span-count 10000 --session-count 100

test-deployment-local: check-local ensure-python-requests
	@python3 test_deployment.py --env local

test-deployment-stress: check-local ensure-python-requests
	@python3 test_deployment.py --env local --span-count 20000

# Unified stress (BACKEND=local|r2|gcs). Legacy aliases keep old Make target names.
stress-test:
	BACKEND=$(or $(BACKEND),local) ./scripts/stress-test.sh

stress-test-r2-ducklake:
	BACKEND=r2 ./scripts/stress-test.sh

stress-test-gcs-ducklake:
	BACKEND=gcs ./scripts/stress-test.sh
