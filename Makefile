# SoftProbe OTLP Backend - Test & Development Makefile
#
# Canonical product bits: make build-release → dist/ (--release --locked; optional USE_CARGO_CHEF=1).
# Docker/image packaging never compiles Rust — it COPY's dist/ only.
#
# Timing SLOs (warm self-hosted Linux):
#   ci-full  ≤ 15m (900s)   workflow hard timeout 45m (cold DuckDB headroom)
#   test-perf ≤ 8m (480s)   workflow timeout 15m
#   release  ≤ 25m (1500s)  workflow timeout 35m
#
# PR gate (ci-full): fmt + lint + tests. When CI=true, tests use --release (one profile).
# Release gate: ci-full + test-perf + build-release → dist/ + publish-docker.
#
# Usage:
#   make setup-local && make ci-full
#   make test-perf
#   make build-release && make package-image
#   make release   # ci-full + test-perf + build-release + publish-docker

.PHONY: help test test-all test-local test-smoke test-quick test-r2 test-gcs test-ci test-perf \
	test-gcp test-gcp-stress test-deployment-local test-deployment-stress \
	stress-test stress-test-r2-ducklake stress-test-gcs-ducklake \
	setup-local teardown-local setup-minio teardown-minio check-minio check-local check-local-postgres check-local-e2e \
	clean build build-release ensure-dist package-image publish-docker test-publish-tags \
	lint fmt check-fmt demo-session duckdb-shell generate-telemetry drop-tables \
	ci-full release doctor-ci help-scripts ensure-python-requests

COMPOSE ?= $(shell command -v docker-compose >/dev/null 2>&1 && echo docker-compose || echo "docker compose")

INTEGRATION_E2E_FEATURE = --features integration-e2e
INTEGRATION_E2E_TESTS = --test tests
# In CI, use --release so test harness shares the build-release profile (one DuckDB compile).
ifeq ($(CI),true)
CARGO_PROFILE_FLAG = --release
else
CARGO_PROFILE_FLAG =
endif
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

help:
	@echo "SoftProbe OTLP Backend - Testing & Development"
	@echo ""
	@echo "Build (host-first; image never runs cargo):"
	@echo "  make build            - Debug build (day-to-day)"
	@echo "  make build-release    - cargo-chef + release --locked → dist/"
	@echo "  make package-image    - docker build from dist/ (local load)"
	@echo "  make publish-docker   - build-release if needed + push (TAG= TAG_LATEST=)"
	@echo "  make release          - ci-full + test-perf + build-release + publish-docker"
	@echo ""
	@echo "Test Targets:"
	@echo "  make test / test-all  - test-quick + test-local (no perf)"
	@echo "  make test-smoke       - Alias of test-quick"
	@echo "  make test-local       - Isolated integration-e2e (MinIO + Postgres)"
	@echo "  make test-perf        - Performance suite only (PERF_SUITE=all|latency|concurrency|stability)"
	@echo "  make test-ci          - CI tests (fails if infra missing when CI=true)"
	@echo "  make ci-full          - fmt + lint + test-ci only (≤15m warm SLO; no release build)"
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
	@echo "🐳 Packaging image from dist/ (no cargo in Docker; linux/amd64)..."
	docker build --platform linux/amd64 -t softprobe/splake:local .

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
	cargo test $(CARGO_PROFILE_FLAG) --lib --test tests -- --test-threads=1

test-smoke: test-quick

test-local: check-local-e2e
	@echo "🧪 Isolated integration-e2e (no perf — use make test-perf)..."
	@export AWS_ACCESS_KEY_ID=$${AWS_ACCESS_KEY_ID:-minioadmin}; \
	export AWS_SECRET_ACCESS_KEY=$${AWS_SECRET_ACCESS_KEY:-minioadmin}; \
	export AWS_REGION=$${AWS_REGION:-us-east-1}; \
	export SPLAKE_RESET_DUCKLAKE=1 E2E_BACKEND=local; \
	./scripts/run-isolated-cargo-tests.sh $(CARGO_PROFILE_FLAG) $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) --list-prefix integration::

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
	./scripts/run-isolated-cargo-tests.sh $(CARGO_PROFILE_FLAG) $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) --list-prefix integration::

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
	./scripts/run-isolated-cargo-tests.sh $(CARGO_PROFILE_FLAG) $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) --list-prefix integration::

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
	./scripts/run-isolated-cargo-tests.sh $(CARGO_PROFILE_FLAG) $(INTEGRATION_E2E_FEATURE) --test integration_perf --tests $$tests; \
	./scripts/slo.sh total test-perf $$(($$(date +%s) - t0)) $(PERF_GOAL_SECS); \
	echo "✅ Performance tests completed!"

test-all: test-quick test-local
	@echo "✅ All tests completed!"

test: test-all

dev-check: check-fmt lint test-quick
	@echo "✅ Development checks passed!"

# PR / main gate: fmt + lint + tests (CI=true → --release profile).
# Product dist/ is built in `make release` (same host path as local publish).
ci-full:
	@set -e; \
	t0=$$(date +%s); \
	./scripts/slo.sh phase check-fmt -- $(MAKE) check-fmt; \
	./scripts/slo.sh phase lint -- $(MAKE) lint; \
	./scripts/slo.sh phase test-ci -- $(MAKE) test-ci; \
	./scripts/slo.sh total ci-full $$(($$(date +%s) - t0)) $(CI_GOAL_SECS); \
	echo "✅ CI checks completed!"

# Release gate: PR checks + perf + unconditional host build-release + image push
# (Docker never compiles). Always rebuild dist/ so self-hosted leftovers cannot
# be published. Pass TAG= / TAG_LATEST= through to publish-docker.
release:
	@set -e; \
	t0=$$(date +%s); \
	./scripts/slo.sh phase ci-full -- $(MAKE) ci-full; \
	./scripts/slo.sh phase test-perf -- $(MAKE) test-perf; \
	./scripts/slo.sh phase build-release -- $(MAKE) build-release; \
	./scripts/slo.sh phase publish-docker -- $(MAKE) publish-docker TAG="$(or $(TAG),latest)" TAG_LATEST="$(or $(TAG_LATEST),1)"; \
	./scripts/slo.sh total release $$(($$(date +%s) - t0)) $(RELEASE_GOAL_SECS); \
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
