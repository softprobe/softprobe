# SoftProbe OTLP Backend — Make-only build/test/release
#
# Host-first: cargo builds on the host → dist/; Docker only COPYs dist/.
# Cache: $(HOME)/.cache/thelake/{cargo,target} via ensure-cache (local + CI).
#
# One Cargo profile per gate (never mix debug + release in the same run):
#   make ci / test / test-perf  → default (dev) — PR gate
#   make release                → --release for test-perf + build-release + publish
#                                 (does not re-run make ci; PR already gated that)
#   make build-release          → --release only (packaging)
#
# Warm SLOs (self-hosted): ci ≤15m | test-perf ≤8m | release ≤25m
#
#   make setup && make ci
#   make test-perf
#   make build-release && make package
#   make release TAG=vX.Y.Z

SHELL := /bin/bash

.PHONY: help ensure-cache doctor setup teardown check-infra \
	clean clean-cache build build-release package publish test-publish-tags \
	lint fmt check-fmt \
	test test-e2e test-perf ci release _release \
	stress test-deploy \
	demo-session duckdb-shell duckdb-shell-prod generate-telemetry drop-tables telemetrygen

COMPOSE ?= $(shell command -v docker-compose >/dev/null 2>&1 && echo docker-compose || echo "docker compose")

# ---- cache (same path on Mac and arc-runner; no sudo) ----
# Durable across `make clean` — use `make clean-cache` to wipe intentionally.
# CARGO_HOME here is only the registry/git cache; rustup's cargo/rustc stay in
# ~/.cargo/bin (must remain on PATH — do not replace the toolchain install).
THELAKE_CACHE_ROOT ?= $(HOME)/.cache/thelake
export CARGO_HOME ?= $(THELAKE_CACHE_ROOT)/cargo
export CARGO_TARGET_DIR ?= $(THELAKE_CACHE_ROOT)/target
export PATH := $(CARGO_HOME)/bin:$(HOME)/.cargo/bin:$(PATH)
export DUCKDB_DOWNLOAD_LIB ?= 1

# Override only via `make release` (or explicit CARGO_PROFILE_FLAG=--release).
# Do not auto-flip on CI=true — that forced a second compile after debug clippy.
CARGO_PROFILE_FLAG ?=

ifeq ($(CI),true)
export CARGO_INCREMENTAL ?= 0
endif

# Shared wall-clock SLO check (CI=true or ENFORCE_SLO=1). Usage after TOTAL=…s:
#   $(call enforce-slo,$$total,$(CI_GOAL_SECS),ci)
define enforce-slo
	if [ "$${CI:-}" = "true" ] || [ "$${ENFORCE_SLO:-0}" = "1" ]; then \
		if [ "$(1)" -gt "$(2)" ]; then echo "$(3) exceeded SLO"; exit 1; fi; \
	fi
endef

INTEGRATION_E2E_FEATURE = --features integration-e2e
INTEGRATION_E2E_TESTS = --test tests
INTEGRATION_PERF_TESTS = \
	performance::perf_union_read_concurrency \
	performance::perf_union_read_latency \
	performance::perf_view_recreate_stability

PERF_SUITE ?= all
export PERF_TARGET_MS ?= 1000
export PERF_CONCURRENCY ?= 8
export PERF_EVENTS_PER_SESSION ?= 1000

CI_GOAL_SECS ?= 900
PERF_GOAL_SECS ?= 480
RELEASE_GOAL_SECS ?= 1500

AR_IMAGE ?= us-central1-docker.pkg.dev/cs-poc-sasxbttlzroculpau4u6e2l/softprobe/splake
CACHE_REF ?= $(AR_IMAGE):buildcache
FALLBACK_BUILDER_NAME ?= thelake-builder
DIST_DIR ?= dist
E2E_BACKEND ?= local
BACKEND ?= local
ENV ?= local
TAG ?= latest
TAG_LATEST ?= 1
LINUX_BUILDER_IMAGE ?= rust:1-bookworm

help:
	@echo "SoftProbe OTLP Backend"
	@echo ""
	@echo "Build:    build | build-release | package | publish"
	@echo "Test:     test | test-e2e | test-perf"
	@echo "Gates:    ci | release"
	@echo "Infra:    setup | teardown | doctor"
	@echo "Stress:   stress BACKEND=local|r2|gcs"
	@echo "Extras:   duckdb-shell | demo-session | drop-tables | generate-telemetry | test-deploy | telemetrygen"
	@echo ""
	@echo "Cache:    $(THELAKE_CACHE_ROOT)  (override THELAKE_CACHE_ROOT=...)"
	@echo "          make clean keeps cache; make clean-cache wipes it"
	@echo "E2E:      E2E_BACKEND=local|gcs|r2 make test-e2e"
	@echo "Publish:  make publish TAG=vX.Y.Z [TAG_LATEST=0]"

ensure-cache:
	@mkdir -p "$(CARGO_HOME)" "$(CARGO_TARGET_DIR)"

doctor: ensure-cache
	@set -e; \
	missing=0; \
	for c in cargo rustc docker curl rg clang; do \
		if ! command -v $$c >/dev/null 2>&1; then echo "missing: $$c"; missing=1; fi; \
	done; \
	if ! command -v mold >/dev/null 2>&1; then echo "warning: mold not found (linux link may fail)"; fi; \
	if ! docker info >/dev/null 2>&1; then echo "missing: usable docker"; missing=1; fi; \
	if [ "$$missing" -ne 0 ]; then exit 1; fi; \
	echo "doctor ok (CARGO_TARGET_DIR=$(CARGO_TARGET_DIR))"

# ---- build ----
build: ensure-cache
	cargo build --locked

# Host release → dist/. On non-linux/amd64, TARGET_PLATFORM=linux/amd64 re-enters via docker.
build-release: ensure-cache
	@set -euo pipefail; \
	if [ "$${IN_LINUX_BUILDER:-0}" != "1" ] && { [ "$${TARGET_PLATFORM:-}" = "linux/amd64" ] || [ "$${FORCE_LINUX_BUILDER:-0}" = "1" ]; }; then \
		echo "linux/amd64 via $(LINUX_BUILDER_IMAGE) (same Make recipe)..."; \
		docker run --rm --platform linux/amd64 \
			-v "$(CURDIR):/app" -w /app \
			-e IN_LINUX_BUILDER=1 \
			-e DUCKDB_DOWNLOAD_LIB \
			-e HOME=/tmp \
			-e THELAKE_CACHE_ROOT=/app/.cache-linux \
			-e CI="$(CI)" \
			"$(LINUX_BUILDER_IMAGE)" \
			bash -lc 'apt-get update -qq && apt-get install -y -qq pkg-config libssl-dev protobuf-compiler clang mold cmake build-essential >/dev/null && make build-release'; \
		exit 0; \
	fi; \
	echo "cargo build --release --locked --bin softprobe-runtime..."; \
	cargo build --release --locked --bin softprobe-runtime; \
	mkdir -p "$(DIST_DIR)"; \
	bin="$(CARGO_TARGET_DIR)/release/softprobe-runtime"; \
	test -x "$$bin"; \
	duckdb_so=$$(find "$(CARGO_TARGET_DIR)/duckdb-download" -type f -name 'libduckdb.so*' -print -quit 2>/dev/null || true); \
	if [ -z "$$duckdb_so" ]; then \
		duckdb_so=$$(find "$(CARGO_TARGET_DIR)/duckdb-download" -type f -name 'libduckdb.dylib*' -print -quit 2>/dev/null || true); \
	fi; \
	test -n "$$duckdb_so"; \
	sh scripts/assert-duckdb-version.sh Cargo.lock "$$duckdb_so"; \
	cp -f "$$bin" "$(DIST_DIR)/softprobe-runtime"; \
	rm -f "$(DIST_DIR)/libduckdb.so" "$(DIST_DIR)/libduckdb.dylib"; \
	case "$$duckdb_so" in \
		*.dylib*) cp -f "$$duckdb_so" "$(DIST_DIR)/libduckdb.dylib" ;; \
		*) cp -f "$$duckdb_so" "$(DIST_DIR)/libduckdb.so" ;; \
	esac; \
	cp -f config.yaml "$(DIST_DIR)/config.yaml"; \
	echo "staged $(DIST_DIR)/"

# Internal: ensure dist/ ready for linux image packaging.
_ensure-dist:
	@test -x "$(DIST_DIR)/softprobe-runtime" -a -f "$(DIST_DIR)/config.yaml" || $(MAKE) build-release
	@if [ ! -f "$(DIST_DIR)/libduckdb.so" ]; then \
		echo "dist/ lacks libduckdb.so — TARGET_PLATFORM=linux/amd64 build-release..."; \
		TARGET_PLATFORM=linux/amd64 $(MAKE) build-release; \
	fi

package: _ensure-dist
	docker build --platform linux/amd64 -t softprobe/splake:local .

publish:
	@set -euo pipefail; \
	TAG="$(TAG)"; TAG_LATEST="$(TAG_LATEST)"; \
	tags="$(AR_IMAGE):$$TAG"; \
	case "$$TAG_LATEST" in 0|false|FALSE|no|NO) ;; *) \
		if [ "$$TAG" != "latest" ]; then tags=$$(printf '%s\n%s' "$$tags" "$(AR_IMAGE):latest"); fi ;; \
	esac; \
	if [ "$${PRINT_TAGS:-0}" = "1" ]; then printf '%s\n' "$$tags"; exit 0; fi; \
	if [ "$${PRINT_BUILDX_ARGS:-0}" = "1" ]; then \
		printf '%s\n' --builder "$(FALLBACK_BUILDER_NAME)" --platform linux/amd64 \
			--cache-from "type=registry,ref=$(CACHE_REF),ignore-error=true" \
			--cache-to "type=registry,ref=$(CACHE_REF),mode=max" \
			--push -t "$(AR_IMAGE):$$TAG" .; \
		exit 0; \
	fi; \
	$(MAKE) --no-print-directory _ensure-dist; \
	name=""; driver=""; \
	set +e; \
	name=$$(docker buildx inspect 2>/dev/null | awk '/^Name:/{print $$2; exit}' | tr -d '\r'); \
	driver=$$(docker buildx inspect 2>/dev/null | awk '/^Driver:/{print $$2; exit}' | tr -d '\r'); \
	set -e; \
	builder="$(FALLBACK_BUILDER_NAME)"; \
	if [ "$$driver" = "docker-container" ] || [ "$$driver" = "kubernetes" ] || [ "$$driver" = "remote" ]; then \
		builder="$$name"; \
	elif docker buildx inspect "$(FALLBACK_BUILDER_NAME)" >/dev/null 2>&1; then \
		builder="$(FALLBACK_BUILDER_NAME)"; docker buildx use "$$builder"; \
	elif [ "$${CI:-}" = "true" ]; then \
		echo "error: need docker-container Buildx builder in CI" >&2; exit 1; \
	else \
		docker buildx create --name "$(FALLBACK_BUILDER_NAME)" --driver docker-container --use; \
		builder="$(FALLBACK_BUILDER_NAME)"; \
	fi; \
	tag_args=(); \
	while IFS= read -r t; do [ -n "$$t" ] && tag_args+=(-t "$$t"); done <<< "$$tags"; \
	echo "publishing (builder=$$builder)"; \
	docker buildx build --builder "$$builder" --platform linux/amd64 \
		--cache-from "type=registry,ref=$(CACHE_REF),ignore-error=true" \
		--cache-to "type=registry,ref=$(CACHE_REF),mode=max" \
		--push "$${tag_args[@]}" .

test-publish-tags:
	@set -euo pipefail; \
	out=$$(PRINT_TAGS=1 TAG_LATEST=0 $(MAKE) --no-print-directory publish TAG=v1.2.3-rc.1); \
	echo "$$out" | grep -qx '.*:v1.2.3-rc.1'; \
	! echo "$$out" | grep -q ':latest$$'; \
	out=$$(PRINT_TAGS=1 $(MAKE) --no-print-directory publish TAG=v1.2.3); \
	echo "$$out" | grep -qx '.*:v1.2.3'; \
	echo "$$out" | grep -qx '.*:latest'; \
	out=$$(PRINT_TAGS=1 $(MAKE) --no-print-directory publish TAG=latest); \
	echo "$$out" | grep -qx '.*:latest'; \
	test "$$(echo "$$out" | wc -l | tr -d ' ')" = "1"; \
	args=$$(PRINT_BUILDX_ARGS=1 $(MAKE) --no-print-directory publish TAG=v1.2.3); \
	echo "$$args" | grep -Fq -- '--builder'; \
	echo "$$args" | grep -Fq -- '--platform'; \
	echo "$$args" | grep -Fq -- 'linux/amd64'; \
	echo "publish tag plan ok"

lint: ensure-cache
	cargo clippy $(CARGO_PROFILE_FLAG) --lib --bin softprobe-runtime -- -D warnings

fmt:
	cargo fmt

check-fmt:
	cargo fmt -- --check

# Workspace artifacts only — does not wipe ~/.cache/thelake (see clean-cache).
clean:
	rm -rf "$(DIST_DIR)" .cache-linux/

clean-cache:
	@echo "wiping $(THELAKE_CACHE_ROOT)..."
	rm -rf "$(THELAKE_CACHE_ROOT)"
	@$(MAKE) --no-print-directory ensure-cache

# ---- infra ----
setup:
	@echo "starting MinIO + DuckLake Postgres..."
	@$(COMPOSE) up -d minio ducklake-postgres
	@sleep 5
	@curl -sf http://localhost:9000/minio/health/live > /dev/null || (echo "MinIO not ready" && exit 1)
	@$(MAKE) --no-print-directory _minio-bucket
	@docker exec ducklake-postgres pg_isready -U ducklake -d ducklake > /dev/null 2>&1 || (echo "Postgres not ready" && exit 1)
	@echo "setup ok"

teardown:
	@$(COMPOSE) down
	@echo "teardown ok"

_minio-bucket:
	@docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1 || true
	@docker exec minio mc mb local/warehouse > /dev/null 2>&1 || \
		(docker exec minio mc ls local/warehouse > /dev/null 2>&1 && true) || \
		(echo "failed to create warehouse bucket" && exit 1)

# Internal: stress local EXIT trap — stop MinIO only (keep Postgres for other work).
_teardown-minio:
	@$(COMPOSE) stop minio > /dev/null 2>&1 || true
	@$(COMPOSE) rm -f minio > /dev/null 2>&1 || true

check-infra:
	@curl -sf http://localhost:9000/minio/health/live > /dev/null || (echo "MinIO down — make setup" && exit 1)
	@docker exec ducklake-postgres pg_isready -U ducklake -d ducklake > /dev/null 2>&1 || (echo "Postgres down — make setup" && exit 1)
	@echo "check-infra ok"

# Internal: MinIO AWS env for isolated e2e/perf.
_export-minio-aws = \
	export AWS_ACCESS_KEY_ID=$${AWS_ACCESS_KEY_ID:-minioadmin}; \
	export AWS_SECRET_ACCESS_KEY=$${AWS_SECRET_ACCESS_KEY:-minioadmin}; \
	export AWS_REGION=$${AWS_REGION:-us-east-1}

# ---- tests ----
test: ensure-cache
	@echo "unit + lightweight tests (no e2e infra)..."
	cargo test $(CARGO_PROFILE_FLAG) --lib --test tests -- --test-threads=1

test-e2e: ensure-cache check-infra
	@set -e; \
	backend="$(E2E_BACKEND)"; \
	echo "integration-e2e E2E_BACKEND=$$backend..."; \
	$(_export-minio-aws); \
	export SPLAKE_RESET_DUCKLAKE=1 E2E_BACKEND=$$backend; \
	case "$$backend" in \
		local) \
			./scripts/run-isolated-cargo-tests.sh $(CARGO_PROFILE_FLAG) $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) --list-prefix integration:: ;; \
		gcs) \
			: "$${GCS_HMAC_ACCESS_KEY_ID:?Set GCS_HMAC_ACCESS_KEY_ID}"; \
			: "$${GCS_HMAC_SECRET:?Set GCS_HMAC_SECRET}"; \
			GCS_BUCKET=$${GCS_BUCKET:-softprobe-datalake-ducklake}; \
			RUN_ID=$$(date +%Y%m%d-%H%M%S)-$$$$; \
			export GCS_BUCKET GCS_HMAC_ACCESS_KEY_ID GCS_HMAC_SECRET; \
			export GCS_E2E_PREFIX="gs://$$GCS_BUCKET/ducklake/e2e/$$RUN_ID/"; \
			echo "GCS prefix $$GCS_E2E_PREFIX"; \
			trap 'gcloud storage rm -r "$$GCS_E2E_PREFIX"** >/dev/null 2>&1 || gcloud storage rm -r "$$GCS_E2E_PREFIX" >/dev/null 2>&1 || true' EXIT; \
			./scripts/run-isolated-cargo-tests.sh $(CARGO_PROFILE_FLAG) $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) --list-prefix integration:: ;; \
		r2) \
			if [ -z "$${E2E_DISABLE_TLS_VALIDATION:-}" ] && ! curl -sf https://www.google.com >/dev/null 2>&1; then \
				export E2E_DISABLE_TLS_VALIDATION=1; \
			fi; \
			./scripts/run-isolated-cargo-tests.sh $(CARGO_PROFILE_FLAG) $(INTEGRATION_E2E_FEATURE) $(INTEGRATION_E2E_TESTS) --list-prefix integration:: ;; \
		*) echo "unknown E2E_BACKEND=$$backend (local|gcs|r2)"; exit 1 ;; \
	esac

test-perf: ensure-cache check-infra
	@set -e; \
	t0=$$(date +%s); \
	case "$(PERF_SUITE)" in \
		all) tests="$(INTEGRATION_PERF_TESTS)" ;; \
		latency) tests="performance::perf_union_read_latency" ;; \
		concurrency) tests="performance::perf_union_read_concurrency" ;; \
		stability) tests="performance::perf_view_recreate_stability" ;; \
		*) echo "unknown PERF_SUITE=$(PERF_SUITE)"; exit 1 ;; \
	esac; \
	$(_export-minio-aws); \
	export SPLAKE_RESET_DUCKLAKE=1 E2E_BACKEND=local; \
	./scripts/run-isolated-cargo-tests.sh $(CARGO_PROFILE_FLAG) $(INTEGRATION_E2E_FEATURE) --test integration_perf --tests $$tests; \
	total=$$(($$(date +%s) - t0)); \
	echo "TOTAL=$${total}s goal=$(PERF_GOAL_SECS)s (test-perf)"; \
	$(call enforce-slo,$$total,$(PERF_GOAL_SECS),test-perf)

ci: ensure-cache
	@set -e; \
	t0=$$(date +%s); \
	echo "PHASE=check-fmt start"; s=$$(date +%s); $(MAKE) check-fmt; echo "PHASE=check-fmt elapsed=$$(($$(date +%s) - $$s))s"; \
	echo "PHASE=lint start"; s=$$(date +%s); $(MAKE) lint; echo "PHASE=lint elapsed=$$(($$(date +%s) - $$s))s"; \
	echo "PHASE=test start"; s=$$(date +%s); $(MAKE) test; echo "PHASE=test elapsed=$$(($$(date +%s) - $$s))s"; \
	if curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1 \
		&& docker exec ducklake-postgres pg_isready -U ducklake -d ducklake >/dev/null 2>&1; then \
		echo "PHASE=test-e2e start"; s=$$(date +%s); $(MAKE) test-e2e; echo "PHASE=test-e2e elapsed=$$(($$(date +%s) - $$s))s"; \
	else \
		if [ "$${CI:-}" = "true" ]; then echo "infra missing in CI — run make setup"; exit 1; fi; \
		echo "infra missing; skipped test-e2e (local). Pre-merge: make setup && make ci"; \
	fi; \
	total=$$(($$(date +%s) - t0)); \
	echo "TOTAL=$${total}s goal=$(CI_GOAL_SECS)s (ci)"; \
	$(call enforce-slo,$$total,$(CI_GOAL_SECS),ci); \
	echo "ci ok"

# Release gate: PR already ran make ci (dev). Here one --release profile for
# perf + binary + push — do not nest make ci (release compile blew the 900s ci SLO).
release:
	@$(MAKE) CARGO_PROFILE_FLAG=--release _release TAG="$(TAG)" TAG_LATEST="$(TAG_LATEST)"

_release:
	@set -e; \
	t0=$$(date +%s); \
	echo "PHASE=test-perf start (profile=$(or $(CARGO_PROFILE_FLAG),dev))"; s=$$(date +%s); $(MAKE) test-perf; echo "PHASE=test-perf elapsed=$$(($$(date +%s) - $$s))s"; \
	echo "PHASE=build-release start"; s=$$(date +%s); $(MAKE) build-release; echo "PHASE=build-release elapsed=$$(($$(date +%s) - $$s))s"; \
	echo "PHASE=publish start"; s=$$(date +%s); \
	$(MAKE) publish TAG="$(TAG)" TAG_LATEST="$(TAG_LATEST)"; \
	echo "PHASE=publish elapsed=$$(($$(date +%s) - $$s))s"; \
	total=$$(($$(date +%s) - t0)); \
	echo "TOTAL=$${total}s goal=$(RELEASE_GOAL_SECS)s (release)"; \
	$(call enforce-slo,$$total,$(RELEASE_GOAL_SECS),release); \
	echo "release ok"

# ---- stress / deploy / extras ----
stress:
	BACKEND=$(BACKEND) ./scripts/stress-test.sh

test-deploy:
	@command -v python3 >/dev/null || (echo "python3 required" && exit 1)
	@python3 -c "import requests" 2>/dev/null || pip3 install --user requests || pip3 install requests
	@set -e; \
	env="$(ENV)"; \
	extra=""; \
	if [ "$${STRESS:-0}" = "1" ]; then \
		if [ "$$env" = "gcp" ]; then extra="--span-count 10000 --session-count 100"; \
		else extra="--span-count 20000"; fi; \
	elif [ -n "$${SPAN_COUNT:-}" ]; then extra="--span-count $$SPAN_COUNT"; fi; \
	python3 test_deployment.py --env "$$env" $$extra

generate-telemetry:
	@python3 scripts/generate_telemetry.py

demo-session:
	@./scripts/demo_session_queries.sh

duckdb-shell:
	@./scripts/interactive_query.sh

duckdb-shell-prod:
	@./scripts/interactive_query_ducklake_production.sh

drop-tables:
	@./scripts/drop_all_tables.sh

telemetrygen:
	@./scripts/telemetrygen_hosted.sh
