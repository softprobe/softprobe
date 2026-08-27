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
# Warm SLOs (self-hosted): ci ≤18m | test-perf ≤8m | release ≤25m
#
#   make setup && make ci
#   make test-perf
#   make build-release && make package
#   make release TAG=vX.Y.Z

SHELL := /bin/bash

.PHONY: help ensure-cache doctor setup teardown check-infra \
	clean clean-cache build build-release package publish test-publish-tags \
	lint fmt check-fmt \
	test test-e2e test-perf ci release _release test-loki-diff test-tempo-diff \
	check-compat-reference-pins check-grafana-reference-pin \
	compat-reference-image compat-reference-version compat-builder-image grafana-reference-version grafana-reference-image grafana-reference-digest \
	test-grafana-static test-grafana-system test-compat \
	stress test-deploy \
	demo-session duckdb-shell duckdb-shell-prod generate-telemetry drop-tables telemetrygen \
	grafana-up grafana-down

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

CI_GOAL_SECS ?= 1080
PERF_GOAL_SECS ?= 480
RELEASE_GOAL_SECS ?= 1500

# Compatibility reference images are derived from the single YAML manifest.
# Override only for a local experiment; check-compat-reference-pins rejects
# drift before a differential gate runs.
COMPAT_REFERENCE_MANIFEST ?= $(CURDIR)/docs/compat/references.v0.yaml
COMPAT_REFERENCE_CANONICAL_MANIFEST ?= $(CURDIR)/docs/compat/references.v0.yaml
# One YAML parse for the selected manifest. Fields are emitted in stable
# protocol order, three tab-delimited values per protocol: image, tag, digest.
COMPAT_REFERENCE_FIELDS := $(shell ruby -ryaml -e 'references = YAML.safe_load(File.read(ARGV.fetch(0))).fetch("references"); %w[prometheus loki tempo grafana].each { |name| reference = references.fetch(name); puts [reference["image"], reference["tag"], reference["digest"]].join("\t") }' "$(COMPAT_REFERENCE_MANIFEST)")
COMPAT_REFERENCE_PROMETHEUS_IMAGE := $(word 1,$(COMPAT_REFERENCE_FIELDS))
COMPAT_REFERENCE_PROMETHEUS_TAG := $(word 2,$(COMPAT_REFERENCE_FIELDS))
COMPAT_REFERENCE_PROMETHEUS_DIGEST := $(word 3,$(COMPAT_REFERENCE_FIELDS))
COMPAT_REFERENCE_LOKI_IMAGE := $(word 4,$(COMPAT_REFERENCE_FIELDS))
COMPAT_REFERENCE_LOKI_TAG := $(word 5,$(COMPAT_REFERENCE_FIELDS))
COMPAT_REFERENCE_LOKI_DIGEST := $(word 6,$(COMPAT_REFERENCE_FIELDS))
COMPAT_REFERENCE_TEMPO_IMAGE := $(word 7,$(COMPAT_REFERENCE_FIELDS))
COMPAT_REFERENCE_TEMPO_TAG := $(word 8,$(COMPAT_REFERENCE_FIELDS))
COMPAT_REFERENCE_TEMPO_DIGEST := $(word 9,$(COMPAT_REFERENCE_FIELDS))
COMPAT_REFERENCE_GRAFANA_IMAGE := $(word 10,$(COMPAT_REFERENCE_FIELDS))
COMPAT_REFERENCE_GRAFANA_TAG := $(word 11,$(COMPAT_REFERENCE_FIELDS))
COMPAT_REFERENCE_GRAFANA_DIGEST := $(word 12,$(COMPAT_REFERENCE_FIELDS))

COMPAT_REFERENCE_PROMETHEUS_MANIFEST := $(COMPAT_REFERENCE_PROMETHEUS_IMAGE)@$(COMPAT_REFERENCE_PROMETHEUS_DIGEST)
COMPAT_REFERENCE_LOKI_MANIFEST := $(COMPAT_REFERENCE_LOKI_IMAGE)@$(COMPAT_REFERENCE_LOKI_DIGEST)
COMPAT_REFERENCE_TEMPO_MANIFEST := $(COMPAT_REFERENCE_TEMPO_IMAGE)@$(COMPAT_REFERENCE_TEMPO_DIGEST)
COMPAT_REFERENCE_GRAFANA_MANIFEST := $(COMPAT_REFERENCE_GRAFANA_IMAGE)@$(COMPAT_REFERENCE_GRAFANA_DIGEST)
COMPAT_REFERENCE_GRAFANA_TAG_IMAGE := $(COMPAT_REFERENCE_GRAFANA_IMAGE):$(COMPAT_REFERENCE_GRAFANA_TAG)
# Loki Phase 2 differential evidence. The test helper writes failure evidence
# below SOFTPROBE_COMPAT_ARTIFACT_DIR/loki/<case>/.
PROMETHEUS_REFERENCE_IMAGE := $(COMPAT_REFERENCE_PROMETHEUS_MANIFEST)
PROMETHEUS_REFERENCE_VERSION := $(COMPAT_REFERENCE_PROMETHEUS_TAG)
PROMETHEUS_REFERENCE_DIGEST ?= $(COMPAT_REFERENCE_PROMETHEUS_DIGEST)

# Loki Phase 2 differential evidence. The test helper writes failure evidence
# below SOFTPROBE_COMPAT_ARTIFACT_DIR/loki/<case>/.
LOKI_REFERENCE_IMAGE := $(COMPAT_REFERENCE_LOKI_MANIFEST)
LOKI_REFERENCE_DIGEST ?= $(COMPAT_REFERENCE_LOKI_DIGEST)
LOKI_DIFF_ARTIFACT_DIR ?= $(CURDIR)/target/compat/loki
LOKI_RAW_ARTIFACT ?= $(LOKI_DIFF_ARTIFACT_DIR)/raw.json
LOKI_NORMALIZED_ARTIFACT ?= $(LOKI_DIFF_ARTIFACT_DIR)/normalized.json
LOKI_DIFF_RAW_ARTIFACT ?= $(LOKI_RAW_ARTIFACT)
LOKI_DIFF_NORMALIZED_ARTIFACT ?= $(LOKI_NORMALIZED_ARTIFACT)
LOKI_DIFF_TIMEOUT_SECS ?= 900

# Tempo Phase 3 differential evidence. The test helper writes failure evidence
# below SOFTPROBE_COMPAT_ARTIFACT_DIR/tempo/<case>/.
TEMPO_REFERENCE_IMAGE := $(COMPAT_REFERENCE_TEMPO_MANIFEST)
TEMPO_REFERENCE_DIGEST ?= $(COMPAT_REFERENCE_TEMPO_DIGEST)
TEMPO_DIFF_ARTIFACT_DIR ?= $(CURDIR)/target/compat/tempo
TEMPO_RAW_ARTIFACT ?= $(TEMPO_DIFF_ARTIFACT_DIR)/raw.json
TEMPO_NORMALIZED_ARTIFACT ?= $(TEMPO_DIFF_ARTIFACT_DIR)/normalized.json
TEMPO_DIFF_RAW_ARTIFACT ?= $(TEMPO_RAW_ARTIFACT)
TEMPO_DIFF_NORMALIZED_ARTIFACT ?= $(TEMPO_NORMALIZED_ARTIFACT)
TEMPO_DIFF_TIMEOUT_SECS ?= 900

# Grafana Phase 4 system evidence. The immutable digest is part of the same
# manifest as the image/tag and is derived for every run.
GRAFANA_REFERENCE_IMAGE := $(COMPAT_REFERENCE_GRAFANA_TAG_IMAGE)
GRAFANA_REFERENCE_VERSION := $(COMPAT_REFERENCE_GRAFANA_TAG)
GRAFANA_REFERENCE_DIGEST ?= $(COMPAT_REFERENCE_GRAFANA_DIGEST)
GRAFANA_COMPOSE_IMAGE := $(COMPAT_REFERENCE_GRAFANA_MANIFEST)
SOFTPROBE_BUILDER_IMAGE ?= rust@sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4
GRAFANA_SYSTEM_ARTIFACT_DIR ?= $(CURDIR)/target/compat/grafana
GRAFANA_SYSTEM_COMPOSE_FILE ?= $(CURDIR)/tests/compat/grafana/docker-compose.ci.yml
GRAFANA_SYSTEM_COMPOSE_PROJECT ?= thelake-grafana-system
GRAFANA_URL ?= http://127.0.0.1:3000
GRAFANA_SYSTEM_TIMEOUT_SECS ?= 900

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
	@echo "Grafana:  grafana-up | grafana-down | test-grafana-prom-smoke | test-grafana-system"
	@echo "Compat:   test-compat | check-compat-reference-pins | test-loki-diff | test-tempo-diff"
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
	@echo "waiting for MinIO..."
	@i=0; \
	until curl -sf http://localhost:9000/minio/health/live > /dev/null; do \
		i=$$((i+1)); \
		if [ $$i -ge 60 ]; then echo "MinIO not ready" >&2; exit 1; fi; \
		sleep 1; \
	done
	@$(MAKE) --no-print-directory _minio-bucket
	@echo "waiting for Postgres..."
	@i=0; \
	until docker exec ducklake-postgres pg_isready -U ducklake -d ducklake > /dev/null 2>&1; do \
		i=$$((i+1)); \
		if [ $$i -ge 60 ]; then echo "Postgres not ready" >&2; exit 1; fi; \
		sleep 1; \
	done
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
	cargo test $(CARGO_PROFILE_FLAG) --lib --test tests --test compat_phase0 -- --test-threads=1

# Phase 1 mini differential vs pinned Prometheus (requires Docker).
test-prom-diff: ensure-cache
	@echo "prometheus mini-diff vs pinned $(PROMETHEUS_REFERENCE_IMAGE) (Docker)..."
	@docker info >/dev/null 2>&1 || (echo "ERROR: Docker required for test-prom-diff"; exit 1)
	PROMETHEUS_REFERENCE_IMAGE="$(PROMETHEUS_REFERENCE_IMAGE)" cargo test $(CARGO_PROFILE_FLAG) --test tests integration::prometheus::diff::mini_diff_vs_pinned_prometheus -- --ignored --test-threads=1 --nocapture

# Curated upstream promqltest subset vs pinned Prometheus (requires Docker).
test-promqltest: ensure-cache
	@echo "curated promqltest vs pinned $(PROMETHEUS_REFERENCE_IMAGE) (Docker)..."
	@docker info >/dev/null 2>&1 || (echo "ERROR: Docker required for test-promqltest"; exit 1)
	PROMETHEUS_REFERENCE_IMAGE="$(PROMETHEUS_REFERENCE_IMAGE)" cargo test $(CARGO_PROFILE_FLAG) --test tests integration::prometheus::promqltest::curated_promqltest_vs_pinned_prometheus -- --ignored --test-threads=1 --nocapture

# All Prometheus differential gates (mini-diff + curated promqltest).
test-prom-compat: test-prom-diff test-promqltest
	@echo "prometheus compatibility gates green"

# Aggregate Phase 5 conformance target. Real mode is the default and retains
# each protocol runner's Docker/reference-service gates. Mock mode is opt-in
# only and is explicitly marked non-evidence by conformance.sh.
COMPAT_CONFORMANCE_MODE ?= real
COMPAT_CONFORMANCE_PROTOCOL ?=
COMPAT_CONFORMANCE_CASE ?=
COMPAT_CONFORMANCE_OUT ?= $(CURDIR)/target/compat/conformance

test-compat: ensure-cache check-compat-reference-pins
	@set -euo pipefail; \
	args=(--out "$(COMPAT_CONFORMANCE_OUT)"); \
	case "$(COMPAT_CONFORMANCE_MODE)" in \
		real) ;; \
		mock) args+=(--mock) ;; \
		*) echo "COMPAT_CONFORMANCE_MODE must be real or mock" >&2; exit 2 ;; \
	esac; \
	if [ -n "$(COMPAT_CONFORMANCE_PROTOCOL)" ]; then args+=(--protocol "$(COMPAT_CONFORMANCE_PROTOCOL)"); fi; \
	if [ -n "$(COMPAT_CONFORMANCE_CASE)" ]; then args+=(--case "$(COMPAT_CONFORMANCE_CASE)"); fi; \
	"$(CURDIR)/scripts/compat/conformance.sh" "$${args[@]}"; \
	if [ "$(COMPAT_CONFORMANCE_MODE)" = real ]; then \
		"$(CURDIR)/scripts/compat/validate-artifacts.sh" --root "$(COMPAT_CONFORMANCE_OUT)" --release-gate; \
	else \
		"$(CURDIR)/scripts/compat/validate-artifacts.sh" --root "$(COMPAT_CONFORMANCE_OUT)"; \
	fi

# Verify every differential image used by Make is sourced from the pinned
# compatibility manifest. CI calls this target before pulling either image.
check-compat-reference-pins:
	@set -euo pipefail; \
	test -f "$(COMPAT_REFERENCE_MANIFEST)" || { echo "missing compatibility reference manifest: $(COMPAT_REFERENCE_MANIFEST)" >&2; exit 1; }; \
	test -f "$(COMPAT_REFERENCE_CANONICAL_MANIFEST)" || { echo "missing canonical compatibility reference manifest: $(COMPAT_REFERENCE_CANONICAL_MANIFEST)" >&2; exit 1; }; \
	PROMETHEUS_REFERENCE_IMAGE="$(PROMETHEUS_REFERENCE_IMAGE)"; \
	prometheus_manifest="$(COMPAT_REFERENCE_PROMETHEUS_MANIFEST)"; \
	loki_manifest="$(COMPAT_REFERENCE_LOKI_MANIFEST)"; \
	tempo_manifest="$(COMPAT_REFERENCE_TEMPO_MANIFEST)"; \
	grafana_manifest="$(COMPAT_REFERENCE_GRAFANA_MANIFEST)"; \
	prometheus_image="$(COMPAT_REFERENCE_PROMETHEUS_IMAGE)"; \
	loki_image="$(COMPAT_REFERENCE_LOKI_IMAGE)"; \
	tempo_image="$(COMPAT_REFERENCE_TEMPO_IMAGE)"; \
	grafana_image="$(COMPAT_REFERENCE_GRAFANA_IMAGE)"; \
	prometheus_tag="$(COMPAT_REFERENCE_PROMETHEUS_TAG)"; \
	loki_tag="$(COMPAT_REFERENCE_LOKI_TAG)"; \
	tempo_tag="$(COMPAT_REFERENCE_TEMPO_TAG)"; \
	grafana_tag="$(COMPAT_REFERENCE_GRAFANA_TAG)"; \
	prometheus_digest_manifest="$(COMPAT_REFERENCE_PROMETHEUS_DIGEST)"; \
	loki_digest_manifest="$(COMPAT_REFERENCE_LOKI_DIGEST)"; \
	tempo_digest_manifest="$(COMPAT_REFERENCE_TEMPO_DIGEST)"; \
	grafana_tag_manifest="$(COMPAT_REFERENCE_GRAFANA_TAG_IMAGE)"; \
	grafana_digest_manifest="$(COMPAT_REFERENCE_GRAFANA_DIGEST)"; \
	test -n "$$prometheus_image" && test -n "$$prometheus_tag" || { echo "prometheus reference requires a non-empty image and tag" >&2; exit 1; }; \
	test -n "$$loki_image" && test -n "$$loki_tag" || { echo "loki reference requires a non-empty image and tag" >&2; exit 1; }; \
	test -n "$$tempo_image" && test -n "$$tempo_tag" || { echo "tempo reference requires a non-empty image and tag" >&2; exit 1; }; \
	test -n "$$grafana_image" && test -n "$$grafana_tag" || { echo "grafana reference requires a non-empty image and tag" >&2; exit 1; }; \
	test -n "$$prometheus_manifest" && test -n "$$loki_manifest" && test -n "$$tempo_manifest" && test -n "$$grafana_manifest" || { echo "missing Prometheus, Loki, Tempo, or Grafana reference in $(COMPAT_REFERENCE_MANIFEST)" >&2; exit 1; }; \
	for reference in \
		"prometheus|$$prometheus_manifest|$$prometheus_digest_manifest|$(PROMETHEUS_REFERENCE_IMAGE)" \
		"loki|$$loki_manifest|$$loki_digest_manifest|$(LOKI_REFERENCE_IMAGE)" \
		"tempo|$$tempo_manifest|$$tempo_digest_manifest|$(TEMPO_REFERENCE_IMAGE)" \
		"grafana|$$grafana_manifest|$$grafana_digest_manifest|$(GRAFANA_COMPOSE_IMAGE)"; do \
		IFS='|' read -r name manifest digest make_image <<<"$$reference"; \
		[[ "$$digest" =~ ^sha256:[0-9a-f]{64}$$ ]] || { echo "$$name reference is missing a valid immutable sha256 digest: $$digest" >&2; exit 1; }; \
		[[ "$$manifest" == *@"$$digest" ]] || { echo "$$name manifest image does not resolve to its declared digest: $$manifest" >&2; exit 1; }; \
		test "$$make_image" = "$$manifest" || { echo "$$name reference drift: Make=$$make_image manifest=$$manifest" >&2; exit 1; }; \
	done; \
	test "$(PROMETHEUS_REFERENCE_IMAGE)" = "$$prometheus_manifest" || { echo "Prometheus reference drift: Make=$(PROMETHEUS_REFERENCE_IMAGE) manifest=$$prometheus_manifest" >&2; exit 1; }; \
	test "$(LOKI_REFERENCE_IMAGE)" = "$$loki_manifest" || { echo "Loki reference drift: Make=$(LOKI_REFERENCE_IMAGE) manifest=$$loki_manifest" >&2; exit 1; }; \
	test "$(TEMPO_REFERENCE_IMAGE)" = "$$tempo_manifest" || { echo "Tempo reference drift: Make=$(TEMPO_REFERENCE_IMAGE) manifest=$$tempo_manifest" >&2; exit 1; }; \
	test "$(GRAFANA_REFERENCE_IMAGE)" = "$$grafana_tag_manifest" || { echo "Grafana tag drift: Make=$(GRAFANA_REFERENCE_IMAGE) manifest=$$grafana_tag_manifest" >&2; exit 1; }; \
	test "$(GRAFANA_COMPOSE_IMAGE)" = "$$grafana_manifest" || { echo "Grafana compose image drift: Make=$(GRAFANA_COMPOSE_IMAGE) manifest=$$grafana_manifest" >&2; exit 1; }; \
	test "$(GRAFANA_REFERENCE_DIGEST)" = "$$grafana_digest_manifest" || { echo "Grafana digest drift: Make=$(GRAFANA_REFERENCE_DIGEST) manifest=$$grafana_digest_manifest" >&2; exit 1; }; \
	if [ "$${COMPAT_REFERENCE_ALLOW_MANIFEST_OVERRIDE:-0}" != 1 ]; then \
		ruby -ryaml -e 'canonical = YAML.load_file(ARGV.fetch(0)).fetch("references"); selected = YAML.load_file(ARGV.fetch(1)).fetch("references"); %w[prometheus loki tempo grafana].each { |name| abort "#{name.capitalize} reference drift from canonical manifest" unless selected.fetch(name) == canonical.fetch(name) }' "$(COMPAT_REFERENCE_CANONICAL_MANIFEST)" "$(COMPAT_REFERENCE_MANIFEST)"; \
	fi; \
	echo "compatibility reference pins match $(COMPAT_REFERENCE_MANIFEST)"; \
	echo "  prometheus: $$prometheus_manifest"; \
	echo "  loki:  $$loki_manifest"; \
	echo "  tempo: $$tempo_manifest"; \
	echo "  grafana: $$grafana_manifest"; \
	ruby "$(CURDIR)/docs/compat/validate.rb" \
		"$(CURDIR)/docs/compat/capability.v0.yaml" \
		"$(CURDIR)/tests/compat/tempo/phase3.json" \
		"$(COMPAT_REFERENCE_MANIFEST)"

compat-reference-image:
	@case "$(SIGNAL)" in \
		prometheus) printf '%s\n' "$(PROMETHEUS_REFERENCE_IMAGE)" ;; \
		loki) printf '%s\n' "$(LOKI_REFERENCE_IMAGE)" ;; \
		tempo) printf '%s\n' "$(TEMPO_REFERENCE_IMAGE)" ;; \
		grafana) printf '%s\n' "$(GRAFANA_REFERENCE_IMAGE)" ;; \
		*) echo "usage: make compat-reference-image SIGNAL=prometheus|loki|tempo|grafana" >&2; exit 2 ;; \
	esac

compat-reference-version:
	@case "$(SIGNAL)" in \
		prometheus) value="$(COMPAT_REFERENCE_PROMETHEUS_TAG)" ;; \
		loki) value="$(COMPAT_REFERENCE_LOKI_TAG)" ;; \
		tempo) value="$(COMPAT_REFERENCE_TEMPO_TAG)" ;; \
		grafana) value="$(COMPAT_REFERENCE_GRAFANA_TAG)" ;; \
		*) echo "usage: make compat-reference-version SIGNAL=prometheus|loki|tempo|grafana" >&2; exit 2 ;; \
	esac; \
	test -n "$$value" || { echo "missing reference tag in $(COMPAT_REFERENCE_MANIFEST)" >&2; exit 1; }; \
	printf '%s\n' "$$value"

compat-builder-image:
	@test -n "$(SOFTPROBE_BUILDER_IMAGE)" || { echo "missing immutable builder image" >&2; exit 1; }
	@printf '%s\n' "$(SOFTPROBE_BUILDER_IMAGE)"

grafana-reference-version:
	@test -n "$(GRAFANA_REFERENCE_VERSION)" || { echo "missing Grafana tag in $(COMPAT_REFERENCE_MANIFEST)" >&2; exit 1; }
	@printf '%s\n' "$(GRAFANA_REFERENCE_VERSION)"

grafana-reference-image:
	@test -n "$(GRAFANA_COMPOSE_IMAGE)" || { echo "missing immutable Grafana image in $(COMPAT_REFERENCE_MANIFEST)" >&2; exit 1; }
	@printf '%s\n' "$(GRAFANA_COMPOSE_IMAGE)"

grafana-reference-digest:
	@[[ "$(GRAFANA_REFERENCE_DIGEST)" =~ ^sha256:[0-9a-fA-F]{64}$$ ]] || { echo "missing Grafana digest in $(COMPAT_REFERENCE_MANIFEST)" >&2; exit 1; }
	@printf '%s\n' "$(GRAFANA_REFERENCE_DIGEST)"

# Validate the manifest-derived immutable digest against the pulled Grafana
# image before the compose harness runs. Inspect the tag image
# ($(GRAFANA_REFERENCE_IMAGE)), not the bare repository, which Docker resolves
# to a possibly-absent :latest tag; retag the digest-pulled image first so the
# check also heals local state after a docker prune.
check-grafana-reference-pin:
	@set -euo pipefail; \
	image="$(GRAFANA_REFERENCE_IMAGE)"; \
	case "$$image" in *@*) repository="$${image%@*}" ;; *) repository="$${image%:*}" ;; esac; \
	digest="$(GRAFANA_REFERENCE_DIGEST)"; \
	test -n "$$image" && test -n "$(GRAFANA_REFERENCE_VERSION)" || { echo "missing Grafana image/version in $(COMPAT_REFERENCE_MANIFEST)" >&2; exit 1; }; \
	[[ "$$digest" =~ ^sha256:[0-9a-fA-F]{64}$$ ]] || { echo "GRAFANA_REFERENCE_DIGEST must be an immutable sha256 digest" >&2; exit 1; }; \
	docker pull "$$repository@$$digest" >/dev/null; \
	case "$$image" in *@*) ;; *) docker tag "$$repository@$$digest" "$$image" ;; esac; \
	repo_digests="$$(docker image inspect --format '{{json .RepoDigests}}' "$$image")"; \
	expected_repo_digest="$$repository@$$digest"; \
	echo "$$repo_digests" | grep -Fq -- "$$expected_repo_digest" || { echo "Grafana digest mismatch: $$image does not resolve to $$digest" >&2; exit 1; }; \
	echo "Grafana reference validated: $$image@$$digest"

# Phase 4 static contracts. Keep these independent of Docker so path,
# provisioning, dashboard, and artifact-redaction regressions fail before the
# service-backed Grafana lane starts.
test-grafana-static: check-compat-reference-pins
	@set -euo pipefail; \
	SOFTPROBE_BUILDER_IMAGE="$(SOFTPROBE_BUILDER_IMAGE)" \
	GRAFANA_COMPOSE_IMAGE="$(GRAFANA_COMPOSE_IMAGE)" \
	"$(CURDIR)/scripts/compat/check-compose-image-pins.sh" "$(CURDIR)/tests/compat/grafana/docker-compose.ci.yml"; \
	for contract in \
		compose_contract_test.sh \
		phase4_contract_test.sh \
		tempo_tenant_contract_test.sh \
		cross_signal_link_contract_test.sh \
		datasource_auth_contract_test.sh \
		manual_digest_contract_test.sh \
		artifact_redaction_test.sh; do \
		path="$(CURDIR)/tests/compat/grafana/e2e/$$contract"; \
		test -f "$$path" || { echo "missing Grafana static contract: $$path" >&2; exit 1; }; \
		GRAFANA_SKIP_STATIC_CONTRACTS=1 bash "$$path"; \
	done

# Phase 4 deterministic compose system lane. The shell harness owns G1-G3 and
# writes structured outcome evidence; compose lifecycle evidence is collected
# here so cleanup runs for both harness failures and successful runs.
test-grafana-system: ensure-cache check-compat-reference-pins
	@set -euo pipefail; \
	artifact_dir="$(GRAFANA_SYSTEM_ARTIFACT_DIR)"; compose_file="$(GRAFANA_SYSTEM_COMPOSE_FILE)"; \
	compose_project="$(GRAFANA_SYSTEM_COMPOSE_PROJECT)"; grafana_url="$(GRAFANA_URL)"; \
	mkdir -p "$$artifact_dir"; \
	: > "$$artifact_dir/summary.txt"; \
	skip_environment() { reason="$$1"; printf 'SKIP: %s\n' "$$reason" | tee "$$artifact_dir/summary.txt"; printf '{"outcome":"environment_skip","reason":"%s"}\n' "$$reason" > "$$artifact_dir/outcome.json"; exit 0; }; \
	if ! docker info >/dev/null 2>&1; then \
		skip_environment "Docker unavailable"; \
	fi; \
	timeout_ok=0; for candidate in timeout gtimeout; do if command -v "$$candidate" >/dev/null 2>&1 && "$$candidate" --version 2>&1 | grep -q 'GNU coreutils'; then timeout_ok=1; break; fi; done; \
	if [ "$$timeout_ok" -ne 1 ]; then skip_environment "GNU timeout unavailable"; fi; \
	SOFTPROBE_BUILDER_IMAGE="$(SOFTPROBE_BUILDER_IMAGE)" \
	GRAFANA_COMPOSE_IMAGE="$(GRAFANA_COMPOSE_IMAGE)" \
	"$(CURDIR)/scripts/compat/check-compose-image-pins.sh" "$$compose_file"; \
	export GRAFANA_REFERENCE_IMAGE="$(GRAFANA_REFERENCE_IMAGE)"; \
	export GRAFANA_REFERENCE_VERSION="$(GRAFANA_REFERENCE_VERSION)"; \
	export GRAFANA_REFERENCE_DIGEST="$(GRAFANA_REFERENCE_DIGEST)"; \
	export GRAFANA_REFERENCE_MANIFEST="$(GRAFANA_REFERENCE_MANIFEST)"; \
	export GRAFANA_COMPOSE_IMAGE="$(GRAFANA_COMPOSE_IMAGE)"; \
	export SOFTPROBE_BUILDER_IMAGE="$(SOFTPROBE_BUILDER_IMAGE)"; \
	$(MAKE) --no-print-directory check-grafana-reference-pin; \
	command -v curl >/dev/null 2>&1 || { echo "FAIL: curl unavailable" | tee "$$artifact_dir/summary.txt"; exit 1; }; \
	command -v python3 >/dev/null 2>&1 || { echo "FAIL: python3 unavailable" | tee "$$artifact_dir/summary.txt"; exit 1; }; \
	test -f "$(CURDIR)/scripts/grafana-system-smoke.sh" || { echo "FAIL: missing Grafana smoke harness" | tee "$$artifact_dir/summary.txt"; exit 1; }; \
	test -f "$$compose_file" || { echo "FAIL: missing compose harness $$compose_file" | tee "$$artifact_dir/summary.txt"; exit 1; }; \
	export GRAFANA_SEED_ARTIFACT_DIR="$$artifact_dir"; \
	export GRAFANA_SEED_IN_COMPOSE=1; \
	compose=( $(COMPOSE) --project-name "$$compose_project" --file "$$compose_file" ); \
	redact() { sed -E \
		-e 's/(Bearer[[:space:]]+)[^[:space:]"'"'"'<>]+/\1[REDACTED]/Ig' \
		-e 's/((api[_-]?key|password|token|secret|authorization|cookie)[[:space:]]*[:=][[:space:]]*)[^[:space:],"'"'"'<>]+/\1[REDACTED]/Ig'; }; \
	collect() { \
		"$${compose[@]}" ps > "$$artifact_dir/compose-ps.raw" 2>&1 || true; \
		"$${compose[@]}" logs --no-color grafana > "$$artifact_dir/compose-logs.raw" 2>&1 || true; \
		"$${compose[@]}" logs --no-color grafana-seed > "$$artifact_dir/compose-seed-logs.raw" 2>&1 || true; \
		redact < "$$artifact_dir/compose-ps.raw" > "$$artifact_dir/compose-ps.txt"; \
		redact < "$$artifact_dir/compose-logs.raw" > "$$artifact_dir/compose-logs.txt"; \
		redact < "$$artifact_dir/compose-seed-logs.raw" > "$$artifact_dir/compose-seed-logs.txt"; \
		rm -f "$$artifact_dir/compose-ps.raw" "$$artifact_dir/compose-logs.raw" "$$artifact_dir/compose-seed-logs.raw"; \
	}; \
	cleanup() { status=$$?; collect; "$${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true; exit $$status; }; \
	trap cleanup EXIT; \
	"$${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true; \
	"$${compose[@]}" up -d --wait || { \
		echo "FAIL: compose harness failed to start" | tee "$$artifact_dir/summary.txt"; \
		echo "--- grafana-seed logs ---" >&2; "$${compose[@]}" logs --no-color grafana-seed >&2 || true; \
		echo "--- softprobe logs (tail) ---" >&2; "$${compose[@]}" logs --no-color --tail 50 softprobe >&2 || true; \
		exit 1; }; \
	GRAFANA_URL="$$grafana_url" \
	GRAFANA_DASHBOARD_DIR="tests/compat/grafana/dashboards/compose" \
	GRAFANA_DASHBOARD_UIDS="compose-cross-signal compose-loki compose-prom compose-tempo" \
	GRAFANA_CHECK_DASHBOARD_QUERIES=1 \
	GRAFANA_RICH_TEMPO_ASSERTIONS=1 \
	GRAFANA_ADMIN_USER="$${GF_SECURITY_ADMIN_USER:-admin}" \
	GRAFANA_ADMIN_PASSWORD="$${GF_SECURITY_ADMIN_PASSWORD:-admin}" \
	ARTIFACT_DIR="$$artifact_dir" \
	"$(CURDIR)/scripts/compat/run-with-timeout" "$(GRAFANA_SYSTEM_TIMEOUT_SECS)" "$(SHELL)" "$(CURDIR)/scripts/grafana-system-smoke.sh"; \
	collect; \
	echo "Grafana system evidence: $$artifact_dir"

# Phase 2 differential vs the pinned Loki reference (explicit Docker gate).
# This target is intentionally not a prerequisite of `test`, `ci`, or release.
# The exported paths are the contract used by evidence-producing harnesses.
test-loki-diff: ensure-cache
	@set -euo pipefail; \
	docker info >/dev/null 2>&1 || { echo "ERROR: Docker required for test-loki-diff" >&2; exit 1; }; \
	mkdir -p "$(LOKI_DIFF_ARTIFACT_DIR)"; \
	echo "Loki differential reference: $(LOKI_REFERENCE_IMAGE)"; \
	echo "LOKI_RAW_ARTIFACT=$(LOKI_RAW_ARTIFACT)"; \
	echo "LOKI_NORMALIZED_ARTIFACT=$(LOKI_NORMALIZED_ARTIFACT)"; \
	LOKI_REFERENCE_IMAGE="$(LOKI_REFERENCE_IMAGE)" \
	LOKI_RAW_ARTIFACT="$(LOKI_RAW_ARTIFACT)" \
	LOKI_NORMALIZED_ARTIFACT="$(LOKI_NORMALIZED_ARTIFACT)" \
	LOKI_DIFF_RAW_ARTIFACT="$(LOKI_DIFF_RAW_ARTIFACT)" \
	LOKI_DIFF_NORMALIZED_ARTIFACT="$(LOKI_DIFF_NORMALIZED_ARTIFACT)" \
	SOFTPROBE_COMPAT_ARTIFACT_DIR="$${SOFTPROBE_COMPAT_ARTIFACT_DIR:-$(LOKI_DIFF_ARTIFACT_DIR)}" \
	"$(CURDIR)/scripts/compat/run-with-timeout" "$(LOKI_DIFF_TIMEOUT_SECS)" cargo test $(CARGO_PROFILE_FLAG) --features integration-e2e --test tests compat_loki::loki_phase2_differential_vs_pinned_loki -- --ignored --test-threads=1 --nocapture

# Phase 3 differential vs the pinned Tempo reference (explicit Docker gate).
# This target is intentionally not a prerequisite of `test`, `ci`, or release.
# The exported paths are the contract used by evidence-producing harnesses.
test-tempo-diff: ensure-cache
	@set -euo pipefail; \
	docker info >/dev/null 2>&1 || { echo "ERROR: Docker required for test-tempo-diff" >&2; exit 1; }; \
	mkdir -p "$(TEMPO_DIFF_ARTIFACT_DIR)"; \
	echo "Tempo differential reference: $(TEMPO_REFERENCE_IMAGE)"; \
	echo "TEMPO_RAW_ARTIFACT=$(TEMPO_RAW_ARTIFACT)"; \
	echo "TEMPO_NORMALIZED_ARTIFACT=$(TEMPO_NORMALIZED_ARTIFACT)"; \
	TEMPO_REFERENCE_IMAGE="$(TEMPO_REFERENCE_IMAGE)" \
	TEMPO_RAW_ARTIFACT="$(TEMPO_RAW_ARTIFACT)" \
	TEMPO_NORMALIZED_ARTIFACT="$(TEMPO_NORMALIZED_ARTIFACT)" \
	TEMPO_DIFF_RAW_ARTIFACT="$(TEMPO_DIFF_RAW_ARTIFACT)" \
	TEMPO_DIFF_NORMALIZED_ARTIFACT="$(TEMPO_DIFF_NORMALIZED_ARTIFACT)" \
	SOFTPROBE_COMPAT_ARTIFACT_DIR="$${SOFTPROBE_COMPAT_ARTIFACT_DIR:-$(TEMPO_DIFF_ARTIFACT_DIR)}" \
	"$(CURDIR)/scripts/compat/run-with-timeout" "$(TEMPO_DIFF_TIMEOUT_SECS)" cargo test $(CARGO_PROFILE_FLAG) --features integration-e2e --test tests compat_tempo::tempo_phase3_differential_vs_pinned_tempo -- --ignored --test-threads=1 --nocapture

# Grafana Prometheus datasource smoke (#27 Prom-only slice; also covered by `make test`).
test-grafana-prom-smoke: ensure-cache
	cargo test $(CARGO_PROFILE_FLAG) --test tests integration::grafana_prom_smoke -- --nocapture

# Manual Grafana inspection: host Softprobe + pinned Grafana 11.2.0 + seeded demo metrics.
# Open http://127.0.0.1:3000 (admin/admin) → Softprobe → Softprobe Prometheus smoke.
grafana-up: ensure-cache
	@chmod +x scripts/grafana-manual-up.sh scripts/grafana-manual-down.sh
	GRAFANA_COMPOSE_IMAGE="$(GRAFANA_COMPOSE_IMAGE)" \
		"$(CURDIR)/scripts/compat/check-compose-image-pins.sh" "$(CURDIR)/tests/compat/grafana/docker-compose.manual.yml"
	GRAFANA_COMPOSE_IMAGE="$(GRAFANA_COMPOSE_IMAGE)" ./scripts/grafana-manual-up.sh

grafana-down:
	@chmod +x scripts/grafana-manual-down.sh
	./scripts/grafana-manual-down.sh

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
# perf + binary + push — do not nest make ci (release compile blew the ci SLO).
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
