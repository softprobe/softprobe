# SP-Istio Agent Makefile
.PHONY: help build test clean push deploy status logs check-deps \
	cluster-up cluster-down docker-build-local \
	deploy-demo forward quickstart docker-build docker-push version \
	apply-wasm deploy-wasm-http copy-wasm-http use-wasm-http dev-quickstart \
	dev-setup dev-reload

# Additional phony targets
.PHONY: check-deps-build check-deps-k8s

# Configuration
BINARY_NAME := sp_istio_agent
WASM_TARGET := wasm32-unknown-unknown
BUILD_DIR := ../../../target/$(WASM_TARGET)/release
WASM_FILE := $(BUILD_DIR)/$(BINARY_NAME).wasm
HASH_FILE := $(WASM_FILE).sha256
CARGO_TARGET_DIR ?= ../../../target
export CARGO_TARGET_DIR

# Kind / Cluster configuration
CLUSTER_NAME := sp-demo-cluster
NAMESPACE := default

# Docker configuration
REGISTRY := softprobe
WASM_IMAGE := $(REGISTRY)/otel-istio-wasm
ENVOY_IMAGE := $(REGISTRY)/sp-envoy
# Auto-extract version from Cargo.toml
VERSION := $(shell grep '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/v\1/')
VERSION ?= latest

# Colors for output
GREEN := \033[32m
BLUE := \033[34m
YELLOW := \033[33m
RED := \033[31m
RESET := \033[0m

define print_success
	@echo "$(GREEN)✅ $(1)$(RESET)"
endef

define print_info
	@echo "$(BLUE)ℹ️  $(1)$(RESET)"
endef

define print_warning
	@echo "$(YELLOW)⚠️  $(1)$(RESET)"
endef

define print_error
	@echo "$(RED)❌ $(1)$(RESET)"
endef

# Default target
help: ## Show this help message
	@echo "SP-Istio Agent - Local Development & Testing"
	@echo "============================================"
	@echo ""
	@echo "Current version: $(VERSION)"
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ { printf "  %-20s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

check-deps-build: ## Check build-only dependencies (used by CI/build)
	$(call print_info,"Checking build dependencies...")
	@command -v cargo >/dev/null || (echo "$(RED)❌ Rust/Cargo not found. Install from https://rustup.rs/$(RESET)" >&2 && exit 1)
	@rustup target list --installed | grep -q $(WASM_TARGET) || (echo "$(BLUE)ℹ️  Installing WASM target...$(RESET)" && rustup target add $(WASM_TARGET))
	$(call print_success,"Build dependencies check completed")

check-deps-k8s: ## Check Kubernetes/Istio tooling (needed for local cluster workflows)
	$(call print_info,"Checking Kubernetes/Istio dependencies...")
	@command -v kubectl >/dev/null || (echo "$(RED)❌ kubectl not found. Install: brew install kubectl$(RESET)" >&2 && exit 1)
	@command -v kind >/dev/null || (echo "$(RED)❌ kind not found. Install: brew install kind$(RESET)" >&2 && exit 1)
	@command -v istioctl >/dev/null || (echo "$(RED)❌ istioctl not found. Install: brew install istioctl$(RESET)" >&2 && exit 1)
	$(call print_success,"Kubernetes/Istio dependencies check completed")

check-deps: check-deps-build check-deps-k8s ## Check all dependencies (build + Kubernetes/Istio)

clean: ## Clean build artifacts
	$(call print_info,"Cleaning build artifacts...")
	@cargo clean
	@rm -f $(HASH_FILE)
	$(call print_success,"Clean completed")

build: check-deps-build ## Build WASM binary
	$(call print_info,"Building WASM binary...")
	@cargo build --target $(WASM_TARGET) --release
	@if [ -f "$(WASM_FILE)" ]; then \
			echo "$(GREEN)✅ WASM binary built: $(WASM_FILE)$(RESET)"; \
			$(MAKE) hash; \
	else \
		echo "$(RED)❌ Build failed!$(RESET)"; \
		exit 1; \
	fi

hash: ## Calculate SHA256 hash of WASM binary
	$(call print_info,"Calculating SHA256 hash...")
	@if command -v sha256sum >/dev/null; then \
		sha256sum $(WASM_FILE) | cut -d' ' -f1 > $(HASH_FILE); \
	elif command -v shasum >/dev/null; then \
		shasum -a 256 $(WASM_FILE) | cut -d' ' -f1 > $(HASH_FILE); \
	else \
		$(call print_error,"Neither sha256sum nor shasum found"); \
		exit 1; \
	fi
	@HASH=$$(cat $(HASH_FILE)); \
	SIZE=$$(ls -lh $(WASM_FILE) | awk '{print $$5}'); \
	echo "$(GREEN)✅ SHA256: $$HASH$(RESET)"; \
	echo "$(BLUE)ℹ️  File size: $$SIZE$(RESET)"

update-configs: ## Update deployment configs with new WASM hash
	$(call print_info,"Updating deployment configurations...")
	@if [ -f "$(HASH_FILE)" ]; then \
		HASH=$$(cat $(HASH_FILE)); \
		if [ -f "deploy/minimal.yaml" ]; then \
			sed -i "" "s/sha256: .*/sha256: $$HASH/" deploy/minimal.yaml; \
			echo "$(GREEN)✅ Updated deploy/minimal.yaml$(RESET)"; \
		fi; \
		if [ -f "deploy/production/sp-istio-agent.yaml" ]; then \
			sed -i "" "s/sha256: .*/sha256: $$HASH/" deploy/production/sp-istio-agent.yaml; \
			echo "$(GREEN)✅ Updated deploy/production/sp-istio-agent.yaml$(RESET)"; \
		fi; \
	else \
		echo "$(RED)❌ Hash file not found. Run 'make build' first$(RESET)"; \
		exit 1; \
	fi

integration-test: build ## Run comprehensive integration test with Softprobe backend verification
	$(call print_info,"Running integration test with Softprobe backend...")
	@docker compose -f test/docker-compose.yml up --build --abort-on-container-exit


docker-build: build ## Build Docker images (auto-versioned from Cargo.toml)
	$(call print_info,"Building Docker images for version $(VERSION)...")
	@HASH=$$(cat $(HASH_FILE)); \
	docker build --platform linux/amd64 --build-arg WASM_SHA256=$$HASH -t $(WASM_IMAGE):$(VERSION) -f "$(CURDIR)/Dockerfile" ../../../
	@docker build -t $(ENVOY_IMAGE):$(VERSION) -f "$(CURDIR)/Dockerfile.envoy" ../../../
	$(call print_success,"Docker images built")

docker-push: docker-build ## Build and push Docker images (auto-versioned from Cargo.toml)
	$(call print_info,"Pushing images to registry...")
	@docker push $(WASM_IMAGE):$(VERSION)
	@docker push $(ENVOY_IMAGE):$(VERSION)
	$(call print_success,"Images pushed to registry")
	$(call print_info,"Cleaning local tags...")
	@docker rmi $(WASM_IMAGE):$(VERSION) $(ENVOY_IMAGE):$(VERSION) || true

push: docker-push ## Alias for docker-push

# ===== Local Kind Cluster Workflow (no registry push) =====

cluster-up: check-deps ## Create Kind cluster with Istio and dependencies
	$(call print_info,"Setting up local Kind cluster $(CLUSTER_NAME)...")
	@if kind get clusters | grep -q "$(CLUSTER_NAME)"; then \
		echo "$(YELLOW)⚠️  Cluster $(CLUSTER_NAME) already exists$(RESET)"; \
	else \
		./scripts/cluster-setup.sh; \
		echo "$(GREEN)✅ Cluster created: $(CLUSTER_NAME)$(RESET)"; \
	fi

cluster-down: ## Delete Kind cluster
	$(call print_info,"Deleting Kind cluster $(CLUSTER_NAME)...")
	@kind delete cluster --name $(CLUSTER_NAME)
	$(call print_success,"Cluster deleted")

docker-build-local: build ## Build Docker image for local Kind testing
	$(call print_info,"Building local Docker image...")
	@HASH=$$(cat $(HASH_FILE)); \
	docker build --platform linux/amd64 --build-arg WASM_SHA256=$$HASH \
	  -t $(WASM_IMAGE):$(VERSION)-local -f "$(CURDIR)/Dockerfile" ../../../
	$(call print_success,"Local Docker image built: $(WASM_IMAGE):$(VERSION)-local")

deploy-demo: ## Deploy demo applications
	$(call print_info,"Deploying demo applications...")
	@kubectl apply -f examples/travel/apps.yaml
	$(call print_success,"Demo applications deployed")

forward: ## Start port forwarding to access demo (Ctrl+C to stop)
	$(call print_info,"Starting port forwarding on 8080...")
	@kubectl port-forward -n istio-system svc/istio-ingressgateway 8080:80

quickstart: cluster-up deploy-demo ## Complete local setup (Kind + local image)
	$(call print_success,"Quickstart completed. Run 'make forward' and open http://localhost:8080")

apply-wasm: ## Apply WasmPlugin and supporting manifests
	$(call print_info,"Applying WasmPlugin manifests...")
	@kubectl apply -f deploy/minimal.yaml
	$(call print_success,"WasmPlugin resources applied")

deploy-wasm-http: ## Deploy HTTP server pod to serve local WASM file
	$(call print_info,"Deploying local WASM HTTP server...")
	@kubectl apply -f deploy/dev/local-wasm-http-server.yaml
	$(call print_info,"Waiting for sp-wasm-http pod to be ready...")
	@kubectl -n istio-system rollout status deploy/sp-wasm-http --timeout=120s || true
	$(call print_success,"WASM HTTP server deployed")

copy-wasm-http: build ## Copy built WASM into the HTTP server pod
	$(call print_info,"Copying WASM into HTTP server pod...")
	@POD=$$(kubectl -n istio-system get pod -l app=sp-wasm-http -o jsonpath='{.items[0].metadata.name}'); \
	if [ -z "$$POD" ]; then echo "$(RED)❌ sp-wasm-http pod not found$(RESET)"; exit 1; fi; \
	kubectl -n istio-system cp $(WASM_FILE) $$POD:/data/plugin.wasm
	$(call print_success,"WASM copied to HTTP server")

use-wasm-http: ## Configure WasmPlugins to load from in-cluster HTTP server
	$(call print_info,"Patching WasmPlugins to use HTTP URL...")
	@kubectl -n istio-system patch wasmplugin sp-istio-agent-client --type=json -p='[{"op":"replace","path":"/spec/url","value":"http://sp-wasm-http.istio-system.svc.cluster.local/plugin.wasm"}]'
	@kubectl -n istio-system patch wasmplugin sp-istio-agent-server --type=json -p='[{"op":"replace","path":"/spec/url","value":"http://sp-wasm-http.istio-system.svc.cluster.local/plugin.wasm"}]'
	$(call print_success,"WasmPlugins updated to HTTP URL")

dev-quickstart: cluster-down cluster-up apply-wasm deploy-wasm-http copy-wasm-http use-wasm-http deploy-demo ## Recreate cluster and use HTTP-served WASM
	$(call print_info,"Restarting sidecars to reload WASM...")
	@kubectl -n istio-system rollout restart deploy istio-ingressgateway || true
	@kubectl -n default rollout restart deploy demo-ota demo-airline || true
	$(call print_info,"Run `make forward` to start port forwarding on 8080...")

dev-setup: apply-wasm deploy-wasm-http use-wasm-http ## One-time setup to enable HTTP-served WASM
	$(call print_success,"Development setup completed. Use 'make dev-reload' for fast reloads")

dev-reload: copy-wasm-http ## Build, copy, and hot-reload WASM via cache-busting WasmPlugin URL
	$(call print_info,"Reloading WASM by cache-busting WasmPlugin URLs...")
	@TS=$$(date +%s); \
	kubectl -n istio-system patch wasmplugin sp-istio-agent-client \
	  --type=json -p='[{"op":"replace","path":"/spec/url","value":"http://sp-wasm-http.istio-system.svc.cluster.local/plugin.wasm?v='"$$TS"'"}]'; \
	kubectl -n istio-system patch wasmplugin sp-istio-agent-server \
	  --type=json -p='[{"op":"replace","path":"/spec/url","value":"http://sp-wasm-http.istio-system.svc.cluster.local/plugin.wasm?v='"$$TS"'"}]'
	$(call print_info,"Restarting ingressgateway to ensure WASM is reloaded...")
	@kubectl -n istio-system rollout restart deploy/istio-ingressgateway || true
	@kubectl -n istio-system rollout status  deploy/istio-ingressgateway --timeout=180s || true
	$(call print_success,"WASM reloaded. You can now send traffic and view logs")

version: ## Show current version from Cargo.toml
	@echo "$(GREEN)Current version: $(VERSION)$(RESET)"

status: ## Check deployment status
	$(call print_info,"Checking deployment status...")
	@kubectl get wasmplugin -n istio-system 2>/dev/null || $(call print_warning,"No WASM plugins found")
	@kubectl get pods -n $(NAMESPACE) 2>/dev/null || $(call print_warning,"No pods found in $(NAMESPACE)")

logs: ## View plugin logs
	$(call print_info,"Viewing plugin logs...")
	@POD=$$(kubectl get pod -n $(NAMESPACE) -o jsonpath='{.items[0].metadata.name}' 2>/dev/null); \
	if [ -n "$$POD" ]; then \
		kubectl logs $$POD -n $(NAMESPACE) -c istio-proxy || echo "$(YELLOW)⚠️  No SP logs found$(RESET)"; \
	else \
		echo "$(YELLOW)⚠️  No pods found in namespace $(NAMESPACE)$(RESET)"; \
	fi

test-logs: ## View logs from test containers
	$(call print_info,"Viewing test container logs...")
	@docker compose -f test/docker-compose.yml logs --tail=50

# Convenience aliases
all: build ## Build everything
rebuild: clean build ## Clean and build
