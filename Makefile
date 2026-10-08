MODULE_DIR := modules/otel/istio-wasm

FORWARDED_TARGETS := help check-deps-build check-deps-k8s check-deps clean build hash \
	update-configs integration-test docker-build docker-push push cluster-up cluster-down \
	docker-build-local deploy-demo forward quickstart apply-wasm deploy-wasm-http \
	copy-wasm-http use-wasm-http dev-quickstart dev-setup dev-reload version status logs \
	test-logs all rebuild

.PHONY: $(FORWARDED_TARGETS)

$(FORWARDED_TARGETS):
	$(MAKE) -C $(MODULE_DIR) $@
