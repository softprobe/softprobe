# Grafana dashboard PromQL bench

Cells **356** (ok 103, errors 253, empty-success 61).

| p50 | p95 | p99 | max |
|-----|-----|-----|-----|
| 15015ms | 15028ms | 15045ms | 15067ms |

| <1s | 1–2s | 2–5s | 5–10s | ≥10s |
|-----|------|------|-------|------|
| 21 | 15 | 32 | 12 | 276 |

## By range

| Range | n | p50 | p95 | max |
|-------|---|-----|-----|-----|
| 1h | 178 | 15014ms | 15027ms | 15067ms |
| 5m | 178 | 15015ms | 15029ms | 15056ms |

## Worst panels

| ms | dash | panel | range | series | error | expr |
|----|------|-------|-------|--------|-------|------|
| 15067 | Astronomy Shop · Ad (Java) | Ads served/s by category | 1h | 0 | http=0 status=None timed out | `sum by (category) (rate(demo_ad_served_total{job="ad"}[5m]))` |
| 15056 | Softprobe Prometheus smoke | demo_ad_served rate | 5m | 0 | http=0 status=None timed out | `sum(rate(demo_ad_served_total[5m]))` |
| 15048 | Softprobe · Classic histogram series | cart latency _count rate | 1h | 0 | http=0 status=None timed out | `sum by (job) (rate(demo_cart_add_item_latency_count[5m]))` |
| 15045 | Softprobe · rate / irate / increase / delta | idelta(k6_vus[5m]) | 5m | 0 | http=0 status=None timed out | `idelta(k6_vus[5m])` |
| 15045 | Astronomy Shop · Ad (Java) | Ads served/s by category | 5m | 0 | http=0 status=None timed out | `sum by (category) (rate(demo_ad_served_total{job="ad"}[5m]))` |
| 15040 | Astronomy Shop · GOLD overview | Loadgen VUs | 5m | 0 | http=0 status=None timed out | `k6_vus` |
| 15040 | Astronomy Shop · Currency & Quote | Quote spanmetrics calls/s | 5m | 0 | http=0 status=None timed out | `sum(rate(traces_span_metrics_calls{job="quote"}[5m]))` |
| 15039 | Astronomy Shop · GOLD overview | Business: items shipped/s | 5m | 0 | http=0 status=None timed out | `sum(rate(demo_shipping_items_shipped[5m]))` |
| 15039 | Softprobe · Aggregations | avg without (method) | 1h | 0 | http=0 status=None timed out | `avg without (method) (k6_http_reqs)` |
| 15038 | Astronomy Shop · Load generator (k6) | Loadgen Go memory used | 1h | 0 | http=0 status=None timed out | `sum(go_memory_used{job="load-generator"})` |
| 15036 | Astronomy Shop · Shipping | Spanmetrics calls/s | 5m | 0 | http=0 status=None timed out | `sum(rate(traces_span_metrics_calls{job="shipping"}[5m]))` |
| 15032 | Astronomy Shop · GOLD overview | RPC server call rate (checkout) | 1h | 0 | http=0 status=None timed out | `sum by (job) (rate(rpc_server_call_duration_count[5m]))` |
| 15031 | Astronomy Shop · Cart (.NET) | Feature-flag evaluations/s | 5m | 0 | http=0 status=None timed out | `sum(rate(feature_flag_evaluation_requests_total{job="cart"}[5m]))` |
| 15030 | Astronomy Shop · Load generator (k6) | Web vital LCP count/s | 5m | 0 | http=0 status=None timed out | `sum(rate(k6_browser_web_vital_lcp_count[5m]))` |
| 15030 | Softprobe Prometheus smoke | demo_ad_served rate | 1h | 0 | http=0 status=None timed out | `sum(rate(demo_ad_served_total[5m]))` |
| 15030 | Softprobe · Arithmetic, compare, set ops | k6_vus * 2 | 1h | 0 | http=0 status=None timed out | `k6_vus * 2` |
| 15029 | Softprobe · Arithmetic, compare, set ops | k6_vus > bool 1 | 5m | 0 | http=0 status=None timed out | `k6_vus > bool 1` |
| 15029 | Softprobe · Arithmetic, compare, set ops | k6_http_reqs unless k6_vus | 5m | 0 | http=0 status=None timed out | `count(k6_http_reqs unless k6_vus)` |
| 15028 | Astronomy Shop · Checkout (Go) | RPC server duration buckets | 5m | 0 | http=0 status=None timed out | `sum by (le) (rate(rpc_server_call_duration_bucket{job="checkout"}[5m]))` |
| 15028 | Astronomy Shop · Recommendation (Python) | Recommendation requests | 5m | 0 | http=0 status=None timed out | `demo_recommendation_requests{job="recommendation"}` |
| 15028 | Astronomy Shop · Cart (.NET) | ASP.NET routing match attempts | 5m | 0 | http=0 status=None timed out | `sum(rate(aspnetcore_routing_match_attempts{job="cart"}[5m]))` |
| 15028 | Softprobe · Classic histogram series | k6_http_req_duration _bucket rate | 5m | 0 | http=0 status=None timed out | `sum by (le) (rate(k6_http_req_duration_bucket[5m]))` |
| 15028 | Astronomy Shop · GOLD overview | HTTP client request rate | 1h | 0 | http=0 status=None timed out | `sum by (job) (rate(http_client_request_duration_count[5m]))` |
| 15028 | Astronomy Shop · Load generator (k6) | HTTP req duration count/s | 5m | 0 | http=0 status=None timed out | `sum(rate(k6_http_req_duration_count[5m]))` |
| 15027 | Softprobe · Selectors & matchers | regex job=~ | 1h | 0 | http=0 status=None timed out | `k6_http_reqs{job=~"load.*"}` |
| 15027 | Astronomy Shop · Payment (Node.js) | Payment transactions (raw) | 1h | 0 | http=0 status=None timed out | `demo_payment_transactions{job="payment"}` |
| 15026 | Astronomy Shop · Frontend (Node.js) | V8 GC duration rate | 5m | 0 | http=0 status=None timed out | `sum(rate(v8js_gc_duration_count{job="frontend"}[5m]))` |
| 15026 | Astronomy Shop · Shipping | Spanmetrics calls/s | 1h | 0 | http=0 status=None timed out | `sum(rate(traces_span_metrics_calls{job="shipping"}[5m]))` |
| 15026 | Astronomy Shop · Recommendation (Python) | Spanmetrics calls/s | 1h | 0 | http=0 status=None timed out | `sum(rate(traces_span_metrics_calls{job="recommendation"}[5m]))` |
| 15026 | Softprobe · Classic histogram series | HTTP server duration _bucket (sample) | 1h | 0 | http=0 status=None timed out | `sum by (le) (http_server_request_duration_bucket)` |
| 15026 | Astronomy Shop · Cart (.NET) | ASP.NET routing match attempts | 1h | 0 | http=0 status=None timed out | `sum(rate(aspnetcore_routing_match_attempts{job="cart"}[5m]))` |
| 15025 | Astronomy Shop · Ad (Java) | JVM threads / CPU util | 1h | 0 | http=0 status=None timed out | `jvm_thread_count{job="ad"}` |
| 15025 | Astronomy Shop · Frontend (Node.js) | Spanmetrics calls/s | 1h | 0 | http=0 status=None timed out | `sum(rate(traces_span_metrics_calls{job="frontend"}[5m]))` |
| 15025 | Astronomy Shop · Frontend (Node.js) | Event loop utilization | 5m | 0 | http=0 status=None timed out | `nodejs_eventloop_utilization{job="frontend"}` |
| 15024 | Astronomy Shop · Checkout (Go) | Go processor limit | 5m | 0 | http=0 status=None timed out | `go_processor_limit{job="checkout"}` |
| 15024 | Softprobe · rate / irate / increase / delta | delta(k6_vus[5m]) | 5m | 0 | http=0 status=None timed out | `delta(k6_vus[5m])` |
| 15024 | Astronomy Shop · Currency & Quote | Quotes (raw) | 5m | 0 | http=0 status=None timed out | `quotes{job="quote"}` |
| 15024 | Astronomy Shop · Shipping | Items shipped/s | 1h | 0 | http=0 status=None timed out | `sum(rate(demo_shipping_items_shipped{job="shipping"}[5m]))` |
| 15024 | Astronomy Shop · GOLD overview | Business: currency conversions/s | 5m | 0 | http=0 status=None timed out | `sum(rate(demo_exchange_conversions_counter[5m]))` |
| 15023 | Softprobe · Aggregations | avg by (job) HTTP duration count rate | 5m | 0 | http=0 status=None timed out | `avg by (job) (rate(http_server_request_duration_count[5m]))` |
| 15023 | Astronomy Shop · GOLD overview | Spanmetrics calls/s by service | 1h | 0 | http=0 status=None timed out | `sum by (job) (rate(traces_span_metrics_calls[5m]))` |
| 15022 | Astronomy Shop · Cart (.NET) | Kestrel active connections | 5m | 0 | http=0 status=None timed out | `kestrel_active_connections{job="cart"}` |
| 15022 | Astronomy Shop · Load generator (k6) | Failed requests (counter) | 1h | 0 | http=0 status=None timed out | `sum(k6_http_req_failed_total)` |
| 15022 | Softprobe · Overview (Astronomy Shop) | ad served rate | 1h | 0 | http=0 status=None timed out | `sum by (category) (rate(demo_ad_served_total[5m]))` |
| 15022 | Astronomy Shop · GOLD overview | Business: payments/s | 5m | 0 | http=0 status=None timed out | `sum(rate(demo_payment_transactions[5m]))` |
| 15022 | Astronomy Shop · Recommendation (Python) | System CPU utilization (host view) | 5m | 0 | http=0 status=None timed out | `avg(system_cpu_utilization{job="recommendation"})` |
| 15022 | Astronomy Shop · Product Catalog | Spanmetrics duration count/s | 5m | 0 | http=0 status=None timed out | `sum(rate(traces_span_metrics_duration_count{job="product-catalog"}[5m]))` |
| 15022 | Softprobe · Aggregations | avg by (job) HTTP duration count rate | 1h | 0 | http=0 status=None timed out | `avg by (job) (rate(http_server_request_duration_count[5m]))` |
| 15021 | Astronomy Shop · Recommendation (Python) | Process memory usage | 5m | 0 | http=0 status=None timed out | `process_memory_usage{job="recommendation"}` |
| 15021 | Astronomy Shop · Cart (.NET) | Add-item latency buckets | 5m | 0 | http=0 status=None timed out | `sum by (le) (rate(demo_cart_add_item_latency_bucket{job="cart"}[5m]))` |
