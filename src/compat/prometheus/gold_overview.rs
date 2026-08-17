//! Astronomy Shop GOLD overview PromQL exprs (metrics-timeseries-layout §10.2 / AC-Q8).
//!
//! Source of truth for the panel list: `tests/compat/grafana/dashboards/astronomy/astronomy-shop-overview.json`.

/// Fifteen GOLD overview range exprs exercised by T-Q8 / F-gold.
pub const GOLD_OVERVIEW_EXPRS: &[&str] = &[
    "sum by (job) (rate(http_server_request_duration_count[5m]))",
    "sum by (job) (rate(traces_span_metrics_calls[5m]))",
    "sum by (job) (rate(rpc_server_call_duration_count[5m]))",
    "sum by (job) (rate(http_client_request_duration_count[5m]))",
    "sum by (category) (rate(demo_ad_served_total[5m]))",
    "sum(rate(demo_cart_add_item_latency_count[5m]))",
    "sum(rate(demo_payment_transactions[5m]))",
    "sum(rate(demo_shipping_items_shipped[5m]))",
    "sum(rate(demo_exchange_conversions_counter[5m]))",
    "sum(rate(quotes[5m]))",
    "k6_vus",
    "sum(rate(k6_iterations[5m]))",
    "sum(k6_http_req_failed_total)",
    "topk(8, avg by (container_name) (container_cpu_utilization))",
    "topk(8, avg by (container_name) (container_memory_percent))",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::promql::parse_promql;

    /// T-Q8 / AC-Q8: design §10.2 list is exactly 15 exprs and each parses.
    #[test]
    fn gold_overview_exprs_are_fifteen_and_parse() {
        assert_eq!(
            GOLD_OVERVIEW_EXPRS.len(),
            15,
            "AC-Q8: expected 15 GOLD exprs from astronomy-shop-overview.json"
        );
        for expr in GOLD_OVERVIEW_EXPRS {
            parse_promql(expr).unwrap_or_else(|e| panic!("AC-Q8 parse failed for `{expr}`: {e:?}"));
        }
    }
}
