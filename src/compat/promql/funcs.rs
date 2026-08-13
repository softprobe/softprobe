//! Shared PromQL subset function name sets (parse allowlist + eval dispatch).

/// Range-vector functions that require a matrix selector argument.
pub(crate) fn is_range_vector_fn(name: &str) -> bool {
    matches!(
        name,
        "rate"
            | "irate"
            | "increase"
            | "delta"
            | "idelta"
            | "sum_over_time"
            | "avg_over_time"
            | "min_over_time"
            | "max_over_time"
            | "count_over_time"
            | "last_over_time"
    )
}

/// Instant-vector math helpers.
pub(crate) fn is_math_fn(name: &str) -> bool {
    matches!(name, "abs" | "ceil" | "floor" | "round")
}
