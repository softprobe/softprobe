//! Deterministic ordering helpers for protocol response encoding.

use std::cmp::Ordering;

/// Sort label pairs by key, then value (Prometheus/Loki style stability).
pub fn cmp_label_pairs(a: &(String, String), b: &(String, String)) -> Ordering {
    a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1))
}

/// Sort samples by timestamp ascending, then value bits for stability.
pub fn cmp_samples_by_ts(a: &(i64, f64), b: &(i64, f64)) -> Ordering {
    a.0.cmp(&b.0).then_with(|| a.1.total_cmp(&b.1))
}

/// Sort series identity strings lexicographically.
pub fn sort_series_ids(ids: &mut [String]) {
    ids.sort();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_pairs_sort_by_key_then_value() {
        let mut pairs = vec![
            ("b".into(), "2".into()),
            ("a".into(), "2".into()),
            ("a".into(), "1".into()),
        ];
        pairs.sort_by(cmp_label_pairs);
        assert_eq!(
            pairs,
            vec![
                ("a".into(), "1".into()),
                ("a".into(), "2".into()),
                ("b".into(), "2".into()),
            ]
        );
    }

    #[test]
    fn samples_sort_by_timestamp() {
        let mut samples = vec![(20, 1.0), (10, 9.0), (10, 1.0)];
        samples.sort_by(cmp_samples_by_ts);
        assert_eq!(samples, vec![(10, 1.0), (10, 9.0), (20, 1.0)]);
    }
}
