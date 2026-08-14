//! Short-TTL Prometheus query_range result cache.
//!
//! Absorbs Grafana refresh storms that repeat the same selector window for
//! a few seconds. Tenant-scoped keys only; no cross-tenant entries.

use once_cell::sync::Lazy;
use serde_json::Value;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const TTL: Duration = Duration::from_secs(15);
const MAX_ENTRIES: usize = 256;
const TIME_BUCKET_MS: i64 = 5_000;

static CACHE: Lazy<Mutex<Inner>> = Lazy::new(|| Mutex::new(Inner::default()));

#[derive(Default)]
struct Inner {
    entries: HashMap<u64, Entry>,
}

struct Entry {
    data: Value,
    expires: Instant,
}

pub fn cache_key(tenant_id: &str, query: &str, start_ms: i64, end_ms: i64, step_ms: i64) -> u64 {
    let bucket = |ms: i64| ms.div_euclid(TIME_BUCKET_MS) * TIME_BUCKET_MS;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    tenant_id.hash(&mut hasher);
    query.hash(&mut hasher);
    bucket(start_ms).hash(&mut hasher);
    bucket(end_ms).hash(&mut hasher);
    step_ms.hash(&mut hasher);
    hasher.finish()
}

pub async fn get(key: u64) -> Option<Value> {
    let mut guard = CACHE.lock().await;
    let now = Instant::now();
    if let Some(entry) = guard.entries.get(&key) {
        if entry.expires > now {
            return Some(entry.data.clone());
        }
    }
    guard.entries.retain(|_, e| e.expires > now);
    None
}

pub async fn put(key: u64, data: Value) {
    let mut guard = CACHE.lock().await;
    let now = Instant::now();
    guard.entries.retain(|_, e| e.expires > now);
    if guard.entries.len() >= MAX_ENTRIES {
        let drop_n = MAX_ENTRIES / 2;
        let keys: Vec<u64> = guard.entries.keys().copied().take(drop_n).collect();
        for k in keys {
            guard.entries.remove(&k);
        }
    }
    guard.entries.insert(
        key,
        Entry {
            data,
            expires: now + TTL,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trip_and_tenant_isolation_key() {
        let k1 = cache_key("t1", "up", 1000, 2000, 15_000);
        let k2 = cache_key("t2", "up", 1000, 2000, 15_000);
        assert_ne!(k1, k2);
        put(k1, Value::String("a".into())).await;
        assert_eq!(get(k1).await, Some(Value::String("a".into())));
        assert_eq!(get(k2).await, None);
    }

    #[test]
    fn time_bucket_collapses_near_windows() {
        let a = cache_key("t", "up", 1_000, 120_000, 15_000);
        let b = cache_key("t", "up", 1_100, 120_100, 15_000);
        assert_eq!(a, b);
        let c = cache_key("t", "up", 10_000, 130_000, 15_000);
        assert_ne!(a, c);
    }
}
