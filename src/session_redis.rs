// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

//! Redis-backed session store (control-plane mode), matching Go `RedisStore` key layout.

use anyhow::Result;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::RngCore;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionStats {
    #[serde(default)]
    pub injected_spans: i64,
    #[serde(default)]
    pub extracted_spans: i64,
    #[serde(default)]
    pub strict_misses: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Session {
    pub id: String,
    pub mode: String,
    pub revision: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub loaded_case: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policy: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fixtures_auth: Vec<u8>,
    #[serde(default)]
    pub stats: SessionStats,
}

#[derive(Clone)]
pub struct RedisStore {
    r: redis::aio::ConnectionManager,
    ttl: Duration,
}

/// Redis key for the session JSON blob: `session:{tenantId}:{sessionId}` (hosted-service doc §4.1).
pub fn redis_session_blob_key(tenant_id: &str, session_id: &str) -> String {
    format!("session:{tenant_id}:{session_id}")
}

/// Redis key for per-session extract path list: `session:{tenantId}:{sessionId}:extracts`.
pub fn redis_session_extracts_key(tenant_id: &str, session_id: &str) -> String {
    format!("session:{tenant_id}:{session_id}:extracts")
}

fn ids_ok(tenant_id: &str, session_id: &str) -> bool {
    !tenant_id.is_empty() && !session_id.is_empty()
}

impl RedisStore {
    /// `redis_url` e.g. `redis://127.0.0.1:6379` or `redis://:pass@host:6379`
    pub async fn connect_url(redis_url: &str, ttl: Duration) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let r = redis::aio::ConnectionManager::new(client).await?;
        Ok(Self { r, ttl })
    }

    pub async fn connect_host_port(
        host: &str,
        port: u16,
        password: Option<&str>,
        ttl: Duration,
    ) -> Result<Self> {
        let url = match password {
            Some(pw) => format!("redis://:{pw}@{host}:{port}/"),
            None => format!("redis://{host}:{port}/"),
        };
        Self::connect_url(&url, ttl).await
    }

    pub async fn create(&mut self, tenant_id: &str, mode: &str) -> Option<Session> {
        if tenant_id.is_empty() {
            return None;
        }
        let id = new_session_id();
        let doc = Session {
            id: id.clone(),
            mode: mode.to_string(),
            revision: 0,
            loaded_case: Vec::new(),
            policy: Vec::new(),
            rules: Vec::new(),
            fixtures_auth: Vec::new(),
            stats: SessionStats::default(),
        };
        self.save(tenant_id, &doc).await.ok()?;
        Some(doc)
    }

    pub async fn get(&mut self, tenant_id: &str, id: &str) -> Option<Session> {
        if !ids_ok(tenant_id, id) {
            return None;
        }
        let key = redis_session_blob_key(tenant_id, id);
        let data: Option<Vec<u8>> = self.r.get(&key).await.ok().flatten();
        data.and_then(|b| serde_json::from_slice(&b).ok())
    }

    async fn save(&mut self, tenant_id: &str, doc: &Session) -> Result<()> {
        if !ids_ok(tenant_id, &doc.id) {
            anyhow::bail!("session save: empty tenant_id or session id");
        }
        let key = redis_session_blob_key(tenant_id, &doc.id);
        let data = serde_json::to_vec(doc)?;
        let _: () = self.r.set_ex(&key, data, self.ttl.as_secs()).await?;
        Ok(())
    }

    pub async fn close(&mut self, tenant_id: &str, id: &str) -> bool {
        if !ids_ok(tenant_id, id) {
            return false;
        }
        let sk = redis_session_blob_key(tenant_id, id);
        let ek = redis_session_extracts_key(tenant_id, id);
        let n: i64 = redis::cmd("DEL")
            .arg(&sk)
            .arg(&ek)
            .query_async(&mut self.r)
            .await
            .unwrap_or(0);
        n > 0
    }

    pub async fn load_case(
        &mut self,
        tenant_id: &str,
        id: &str,
        loaded_case: Vec<u8>,
    ) -> Option<Session> {
        self.mutate(tenant_id, id, |d| d.loaded_case = loaded_case)
            .await
    }

    pub async fn apply_policy(
        &mut self,
        tenant_id: &str,
        id: &str,
        policy: Vec<u8>,
    ) -> Option<Session> {
        self.mutate(tenant_id, id, |d| d.policy = policy).await
    }

    pub async fn apply_rules(
        &mut self,
        tenant_id: &str,
        id: &str,
        rules: Vec<u8>,
    ) -> Option<Session> {
        self.mutate(tenant_id, id, |d| d.rules = rules).await
    }

    pub async fn apply_fixtures_auth(
        &mut self,
        tenant_id: &str,
        id: &str,
        fixtures: Vec<u8>,
    ) -> Option<Session> {
        self.mutate(tenant_id, id, |d| d.fixtures_auth = fixtures)
            .await
    }

    async fn mutate<F>(&mut self, tenant_id: &str, id: &str, f: F) -> Option<Session>
    where
        F: FnOnce(&mut Session),
    {
        if !ids_ok(tenant_id, id) {
            return None;
        }
        let mut doc = self.get(tenant_id, id).await?;
        doc.revision += 1;
        f(&mut doc);
        self.save(tenant_id, &doc).await.ok()?;
        Some(doc)
    }

    pub async fn list(&mut self, tenant_id: &str) -> Vec<Session> {
        if tenant_id.is_empty() {
            return Vec::new();
        }
        let pattern = format!("session:{tenant_id}:*");
        let keys: Vec<String> = self.r.keys(&pattern).await.unwrap_or_default();
        let mut out = Vec::new();
        for key in keys {
            if key.ends_with(":extracts") {
                continue;
            }
            let data: Option<Vec<u8>> = self.r.get(&key).await.ok().flatten();
            if let Some(b) = data {
                if let Ok(s) = serde_json::from_slice::<Session>(&b) {
                    out.push(s);
                }
            }
        }
        out
    }

    pub async fn record_injected_spans(
        &mut self,
        tenant_id: &str,
        id: &str,
        n: i64,
    ) -> Option<Session> {
        self.mutate_stats(tenant_id, id, |s| s.injected_spans += n)
            .await
    }

    pub async fn record_extracted_spans(
        &mut self,
        tenant_id: &str,
        id: &str,
        n: i64,
    ) -> Option<Session> {
        self.mutate_stats(tenant_id, id, |s| s.extracted_spans += n)
            .await
    }

    pub async fn record_strict_miss(
        &mut self,
        tenant_id: &str,
        id: &str,
        n: i64,
    ) -> Option<Session> {
        self.mutate_stats(tenant_id, id, |s| s.strict_misses += n)
            .await
    }

    async fn mutate_stats<F>(&mut self, tenant_id: &str, id: &str, f: F) -> Option<Session>
    where
        F: FnOnce(&mut SessionStats),
    {
        if !ids_ok(tenant_id, id) {
            return None;
        }
        let mut doc = self.get(tenant_id, id).await?;
        f(&mut doc.stats);
        self.save(tenant_id, &doc).await.ok()?;
        Some(doc)
    }
}

fn new_session_id() -> String {
    let mut raw = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut raw);
    format!("sess_{}", URL_SAFE_NO_PAD.encode(raw))
}

impl Default for SessionStats {
    fn default() -> Self {
        Self {
            injected_spans: 0,
            extracted_spans: 0,
            strict_misses: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{redis_session_blob_key, redis_session_extracts_key, Session, SessionStats};

    #[test]
    fn session_redis_keys_embed_tenant_and_session_segments() {
        assert_eq!(redis_session_blob_key("t1", "sess_x"), "session:t1:sess_x");
        assert_eq!(
            redis_session_extracts_key("t1", "sess_x"),
            "session:t1:sess_x:extracts"
        );
    }

    #[test]
    fn session_roundtrip_json() {
        let s = Session {
            id: "id1".into(),
            mode: "replay".into(),
            revision: 3,
            loaded_case: b"case".to_vec(),
            policy: b"pol".to_vec(),
            rules: b"rules".to_vec(),
            fixtures_auth: vec![],
            stats: SessionStats {
                injected_spans: 2,
                extracted_spans: 1,
                strict_misses: 0,
            },
        };
        let v = serde_json::to_vec(&s).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&v).unwrap();
        assert!(
            value.get("tenantId").is_none(),
            "tenant namespacing is in the Redis key, not the session JSON blob"
        );
        let d: Session = serde_json::from_slice(&v).unwrap();
        assert_eq!(d.id, s.id);
        assert_eq!(d.stats.injected_spans, 2);
    }

    #[test]
    fn session_stats_default_zero() {
        let s = SessionStats::default();
        assert_eq!(s.injected_spans, 0);
    }
}
