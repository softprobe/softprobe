//! Redis-backed session store (hosted mode), matching Go `RedisStore` key layout.

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
    pub tenant_id: String,
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
    tenant_id: String,
    ttl: Duration,
}

impl RedisStore {
    /// `redis_url` e.g. `redis://127.0.0.1:6379` or `redis://:pass@host:6379`
    pub async fn connect_url(
        redis_url: &str,
        tenant_id: impl Into<String>,
        ttl: Duration,
    ) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let r = redis::aio::ConnectionManager::new(client).await?;
        Ok(Self {
            r,
            tenant_id: tenant_id.into(),
            ttl,
        })
    }

    pub async fn connect_host_port(
        host: &str,
        port: u16,
        password: Option<&str>,
        tenant_id: impl Into<String>,
        ttl: Duration,
    ) -> Result<Self> {
        let url = match password {
            Some(pw) => format!("redis://:{pw}@{host}:{port}/"),
            None => format!("redis://{host}:{port}/"),
        };
        Self::connect_url(&url, tenant_id, ttl).await
    }

    fn session_key(&self, id: &str) -> String {
        format!("session:{}:{}", self.tenant_id, id)
    }

    fn extracts_key(&self, id: &str) -> String {
        format!("session:{}:{}:extracts", self.tenant_id, id)
    }

    pub async fn create(&mut self, mode: &str) -> Session {
        let id = new_session_id();
        let doc = Session {
            id: id.clone(),
            tenant_id: self.tenant_id.clone(),
            mode: mode.to_string(),
            revision: 0,
            loaded_case: Vec::new(),
            policy: Vec::new(),
            rules: Vec::new(),
            fixtures_auth: Vec::new(),
            stats: SessionStats::default(),
        };
        let _ = self.save(&doc).await;
        doc
    }

    pub async fn get(&mut self, id: &str) -> Option<Session> {
        let key = self.session_key(id);
        let data: Option<Vec<u8>> = self.r.get(&key).await.ok().flatten();
        data.and_then(|b| serde_json::from_slice(&b).ok())
    }

    async fn save(&mut self, doc: &Session) -> Result<()> {
        let key = self.session_key(&doc.id);
        let data = serde_json::to_vec(doc)?;
        let _: () = self.r.set_ex(&key, data, self.ttl.as_secs()).await?;
        Ok(())
    }

    pub async fn close(&mut self, id: &str) -> bool {
        let sk = self.session_key(id);
        let ek = self.extracts_key(id);
        let n: i64 = redis::cmd("DEL")
            .arg(&sk)
            .arg(&ek)
            .query_async(&mut self.r)
            .await
            .unwrap_or(0);
        n > 0
    }

    pub async fn load_case(&mut self, id: &str, loaded_case: Vec<u8>) -> Option<Session> {
        self.mutate(id, |d| d.loaded_case = loaded_case).await
    }

    pub async fn apply_policy(&mut self, id: &str, policy: Vec<u8>) -> Option<Session> {
        self.mutate(id, |d| d.policy = policy).await
    }

    pub async fn apply_rules(&mut self, id: &str, rules: Vec<u8>) -> Option<Session> {
        self.mutate(id, |d| d.rules = rules).await
    }

    pub async fn apply_fixtures_auth(&mut self, id: &str, fixtures: Vec<u8>) -> Option<Session> {
        self.mutate(id, |d| d.fixtures_auth = fixtures).await
    }

    async fn mutate<F>(&mut self, id: &str, f: F) -> Option<Session>
    where
        F: FnOnce(&mut Session),
    {
        let mut doc = self.get(id).await?;
        doc.revision += 1;
        f(&mut doc);
        self.save(&doc).await.ok()?;
        Some(doc)
    }

    pub async fn list(&mut self) -> Vec<Session> {
        let pattern = format!("session:{}:*", self.tenant_id);
        let keys: Vec<String> = self.r.keys(&pattern).await.unwrap_or_default();
        let mut out = Vec::new();
        for key in keys {
            if key.ends_with(":extracts") {
                continue;
            }
            let data: Option<Vec<u8>> = self.r.get(&key).await.ok().flatten();
            if let Some(b) = data {
                if let Ok(s) = serde_json::from_slice::<Session>(&b) {
                    if s.tenant_id == self.tenant_id {
                        out.push(s);
                    }
                }
            }
        }
        out
    }

    pub async fn record_injected_spans(&mut self, id: &str, n: i64) -> Option<Session> {
        self.mutate_stats(id, |s| s.injected_spans += n).await
    }

    pub async fn record_extracted_spans(&mut self, id: &str, n: i64) -> Option<Session> {
        self.mutate_stats(id, |s| s.extracted_spans += n).await
    }

    pub async fn record_strict_miss(&mut self, id: &str, n: i64) -> Option<Session> {
        self.mutate_stats(id, |s| s.strict_misses += n).await
    }

    async fn mutate_stats<F>(&mut self, id: &str, f: F) -> Option<Session>
    where
        F: FnOnce(&mut SessionStats),
    {
        let mut doc = self.get(id).await?;
        f(&mut doc.stats);
        self.save(&doc).await.ok()?;
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
