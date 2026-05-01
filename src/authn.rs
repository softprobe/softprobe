//! Resolve API keys to tenant identity via the configured auth HTTP service.

use anyhow::{anyhow, Result};
use dashmap::DashMap;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct TenantInfo {
    pub tenant_id: String,
    pub bucket_name: String,
    pub dataset_id: String,
}

#[derive(Clone)]
pub struct Resolver {
    url: String,
    ttl: Duration,
    cache: Arc<DashMap<String, (TenantInfo, Instant)>>,
    client: reqwest::Client,
}

impl Resolver {
    pub fn new(url: impl Into<String>, ttl: Duration) -> Self {
        Self {
            url: url.into(),
            ttl,
            cache: Arc::new(DashMap::new()),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("http client"),
        }
    }

    pub async fn resolve(&self, api_key: &str) -> Result<TenantInfo> {
        if api_key.is_empty() {
            return Err(anyhow!("authn: empty API key"));
        }

        if let Some(entry) = self.cache.get(api_key) {
            if entry.1.elapsed() < self.ttl {
                return Ok(entry.0.clone());
            }
        }

        let info = self.call_auth_service(api_key).await?;

        self.cache
            .insert(api_key.to_string(), (info.clone(), Instant::now()));
        Ok(info)
    }

    async fn call_auth_service(&self, api_key: &str) -> Result<TenantInfo> {
        let body = serde_json::to_string(&serde_json::json!({ "apiKey": api_key }))?;

        let resp = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| anyhow!("authn: auth service unreachable: {e}"))?;

        #[derive(Deserialize)]
        struct Outer {
            success: bool,
            data: Option<Data>,
        }
        #[derive(Deserialize)]
        struct Data {
            #[serde(rename = "tenantId")]
            tenant_id: String,
            resources: Option<Vec<Resource>>,
        }
        #[derive(Deserialize)]
        struct Resource {
            #[serde(rename = "resourceType")]
            resource_type: String,
            #[serde(rename = "configJson")]
            config_json: String,
        }
        #[derive(Deserialize)]
        struct BqCfg {
            #[serde(rename = "dataset_id")]
            dataset_id: String,
            #[serde(rename = "bucket_name")]
            bucket_name: String,
        }

        let payload: Outer = resp
            .json()
            .await
            .map_err(|e| anyhow!("authn: decode response: {e}"))?;

        if !payload.success {
            return Err(anyhow!("authn: invalid API key"));
        }
        let data = payload
            .data
            .ok_or_else(|| anyhow!("authn: invalid API key"))?;

        let mut info = TenantInfo {
            tenant_id: data.tenant_id.clone(),
            bucket_name: String::new(),
            dataset_id: String::new(),
        };

        if let Some(resources) = data.resources {
            for res in resources {
                if res.resource_type != "BIGQUERY_DATASET"
                    && res.resource_type != "BIGQUERY_STORAGE"
                {
                    continue;
                }
                if let Ok(cfg) = serde_json::from_str::<BqCfg>(&res.config_json) {
                    info.dataset_id = cfg.dataset_id;
                    info.bucket_name = cfg.bucket_name;
                    break;
                }
            }
        }

        if info.tenant_id.is_empty() {
            return Err(anyhow!("authn: auth response missing tenantId"));
        }
        Ok(info)
    }
}
