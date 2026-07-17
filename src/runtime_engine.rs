// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity appears only at auth -> RuntimeEngine mapping.
// Operational APIs MUST NOT accept tenant_id or scope parameters.
// ============================================================================

//! Per-tenant [`RuntimeEngine`] cache and tenant-bound session helper.

use crate::authn::TenantInfo;
use crate::catalog::DropdownCatalog;
use crate::config::{Config, DuckLakeConfig};
use crate::control_plane::ControlPlaneRuntime;
use crate::ingest_engine::{IngestEngine, IngestPipeline};
use crate::promotion::{
    ensure_promotion_metadata_tables, load_active_telemetry_columns_manifests,
    PromotionSpecLoadError, TelemetryColumnsManifest,
};
use crate::query::{self as query_mod, QueryEngine};
use crate::session_redis::{RedisStore, Session};
use crate::storage::Storage;
use anyhow::{anyhow, bail, Context, Result};
use dashmap::DashMap;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

pub type TenantId = String;

/// Redis session store with a fixed tenant id (keys use `session:{tenantId}:` only inside [`RedisStore`]).
pub struct TenantSessionStore {
    tenant_id: String,
    store: Arc<Mutex<RedisStore>>,
}

impl TenantSessionStore {
    pub fn new(tenant_id: String, store: Arc<Mutex<RedisStore>>) -> Self {
        Self { tenant_id, store }
    }

    pub async fn create(&self, mode: &str) -> Option<Session> {
        self.store.lock().await.create(&self.tenant_id, mode).await
    }

    pub async fn list(&self) -> Vec<Session> {
        self.store.lock().await.list(&self.tenant_id).await
    }

    pub async fn get(&self, id: &str) -> Option<Session> {
        self.store.lock().await.get(&self.tenant_id, id).await
    }

    pub async fn close(&self, id: &str) -> bool {
        self.store.lock().await.close(&self.tenant_id, id).await
    }

    pub async fn load_case(&self, id: &str, loaded_case: Vec<u8>) -> Option<Session> {
        self.store
            .lock()
            .await
            .load_case(&self.tenant_id, id, loaded_case)
            .await
    }

    pub async fn apply_policy(&self, id: &str, policy: Vec<u8>) -> Option<Session> {
        self.store
            .lock()
            .await
            .apply_policy(&self.tenant_id, id, policy)
            .await
    }

    pub async fn apply_rules(&self, id: &str, rules: Vec<u8>) -> Option<Session> {
        self.store
            .lock()
            .await
            .apply_rules(&self.tenant_id, id, rules)
            .await
    }

    pub async fn apply_fixtures_auth(&self, id: &str, fixtures: Vec<u8>) -> Option<Session> {
        self.store
            .lock()
            .await
            .apply_fixtures_auth(&self.tenant_id, id, fixtures)
            .await
    }

    pub async fn record_injected_spans(&self, id: &str, n: i64) -> Option<Session> {
        self.store
            .lock()
            .await
            .record_injected_spans(&self.tenant_id, id, n)
            .await
    }

    pub async fn record_strict_miss(&self, id: &str, n: i64) -> Option<Session> {
        self.store
            .lock()
            .await
            .record_strict_miss(&self.tenant_id, id, n)
            .await
    }
}

/// One tenant's ingest + query + storage + sessions (canonical tenant-bound surface).
pub struct RuntimeEngine {
    pub tenant_id: String,
    pub scope: DuckLakeScope,
    pub storage: Arc<Storage>,
    pub ingest: Arc<IngestEngine>,
    pub query: Arc<QueryEngine>,
    /// Present when the process runs with Redis control-plane wiring.
    pub sessions: Option<Arc<TenantSessionStore>>,
    pub dropdown_catalog: Option<Arc<DropdownCatalog>>,
}

/// Global cache: `tenantId` -> tenant-bound runtime (unbounded until restart).
pub struct RuntimeEngineManager {
    config: Arc<Config>,
    engines: DashMap<String, Arc<RuntimeEngine>>,
    creation_locks: DashMap<String, Arc<Mutex<()>>>,
    control_plane: Option<ControlPlaneRuntime>,
    scope_registry: Option<DuckLakeScopeResolver>,
    #[cfg(test)]
    build_counter: AtomicUsize,
}

impl RuntimeEngineManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<Config>,
        control_plane: Option<ControlPlaneRuntime>,
        scope_registry: Option<DuckLakeScopeResolver>,
    ) -> Self {
        Self {
            config,
            engines: DashMap::new(),
            creation_locks: DashMap::new(),
            control_plane,
            scope_registry,
            #[cfg(test)]
            build_counter: AtomicUsize::new(0),
        }
    }

    pub fn control_plane(&self) -> Option<&ControlPlaneRuntime> {
        self.control_plane.as_ref()
    }

    pub fn scope_registry(&self) -> Option<&DuckLakeScopeResolver> {
        self.scope_registry.as_ref()
    }

    /// Drop cached engine (e.g. after provisioning changes scope).
    pub fn invalidate(&self, tenant_id: &str) {
        self.engines.remove(tenant_id);
    }

    #[cfg(test)]
    pub fn build_count(&self) -> usize {
        self.build_counter.load(Ordering::Relaxed)
    }

    /// Resolve registry scope (when configured) and return or construct a cached [`RuntimeEngine`].
    pub async fn engine_for(&self, tenant_id: &str) -> Result<Arc<RuntimeEngine>> {
        if let Some(r) = self.engines.get(tenant_id) {
            return Ok(r.clone());
        }
        let lock = self
            .creation_locks
            .entry(tenant_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _hold = lock.lock().await;
        if let Some(r) = self.engines.get(tenant_id) {
            return Ok(r.clone());
        }
        let engine = self.build_engine(tenant_id).await?;
        self.engines.insert(tenant_id.to_string(), engine.clone());
        Ok(engine)
    }

    pub async fn engine_for_tenant(&self, tenant: &TenantInfo) -> Result<Arc<RuntimeEngine>> {
        self.engine_for(&tenant.tenant_id).await
    }

    async fn build_engine(&self, tenant_id: &str) -> Result<Arc<RuntimeEngine>> {
        #[cfg(test)]
        self.build_counter.fetch_add(1, Ordering::Relaxed);
        let resolver = self.scope_registry.as_ref();
        let scope = if let Some(resolver) = resolver {
            resolver.resolve_or_create(tenant_id).await?
        } else {
            let ducklake = self.config.ducklake_or_default();
            DuckLakeScope {
                metadata_schema: ducklake.metadata_schema,
                data_path: ducklake.data_path,
            }
        };

        let sessions = self.control_plane.as_ref().map(|cp| {
            Arc::new(TenantSessionStore::new(
                tenant_id.to_string(),
                cp.session_store.clone(),
            ))
        });

        let dropdown_catalog = DropdownCatalog::connect(self.config.as_ref()).await?;
        let storage = Arc::new(
            IngestPipeline::build_tenant_storage(
                self.config.as_ref(),
                dropdown_catalog.clone(),
                resolver.cloned(),
                tenant_id.to_string(),
                scope.clone(),
            )
            .await?,
        );
        let ingest = Arc::new(IngestEngine::from_storage(storage.clone()));
        let query = Arc::new(
            query_mod::create_query_engine_for_scope(self.config.as_ref(), storage.clone(), &scope)
                .await?,
        );
        Ok(Arc::new(RuntimeEngine {
            tenant_id: tenant_id.to_string(),
            scope,
            storage,
            ingest,
            query,
            sessions,
            dropdown_catalog,
        }))
    }
}

/// DuckLake storage scope resolved from the durable scope registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuckLakeScope {
    /// SQL schema inside the shared Postgres DuckLake metadata database.
    pub metadata_schema: String,
    /// Object-store data path for this deployment (from config).
    pub data_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeProvisioningRequest {
    pub scope_id: String,
    pub metadata_schema: String,
    pub data_path: String,
}

#[derive(Clone)]
pub struct DuckLakeScopeResolver {
    pool: Pool,
    registry_schema: String,
    default_scope: DuckLakeScope,
}

impl DuckLakeScopeResolver {
    pub async fn connect(config: &Config) -> Result<Option<Self>> {
        let dl = config.ducklake_or_default();
        if dl.catalog_type != "postgres" {
            return Ok(None);
        }
        let resolver = Self::build_pool(&dl)?;
        resolver.ensure_registry().await?;
        resolver.ensure_scope().await?;
        Ok(Some(resolver))
    }

    fn build_pool(dl: &DuckLakeConfig) -> Result<Self> {
        let mut pg = tokio_postgres::Config::new();
        parse_postgres_kv_config(&mut pg, &dl.metadata_path)?;
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
            ..Default::default()
        };
        let mgr = Manager::from_config(pg, NoTls, mgr_config);
        let pool = Pool::builder(mgr).max_size(8).build()?;
        let default_scope = DuckLakeScope {
            metadata_schema: dl.metadata_schema.clone(),
            data_path: dl.data_path.clone(),
        };
        Ok(Self {
            pool,
            registry_schema: dl.metadata_schema.clone(),
            default_scope,
        })
    }

    async fn ensure_scope(&self) -> Result<()> {
        self.ensure_scope_tables(&self.default_scope).await
    }

    async fn ensure_registry(&self) -> Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                &format!(
                    "CREATE SCHEMA IF NOT EXISTS {};",
                    quote_pg_ident(&self.registry_schema)
                ),
                &[],
            )
            .await?;
        client
            .execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS {}.scope_registry (
  scope_id TEXT PRIMARY KEY,
  ducklake_metadata_schema TEXT NOT NULL UNIQUE,
  data_path TEXT NOT NULL,
  provisioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);"#,
                    quote_pg_ident(&self.registry_schema)
                ),
                &[],
            )
            .await?;
        Ok(())
    }

    async fn ensure_scope_tables(&self, scope: &DuckLakeScope) -> Result<()> {
        let client = self.pool.get().await?;
        ensure_promotion_metadata_tables(&client, &scope.metadata_schema).await?;
        Ok(())
    }

    /// Resolve the DuckLake scope for `scope_id` from the durable registry.
    ///
    /// When `scope_id` is empty, returns the process-default scope.
    pub async fn resolve_or_create(&self, scope_id: &str) -> Result<DuckLakeScope> {
        if scope_id.trim().is_empty() {
            self.ensure_scope().await?;
            return Ok(self.default_scope.clone());
        }
        self.resolve_scope(scope_id).await
    }

    /// Idempotently create or verify a scope registry entry and its metadata tables.
    pub async fn provision_scope(
        &self,
        request: ScopeProvisioningRequest,
    ) -> Result<DuckLakeScope> {
        if request.scope_id.trim().is_empty() {
            bail!("scope_id is required");
        }
        if request.metadata_schema.trim().is_empty() {
            bail!("ducklake metadata schema is required");
        }
        if request.data_path.trim().is_empty() {
            bail!("ducklake data path is required");
        }

        let scope = DuckLakeScope {
            metadata_schema: request.metadata_schema,
            data_path: request.data_path,
        };
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                &format!(
                    r#"INSERT INTO {}.scope_registry
  (scope_id, ducklake_metadata_schema, data_path)
VALUES ($1, $2, $3)
ON CONFLICT (scope_id) DO UPDATE SET
  updated_at = {}.scope_registry.updated_at
WHERE {}.scope_registry.ducklake_metadata_schema = EXCLUDED.ducklake_metadata_schema
  AND {}.scope_registry.data_path = EXCLUDED.data_path
RETURNING scope_id;"#,
                    quote_pg_ident(&self.registry_schema),
                    quote_pg_ident(&self.registry_schema),
                    quote_pg_ident(&self.registry_schema),
                    quote_pg_ident(&self.registry_schema)
                ),
                &[&request.scope_id, &scope.metadata_schema, &scope.data_path],
            )
            .await?;
        if row.is_none() {
            bail!("scope conflict for scope {}", request.scope_id);
        }

        self.ensure_scope_tables(&scope).await?;
        Ok(scope)
    }

    /// Resolve an existing scope registry entry.
    pub async fn resolve_scope(&self, scope_id: &str) -> Result<DuckLakeScope> {
        if scope_id.trim().is_empty() {
            bail!("scope_id is required");
        }

        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT ducklake_metadata_schema, data_path FROM {}.scope_registry WHERE scope_id = $1;",
                    quote_pg_ident(&self.registry_schema)
                ),
                &[&scope_id],
            )
            .await?;
        let Some(row) = row else {
            bail!("unknown scope: {scope_id}");
        };
        let scope = DuckLakeScope {
            metadata_schema: row.get(0),
            data_path: row.get(1),
        };
        self.ensure_scope_tables(&scope).await?;
        Ok(scope)
    }

    /// List all provisioned DuckLake scopes from the registry (for maintenance).
    pub async fn list_scopes(&self) -> Result<Vec<DuckLakeScope>> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                &format!(
                    "SELECT ducklake_metadata_schema, data_path FROM {}.scope_registry ORDER BY scope_id;",
                    quote_pg_ident(&self.registry_schema)
                ),
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| DuckLakeScope {
                metadata_schema: row.get(0),
                data_path: row.get(1),
            })
            .collect())
    }

    /// Resolve scope and load active telemetry column manifests from Postgres.
    pub async fn load_active_telemetry_columns_manifests(
        &self,
        scope_id: &str,
    ) -> Result<(DuckLakeScope, Vec<TelemetryColumnsManifest>)> {
        let scope = if scope_id.is_empty() {
            self.resolve_or_create(scope_id).await?
        } else {
            self.resolve_scope(scope_id).await?
        };
        let client = self.pool.get().await?;
        let manifests = load_active_telemetry_columns_manifests(&client, &scope.metadata_schema)
            .await
            .map_err(|err| match err {
                PromotionSpecLoadError::Postgres(e) => anyhow!(e),
                PromotionSpecLoadError::InvalidRowManifest { spec_id, source } => {
                    anyhow!("promotion spec {spec_id} is invalid: {source}")
                }
            })?;
        Ok((scope, manifests))
    }

    /// Load active telemetry promotion manifests for an already bound scope.
    pub async fn load_active_telemetry_columns_manifests_for_scope(
        &self,
        scope: &DuckLakeScope,
    ) -> Result<Vec<TelemetryColumnsManifest>> {
        let client = self.pool.get().await?;
        let manifests = load_active_telemetry_columns_manifests(&client, &scope.metadata_schema)
            .await
            .map_err(|err| match err {
                PromotionSpecLoadError::Postgres(e) => anyhow!(e),
                PromotionSpecLoadError::InvalidRowManifest { spec_id, source } => {
                    anyhow!("promotion spec {spec_id} is invalid: {source}")
                }
            })?;
        Ok(manifests)
    }

    /// Record one successfully applied telemetry promotion manifest in the metadata schema.
    pub async fn record_active_telemetry_promotion_spec(
        &self,
        scope: &DuckLakeScope,
        manifest_yaml: &str,
        target_tables: &[String],
    ) -> Result<String> {
        let mut hasher = DefaultHasher::new();
        manifest_yaml.hash(&mut hasher);
        let manifest_hash = format!("{:016x}", hasher.finish());
        let spec_id = format!("telemetry_columns_{manifest_hash}");
        let client = self.pool.get().await?;
        client
            .execute(
                &format!(
                    r#"INSERT INTO "{}".promotion_specs
  (spec_id, spec_version, target_kind, target_tables, manifest_json, manifest_hash, status)
VALUES ($1, 'softprobe.promotion.v1', 'telemetry_columns', $2, $3, $4, 'active')
ON CONFLICT (spec_id) DO UPDATE SET
  target_tables = EXCLUDED.target_tables,
  manifest_json = EXCLUDED.manifest_json,
  manifest_hash = EXCLUDED.manifest_hash,
  status = 'active',
  applied_at = NOW();"#,
                    scope.metadata_schema.replace('"', "\"\"")
                ),
                &[
                    &spec_id,
                    &target_tables.join(","),
                    &manifest_yaml,
                    &manifest_hash,
                ],
            )
            .await?;
        Ok(spec_id)
    }

    /// Record one successfully applied business table promotion manifest in metadata.
    pub async fn record_active_business_promotion_spec(
        &self,
        scope: &DuckLakeScope,
        manifest_yaml: &str,
        table_name: &str,
    ) -> Result<String> {
        let mut hasher = DefaultHasher::new();
        manifest_yaml.hash(&mut hasher);
        let manifest_hash = format!("{:016x}", hasher.finish());
        let spec_id = format!("business_table_{}_{}", table_name, manifest_hash);
        let client = self.pool.get().await?;
        client
            .execute(
                &format!(
                    r#"INSERT INTO "{}".promotion_specs
  (spec_id, spec_version, target_kind, target_tables, manifest_json, manifest_hash, status)
VALUES ($1, 'softprobe.promotion.v1', 'business_table', $2, $3, $4, 'active')
ON CONFLICT (spec_id) DO UPDATE SET
  target_tables = EXCLUDED.target_tables,
  manifest_json = EXCLUDED.manifest_json,
  manifest_hash = EXCLUDED.manifest_hash,
  status = 'active',
  applied_at = NOW();"#,
                    scope.metadata_schema.replace('"', "\"\"")
                ),
                &[&spec_id, &table_name, &manifest_yaml, &manifest_hash],
            )
            .await?;
        Ok(spec_id)
    }
}

fn parse_postgres_kv_config(pg: &mut tokio_postgres::Config, metadata_path: &str) -> Result<()> {
    let conn_str = metadata_path
        .trim()
        .strip_prefix("postgres:")
        .unwrap_or(metadata_path.trim())
        .trim();
    for part in conn_str.split_whitespace() {
        let (k, v) = part
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid postgres kv segment in metadata_path: {}", part))?;
        let v = v.trim_matches('\'');
        match k {
            "host" => {
                pg.host(v);
            }
            "port" => {
                pg.port(v.parse().context("metadata_path port")?);
            }
            "dbname" | "database" => {
                pg.dbname(v);
            }
            "user" | "username" => {
                pg.user(v);
            }
            "password" => {
                pg.password(v);
            }
            _ => {}
        }
    }
    Ok(())
}

fn quote_pg_ident(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}
