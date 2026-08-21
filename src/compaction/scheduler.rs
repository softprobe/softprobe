use crate::catalog::DropdownCatalog;
use crate::compaction::executor::MaintenanceExecutor;
use crate::config::Config;
use crate::runtime_engine::DuckLakeScopeResolver;
use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Wake often enough for snapshot expiry without forcing TWCS at that rate.
pub fn scheduler_wake_seconds(
    metadata_enabled: bool,
    compaction_enabled: bool,
    metadata_interval_seconds: u64,
    compaction_interval_seconds: u64,
) -> Option<u64> {
    match (metadata_enabled, compaction_enabled) {
        (false, false) => None,
        (true, false) => Some(metadata_interval_seconds.max(1)),
        (false, true) => Some(compaction_interval_seconds.max(1)),
        (true, true) => Some(
            metadata_interval_seconds
                .min(compaction_interval_seconds)
                .max(1),
        ),
    }
}

/// TWCS/ladder is due on its own interval, not on every metadata tick (AC-Q9).
///
/// Allow 1s early: `tokio::time::interval` often fires a tick slightly before
/// the full period, which previously skipped every other compact and let
/// open-day Parquet climb to 100+ files between merges (Grafana >100ms).
pub fn compaction_due(elapsed_secs: u64, compaction_interval_seconds: u64) -> bool {
    compaction_interval_seconds > 0 && elapsed_secs + 1 >= compaction_interval_seconds
}

pub async fn start_maintenance_scheduler(
    config: &Config,
    dropdown_catalog: Option<Arc<DropdownCatalog>>,
    scope_registry: Option<DuckLakeScopeResolver>,
) -> Result<Option<JoinHandle<()>>> {
    let metadata_enabled = config.maintenance.metadata_enabled;
    let compaction_enabled = config.maintenance.enabled;
    let Some(interval_seconds) = scheduler_wake_seconds(
        metadata_enabled,
        compaction_enabled,
        config.maintenance.metadata_interval_seconds,
        config.maintenance.interval_seconds,
    ) else {
        return Ok(None);
    };
    let compact_interval = config.maintenance.interval_seconds.max(1);

    let executor = MaintenanceExecutor::new(config, dropdown_catalog, scope_registry).await?;
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_seconds));
        // Long TWCS merges (multi-second) must not Burst-catch-up into a
        // compaction=false metadata-only pass that wastes the next slot —
        // that pattern left open-day Parquet at 100+ files for Grafana.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // First tick should compact; then wait `compact_interval`.
        let mut last_compact = Instant::now()
            .checked_sub(Duration::from_secs(compact_interval))
            .unwrap_or_else(Instant::now);
        loop {
            ticker.tick().await;
            // Compact whenever the TWCS interval has elapsed (2s early slack for
            // timer jitter). Do not require a perfect second boundary — missed
            // Burst/Delay ticks previously left open-day files at 100+.
            let run_compaction = compaction_enabled
                && last_compact.elapsed() + Duration::from_secs(2)
                    >= Duration::from_secs(compact_interval);
            match executor.run_pass(run_compaction).await {
                Ok(summary) => {
                    if run_compaction {
                        last_compact = Instant::now();
                    }
                    info!(
                        "Maintenance run complete for {} tables (compaction={})",
                        summary.tables.len(),
                        run_compaction
                    );
                }
                Err(err) => {
                    warn!("Maintenance run failed: {}", err);
                }
            }
        }
    });

    Ok(Some(handle))
}

#[cfg(test)]
mod tests {
    use super::{compaction_due, scheduler_wake_seconds, start_maintenance_scheduler};
    use crate::config::Config;

    #[tokio::test]
    async fn scheduler_skips_when_compaction_and_metadata_disabled() {
        let mut c = Config::default();
        c.maintenance.enabled = false;
        c.maintenance.metadata_enabled = false;
        let out = start_maintenance_scheduler(&c, None, None)
            .await
            .expect("scheduler");
        assert!(out.is_none());
    }

    #[test]
    fn wake_uses_metadata_interval_when_both_enabled() {
        let cfg = Config::default();
        assert_eq!(cfg.maintenance.metadata_interval_seconds, 60);
        assert_eq!(cfg.maintenance.interval_seconds, 300);
        assert_eq!(
            scheduler_wake_seconds(true, true, 60, 300),
            Some(60),
            "wake for expiry; TWCS must not inherit this as its merge period"
        );
    }

    #[test]
    fn twcs_does_not_run_every_metadata_tick() {
        assert!(!compaction_due(0, 300));
        assert!(!compaction_due(60, 300));
        assert!(!compaction_due(298, 300));
        assert!(
            compaction_due(299, 300),
            "1s early slack for interval jitter"
        );
        assert!(compaction_due(300, 300));
        assert!(compaction_due(301, 300));
    }
}
