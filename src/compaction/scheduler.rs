use crate::catalog::DropdownCatalog;
use crate::compaction::executor::MaintenanceExecutor;
use crate::config::Config;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub async fn start_maintenance_scheduler(
    config: &Config,
    dropdown_catalog: Option<Arc<DropdownCatalog>>,
) -> Result<Option<JoinHandle<()>>> {
    let metadata_enabled = config.compaction.metadata_maintenance_enabled;
    let compaction_enabled = config.compaction.enabled;

    if !metadata_enabled && !compaction_enabled {
        return Ok(None);
    }

    let interval_seconds = if metadata_enabled && compaction_enabled {
        std::cmp::min(
            config.compaction.metadata_maintenance_interval_seconds,
            config.compaction.compaction_interval_seconds,
        )
    } else if metadata_enabled {
        config.compaction.metadata_maintenance_interval_seconds
    } else {
        config.compaction.compaction_interval_seconds
    };

    let executor = MaintenanceExecutor::new(config, dropdown_catalog).await?;
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_seconds));
        loop {
            ticker.tick().await;
            match executor.run_once().await {
                Ok(summary) => {
                    info!(
                        "Maintenance run complete for {} tables",
                        summary.tables.len()
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
    use super::start_maintenance_scheduler;
    use crate::config::Config;

    #[tokio::test]
    async fn scheduler_skips_when_compaction_and_metadata_disabled() {
        let mut c = Config::default();
        c.compaction.enabled = false;
        c.compaction.metadata_maintenance_enabled = false;
        let out = start_maintenance_scheduler(&c, None)
            .await
            .expect("scheduler");
        assert!(out.is_none());
    }
}
