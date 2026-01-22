use anyhow::{anyhow, Result};
use std::future::Future;
use std::time::{Duration, Instant};

/// Poll `check` until it returns `Ok(true)` or timeout is reached.
///
/// - `Ok(false)` keeps polling
/// - `Err(e)` is remembered and polling continues (useful when the system is still warming up)
pub async fn wait_for<F, Fut>(timeout: Duration, interval: Duration, mut check: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let start = Instant::now();
    let mut last_err: Option<anyhow::Error> = None;

    loop {
        match check().await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(err) => last_err = Some(err),
        }

        if start.elapsed() >= timeout {
            if let Some(err) = last_err {
                return Err(anyhow!(
                    "timed out after {:?}: last error: {}",
                    timeout,
                    err
                ));
            }
            return Err(anyhow!("timed out after {:?}", timeout));
        }

        tokio::time::sleep(interval).await;
    }
}
