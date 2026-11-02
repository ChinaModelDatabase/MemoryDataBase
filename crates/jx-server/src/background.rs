use jx_engine::actor::ShardRuntime;
use std::sync::Arc;
use std::time::Duration;

/// Spawn the background TTL expire cycle: every 100ms call
/// `rt.expire_cycle(now, 256)`. Returns a join handle so the server
/// lifecycle can observe/cancel it later.
pub fn spawn_background(rt: Arc<ShardRuntime>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let now = rt.now();
            let removed = rt.expire_cycle(now, 256);
            if removed > 0 {
                tracing::debug!("expired {removed} keys");
            }
        }
    })
}
