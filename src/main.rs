use std::env;

use anyhow::Result;
use rustper::config::Config;
use tracing_subscriber::EnvFilter;

/// The hot path allocates in bursts (arena chunks per batch, librdkafka's own
/// queues). jemalloc handles that pattern better than the system allocator and
/// returns pages more predictably, which shows up directly in RSS.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let config_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "config/local.toml".to_owned());
    rustper::topology::run(Config::load(config_path)?).await
}
