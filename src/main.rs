use std::env;

use anyhow::Result;
use rustper::config::Config;
use tracing_subscriber::EnvFilter;

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
