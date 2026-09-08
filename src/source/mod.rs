mod kafka;

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use std::sync::Arc;

use crate::{config::SourceConfig, metrics::Metrics, topology::Fanout};

#[async_trait]
pub trait Source: Send + 'static {
    async fn run(self: Box<Self>, output: Fanout, shutdown: CancellationToken) -> Result<()>;
}

pub fn build(id: &str, config: &SourceConfig, metrics: Arc<Metrics>) -> Result<Box<dyn Source>> {
    match config {
        SourceConfig::Kafka(config) => Ok(Box::new(kafka::KafkaSource::new(id, config, metrics)?)),
    }
}
