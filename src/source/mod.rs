mod kafka;

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{config::SourceConfig, topology::Fanout};

#[async_trait]
pub trait Source: Send + 'static {
    async fn run(self: Box<Self>, output: Fanout, shutdown: CancellationToken) -> Result<()>;
}

pub fn build(id: &str, config: &SourceConfig) -> Result<Box<dyn Source>> {
    match config {
        SourceConfig::Kafka(config) => Ok(Box::new(kafka::KafkaSource::new(id, config)?)),
    }
}
