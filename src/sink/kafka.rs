use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::future::try_join_all;
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};

use crate::{
    config::KafkaSinkConfig,
    event::Event,
    sink::{BatchSettings, Sink},
};

pub struct KafkaSink {
    id: String,
    topic: String,
    timeout: Duration,
    producer: FutureProducer,
    batch: BatchSettings,
}

impl KafkaSink {
    pub fn new(id: &str, config: &KafkaSinkConfig) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("compression.type", "lz4")
            .set("linger.ms", "5")
            .set("batch.num.messages", "10000")
            .set("message.timeout.ms", config.delivery_timeout_ms.to_string())
            .create()
            .with_context(|| format!("cannot create Kafka sink {id:?}"))?;
        Ok(Self {
            id: id.into(),
            topic: config.topic.clone(),
            timeout: Duration::from_millis(config.delivery_timeout_ms),
            producer,
            batch: BatchSettings {
                max_events: config.batch_max_events,
                max_bytes: config.batch_max_bytes,
                linger: Duration::from_millis(config.batch_linger_ms),
            },
        })
    }
}

#[async_trait]
impl Sink for KafkaSink {
    fn name(&self) -> &str {
        &self.id
    }

    fn batch_settings(&self) -> BatchSettings {
        self.batch
    }

    async fn write_batches(&mut self, batches: &[&[Event]]) -> Result<()> {
        try_join_all(
            batches
                .iter()
                .flat_map(|batch| batch.iter())
                .map(|event| async {
                    let mut record =
                        FutureRecord::<[u8], [u8]>::to(&self.topic).payload(&event.payload);
                    if let Some(key) = &event.key {
                        record = record.key(key);
                    }
                    self.producer
                        .send(record, Timeout::After(self.timeout))
                        .await
                        .map_err(|(error, _)| error)
                        .context("Kafka delivery failed")?;
                    Ok::<(), anyhow::Error>(())
                }),
        )
        .await?;
        Ok(())
    }
}
