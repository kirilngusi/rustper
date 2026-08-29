use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use clickhouse::{Client, Row};
use serde::Serialize;

use std::time::Duration;

use crate::{
    config::ClickhouseSinkConfig,
    event::EventBatch,
    sink::{BatchSettings, Sink},
};

#[derive(Row, Serialize)]
struct EventRow<'a> {
    timestamp_ms: i64,
    source_component: &'a str,
    source_topic: &'a str,
    source_partition: i32,
    source_offset: i64,
    #[serde(with = "serde_bytes")]
    key: &'a [u8],
    #[serde(with = "serde_bytes")]
    payload: &'a [u8],
}

pub struct ClickhouseSink {
    id: String,
    table: String,
    client: Client,
    batch: BatchSettings,
}

impl ClickhouseSink {
    pub fn new(id: &str, config: &ClickhouseSinkConfig) -> Self {
        let client = Client::default()
            .with_url(&config.endpoint)
            .with_database(&config.database)
            .with_user(&config.user)
            .with_password(&config.password);
        Self {
            id: id.into(),
            table: config.table.clone(),
            client,
            batch: BatchSettings {
                max_events: config.batch_max_events,
                max_bytes: config.batch_max_bytes,
                linger: Duration::from_millis(config.batch_linger_ms),
            },
        }
    }
}

#[async_trait]
impl Sink for ClickhouseSink {
    fn name(&self) -> &str {
        &self.id
    }

    fn batch_settings(&self) -> BatchSettings {
        self.batch
    }

    async fn write_batches(&self, batches: &[Arc<EventBatch>]) -> Result<()> {
        let mut insert = self
            .client
            .insert::<EventRow<'_>>(&self.table)
            .await
            .with_context(|| format!("ClickHouse insert into {:?} failed", self.table))?;
        for event in batches.iter().flat_map(|batch| batch.events()) {
            let row = EventRow {
                timestamp_ms: event.timestamp_ms.unwrap_or_default(),
                source_component: &event.source.component_id,
                source_topic: &event.source.topic,
                source_partition: event.source.partition,
                source_offset: event.source.offset,
                key: event.key.as_deref().unwrap_or_default(),
                payload: &event.payload,
            };
            insert
                .write(&row)
                .await
                .context("cannot encode ClickHouse row")?;
        }
        insert.end().await.context("ClickHouse rejected batch")?;
        Ok(())
    }
}
