mod json;

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use clickhouse::{Client, Row};
use serde::Serialize;
use tracing::warn;

use std::time::Duration;

use crate::{
    config::{ClickhouseSchema, ClickhouseSinkConfig},
    event::EventBatch,
    sink::{BatchSettings, Sink, WriteOutcome},
};

use json::MetadataColumns;

/// Starting size for the row buffer. Grows once and is then reused for the
/// lifetime of a write rather than reallocating per row.
const ROW_BUFFER_BYTES: usize = 256 * 1024;

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
    schema: ClickhouseSchema,
    metadata: MetadataColumns,
}

impl ClickhouseSink {
    pub fn new(id: &str, config: &ClickhouseSinkConfig) -> Self {
        let mut client = Client::default()
            .with_url(&config.endpoint)
            .with_database(&config.database)
            .with_user(&config.user)
            .with_password(&config.password);
        // Lets ClickHouse skip rows the router cannot cheaply reject itself,
        // rather than failing an otherwise good batch.
        if config.allow_errors_num > 0 {
            client = client.with_setting(
                "input_format_allow_errors_num",
                config.allow_errors_num.to_string(),
            );
        }
        if config.allow_errors_ratio > 0.0 {
            client = client.with_setting(
                "input_format_allow_errors_ratio",
                config.allow_errors_ratio.to_string(),
            );
        }
        Self {
            id: id.into(),
            table: config.table.clone(),
            client,
            batch: BatchSettings {
                max_events: config.batch_max_events,
                max_bytes: config.batch_max_bytes,
                linger: Duration::from_millis(config.batch_linger_ms),
            },
            schema: config.schema,
            metadata: MetadataColumns {
                topic: config.metadata_columns.topic.clone(),
                partition: config.metadata_columns.partition.clone(),
                offset: config.metadata_columns.offset.clone(),
                timestamp: config.metadata_columns.timestamp.clone(),
                key: config.metadata_columns.key.clone(),
            },
        }
    }

    /// Writes the seven fixed columns through the typed `RowBinary` path.
    async fn write_event_rows(&self, batches: &[Arc<EventBatch>]) -> Result<WriteOutcome> {
        let mut insert = self
            .client
            .insert::<EventRow<'_>>(&self.table)
            .await
            .with_context(|| format!("ClickHouse insert into {:?} failed", self.table))?;
        let mut written = 0_u64;
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
            written += 1;
        }
        insert.end().await.context("ClickHouse rejected batch")?;
        Ok(WriteOutcome::written(written))
    }

    /// Streams the payloads as `JSONEachRow`, letting ClickHouse map by column
    /// name and convert every type.
    async fn write_json_rows(&self, batches: &[Arc<EventBatch>]) -> Result<WriteOutcome> {
        let mut buffer = Vec::with_capacity(ROW_BUFFER_BYTES);
        let mut outcome = WriteOutcome::default();
        let mut first_rejection = None;

        for event in batches.iter().flat_map(|batch| batch.events()) {
            match json::write_row(&mut buffer, event, &self.metadata) {
                Ok(()) => outcome.written += 1,
                Err(rejection) => {
                    outcome.dropped += 1;
                    first_rejection.get_or_insert((rejection, event.source.offset));
                }
            }
        }

        if let Some((rejection, offset)) = first_rejection {
            // One line per write, not per row: a topic full of malformed
            // payloads must not turn the log into the bottleneck.
            warn!(
                component = %self.id,
                dropped = outcome.dropped,
                first_bad_offset = offset,
                reason = rejection.as_str(),
                "dropped rows that could not be written as JSON",
            );
        }
        if outcome.written == 0 {
            return Ok(outcome);
        }

        let mut insert = self
            .client
            .insert_formatted_with(format!(
                "INSERT INTO {} FORMAT JSONEachRow",
                escape_identifier(&self.table)
            ))
            .buffered_with_capacity(ROW_BUFFER_BYTES);
        insert.write_buffered(&buffer);
        insert
            .end()
            .await
            .with_context(|| format!("ClickHouse rejected the batch for {:?}", self.table))?;
        Ok(outcome)
    }
}

/// Quotes a table identifier for interpolation into the INSERT statement.
fn escape_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "\\`"))
}

#[async_trait]
impl Sink for ClickhouseSink {
    fn name(&self) -> &str {
        &self.id
    }

    fn batch_settings(&self) -> BatchSettings {
        self.batch
    }

    async fn write_batches(&self, batches: &[Arc<EventBatch>]) -> Result<WriteOutcome> {
        match self.schema {
            ClickhouseSchema::Event => self.write_event_rows(batches).await,
            ClickhouseSchema::Json => self.write_json_rows(batches).await,
        }
    }
}
