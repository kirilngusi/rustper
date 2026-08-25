mod clickhouse;
mod kafka;

use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{config::SinkConfig, event::Event, topology::SinkEnvelope};

#[async_trait]
pub trait Sink: Send + 'static {
    fn name(&self) -> &str;
    fn batch_settings(&self) -> BatchSettings;
    async fn write_batches(&mut self, batches: &[&[Event]]) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
pub struct BatchSettings {
    pub max_events: usize,
    pub max_bytes: usize,
    pub linger: Duration,
}

pub fn build(id: &str, config: &SinkConfig) -> Result<Box<dyn Sink>> {
    match config {
        SinkConfig::Kafka(config) => Ok(Box::new(kafka::KafkaSink::new(id, config)?)),
        SinkConfig::Clickhouse(config) => Ok(Box::new(clickhouse::ClickhouseSink::new(id, config))),
    }
}

pub async fn run(
    mut sink: Box<dyn Sink>,
    mut input: mpsc::Receiver<SinkEnvelope>,
    shutdown: CancellationToken,
) -> Result<()> {
    tracing::info!(component = sink.name(), "sink started");
    let settings = sink.batch_settings();
    loop {
        let first = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            item = input.recv() => match item { Some(item) => item, None => return Ok(()) },
        };
        let mut event_count = first.events.len();
        let mut byte_count: usize = first.events.iter().map(Event::estimated_size).sum();
        let mut envelopes = vec![first];
        let deadline = tokio::time::Instant::now() + settings.linger;

        while event_count < settings.max_events && byte_count < settings.max_bytes {
            match tokio::time::timeout_at(deadline, input.recv()).await {
                Ok(Some(envelope)) => {
                    event_count += envelope.events.len();
                    byte_count += envelope
                        .events
                        .iter()
                        .map(Event::estimated_size)
                        .sum::<usize>();
                    envelopes.push(envelope);
                    if event_count >= settings.max_events || byte_count >= settings.max_bytes {
                        break;
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }

        let batches: Vec<&[Event]> = envelopes
            .iter()
            .map(|item| item.events.as_slice())
            .collect();
        match sink.write_batches(&batches).await {
            Ok(()) => envelopes.into_iter().for_each(SinkEnvelope::delivered),
            Err(error) => {
                envelopes.into_iter().for_each(SinkEnvelope::failed);
                return Err(error);
            }
        }
    }
}
