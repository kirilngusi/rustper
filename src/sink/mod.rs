mod clickhouse;
mod kafka;

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use tokio::{
    sync::mpsc,
    task::JoinSet,
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    config::{DeliverySettings, SinkConfig},
    event::EventBatch,
    topology::SinkEnvelope,
};

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn batch_settings(&self) -> BatchSettings;
    /// Writes coalesced batches.
    ///
    /// Takes `&self` so a sink can have several writes in flight at once; any
    /// per-write mutable state belongs inside the implementation.
    async fn write_batches(&self, batches: &[Arc<EventBatch>]) -> Result<()>;
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

/// Writes one coalesced group, retrying transient failures with exponential
/// backoff.
///
/// Exhausting the retries is deliberately fatal: the batch's offsets are never
/// committed, so terminating replays them on restart. Silently dropping the
/// batch is the one outcome that would break at-least-once.
async fn write_with_retry(
    sink: &dyn Sink,
    batches: &[Arc<EventBatch>],
    settings: DeliverySettings,
) -> Result<()> {
    let mut backoff = settings.retry_initial_backoff;
    for attempt in 1..=settings.retry_max_attempts {
        match sink.write_batches(batches).await {
            Ok(()) => return Ok(()),
            Err(error) if attempt < settings.retry_max_attempts => {
                warn!(
                    component = sink.name(),
                    attempt,
                    max_attempts = settings.retry_max_attempts,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %error,
                    "sink write failed, retrying",
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(settings.retry_max_backoff);
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("retry_max_attempts is at least 1")
}

/// Reaps one finished write, propagating its failure.
async fn reap(writes: &mut JoinSet<Result<()>>) -> Result<()> {
    match writes.join_next().await {
        Some(joined) => joined?,
        None => std::future::pending().await,
    }
}

pub async fn run(
    sink: Box<dyn Sink>,
    settings: DeliverySettings,
    mut input: mpsc::Receiver<SinkEnvelope>,
    shutdown: CancellationToken,
) -> Result<()> {
    info!(
        component = sink.name(),
        max_concurrent_writes = settings.max_concurrent_writes,
        "sink started",
    );
    let sink: Arc<dyn Sink> = Arc::from(sink);
    let batching = sink.batch_settings();
    let mut writes: JoinSet<Result<()>> = JoinSet::new();
    // One timer reused for every coalescing window, rather than a fresh timeout
    // future per received envelope.
    let deadline = sleep_until(Instant::now());
    tokio::pin!(deadline);

    'accept: loop {
        // Wait for work, while still reaping writes that finish meanwhile.
        let first = loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break 'accept,
                result = reap(&mut writes), if !writes.is_empty() => result?,
                item = input.recv() => match item {
                    Some(item) => break item,
                    None => break 'accept,
                },
            }
        };

        let mut event_count = first.batch().len();
        let mut byte_count = first.batch().bytes();
        let mut envelopes = vec![first];
        deadline.as_mut().reset(Instant::now() + batching.linger);

        while event_count < batching.max_events && byte_count < batching.max_bytes {
            tokio::select! {
                biased;
                result = reap(&mut writes), if !writes.is_empty() => result?,
                _ = &mut deadline => break,
                item = input.recv() => match item {
                    Some(envelope) => {
                        event_count += envelope.batch().len();
                        byte_count += envelope.batch().bytes();
                        envelopes.push(envelope);
                    }
                    None => break,
                },
            }
        }

        // Free a write slot before dispatching.
        while writes.len() >= settings.max_concurrent_writes {
            reap(&mut writes).await?;
        }

        let sink = Arc::clone(&sink);
        writes.spawn(async move {
            let batches: Vec<Arc<EventBatch>> = envelopes
                .iter()
                .map(|envelope| Arc::clone(envelope.batch()))
                .collect();
            match write_with_retry(sink.as_ref(), &batches, settings).await {
                Ok(()) => {
                    envelopes.into_iter().for_each(SinkEnvelope::delivered);
                    Ok(())
                }
                Err(error) => {
                    envelopes.into_iter().for_each(SinkEnvelope::failed);
                    Err(error)
                }
            }
        });
    }

    // Let in-flight writes acknowledge before the task ends; dropping them
    // would fail batches that had actually reached the destination.
    let mut outcome = Ok(());
    while let Some(joined) = writes.join_next().await {
        let result = joined.map_err(anyhow::Error::from).and_then(|inner| inner);
        if result.is_err() && outcome.is_ok() {
            outcome = result;
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use tokio::sync::Notify;

    use super::{BatchSettings, Sink, write_with_retry};
    use crate::config::DeliverySettings;
    use crate::event::{Event, EventBatch, SourceMetadata};
    use std::time::Duration;

    struct FlakySink {
        attempts: AtomicUsize,
        fail_until: usize,
        released: Notify,
    }

    #[async_trait]
    impl Sink for FlakySink {
        fn name(&self) -> &str {
            "flaky"
        }
        fn batch_settings(&self) -> BatchSettings {
            BatchSettings {
                max_events: 1,
                max_bytes: 1,
                linger: Duration::ZERO,
            }
        }
        async fn write_batches(&self, _: &[Arc<EventBatch>]) -> Result<()> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= self.fail_until {
                bail!("transient failure {attempt}");
            }
            self.released.notify_one();
            Ok(())
        }
    }

    fn settings(max_attempts: u32) -> DeliverySettings {
        DeliverySettings {
            max_concurrent_writes: 1,
            retry_max_attempts: max_attempts,
            retry_initial_backoff: Duration::from_millis(1),
            retry_max_backoff: Duration::from_millis(2),
        }
    }

    fn batch() -> Vec<Arc<EventBatch>> {
        vec![Arc::new(EventBatch::new(vec![Event {
            key: None,
            payload: "x".into(),
            timestamp_ms: None,
            source: SourceMetadata {
                component_id: "s".into(),
                topic: "t".into(),
                partition: 0,
                offset: 0,
            },
        }]))]
    }

    #[tokio::test]
    async fn transient_failures_are_retried() {
        let sink = FlakySink {
            attempts: AtomicUsize::new(0),
            fail_until: 2,
            released: Notify::new(),
        };
        write_with_retry(&sink, &batch(), settings(5))
            .await
            .unwrap();
        assert_eq!(sink.attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn exhausted_retries_surface_the_error() {
        let sink = FlakySink {
            attempts: AtomicUsize::new(0),
            fail_until: usize::MAX,
            released: Notify::new(),
        };
        let error = write_with_retry(&sink, &batch(), settings(3))
            .await
            .unwrap_err();
        assert_eq!(sink.attempts.load(Ordering::SeqCst), 3);
        assert!(error.to_string().contains("transient failure 3"));
    }
}
