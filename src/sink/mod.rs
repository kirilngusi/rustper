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
    metrics::SinkMetrics,
    topology::SinkEnvelope,
};

/// Relaxed ordering: these counters are statistics, never synchronisation.
const ORDER: std::sync::atomic::Ordering = std::sync::atomic::Ordering::Relaxed;

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn batch_settings(&self) -> BatchSettings;
    /// Writes coalesced batches.
    ///
    /// Takes `&self` so a sink can have several writes in flight at once; any
    /// per-write mutable state belongs inside the implementation.
    async fn write_batches(&self, batches: &[Arc<EventBatch>]) -> Result<WriteOutcome>;
}

/// What a write actually did.
///
/// Returned rather than counted in place so sinks hold no metrics state and
/// stay testable on their own; [`run`] folds the outcome into the counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteOutcome {
    pub written: u64,
    /// Rows the sink refused. These are acknowledged, not retried: the batch
    /// succeeded, and a malformed row would fail again on every replay.
    pub dropped: u64,
}

impl WriteOutcome {
    pub fn written(written: u64) -> Self {
        Self {
            written,
            dropped: 0,
        }
    }
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
    metrics: &SinkMetrics,
) -> Result<WriteOutcome> {
    let mut backoff = settings.retry_initial_backoff;
    for attempt in 1..=settings.retry_max_attempts {
        match sink.write_batches(batches).await {
            Ok(outcome) => return Ok(outcome),
            Err(error) if attempt < settings.retry_max_attempts => {
                metrics.retries.fetch_add(1, ORDER);
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
            Err(error) => {
                metrics.write_errors.fetch_add(1, ORDER);
                return Err(error);
            }
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
    metrics: Arc<crate::metrics::Metrics>,
    mut input: mpsc::Receiver<SinkEnvelope>,
    shutdown: CancellationToken,
) -> Result<()> {
    info!(
        component = sink.name(),
        max_concurrent_writes = settings.max_concurrent_writes,
        "sink started",
    );
    let sink: Arc<dyn Sink> = Arc::from(sink);
    let component: Arc<str> = Arc::from(sink.name());
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

        // Queue depth is sampled here rather than tracked on every send: this is
        // the point where a backed-up sink actually shows up.
        metrics
            .sink(&component)
            .queue_depth
            .store(input.len() as u64, ORDER);

        let sink = Arc::clone(&sink);
        let metrics = Arc::clone(&metrics);
        let component = Arc::clone(&component);
        writes.spawn(async move {
            let batches: Vec<Arc<EventBatch>> = envelopes
                .iter()
                .map(|envelope| Arc::clone(envelope.batch()))
                .collect();
            let counters = metrics.sink(&component);
            match write_with_retry(sink.as_ref(), &batches, settings, counters).await {
                Ok(outcome) => {
                    counters.events_out.fetch_add(outcome.written, ORDER);
                    counters.events_dropped.fetch_add(outcome.dropped, ORDER);
                    counters.batches_written.fetch_add(1, ORDER);
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

    use super::{BatchSettings, Sink, WriteOutcome, write_with_retry};
    use crate::config::DeliverySettings;
    use crate::event::{Event, EventBatch, SourceMetadata};
    use crate::metrics::SinkMetrics;
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
        async fn write_batches(&self, batches: &[Arc<EventBatch>]) -> Result<WriteOutcome> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= self.fail_until {
                bail!("transient failure {attempt}");
            }
            self.released.notify_one();
            Ok(WriteOutcome::written(
                batches.iter().map(|b| b.len() as u64).sum(),
            ))
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
        let counters = SinkMetrics::default();
        let outcome = write_with_retry(&sink, &batch(), settings(5), &counters)
            .await
            .unwrap();
        assert_eq!(sink.attempts.load(Ordering::SeqCst), 3);
        assert_eq!(outcome, WriteOutcome::written(1));
        assert_eq!(
            counters.retries.load(Ordering::SeqCst),
            2,
            "each retried attempt is counted",
        );
        assert_eq!(counters.write_errors.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn exhausted_retries_surface_the_error() {
        let sink = FlakySink {
            attempts: AtomicUsize::new(0),
            fail_until: usize::MAX,
            released: Notify::new(),
        };
        let counters = SinkMetrics::default();
        let error = write_with_retry(&sink, &batch(), settings(3), &counters)
            .await
            .unwrap_err();
        assert_eq!(sink.attempts.load(Ordering::SeqCst), 3);
        assert!(error.to_string().contains("transient failure 3"));
        assert_eq!(counters.retries.load(Ordering::SeqCst), 2);
        assert_eq!(
            counters.write_errors.load(Ordering::SeqCst),
            1,
            "the final failure is counted once, not once per attempt",
        );
    }
}
