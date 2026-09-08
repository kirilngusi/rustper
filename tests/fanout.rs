//! End-to-end checks of the fan-out and sink runtime, with the brokers replaced
//! by controllable in-process sinks.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, bail};
use async_trait::async_trait;
use rustper::{
    config::DeliverySettings,
    event::{Event, EventBatch, SourceMetadata},
    metrics::Metrics,
    sink::{BatchSettings, Sink, WriteOutcome},
    topology::{Fanout, SinkEnvelope},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct TestSink {
    id: String,
    settings: BatchSettings,
    events: Arc<AtomicU64>,
    writes: Arc<AtomicUsize>,
    fail_first: usize,
}

#[async_trait]
impl Sink for TestSink {
    fn name(&self) -> &str {
        &self.id
    }

    fn batch_settings(&self) -> BatchSettings {
        self.settings
    }

    async fn write_batches(&self, batches: &[Arc<EventBatch>]) -> Result<WriteOutcome> {
        let write = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        if write <= self.fail_first {
            bail!("injected failure on write {write}");
        }
        for batch in batches {
            self.events.fetch_add(batch.len() as u64, Ordering::SeqCst);
        }
        Ok(WriteOutcome::written(
            batches.iter().map(|batch| batch.len() as u64).sum(),
        ))
    }
}

fn batch(count: usize, first_offset: i64) -> EventBatch {
    let component: Arc<str> = Arc::from("source");
    let topic: Arc<str> = Arc::from("input");
    EventBatch::new(
        (0..count)
            .map(|index| Event {
                key: None,
                payload: "payload".into(),
                timestamp_ms: Some(1),
                source: SourceMetadata {
                    component_id: Arc::clone(&component),
                    topic: Arc::clone(&topic),
                    partition: 0,
                    offset: first_offset + index as i64,
                },
            })
            .collect(),
    )
}

fn settings(retries: u32) -> DeliverySettings {
    DeliverySettings {
        max_concurrent_writes: 1,
        retry_max_attempts: retries,
        retry_initial_backoff: Duration::from_millis(1),
        retry_max_backoff: Duration::from_millis(2),
    }
}

/// Everything a test needs to drive one running sink.
struct RunningSink {
    input: mpsc::Sender<SinkEnvelope>,
    events: Arc<AtomicU64>,
    writes: Arc<AtomicUsize>,
    metrics: Arc<Metrics>,
    id: String,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<Result<()>>,
}

fn spawn_sink(
    id: &str,
    fail_first: usize,
    delivery: DeliverySettings,
    linger: Duration,
) -> RunningSink {
    let (tx, rx) = mpsc::channel(8);
    let events = Arc::new(AtomicU64::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let sink = Box::new(TestSink {
        id: id.to_owned(),
        settings: BatchSettings {
            max_events: 10_000,
            max_bytes: 1024 * 1024,
            linger,
        },
        events: Arc::clone(&events),
        writes: Arc::clone(&writes),
        fail_first,
    });
    let token = CancellationToken::new();
    let child = token.child_token();
    let metrics = Metrics::new(["source"], [id]);
    let sink_metrics = Arc::clone(&metrics);
    let task =
        tokio::spawn(
            async move { rustper::sink::run(sink, delivery, sink_metrics, rx, child).await },
        );
    RunningSink {
        input: tx,
        events,
        writes,
        metrics,
        id: id.to_owned(),
        shutdown: token,
        task,
    }
}

#[tokio::test]
async fn every_sink_receives_every_event() {
    let first = spawn_sink("a", 0, settings(1), Duration::from_millis(1));
    let second = spawn_sink("b", 0, settings(1), Duration::from_millis(1));
    let fanout = Fanout::new(vec![first.input.clone(), second.input.clone()]);

    for round in 0..5 {
        fanout.send(batch(10, round * 10)).await.unwrap();
    }

    first.shutdown.cancel();
    second.shutdown.cancel();
    first.task.await.unwrap().unwrap();
    second.task.await.unwrap().unwrap();
    assert_eq!(first.events.load(Ordering::SeqCst), 50);
    assert_eq!(second.events.load(Ordering::SeqCst), 50);
}

#[tokio::test]
async fn a_transient_sink_failure_is_retried_and_the_batch_is_acknowledged() {
    let sink = spawn_sink("flaky", 2, settings(5), Duration::from_millis(1));
    let fanout = Fanout::new(vec![sink.input.clone()]);

    fanout.send(batch(4, 0)).await.unwrap();

    sink.shutdown.cancel();
    sink.task.await.unwrap().unwrap();
    assert_eq!(
        sink.writes.load(Ordering::SeqCst),
        3,
        "two failures then success"
    );
    assert_eq!(sink.events.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn an_unrecoverable_sink_failure_rejects_the_batch() {
    let sink = spawn_sink("broken", usize::MAX, settings(2), Duration::ZERO);
    let fanout = Fanout::new(vec![sink.input.clone()]);

    let error = fanout.send(batch(4, 0)).await.unwrap_err();
    assert!(
        error.to_string().contains("rejected the batch"),
        "the source must learn the batch was not delivered, got: {error}",
    );
    assert!(
        sink.task.await.unwrap().is_err(),
        "the sink reports the failure"
    );
}

#[tokio::test]
async fn coalescing_merges_queued_batches_into_one_write() {
    // A long linger with a slow first write lets several batches queue up; they
    // must reach the sink as a single coalesced write.
    let sink = spawn_sink("coalescing", 0, settings(1), Duration::from_millis(50));
    let fanout = Fanout::new(vec![sink.input.clone()]);

    let sends = (0..4)
        .map(|round| {
            let fanout = fanout.clone();
            tokio::spawn(async move { fanout.send(batch(10, round * 10)).await })
        })
        .collect::<Vec<_>>();
    for send in sends {
        send.await.unwrap().unwrap();
    }

    sink.shutdown.cancel();
    sink.task.await.unwrap().unwrap();
    assert_eq!(sink.events.load(Ordering::SeqCst), 40);
    assert!(
        sink.writes.load(Ordering::SeqCst) < 4,
        "batches must coalesce, saw {} writes",
        sink.writes.load(Ordering::SeqCst),
    );
}

#[tokio::test]
async fn counters_track_writes_retries_and_failures_through_the_runtime() {
    let sink = spawn_sink("counted", 2, settings(5), Duration::from_millis(1));
    let fanout = Fanout::new(vec![sink.input.clone()]);

    fanout.send(batch(6, 0)).await.unwrap();

    sink.shutdown.cancel();
    sink.task.await.unwrap().unwrap();

    let counters = sink.metrics.sink(&sink.id);
    assert_eq!(counters.events_out.load(Ordering::SeqCst), 6);
    assert_eq!(counters.batches_written.load(Ordering::SeqCst), 1);
    assert_eq!(
        counters.retries.load(Ordering::SeqCst),
        2,
        "two injected failures were retried",
    );
    assert_eq!(counters.write_errors.load(Ordering::SeqCst), 0);
    assert_eq!(counters.events_dropped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_unrecoverable_failure_is_counted_and_nothing_is_reported_written() {
    let sink = spawn_sink("counted_bad", usize::MAX, settings(2), Duration::ZERO);
    let fanout = Fanout::new(vec![sink.input.clone()]);

    assert!(fanout.send(batch(4, 0)).await.is_err());
    assert!(sink.task.await.unwrap().is_err());

    let counters = sink.metrics.sink(&sink.id);
    assert_eq!(counters.write_errors.load(Ordering::SeqCst), 1);
    assert_eq!(
        counters.events_out.load(Ordering::SeqCst),
        0,
        "a failed write must not inflate the written count",
    );
    assert_eq!(counters.batches_written.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_fanout_with_no_destinations_succeeds() {
    assert!(Fanout::new(Vec::new()).send(batch(1, 0)).await.is_ok());
}
