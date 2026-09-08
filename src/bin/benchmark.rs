//! In-process microbenchmark of the router's hot path.
//!
//! This exercises the code the Kafka source and sink runtime actually run —
//! `BatchBuffer` normalisation, `EventBatch`, `Fanout`, bounded sink queues,
//! sink-side coalescing and acknowledgement — with the brokers replaced by a
//! sink that only counts. It therefore measures the router's own overhead and
//! nothing else. It is not an end-to-end number: see `docs/PERFORMANCE.md` for
//! the Kafka benchmark procedure.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use async_trait::async_trait;
use rustper::{
    config::DeliverySettings,
    event::{BatchBuffer, Event, EventBatch, SourceMetadata},
    metrics::Metrics,
    sink::{BatchSettings, Sink, WriteOutcome},
    topology::Fanout,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Matches the router binary, so the benchmark measures the allocator that
/// production actually runs on.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const DEFAULT_MESSAGE_COUNT: u64 = 1_000_000;
const PAYLOAD_SIZE: usize = 1024;
const OUTPUT_COUNT: usize = 3;
const BATCH_SIZE: usize = 1_000;
const QUEUE_CAPACITY: usize = 32;
/// Mirrors the source's `max_in_flight_batches`. Without a window the producer
/// would serialise on each batch's acknowledgement and measure sink linger
/// rather than router overhead.
const MAX_IN_FLIGHT: usize = 8;

/// A sink that accepts everything, so the measurement is router overhead only.
struct CountingSink {
    id: String,
    events: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

#[async_trait]
impl Sink for CountingSink {
    fn name(&self) -> &str {
        &self.id
    }

    fn batch_settings(&self) -> BatchSettings {
        BatchSettings {
            max_events: 10_000,
            max_bytes: 16 * 1024 * 1024,
            linger: Duration::from_millis(5),
        }
    }

    async fn write_batches(&self, batches: &[Arc<EventBatch>]) -> Result<WriteOutcome> {
        for batch in batches {
            self.events.fetch_add(batch.len() as u64, Ordering::Relaxed);
            self.bytes
                .fetch_add(batch.bytes() as u64, Ordering::Relaxed);
        }
        Ok(WriteOutcome::written(
            batches.iter().map(|batch| batch.len() as u64).sum(),
        ))
    }
}

/// `MESSAGE_COUNT=<n>` lengthens the run so CPU and RSS can be sampled.
fn message_count() -> u64 {
    std::env::var("MESSAGE_COUNT")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(DEFAULT_MESSAGE_COUNT)
}

#[tokio::main]
async fn main() -> Result<()> {
    let message_count = message_count();
    let component: Arc<str> = Arc::from("benchmark");
    let topic: Arc<str> = Arc::from("input");
    let events = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let shutdown = CancellationToken::new();
    let sink_ids: Vec<String> = (0..OUTPUT_COUNT).map(|i| format!("sink-{i}")).collect();
    let metrics = Metrics::new(["bench"], sink_ids.iter().map(String::as_str));

    let mut destinations = Vec::with_capacity(OUTPUT_COUNT);
    let mut workers = Vec::with_capacity(OUTPUT_COUNT);
    for index in 0..OUTPUT_COUNT {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        destinations.push(tx);
        let sink = Box::new(CountingSink {
            id: format!("sink-{index}"),
            events: Arc::clone(&events),
            bytes: Arc::clone(&bytes),
        });
        let settings = DeliverySettings {
            max_concurrent_writes: 1,
            retry_max_attempts: 1,
            retry_initial_backoff: Duration::from_millis(1),
            retry_max_backoff: Duration::from_millis(1),
        };
        let token = shutdown.child_token();
        let metrics = Arc::clone(&metrics);
        workers.push(tokio::spawn(async move {
            rustper::sink::run(sink, settings, metrics, rx, token).await
        }));
    }
    let fanout = Fanout::new(destinations);

    let payload = vec![42_u8; PAYLOAD_SIZE];
    let mut buffer = BatchBuffer::default();
    let started = Instant::now();

    let mut in_flight: tokio::task::JoinSet<Result<()>> = tokio::task::JoinSet::new();
    let mut produced = 0_u64;
    while produced < message_count {
        let take = BATCH_SIZE.min((message_count - produced) as usize);
        let mut batch = Vec::with_capacity(take);
        for offset in 0..take {
            let key = (produced + offset as u64).to_be_bytes();
            batch.push(Event {
                key: Some(buffer.copy(&key)),
                payload: buffer.copy(&payload),
                timestamp_ms: None,
                source: SourceMetadata {
                    component_id: Arc::clone(&component),
                    topic: Arc::clone(&topic),
                    partition: (produced % 6) as i32,
                    offset: (produced + offset as u64) as i64,
                },
            });
        }
        produced += take as u64;
        while in_flight.len() >= MAX_IN_FLIGHT {
            in_flight
                .join_next()
                .await
                .expect("in-flight set is not empty")??;
        }
        let batch = EventBatch::new(batch);
        let fanout = fanout.clone();
        in_flight.spawn(async move { fanout.send(batch).await });
    }
    while let Some(joined) = in_flight.join_next().await {
        joined??;
    }

    let elapsed = started.elapsed().as_secs_f64();
    shutdown.cancel();
    for worker in workers {
        worker.await??;
    }

    let deliveries = events.load(Ordering::Relaxed);
    let delivered_bytes = bytes.load(Ordering::Relaxed);
    assert_eq!(
        deliveries,
        message_count * OUTPUT_COUNT as u64,
        "every sink must observe every message",
    );

    println!("messages in:       {message_count}");
    println!("outputs:           {OUTPUT_COUNT}");
    println!("source batch:      {BATCH_SIZE}");
    println!("total deliveries:  {deliveries}");
    println!("elapsed:           {elapsed:.3} s");
    println!(
        "input throughput:  {:.0} msg/s",
        message_count as f64 / elapsed
    );
    println!(
        "output throughput: {:.0} deliveries/s",
        deliveries as f64 / elapsed
    );
    println!(
        "accounted memory:  {:.2} GiB/s",
        delivered_bytes as f64 / elapsed / 1024_f64.powi(3)
    );
    Ok(())
}
