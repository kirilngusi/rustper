use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use rdkafka::{
    ClientConfig, ClientContext, Message,
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    message::BorrowedMessage,
    topic_partition_list::{Offset, TopicPartitionList},
};
use tokio::time::{Instant as TokioInstant, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    config::KafkaSourceConfig,
    event::{BatchBuffer, Event, EventBatch, SourceMetadata},
    metrics::{Metrics, SourceMetrics},
    source::Source,
    topology::Fanout,
};

/// Relaxed ordering: these counters are statistics, never synchronisation.
const ORDER: std::sync::atomic::Ordering = std::sync::atomic::Ordering::Relaxed;

/// Counts rebalances, which are otherwise invisible from inside the process.
///
/// A rebalance stops consumption for seconds at a time. During benchmarking a
/// 25-second stall could only be attributed by reading the broker log, because
/// nothing in the router knew it had been fenced and re-joined.
pub struct MetricsContext {
    metrics: Arc<Metrics>,
    component: Arc<str>,
}

impl ClientContext for MetricsContext {}

impl ConsumerContext for MetricsContext {
    fn post_rebalance(&self, _: &rdkafka::consumer::BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        // Only the assignment half is counted; every rebalance produces exactly
        // one, so counting both would double every event.
        if let Rebalance::Assign(partitions) = rebalance {
            self.metrics
                .source(&self.component)
                .rebalances
                .fetch_add(1, ORDER);
            tracing::warn!(
                component = %self.component,
                partitions = partitions.count(),
                "consumer group rebalanced; consumption stalls while this happens",
            );
        }
    }
}

type MeteredConsumer = StreamConsumer<MetricsContext>;

pub struct KafkaSource {
    id: Arc<str>,
    config: KafkaSourceConfig,
    consumer: MeteredConsumer,
    metrics: Arc<Metrics>,
}

impl KafkaSource {
    pub fn new(id: &str, config: &KafkaSourceConfig, metrics: Arc<Metrics>) -> Result<Self> {
        let component: Arc<str> = Arc::from(id);
        let mut client = ClientConfig::new();
        client
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("session.timeout.ms", config.session_timeout_ms.to_string())
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set(
                "queued.max.messages.kbytes",
                config.queued_max_messages_kbytes.to_string(),
            )
            .set(
                "queued.min.messages",
                config.queued_min_messages.to_string(),
            );
        for (key, value) in &config.client_config {
            client.set(key, value);
        }
        let consumer: MeteredConsumer = client
            .create_with_context(MetricsContext {
                metrics: Arc::clone(&metrics),
                component: Arc::clone(&component),
            })
            .context("cannot create Kafka source")?;
        let topics: Vec<&str> = config.topics.iter().map(String::as_str).collect();
        consumer
            .subscribe(&topics)
            .context("cannot subscribe to Kafka topics")?;
        Ok(Self {
            id: component,
            config: config.clone(),
            consumer,
            metrics,
        })
    }
}

struct BatchCompletion {
    sequence: u64,
    offsets: TopicPartitionList,
    events: u64,
    bytes: usize,
}

/// Interns topic names so each event holds a shared `Arc<str>` instead of its
/// own `String`.
///
/// A batch almost always comes from one topic, so the single-entry `last` slot
/// answers nearly every lookup with a pointer comparison and skips hashing the
/// topic name once per message.
#[derive(Default)]
struct TopicCache {
    last: Option<Arc<str>>,
    interned: HashMap<String, Arc<str>>,
}

impl TopicCache {
    fn intern(&mut self, topic: &str) -> Arc<str> {
        if let Some(last) = &self.last
            && last.as_ref() == topic
        {
            return Arc::clone(last);
        }
        let interned = match self.interned.get(topic) {
            Some(interned) => Arc::clone(interned),
            None => {
                let interned: Arc<str> = Arc::from(topic);
                self.interned
                    .insert(topic.to_owned(), Arc::clone(&interned));
                interned
            }
        };
        self.last = Some(Arc::clone(&interned));
        interned
    }
}

fn to_event(
    component_id: &Arc<str>,
    topics: &mut TopicCache,
    buffer: &mut BatchBuffer,
    message: &BorrowedMessage<'_>,
) -> Event {
    Event {
        key: message.key().map(|key| buffer.copy(key)),
        payload: message
            .payload()
            .map(|p| buffer.copy(p))
            .unwrap_or_default(),
        timestamp_ms: message.timestamp().to_millis(),
        source: SourceMetadata {
            component_id: Arc::clone(component_id),
            topic: topics.intern(message.topic()),
            partition: message.partition(),
            offset: message.offset(),
        },
    }
}

fn offsets_for(events: &[Event]) -> Result<TopicPartitionList> {
    // A batch spans a handful of (topic, partition) pairs at most, so a linear
    // scan beats hashing the topic name once per event.
    let mut next_offsets: Vec<(&str, i32, i64)> = Vec::new();
    for event in events {
        let topic = event.source.topic.as_ref();
        let partition = event.source.partition;
        let next = event.source.offset + 1;
        match next_offsets
            .iter_mut()
            .find(|(seen, seen_partition, _)| *seen_partition == partition && *seen == topic)
        {
            Some((_, _, offset)) => *offset = (*offset).max(next),
            None => next_offsets.push((topic, partition, next)),
        }
    }
    let mut offsets = TopicPartitionList::new();
    for (topic, partition, offset) in next_offsets {
        offsets
            .add_partition_offset(topic, partition, Offset::Offset(offset))
            .context("cannot record next offsets for Kafka batch")?;
    }
    Ok(offsets)
}

fn commit_contiguous(
    consumer: &MeteredConsumer,
    completed: &mut BTreeMap<u64, BatchCompletion>,
    next_sequence: &mut u64,
    completion: BatchCompletion,
    counters: &SourceMetrics,
) -> Result<(u64, u64)> {
    completed.insert(completion.sequence, completion);
    let mut committed_events = 0;
    let mut committed_bytes = 0;
    while let Some(completion) = completed.remove(next_sequence) {
        consumer
            .commit(&completion.offsets, CommitMode::Async)
            .context("cannot commit Kafka batch")?;
        committed_events += completion.events;
        committed_bytes += completion.bytes as u64;
        counters.commits.fetch_add(1, ORDER);
        *next_sequence += 1;
    }
    Ok((committed_events, committed_bytes))
}

/// Reports throughput over the interval since the previous report, not as a
/// running average since the first message. A cumulative average keeps sinking
/// toward the mean and hides both ramp-up and stalls.
struct ThroughputReporter {
    component: Arc<str>,
    every: u64,
    next_report: u64,
    window_started: Instant,
    window_events: u64,
    total_started: Instant,
}

impl ThroughputReporter {
    fn new(component: Arc<str>, every: u64) -> Self {
        let now = Instant::now();
        Self {
            component,
            every: every.max(1),
            next_report: every.max(1),
            window_started: now,
            window_events: 0,
            total_started: now,
        }
    }

    fn record(&mut self, events: u64, total: u64) {
        self.window_events += events;
        if total < self.next_report {
            return;
        }
        let window_seconds = self.window_started.elapsed().as_secs_f64();
        let total_seconds = self.total_started.elapsed().as_secs_f64();
        info!(
            component = %self.component,
            processed = total,
            interval_messages_per_second = self.window_events as f64 / window_seconds.max(f64::MIN_POSITIVE),
            average_messages_per_second = total as f64 / total_seconds.max(f64::MIN_POSITIVE),
            elapsed_seconds = total_seconds,
            "source throughput",
        );
        self.next_report = (total / self.every + 1) * self.every;
        self.window_started = Instant::now();
        self.window_events = 0;
    }
}

const REPORT_EVERY: u64 = 100_000;

#[async_trait]
impl Source for KafkaSource {
    async fn run(self: Box<Self>, output: Fanout, shutdown: CancellationToken) -> Result<()> {
        info!(component = %self.id, topics = ?self.config.topics, "source started");
        let batch_size = self.config.batch_size;
        let linger = Duration::from_millis(self.config.batch_linger_ms);

        let counters = self.metrics.source(&self.id);
        let mut processed = 0_u64;
        let mut reporter = ThroughputReporter::new(Arc::clone(&self.id), REPORT_EVERY);
        let mut topics = TopicCache::default();
        let mut buffer = BatchBuffer::default();
        let mut in_flight: tokio::task::JoinSet<Result<BatchCompletion>> =
            tokio::task::JoinSet::new();
        let mut sequence = 0_u64;
        let mut next_commit_sequence = 0_u64;
        let mut in_flight_bytes = 0_usize;
        let mut completed = BTreeMap::new();

        let mut pending: Vec<Event> = Vec::with_capacity(batch_size);
        // One timer for the whole batch. Arming a `timeout` per `recv` would
        // register and cancel a timer entry for every single message.
        let deadline = sleep_until(TokioInstant::now());
        tokio::pin!(deadline);

        loop {
            let admits_batch = in_flight.len() < self.config.max_in_flight_batches
                && in_flight_bytes < self.config.max_in_flight_bytes;

            // A batch is dispatched from two places, so the closure keeps the
            // bookkeeping in one spot.
            macro_rules! dispatch {
                () => {{
                    let events = std::mem::replace(&mut pending, Vec::with_capacity(batch_size));
                    let offsets = offsets_for(&events)?;
                    let batch = EventBatch::new(events);
                    let event_count = batch.len() as u64;
                    let event_bytes = batch.bytes();
                    in_flight_bytes = in_flight_bytes.saturating_add(event_bytes);
                    counters.batches_in.fetch_add(1, ORDER);
                    counters
                        .in_flight_bytes
                        .store(in_flight_bytes as u64, ORDER);
                    counters
                        .in_flight_batches
                        .store(in_flight.len() as u64 + 1, ORDER);
                    let batch_sequence = sequence;
                    sequence += 1;
                    let fanout = output.clone();
                    in_flight.spawn(async move {
                        fanout.send(batch).await?;
                        Ok::<_, anyhow::Error>(BatchCompletion {
                            sequence: batch_sequence,
                            offsets,
                            events: event_count,
                            bytes: event_bytes,
                        })
                    });
                }};
            }

            tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    // In-flight batches are dropped without committing their
                    // offsets, so Kafka replays them on restart. This preserves
                    // at-least-once semantics: no data is lost, but already
                    // delivered batches may be redelivered.
                    in_flight.abort_all();
                    return Ok(());
                },

                // Servicing completions stays possible while a batch is being
                // assembled, so commits and the byte budget are never held
                // hostage by the linger window.
                joined = in_flight.join_next(), if !in_flight.is_empty() => {
                    let completion = joined.expect("in-flight set is not empty")
                        .context("fan-out task panicked")??;
                    in_flight_bytes = in_flight_bytes.saturating_sub(completion.bytes);
                    counters.in_flight_bytes.store(in_flight_bytes as u64, ORDER);
                    counters.in_flight_batches.store(in_flight.len() as u64, ORDER);
                    let (committed, committed_bytes) = commit_contiguous(
                        &self.consumer, &mut completed, &mut next_commit_sequence, completion,
                        counters,
                    )?;
                    processed += committed;
                    // Counted at commit rather than at consume: an event only
                    // really made it once every sink acknowledged it.
                    counters.events_in.fetch_add(committed, ORDER);
                    counters.bytes_in.fetch_add(committed_bytes, ORDER);
                    reporter.record(committed, processed);
                },

                // Gated on `admits_batch` as well, so an elapsed timer cannot
                // spin while the in-flight window is full.
                _ = &mut deadline, if !pending.is_empty() && admits_batch => dispatch!(),

                result = self.consumer.recv(), if admits_batch && pending.len() < batch_size => {
                    let message = result.context("Kafka consume failed")?;
                    if pending.is_empty() {
                        deadline.as_mut().reset(TokioInstant::now() + linger);
                    }
                    pending.push(to_event(&self.id, &mut topics, &mut buffer, &message));
                    if pending.len() >= batch_size {
                        dispatch!();
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rdkafka::topic_partition_list::Offset;

    use super::{TopicCache, offsets_for};
    use crate::event::{Event, SourceMetadata};

    fn event(topic: &str, partition: i32, offset: i64) -> Event {
        Event {
            key: None,
            payload: "x".into(),
            timestamp_ms: None,
            source: SourceMetadata {
                component_id: Arc::from("source"),
                topic: Arc::from(topic),
                partition,
                offset,
            },
        }
    }

    #[test]
    fn next_offset_is_highest_consumed_offset_plus_one() {
        let events = vec![
            event("a", 0, 10),
            event("a", 0, 12), // same partition: the highest offset wins
            event("a", 1, 5),
            event("b", 0, 3),
        ];
        let offsets = offsets_for(&events).unwrap();
        assert_eq!(
            offsets.find_partition("a", 0).unwrap().offset(),
            Offset::Offset(13)
        );
        assert_eq!(
            offsets.find_partition("a", 1).unwrap().offset(),
            Offset::Offset(6)
        );
        assert_eq!(
            offsets.find_partition("b", 0).unwrap().offset(),
            Offset::Offset(4)
        );
    }

    #[test]
    fn out_of_order_offsets_never_move_a_commit_backwards() {
        let events = vec![event("a", 0, 12), event("a", 0, 4), event("a", 0, 9)];
        assert_eq!(
            offsets_for(&events)
                .unwrap()
                .find_partition("a", 0)
                .unwrap()
                .offset(),
            Offset::Offset(13)
        );
    }

    #[test]
    fn empty_events_yield_no_offsets() {
        let offsets = offsets_for(&[]).unwrap();
        assert!(offsets.elements().is_empty());
    }

    #[test]
    fn topic_cache_returns_one_shared_arc_per_topic() {
        let mut cache = TopicCache::default();
        let first = cache.intern("a");
        let second = cache.intern("a");
        let other = cache.intern("b");
        let first_again = cache.intern("a");
        assert!(Arc::ptr_eq(&first, &second), "repeat lookup must be shared");
        assert!(
            Arc::ptr_eq(&first, &first_again),
            "map lookup must be shared"
        );
        assert_eq!(other.as_ref(), "b");
    }
}
