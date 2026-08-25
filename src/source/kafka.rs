use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::BorrowedMessage,
    topic_partition_list::{Offset, TopicPartitionList},
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    config::KafkaSourceConfig,
    event::{Event, SourceMetadata},
    source::Source,
    topology::Fanout,
};

pub struct KafkaSource {
    id: Arc<str>,
    config: KafkaSourceConfig,
    consumer: StreamConsumer,
}

impl KafkaSource {
    pub fn new(id: &str, config: &KafkaSourceConfig) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("session.timeout.ms", config.session_timeout_ms.to_string())
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .context("cannot create Kafka source")?;
        let topics: Vec<&str> = config.topics.iter().map(String::as_str).collect();
        consumer
            .subscribe(&topics)
            .context("cannot subscribe to Kafka topics")?;
        Ok(Self {
            id: Arc::from(id),
            config: config.clone(),
            consumer,
        })
    }
}

struct BatchCompletion {
    sequence: u64,
    offsets: TopicPartitionList,
    events: u64,
    bytes: usize,
}

fn to_event(
    component_id: &Arc<str>,
    topics: &mut HashMap<String, Arc<str>>,
    message: &BorrowedMessage<'_>,
) -> Event {
    let topic = topics
        .entry(message.topic().to_owned())
        .or_insert_with(|| Arc::from(message.topic()))
        .clone();
    Event {
        key: message.key().map(Bytes::copy_from_slice),
        payload: message
            .payload()
            .map(Bytes::copy_from_slice)
            .unwrap_or_default(),
        timestamp_ms: message.timestamp().to_millis(),
        source: SourceMetadata {
            component_id: Arc::clone(component_id),
            topic,
            partition: message.partition(),
            offset: message.offset(),
        },
    }
}

fn offsets_for(events: &[Event]) -> Result<TopicPartitionList> {
    let mut next_offsets: HashMap<(Arc<str>, i32), i64> = HashMap::new();
    for event in events {
        next_offsets
            .entry((Arc::clone(&event.source.topic), event.source.partition))
            .and_modify(|offset| *offset = (*offset).max(event.source.offset + 1))
            .or_insert(event.source.offset + 1);
    }
    let mut offsets = TopicPartitionList::new();
    for ((topic, partition), offset) in next_offsets {
        offsets
            .add_partition_offset(&topic, partition, Offset::Offset(offset))
            .map_err(|error| anyhow!(error))?;
    }
    Ok(offsets)
}

fn commit_contiguous(
    consumer: &StreamConsumer,
    completed: &mut BTreeMap<u64, BatchCompletion>,
    next_sequence: &mut u64,
    completion: BatchCompletion,
) -> Result<u64> {
    completed.insert(completion.sequence, completion);
    let mut committed_events = 0;
    while let Some(completion) = completed.remove(next_sequence) {
        consumer
            .commit(&completion.offsets, CommitMode::Async)
            .map_err(|error| anyhow!(error))
            .context("cannot commit Kafka batch")?;
        committed_events += completion.events;
        *next_sequence += 1;
    }
    Ok(committed_events)
}

#[async_trait]
impl Source for KafkaSource {
    async fn run(self: Box<Self>, output: Fanout, shutdown: CancellationToken) -> Result<()> {
        info!(component = %self.id, topics = ?self.config.topics, "source started");
        let mut processed = 0_u64;
        let mut measurement_started: Option<std::time::Instant> = None;
        let mut next_report = 100_000_u64;
        let mut topic_cache = HashMap::new();
        let mut in_flight: tokio::task::JoinSet<Result<BatchCompletion>> =
            tokio::task::JoinSet::new();
        let mut sequence = 0_u64;
        let mut next_commit_sequence = 0_u64;
        let mut completed = BTreeMap::new();
        let mut in_flight_bytes = 0_usize;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    in_flight.abort_all();
                    return Ok(());
                },
                joined = in_flight.join_next(), if !in_flight.is_empty() => {
                    let completion = joined.expect("in-flight set is not empty")
                        .context("fan-out task panicked")??;
                    in_flight_bytes = in_flight_bytes.saturating_sub(completion.bytes);
                    processed += commit_contiguous(
                        &self.consumer, &mut completed, &mut next_commit_sequence, completion,
                    )?;
                    if processed >= next_report {
                        let seconds = measurement_started.unwrap().elapsed().as_secs_f64();
                        info!(component = %self.id, processed, seconds,
                            messages_per_second = processed as f64 / seconds, "source throughput");
                        next_report = (processed / 100_000 + 1) * 100_000;
                    }
                },
                result = self.consumer.recv(), if in_flight.len() < self.config.max_in_flight_batches
                    && in_flight_bytes < self.config.max_in_flight_bytes => {
                    measurement_started.get_or_insert_with(std::time::Instant::now);
                    let first = result.context("Kafka consume failed")?;
                    let mut events = Vec::with_capacity(self.config.batch_size);
                    events.push(to_event(&self.id, &mut topic_cache, &first));
                    let deadline = tokio::time::Instant::now()
                        + Duration::from_millis(self.config.batch_linger_ms);
                    while events.len() < self.config.batch_size {
                        match tokio::time::timeout_at(deadline, self.consumer.recv()).await {
                            Ok(Ok(message)) => events.push(to_event(&self.id, &mut topic_cache, &message)),
                            Ok(Err(error)) => return Err(error).context("Kafka consume failed"),
                            Err(_) => break,
                        }
                    }
                    let offsets = offsets_for(&events)?;
                    let event_count = events.len() as u64;
                    let event_bytes = events.iter().map(Event::estimated_size).sum::<usize>();
                    in_flight_bytes = in_flight_bytes.saturating_add(event_bytes);
                    let batch_sequence = sequence;
                    sequence += 1;
                    let fanout = output.clone();
                    in_flight.spawn(async move {
                        fanout.send(events).await?;
                        Ok::<_, anyhow::Error>(BatchCompletion {
                            sequence: batch_sequence, offsets, events: event_count, bytes: event_bytes,
                        })
                    });
                }
            }
        }
    }
}
