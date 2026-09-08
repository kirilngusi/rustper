use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use rdkafka::{
    ClientConfig, ClientContext,
    error::{KafkaError, RDKafkaErrorCode},
    message::DeliveryResult,
    producer::{BaseRecord, ProducerContext, ThreadedProducer},
};
use tokio::sync::oneshot;

use crate::{
    config::KafkaSinkConfig,
    event::EventBatch,
    sink::{BatchSettings, Sink, WriteOutcome},
};

/// How long to wait before re-offering a record librdkafka refused because its
/// queue was full.
const QUEUE_FULL_BACKOFF: Duration = Duration::from_millis(2);

/// Tracks the delivery reports of one write.
///
/// The previous implementation awaited one `FutureProducer` future per record,
/// which cost a oneshot channel and a `FuturesOrdered` node for every message.
/// Here every record of a write shares one tracker: the hot path per delivery
/// report is a single atomic decrement, and the whole write wakes once.
struct BatchTracker {
    remaining: AtomicUsize,
    failed: AtomicBool,
    error: Mutex<Option<KafkaError>>,
    notify: Mutex<Option<oneshot::Sender<()>>>,
}

impl BatchTracker {
    fn new(records: usize) -> (Arc<Self>, oneshot::Receiver<()>) {
        let (notify, done) = oneshot::channel();
        let tracker = Arc::new(Self {
            remaining: AtomicUsize::new(records),
            failed: AtomicBool::new(false),
            error: Mutex::new(None),
            notify: Mutex::new(Some(notify)),
        });
        (tracker, done)
    }

    /// Settles `count` records, recording the first error seen.
    fn settle(&self, count: usize, error: Option<KafkaError>) {
        if count == 0 {
            return;
        }
        if let Some(error) = error
            && !self.failed.swap(true, Ordering::AcqRel)
        {
            *self.error.lock().expect("tracker error mutex poisoned") = Some(error);
        }
        if self.remaining.fetch_sub(count, Ordering::AcqRel) == count
            && let Some(notify) = self
                .notify
                .lock()
                .expect("tracker notify mutex poisoned")
                .take()
        {
            let _ = notify.send(());
        }
    }

    fn take_error(&self) -> Option<KafkaError> {
        self.error
            .lock()
            .expect("tracker error mutex poisoned")
            .take()
    }
}

struct BatchContext;

impl ClientContext for BatchContext {}

impl ProducerContext for BatchContext {
    type DeliveryOpaque = Arc<BatchTracker>;

    fn delivery(&self, result: &DeliveryResult<'_>, tracker: Arc<BatchTracker>) {
        tracker.settle(1, result.as_ref().err().map(|(error, _)| error.clone()));
    }
}

pub struct KafkaSink {
    id: String,
    topic: String,
    enqueue_timeout: Duration,
    producer: ThreadedProducer<BatchContext>,
    batch: BatchSettings,
}

impl KafkaSink {
    pub fn new(id: &str, config: &KafkaSinkConfig) -> Result<Self> {
        let mut client = ClientConfig::new();
        client
            .set("bootstrap.servers", &config.brokers)
            .set("enable.idempotence", config.enable_idempotence.to_string())
            .set("acks", &config.acks)
            .set("compression.type", &config.compression_type)
            .set("linger.ms", config.producer_linger_ms.to_string())
            .set(
                "batch.num.messages",
                config.producer_batch_num_messages.to_string(),
            )
            .set(
                "queue.buffering.max.messages",
                config.queue_buffering_max_messages.to_string(),
            )
            .set(
                "queue.buffering.max.kbytes",
                config.queue_buffering_max_kbytes.to_string(),
            )
            .set("message.timeout.ms", config.delivery_timeout_ms.to_string());
        for (key, value) in &config.client_config {
            client.set(key, value);
        }
        let producer = client
            .create_with_context(BatchContext)
            .with_context(|| format!("cannot create Kafka sink {id:?}"))?;
        Ok(Self {
            id: id.into(),
            topic: config.topic.clone(),
            enqueue_timeout: Duration::from_millis(config.delivery_timeout_ms),
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

    async fn write_batches(&self, batches: &[Arc<EventBatch>]) -> Result<WriteOutcome> {
        let total: usize = batches.iter().map(|batch| batch.len()).sum();
        if total == 0 {
            return Ok(WriteOutcome::default());
        }
        let (tracker, done) = BatchTracker::new(total);
        let enqueue_deadline = tokio::time::Instant::now() + self.enqueue_timeout;

        let mut enqueued = 0_usize;
        let mut enqueue_error = None;
        'enqueue: for event in batches.iter().flat_map(|batch| batch.events()) {
            let mut record = BaseRecord::with_opaque_to(&self.topic, Arc::clone(&tracker))
                .payload(&event.payload[..]);
            if let Some(key) = &event.key {
                record = record.key(&key[..]);
            }
            if let Some(timestamp) = event.timestamp_ms {
                record = record.timestamp(timestamp);
            }
            loop {
                match self.producer.send(record) {
                    Ok(()) => {
                        enqueued += 1;
                        continue 'enqueue;
                    }
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned)) => {
                        if tokio::time::Instant::now() >= enqueue_deadline {
                            enqueue_error = Some(anyhow!(
                                "librdkafka queue stayed full for {:?}; raise \
                                 queue_buffering_max_messages/kbytes or lower batch_max_events",
                                self.enqueue_timeout
                            ));
                            break 'enqueue;
                        }
                        record = returned;
                        tokio::time::sleep(QUEUE_FULL_BACKOFF).await;
                    }
                    Err((error, _)) => {
                        enqueue_error = Some(anyhow!(error).context("cannot enqueue Kafka record"));
                        break 'enqueue;
                    }
                }
            }
        }

        // Records that were never enqueued will never produce a delivery
        // report, so settle them here or the write would wait forever.
        if enqueue_error.is_some() {
            tracker.settle(total - enqueued, None);
        }
        done.await
            .map_err(|_| anyhow!("Kafka delivery tracker dropped"))?;

        if let Some(error) = enqueue_error {
            return Err(error);
        }
        match tracker.take_error() {
            Some(error) => Err(anyhow!(error).context("Kafka delivery failed")),
            None => Ok(WriteOutcome::written(total as u64)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BatchTracker;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    #[tokio::test]
    async fn a_write_completes_once_every_record_is_settled() {
        let (tracker, done) = BatchTracker::new(3);
        tracker.settle(1, None);
        tracker.settle(1, None);

        tracker.settle(1, None);
        done.await.unwrap();
        assert!(tracker.take_error().is_none());
    }

    #[tokio::test]
    async fn the_first_delivery_error_is_reported() {
        let (tracker, done) = BatchTracker::new(3);
        let first = KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut);
        let second = KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull);
        tracker.settle(1, Some(first));
        tracker.settle(1, Some(second));
        tracker.settle(1, None);
        done.await.unwrap();
        let error = tracker.take_error().expect("an error was recorded");
        assert!(
            error.to_string().contains("Message timed out"),
            "got {error}"
        );
    }

    #[tokio::test]
    async fn abandoned_records_still_release_the_write() {
        let (tracker, done) = BatchTracker::new(10);
        tracker.settle(4, None);
        // Six records were never enqueued: settling them must wake the write
        // instead of leaving it parked forever.
        tracker.settle(6, None);
        done.await.unwrap();
    }
}
