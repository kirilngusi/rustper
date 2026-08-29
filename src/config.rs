use std::{collections::HashMap, fs, path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub sources: HashMap<String, SourceConfig>,
    pub sinks: HashMap<String, SinkConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    Kafka(KafkaSourceConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaSourceConfig {
    pub brokers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    #[serde(default = "default_session_timeout_ms")]
    pub session_timeout_ms: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_batch_linger_ms")]
    pub batch_linger_ms: u64,
    #[serde(default = "default_max_in_flight_batches")]
    pub max_in_flight_batches: usize,
    #[serde(default = "default_max_in_flight_bytes")]
    pub max_in_flight_bytes: usize,
    /// librdkafka `queued.max.messages.kbytes`. This is the consumer-side fetch
    /// buffer and it is charged on top of `max_in_flight_bytes`.
    #[serde(default = "default_queued_max_messages_kbytes")]
    pub queued_max_messages_kbytes: u64,
    /// librdkafka `queued.min.messages`.
    #[serde(default = "default_queued_min_messages")]
    pub queued_min_messages: u64,
    /// librdkafka `max.poll.interval.ms`. The consumer leaves the group if the
    /// router does not call `recv` within this window.
    #[serde(default = "default_max_poll_interval_ms")]
    pub max_poll_interval_ms: u64,
    /// Escape hatch for any other librdkafka property; applied last.
    #[serde(default)]
    pub client_config: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkConfig {
    Kafka(KafkaSinkConfig),
    Clickhouse(ClickhouseSinkConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaSinkConfig {
    pub inputs: Vec<String>,
    pub brokers: String,
    pub topic: String,
    #[serde(default = "default_delivery_timeout_ms")]
    pub delivery_timeout_ms: u64,
    #[serde(default = "default_buffer_capacity")]
    pub buffer_capacity: usize,
    #[serde(default = "default_sink_batch_events")]
    pub batch_max_events: usize,
    #[serde(default = "default_sink_batch_bytes")]
    pub batch_max_bytes: usize,
    #[serde(default = "default_kafka_sink_linger_ms")]
    pub batch_linger_ms: u64,
    /// Batches this sink may have in flight at once.
    ///
    /// Values above 1 let the next batch coalesce while the current write is on
    /// the wire, which is what removes the `1 / (linger + write latency)`
    /// throughput ceiling. It also means batches can reach the destination out
    /// of order, so it stays at 1 unless the topology tolerates that.
    #[serde(default = "default_max_concurrent_writes")]
    pub max_concurrent_writes: usize,
    /// Total write attempts per batch, including the first.
    #[serde(default = "default_retry_max_attempts")]
    pub retry_max_attempts: u32,
    #[serde(default = "default_retry_initial_backoff_ms")]
    pub retry_initial_backoff_ms: u64,
    #[serde(default = "default_retry_max_backoff_ms")]
    pub retry_max_backoff_ms: u64,
    /// librdkafka `acks`. `"all"` is the durable default; `"1"` trades
    /// durability for throughput.
    #[serde(default = "default_acks")]
    pub acks: String,
    /// librdkafka `enable.idempotence`. When true librdkafka caps in-flight
    /// requests at 5 and forces `acks=all`, which bounds throughput.
    #[serde(default = "default_true")]
    pub enable_idempotence: bool,
    #[serde(default = "default_compression_type")]
    pub compression_type: String,
    /// librdkafka `linger.ms`, distinct from the router-side `batch_linger_ms`.
    #[serde(default = "default_producer_linger_ms")]
    pub producer_linger_ms: u64,
    #[serde(default = "default_producer_batch_num_messages")]
    pub producer_batch_num_messages: u64,
    /// librdkafka `queue.buffering.max.messages`, per sink.
    #[serde(default = "default_queue_buffering_max_messages")]
    pub queue_buffering_max_messages: u64,
    /// librdkafka `queue.buffering.max.kbytes`, per sink. The librdkafka
    /// default is 1 GiB, which dwarfs the router's own budgets.
    #[serde(default = "default_queue_buffering_max_kbytes")]
    pub queue_buffering_max_kbytes: u64,
    /// Escape hatch for any other librdkafka property; applied last.
    #[serde(default)]
    pub client_config: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseSinkConfig {
    pub inputs: Vec<String>,
    pub endpoint: String,
    #[serde(default = "default_database")]
    pub database: String,
    pub table: String,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_buffer_capacity")]
    pub buffer_capacity: usize,
    #[serde(default = "default_sink_batch_events")]
    pub batch_max_events: usize,
    #[serde(default = "default_sink_batch_bytes")]
    pub batch_max_bytes: usize,
    #[serde(default = "default_clickhouse_sink_linger_ms")]
    pub batch_linger_ms: u64,
    /// Batches this sink may have in flight at once.
    ///
    /// Values above 1 let the next batch coalesce while the current write is on
    /// the wire, which is what removes the `1 / (linger + write latency)`
    /// throughput ceiling. It also means batches can reach the destination out
    /// of order, so it stays at 1 unless the topology tolerates that.
    #[serde(default = "default_max_concurrent_writes")]
    pub max_concurrent_writes: usize,
    /// Total write attempts per batch, including the first.
    #[serde(default = "default_retry_max_attempts")]
    pub retry_max_attempts: u32,
    #[serde(default = "default_retry_initial_backoff_ms")]
    pub retry_initial_backoff_ms: u64,
    #[serde(default = "default_retry_max_backoff_ms")]
    pub retry_max_backoff_ms: u64,
}

/// Delivery behaviour resolved into the types the sink runtime uses.
#[derive(Debug, Clone, Copy)]
pub struct DeliverySettings {
    pub max_concurrent_writes: usize,
    pub retry_max_attempts: u32,
    pub retry_initial_backoff: Duration,
    pub retry_max_backoff: Duration,
}

/// Both sink configs carry the same four delivery fields inline, because
/// `#[serde(flatten)]` would force us to give up `deny_unknown_fields` and with
/// it the ability to reject a mistyped config key.
macro_rules! delivery_settings {
    ($config:expr) => {
        DeliverySettings {
            max_concurrent_writes: $config.max_concurrent_writes.max(1),
            retry_max_attempts: $config.retry_max_attempts.max(1),
            retry_initial_backoff: Duration::from_millis($config.retry_initial_backoff_ms),
            retry_max_backoff: Duration::from_millis($config.retry_max_backoff_ms),
        }
    };
}

fn default_session_timeout_ms() -> u64 {
    // Kafka's own default. A 10 s timeout looks harmless but makes the broker
    // fence the consumer whenever a heartbeat is delayed by a saturated
    // connection, and each rebalance costs seconds of stalled consumption.
    45_000
}
fn default_batch_size() -> usize {
    1_000
}
fn default_batch_linger_ms() -> u64 {
    5
}
fn default_max_in_flight_batches() -> usize {
    8
}
fn default_max_in_flight_bytes() -> usize {
    256 * 1024 * 1024
}
fn default_queued_max_messages_kbytes() -> u64 {
    65_536
}
fn default_queued_min_messages() -> u64 {
    100_000
}
fn default_max_poll_interval_ms() -> u64 {
    300_000
}
fn default_delivery_timeout_ms() -> u64 {
    30_000
}
fn default_buffer_capacity() -> usize {
    32
}
fn default_sink_batch_events() -> usize {
    10_000
}
fn default_sink_batch_bytes() -> usize {
    16 * 1024 * 1024
}
fn default_kafka_sink_linger_ms() -> u64 {
    5
}
fn default_clickhouse_sink_linger_ms() -> u64 {
    50
}
fn default_database() -> String {
    "default".into()
}
fn default_acks() -> String {
    "all".into()
}
fn default_true() -> bool {
    true
}
fn default_compression_type() -> String {
    "lz4".into()
}
fn default_producer_linger_ms() -> u64 {
    5
}
fn default_producer_batch_num_messages() -> u64 {
    10_000
}
fn default_queue_buffering_max_messages() -> u64 {
    100_000
}
fn default_queue_buffering_max_kbytes() -> u64 {
    1_048_576
}
fn default_max_concurrent_writes() -> usize {
    1
}
fn default_retry_max_attempts() -> u32 {
    5
}
fn default_retry_initial_backoff_ms() -> u64 {
    100
}
fn default_retry_max_backoff_ms() -> u64 {
    5_000
}

impl SinkConfig {
    pub fn inputs(&self) -> &[String] {
        match self {
            Self::Kafka(c) => &c.inputs,
            Self::Clickhouse(c) => &c.inputs,
        }
    }

    pub fn buffer_capacity(&self) -> usize {
        match self {
            Self::Kafka(c) => c.buffer_capacity,
            Self::Clickhouse(c) => c.buffer_capacity,
        }
    }

    /// Returns the `(max_events, max_bytes)` batch limits for this sink.
    pub fn batch_limits(&self) -> (usize, usize) {
        match self {
            Self::Kafka(c) => (c.batch_max_events, c.batch_max_bytes),
            Self::Clickhouse(c) => (c.batch_max_events, c.batch_max_bytes),
        }
    }

    pub fn runtime_settings(&self) -> DeliverySettings {
        match self {
            Self::Kafka(c) => delivery_settings!(c),
            Self::Clickhouse(c) => delivery_settings!(c),
        }
    }
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("cannot read config {}", path.display()))?;
        let config: Self =
            toml::from_str(&raw).with_context(|| format!("invalid TOML in {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.sources.is_empty() {
            bail!("at least one source is required");
        }
        if self.sinks.is_empty() {
            bail!("at least one sink is required");
        }
        for (id, sink) in &self.sinks {
            if sink.inputs().is_empty() {
                bail!("sink {id:?} has no inputs");
            }
            if sink.buffer_capacity() == 0 {
                bail!("sink {id:?} buffer_capacity must be > 0");
            }
            let (max_events, max_bytes) = sink.batch_limits();
            if max_events == 0 || max_bytes == 0 {
                bail!("sink {id:?} batch limits must be > 0");
            }
            let delivery = sink.runtime_settings();
            if delivery.retry_initial_backoff > delivery.retry_max_backoff {
                bail!("sink {id:?} retry_initial_backoff_ms must be <= retry_max_backoff_ms");
            }
            for input in sink.inputs() {
                if !self.sources.contains_key(input) {
                    bail!("sink {id:?} references unknown input {input:?}");
                }
            }
        }
        for (id, source) in &self.sources {
            let SourceConfig::Kafka(source) = source;
            if source.topics.is_empty() {
                bail!("source {id:?} has no topics");
            }
            if source.batch_size == 0
                || source.max_in_flight_batches == 0
                || source.max_in_flight_bytes == 0
            {
                bail!("source {id:?} batch and in-flight limits must be > 0");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, SinkConfig};

    fn parse(raw: &str) -> Config {
        toml::from_str(raw).unwrap()
    }

    const MINIMAL: &str = r#"
        [sources.in]
        type = "kafka"
        brokers = "localhost:9092"
        group_id = "router"
        topics = ["input"]

        [sinks.out]
        type = "kafka"
        inputs = ["in"]
        brokers = "localhost:9092"
        topic = "output"
    "#;

    #[test]
    fn parses_vector_style_topology() {
        let config = parse(MINIMAL);
        config.validate().unwrap();
        assert_eq!(config.sources.len(), 1);
        assert_eq!(config.sinks["out"].buffer_capacity(), 32);
    }

    #[test]
    fn delivery_defaults_preserve_sequential_ordering() {
        let settings = parse(MINIMAL).sinks["out"].runtime_settings();
        assert_eq!(settings.max_concurrent_writes, 1);
        assert_eq!(settings.retry_max_attempts, 5);
    }

    #[test]
    fn librdkafka_knobs_are_configurable() {
        let config = parse(&format!(
            "{MINIMAL}\n\
             acks = \"1\"\n\
             enable_idempotence = false\n\
             max_concurrent_writes = 4\n\
             queue_buffering_max_kbytes = 65536\n\
             client_config = {{ \"socket.nagle.disable\" = \"true\" }}\n"
        ));
        config.validate().unwrap();
        let SinkConfig::Kafka(sink) = &config.sinks["out"] else {
            panic!("expected a Kafka sink");
        };
        assert_eq!(sink.acks, "1");
        assert!(!sink.enable_idempotence);
        assert_eq!(sink.queue_buffering_max_kbytes, 65_536);
        assert_eq!(sink.client_config["socket.nagle.disable"], "true");
        assert_eq!(
            config.sinks["out"].runtime_settings().max_concurrent_writes,
            4
        );
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let raw = format!("{MINIMAL}\nbatch_max_evnets = 10\n");
        assert!(
            toml::from_str::<Config>(&raw).is_err(),
            "typo must not be silently ignored"
        );
    }

    #[test]
    fn backwards_backoff_window_is_rejected() {
        let raw = format!("{MINIMAL}\nretry_initial_backoff_ms = 9000\n");
        assert!(parse(&raw).validate().is_err());
    }
}

#[cfg(test)]
mod shipped_config_tests {
    use super::Config;

    /// The shipped configs are the first thing a reader runs. `deny_unknown_fields`
    /// makes a stale key a hard error, so they are validated here rather than at
    /// someone's first `cargo run`.
    #[test]
    fn every_shipped_config_parses_and_validates() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/config");
        let mut checked = 0;
        for entry in std::fs::read_dir(dir).expect("config directory") {
            let path = entry.expect("directory entry").path();
            if path.extension().is_none_or(|ext| ext != "toml") {
                continue;
            }
            Config::load(&path).unwrap_or_else(|e| panic!("{}: {e:#}", path.display()));
            checked += 1;
        }
        assert!(checked > 0, "no configs were checked");
    }
}
