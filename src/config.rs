use std::{collections::HashMap, fs, path::Path};

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
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkConfig {
    Kafka(KafkaSinkConfig),
    Clickhouse(ClickhouseSinkConfig),
}

#[derive(Debug, Clone, Deserialize)]
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
}

#[derive(Debug, Clone, Deserialize)]
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
}

fn default_session_timeout_ms() -> u64 {
    10_000
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
            let (max_events, max_bytes) = match sink {
                SinkConfig::Kafka(c) => (c.batch_max_events, c.batch_max_bytes),
                SinkConfig::Clickhouse(c) => (c.batch_max_events, c.batch_max_bytes),
            };
            if max_events == 0 || max_bytes == 0 {
                bail!("sink {id:?} batch limits must be > 0");
            }
            for input in sink.inputs() {
                if !self.sources.contains_key(input) {
                    bail!("sink {id:?} references unknown input {input:?}");
                }
            }
        }
        for (id, source) in &self.sources {
            let SourceConfig::Kafka(source) = source;
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
    use super::Config;

    #[test]
    fn parses_vector_style_topology() {
        let config: Config = toml::from_str(
            r#"
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
        "#,
        )
        .unwrap();
        config.validate().unwrap();
        assert_eq!(config.sources.len(), 1);
        assert_eq!(config.sinks["out"].buffer_capacity(), 32);
    }
}
