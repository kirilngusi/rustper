//! Checks whether a ClickHouse table satisfies the sink's schema contract.
//!
//! The ClickHouse sink writes a fixed set of seven columns. Rather than
//! discovering a mismatch when the router is already consuming, point this at a
//! candidate table: it runs one real write through the production sink code and
//! prints whatever ClickHouse says.
//!
//! ```text
//! cargo run --release --example ch_schema_probe -- my_table
//! ```
//!
//! Connection details come from `CLICKHOUSE_URL`, `CLICKHOUSE_DB`,
//! `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD`, defaulting to the
//! `docker-compose.yml` service.
//!
//! Each column receives a distinctive value, so reading the row back shows
//! immediately whether anything landed in the wrong place:
//!
//! ```sql
//! SELECT * FROM my_table ORDER BY source_offset DESC LIMIT 1 FORMAT Vertical;
//! ```
//!
//! The row it writes is real. Use a scratch table, or delete it afterwards.

use std::{env, sync::Arc};

use rustper::{
    config::{ClickhouseSinkConfig, SinkConfig},
    event::{Event, EventBatch, SourceMetadata},
    sink::build,
};

fn env_or(key: &str, fallback: &str) -> String {
    env::var(key).unwrap_or_else(|_| fallback.to_owned())
}

fn config(table: &str) -> SinkConfig {
    SinkConfig::Clickhouse(ClickhouseSinkConfig {
        inputs: vec!["probe".into()],
        endpoint: env_or("CLICKHOUSE_URL", "http://localhost:8123"),
        database: env_or("CLICKHOUSE_DB", "default"),
        table: table.to_owned(),
        user: env_or("CLICKHOUSE_USER", "default"),
        password: env_or("CLICKHOUSE_PASSWORD", "rustper"),
        buffer_capacity: 1,
        batch_max_events: 1,
        batch_max_bytes: 1 << 20,
        batch_linger_ms: 0,
        max_concurrent_writes: 1,
        retry_max_attempts: 1,
        retry_initial_backoff_ms: 1,
        retry_max_backoff_ms: 1,
    })
}

fn probe_batch() -> Arc<EventBatch> {
    Arc::new(EventBatch::new(vec![Event {
        key: Some("PROBE-KEY".into()),
        payload: "PROBE-PAYLOAD".into(),
        timestamp_ms: Some(1_700_000_000_123),
        source: SourceMetadata {
            component_id: Arc::from("PROBE-COMPONENT"),
            topic: Arc::from("PROBE-TOPIC"),
            partition: 42,
            offset: 99,
        },
    }]))
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let Some(table) = env::args().nth(1) else {
        eprintln!("usage: ch_schema_probe <table>");
        return std::process::ExitCode::FAILURE;
    };
    let sink = match build("probe", &config(&table)) {
        Ok(sink) => sink,
        Err(error) => {
            eprintln!("cannot build the ClickHouse sink: {error:#}");
            return std::process::ExitCode::FAILURE;
        }
    };

    match sink.write_batches(&[probe_batch()]).await {
        Ok(()) => {
            println!("OK: {table:?} accepts the sink's schema.");
            println!("Wrote one probe row (source_offset = 99); delete it when done.");
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            println!("REJECTED: {table:?} does not satisfy the schema contract.");
            println!("\n{error:#}\n");
            println!("The sink requires exactly these columns:");
            for (name, ty) in [
                ("timestamp_ms", "Int64 or DateTime64(3)"),
                ("source_component", "String or LowCardinality(String)"),
                ("source_topic", "String or LowCardinality(String)"),
                ("source_partition", "Int32"),
                ("source_offset", "Int64"),
                ("key", "String"),
                ("payload", "String"),
            ] {
                println!("  {name:<17} {ty}");
            }
            println!("\nColumn order does not matter. Any additional column needs a DEFAULT.");
            std::process::ExitCode::FAILURE
        }
    }
}
