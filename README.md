# rustper

A Kafka fan-out router in Rust. One source normalises records into a
transport-neutral event, every configured sink receives the same batch, and the
source commits only the contiguous prefix of batches that every sink has
acknowledged.

Delivery is at-least-once. A batch that any sink rejects is never committed, so
Kafka replays it; sinks that already accepted it will see it again.

Sources: Kafka. Sinks: Kafka, ClickHouse.

> **Status: this is a learning and benchmarking project, not production
> infrastructure.** There are no metrics, no latency instrumentation, and the
> ClickHouse sink writes a fixed schema. Read [Limitations](#limitations) before
> depending on it.

## Quick start

```bash
docker compose up -d

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic rustper-input --partitions 6 --replication-factor 1

cargo run --release
```

The default config is `config/local.toml`; pass a different path as the first
argument.

## Running in Docker

```bash
docker compose --profile router up -d --build
docker compose logs -f rustper
```

Multi-stage build on a slim Debian runtime, ~113 MB, running as non-root
UID/GID `10001`. Compose mounts `config/docker.toml`; Kafka listens on
`kafka:19092` inside the Docker network and `localhost:9092` from the host.

Building the image on its own:

```bash
docker build --tag rustper:local .
docker run --rm \
  --network rustper_default \
  --volume "$PWD/config/docker.toml:/etc/rustper/config.toml:ro" \
  rustper:local
```

## Configuration

Configs are TOML with `deny_unknown_fields`, so a mistyped key is a startup
error rather than a silently ignored line. Any librdkafka property without a
dedicated field goes through `client_config`.

The knobs that matter most (`docs/PERFORMANCE.md` §5 explains each one):

```toml
# source: in-flight window and micro-batching
batch_size = 1000
batch_linger_ms = 5
max_in_flight_batches = 8
max_in_flight_bytes = 268435456
session_timeout_ms = 45000    # below ~45 s the broker fences the consumer
                              # under load; every rebalance stalls consumption

# sink: coalescing, write concurrency, retry
batch_max_events = 10000
batch_max_bytes = 16777216
max_concurrent_writes = 1     # > 1 is faster, but batches may arrive out of order
retry_max_attempts = 5

# Kafka sink: durability and librdkafka's buffer ceiling
acks = "all"
enable_idempotence = true     # librdkafka caps in-flight requests at 5 when on
queue_buffering_max_kbytes = 1048576
```

## ClickHouse sink: schema contract

The sink writes a **fixed set of seven columns**. There is no column mapping and
no way to project the payload into typed columns. Point it at an existing table
only if that table satisfies the contract below.

| Column | Required type |
| --- | --- |
| `timestamp_ms` | `Int64` (or `DateTime64(3)`) |
| `source_component` | `String` or `LowCardinality(String)` |
| `source_topic` | `String` or `LowCardinality(String)` |
| `source_partition` | `Int32` |
| `source_offset` | `Int64` |
| `key` | `String` |
| `payload` | `String` |

A table that works:

```sql
CREATE TABLE rustper_events
(
    timestamp_ms     Int64,
    source_component String,
    source_topic     String,
    source_partition Int32,
    source_offset    Int64,
    key              String,
    payload          String
)
ENGINE = MergeTree
ORDER BY (source_topic, source_partition, source_offset);
```

Under the hood the client issues `DESCRIBE TABLE` once per table, then
`INSERT INTO tbl(col, col, ...) FORMAT RowBinaryWithNamesAndTypes` with the
columns named explicitly. Verified behaviour against ClickHouse 25.8:

| Your schema | Result |
| --- | --- |
| Columns declared in any other order | works — matching is by name, not position |
| Extra column **with** a `DEFAULT` | works, ClickHouse fills it |
| `timestamp_ms DateTime64(3)` | works, `1700000000123` reads back as `2023-11-14 22:13:20.123` |
| `LowCardinality(String)` | works |
| Extra column **without** a default | rejected: "non-default columns are missing" |
| A missing or renamed column | rejected: "database schema has no column named ..." |
| `key Nullable(String)` | rejected: type mismatch |
| `source_partition UInt32` instead of `Int32` | rejected: type mismatch |
| `timestamp_ms DateTime` (32-bit) | rejected: type mismatch |

Mismatches fail fast with a clear message, so you find out at startup rather
than through corrupted rows.

> **The schema is cached and never invalidated.** `DESCRIBE TABLE` runs once per
> table for the lifetime of the process. If you `ALTER TABLE` while the router
> is running, it keeps using the stale schema: a column added without a default
> is silently filled with the type default instead of raising an error, and the
> same table rejects the insert outright on the next restart. **Restart the
> router after any ClickHouse migration.**

To try it, create the table above and run:

```bash
cargo run --release -- config/clickhouse.toml
```

## Performance

Measured on an Apple M1 Pro with the broker in a Docker VM on the same machine.
Full methodology and caveats in `docs/PERFORMANCE.md` §3.

| | End-to-end via Kafka | In-process (no broker) |
| --- | --- | --- |
| Throughput | 348,502 msg/s steady (255,704 average) | 1,028,164 msg/s |
| Fan-out | ~697,000 deliveries/s | 3,084,493 deliveries/s |
| CPU | 1.25 core (4.69 CPU-s per million msg) | 0.54 core (0.52 CPU-s per million msg) |
| RSS | 195 MiB median, 384 MiB peak | 18 MiB |

The router's own logic is 11% of the end-to-end CPU budget; librdkafka,
compression and syscalls are the other 89%.

These numbers are `n = 1` per configuration, contain no latency measurement at
any percentile, and were never taken on Linux or bare metal. `docs/PERFORMANCE.md`
§8 lists exactly what they do not answer.

| Command | What it measures |
| --- | --- |
| `cargo run --release --bin benchmark` | router overhead alone, no broker |
| `cargo run --release --example normalize_ab` | arena copy vs. one alloc per field |
| `cargo run --release --example ch_schema_probe -- <table>` | how a ClickHouse schema is accepted or rejected |
| `cargo run --release --bin kafka-load` | load generator for end-to-end runs |

```bash
KAFKA_TOPIC=rustper-input MESSAGE_COUNT=100000 \
  cargo run --release --bin kafka-load
```

## Limitations

- **No metrics.** No Prometheus endpoint, no latency histogram, no rebalance
  counter. A 25-second consumption stall caused by consumer-group rebalancing
  was invisible from inside the process during benchmarking — it could only be
  found in the broker log.
- **ClickHouse schema is fixed** to the seven columns above.
- **One source type.** Kafka only.
- **No disk buffering.** Anything in flight when the process dies is replayed
  from Kafka, which is correct for at-least-once but means no durability beyond
  the broker's retention.
- **Exhausted retries terminate the router.** This is deliberate: the batch's
  offsets are never committed, so a restart replays it. Dropping the batch is
  the one outcome that would break at-least-once.

## Reading the code

1. `docs/PERFORMANCE.md` — hot path architecture, benchmarks, scaling guide.
2. `docs/LEARNING_NOTES.md` — design rationale, and Rust explained for Go
   developers. *(Both docs are currently written in Vietnamese.)*
3. `src/main.rs` — load config, start the runtime.
4. `src/config.rs` — Serde structs for the TOML.
5. `src/topology.rs` — wiring, fan-out, buffers, acknowledgement.
6. `src/source/kafka.rs` — consume, micro-batch, commit offsets.
7. `src/event.rs` — `Event`, `EventBatch`, `BatchBuffer` (arena copy).
8. `src/sink/` — the `Sink` trait plus the Kafka and ClickHouse implementations.

### Coming from Go

| Go | Rust in this project |
| --- | --- |
| `go func()` | `tokio::spawn(async move { ... })` |
| `chan *EventBatch` | `tokio::sync::mpsc::Sender<SinkEnvelope>` |
| closing every sender | `drop(outputs)` |
| `<-ch` until the channel closes | `while let Some(x) = receiver.recv().await` |
| `(value, error)` | `Result<T, E>` |

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
