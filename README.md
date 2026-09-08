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

## ClickHouse sink

Two modes. `schema = "event"` is the default and unchanged; `schema = "json"`
writes into a table you already own.

### `schema = "json"`: your own table

The payload is treated as a JSON object whose keys are column names, streamed
with `FORMAT JSONEachRow`. **ClickHouse performs every type conversion**, so the
router never needs to know that a column is `Decimal(18, 4)`, and every
ClickHouse type works without the router supporting it explicitly.

```toml
[sinks.orders]
type = "clickhouse"
inputs = ["kafka_in"]
endpoint = "http://localhost:8123"
table = "orders"
schema = "json"

[sinks.orders.metadata_columns]   # optional; only declared keys are injected
topic     = "_topic"
partition = "_partition"
offset    = "_offset"
timestamp = "_event_ts"
key       = "_key"
```

Given `{"order_id":1,"amount":"19.99","tags":["new"]}` on `orders-in`, a table
of `order_id UInt64, amount Decimal(18,4), tags Array(String), _topic
LowCardinality(String), _partition Int32, _offset Int64` receives exactly that,
with the Kafka coordinates filled in.

Rows the router can reject in constant time — an empty payload, or one that is
not a JSON object — are dropped, counted in `rustper_sink_events_dropped_total`,
and summarised in one warning per write rather than one per row. Anything
subtler, such as a value that does not fit its column, is left to ClickHouse:
set `allow_errors_num` or `allow_errors_ratio` and it skips those rows instead
of failing the batch.

> `json` mode does **not** validate the schema at startup. Unlike `event` mode
> it issues no `DESCRIBE TABLE`, so a mismatched table is only discovered on the
> first real insert. Check a table before deploying with
> `cargo run --release --example ch_schema_probe -- my_table json`.

**Reshaping data is ClickHouse's job, not the router's.** rustper has no
transformation language on purpose. Land raw rows in a staging table and attach
a materialized view; ClickHouse does that work in vectorized C++ over whole
blocks, which no row-by-row router can match.

### `schema = "event"`: the built-in schema

The default mode writes a **fixed set of seven columns**. Point it at an
existing table only if that table satisfies the contract below, or use
`schema = "json"` instead.

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
| `cargo run --release --example ch_schema_probe -- <table> [event\|json]` | whether a ClickHouse table is accepted |
| `cargo run --release --bin kafka-load` | load generator for end-to-end runs |

```bash
KAFKA_TOPIC=rustper-input MESSAGE_COUNT=100000 \
  cargo run --release --bin kafka-load
```

## Metrics

Two reporting paths, because not everyone runs Prometheus.

```toml
[metrics]
log_interval_seconds = 10     # 0 disables; on by default
listen = "0.0.0.0:9100"       # absent means no listener at all
```

The log reporter needs nothing but `RUST_LOG=info` and emits one structured line
per component per interval:

```text
INFO source metrics component=kafka_in events_per_second=348502 events_total=15000000 in_flight_batches=8 rebalances=1
INFO sink metrics component=orders events_per_second=348502 dropped_per_second=0 queue_depth=0 retries=0 write_errors=0
```

Rates are always the delta over the interval, never a running average since
startup — an average is dragged down by ramp-up and hides stalls entirely.

When `listen` is set, `/metrics` serves the Prometheus text format and every
other path returns 404. `rustper_source_rebalances_total` is the one to watch:
a rebalance stalls consumption for seconds and is otherwise invisible from
inside the process. It counts the initial assignment too, so alert on its rate
rather than its total.

## Limitations

- **No latency measurement.** Throughput, drops, retries and rebalances are
  exported; percentiles are not.
- **No transformation language.** `schema = "json"` maps by column name and
  nothing else; reshaping belongs in a ClickHouse materialized view.
- **`json` mode has no startup schema validation**, unlike `event` mode.
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
