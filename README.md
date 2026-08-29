# rustper

Message router kiểu Vector-lite viết bằng Rust. Hiện hỗ trợ Kafka source và
Kafka/ClickHouse sinks, topology cấu hình bằng `inputs`, bounded buffer riêng cho
mỗi sink và end-to-end acknowledgement theo batch.

## Chạy thử

```bash
docker compose up -d

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic rustper-input --partitions 6 --replication-factor 1

cargo run --release
```

## Chạy router bằng Docker

```bash
docker compose --profile router up -d --build
docker compose logs -f rustper
```

Image dùng multi-stage build và runtime Debian tối giản. Process chạy bằng user
non-root UID/GID `10001`. Compose mount `config/docker.toml`; Kafka dùng listener
`kafka:19092` bên trong Docker network và `localhost:9092` từ host.

Build image riêng:

```bash
docker build --tag rustper:local .
docker run --rm \
  --network rustper_default \
  --volume "$PWD/config/docker.toml:/etc/rustper/config.toml:ro" \
  rustper:local
```

Config mặc định nằm tại `config/local.toml`; truyền path khác làm argument đầu tiên.

Các knob hiệu năng chính (`docs/PERFORMANCE.md` §5 giải thích từng cái):

```toml
# source: cửa sổ in-flight và micro-batch
max_in_flight_batches = 8
max_in_flight_bytes = 268435456
batch_size = 1000
batch_linger_ms = 5

# sink: coalescing, write concurrency, retry
batch_max_events = 10000
batch_max_bytes = 16777216
max_concurrent_writes = 1   # > 1 nhanh hơn, nhưng batch có thể tới lệch thứ tự
retry_max_attempts = 5

# sink Kafka: durability và trần buffer của librdkafka
acks = "all"
enable_idempotence = true    # librdkafka giới hạn in-flight ở 5 khi bật
queue_buffering_max_kbytes = 1048576
```

Config dùng `deny_unknown_fields`, nên một key gõ sai sẽ báo lỗi thay vì bị bỏ qua
im lặng. Property librdkafka nào không có field riêng thì đặt qua `client_config`.

Load test:

```bash
KAFKA_TOPIC=rustper-input MESSAGE_COUNT=100000 \
  cargo run --release --bin kafka-load
```

## Đọc code theo thứ tự

1. `docs/LEARNING_NOTES.md`: giải thích thiết kế và Rust cho người biết Go.
2. `docs/PERFORMANCE.md`: kiến trúc hot path, CPU/RAM benchmark và scaling guide.
3. `src/main.rs`: load config và khởi động runtime.
4. `src/config.rs`: struct + Serde để parse TOML.
5. `src/topology.rs`: nối component, fan-out, buffer và acknowledgement.
6. `src/source/kafka.rs`: consume, micro-batch và commit offset.
7. `src/sink/`: `Sink` trait, Kafka và ClickHouse implementations.
8. `src/event.rs`: `Event`, `EventBatch`, `BatchBuffer` (arena copy).
9. `src/bin/kafka_load.rs`: Kafka load generator.

## Go sang Rust

| Go | Rust trong project |
| --- | --- |
| `go func()` | `tokio::spawn(async move { ... })` |
| `chan *EventBatch` | `tokio::sync::mpsc::Sender<SinkEnvelope>` |
| đóng mọi sender | `drop(outputs)` |
| `<-ch` đến khi channel đóng | `while let Some(x) = receiver.recv().await` |
| `(value, error)` | `Result<T, E>` |

## Benchmark

| Lệnh | Đo cái gì |
| --- | --- |
| `cargo run --release --bin benchmark` | overhead của riêng router, không broker |
| `cargo run --release --example normalize_ab` | arena copy so với alloc mỗi field |
| `cargo run --release --bin kafka-load` | load generator cho benchmark end-to-end |

Số end-to-end hiện có trong `docs/PERFORMANCE.md` là **producer-bound và n = 1**;
§3.5 và §3.6 nói rõ nó chứng minh được gì và đo lại cho đúng bằng cách nào.

Để test ClickHouse, tạo bảng như trong `docs/LEARNING_NOTES.md`, sau đó chạy:

```bash
cargo run --release -- config/clickhouse.toml
```
