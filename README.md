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

Các knob hiệu năng chính:

```toml
max_in_flight_batches = 8
max_in_flight_bytes = 268435456
batch_max_events = 10000
batch_max_bytes = 16777216
batch_linger_ms = 5
```

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
8. `src/bin/kafka_load.rs`: Kafka load generator.

## Go sang Rust

| Go | Rust trong project |
| --- | --- |
| `go func()` | `tokio::spawn(async move { ... })` |
| `chan *EventBatch` | `tokio::sync::mpsc::Sender<SinkEnvelope>` |
| đóng mọi sender | `drop(outputs)` |
| `<-ch` đến khi channel đóng | `while let Some(x) = receiver.recv().await` |
| `(value, error)` | `Result<T, E>` |

`benchmark` đo core in-memory; `kafka-load` dùng cho benchmark Kafka end-to-end.

Để test ClickHouse, tạo bảng như trong `docs/LEARNING_NOTES.md`, sau đó chạy:

```bash
cargo run --release -- config/clickhouse.toml
```
