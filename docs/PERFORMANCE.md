# Architecture, performance và scaling

Tài liệu này mô tả hot path hiện tại, kết quả đo local và cách capacity planning.
Các số liệu là baseline của một máy, không phải cam kết production hay bằng chứng
rustper nhanh hơn Vector, Benthos, Flink hoặc một sản phẩm khác.

## 1. Runtime architecture

```text
                         ┌─ bounded queue ─► KafkaSink task
KafkaSource ─► Fanout ───┤
                         └─ bounded queue ─► ClickhouseSink task
     │                                           │
     └──────── contiguous offset commit ◄────────┘
```

- Mỗi source và sink là một Tokio task.
- Kafka source chuẩn hóa record thành `Event` trung lập.
- Một source có tối đa `max_in_flight_batches` batch chưa acknowledge.
- Fanout chia sẻ `Arc<Vec<Event>>`; payload không được clone theo số sink.
- Mỗi sink có bounded channel và tự coalesce theo events, bytes hoặc timeout.
- Batch có thể hoàn thành lệch thứ tự, nhưng Kafka source chỉ commit chuỗi batch
  liên tục đã thành công.
- Khi một sink chậm, cửa sổ in-flight/byte budget đầy và source ngừng gọi `recv`.
  Backpressure vì vậy truyền ngược về Kafka.

Các file tương ứng:

- `src/source/kafka.rs`: consume, event conversion, in-flight window, ordered commit.
- `src/topology.rs`: fanout và shared acknowledgement.
- `src/sink/mod.rs`: queue consumer và sink-side coalescing.
- `src/sink/kafka.rs`, `src/sink/clickhouse.rs`: protocol-specific delivery.

## 2. Vì sao hot path có overhead thấp

### Không có garbage collector

Rust dùng ownership và deterministic drop. Router không có GC pause và không cần
heap tracing. Điều này có lợi cho tail latency, nhưng không tự động làm mọi chương
trình Rust nhanh: allocation, copy và lock vẫn phải được thiết kế cẩn thận.

### Một lần copy tại transport boundary

Record được copy một lần từ buffer mượn của librdkafka sang `Bytes`. Sau đó cùng
payload được chia sẻ giữa các sink bằng `Arc`. `component_id` và `topic` dùng
`Arc<str>`, tránh hai allocation `String` trên mỗi event.

### Bounded concurrency thay vì spawn vô hạn

Router giữ một cửa sổ batch cố định, không spawn task vô hạn theo từng message.
Điều này giảm scheduler overhead và tạo memory ceiling có thể cấu hình.

### Batching ở đúng tầng

- Source batching giảm channel/ack/commit overhead.
- Kafka sink batching tạo nhiều delivery in-flight để librdkafka coalesce, compress
  và gửi request lớn.
- ClickHouse sink gom nhiều source batch thành một `INSERT`, giảm HTTP round-trip và
  số part nhỏ.

### Native Kafka client

`rust-rdkafka` dùng librdkafka, nên metadata, compression, retry, batching và network
polling nằm trong native client đã được tối ưu lâu năm.

Những đặc điểm trên giải thích vì sao rustper có thể cạnh tranh về overhead. Chúng
không chứng minh nó nhanh hơn công cụ khác; muốn khẳng định cần benchmark cùng
hardware, topology, durability, compression, payload và protocol settings.

## 3. Kết quả đo local

### Môi trường

```text
Date:              2026-08-26 (Asia/Ho_Chi_Minh)
Machine:           Apple M1 Pro, 10 CPU cores, 32 GiB RAM
OS:                macOS 15.5
Kafka:             apache/kafka:4.1.0, single broker, replication factor 1
Docker memory cap: 15.66 GiB
Router:            release build
Payload:           1 KiB
Input partitions:  6
Outputs:           2 Kafka topics
Source batch:      1,000 events / 5 ms
In-flight window:  8 batches / 256 MiB
Sink batch:        10,000 events / 16 MiB / 5 ms
```

### Throughput

Workload dài 5 triệu input messages:

```text
Load producer:        130,847 input msg/s
Router steady-state: ~133,000 input msg/s
Logical fanout:      ~266,000 deliveries/s
Input payload rate:  ~130 MiB/s
Logical output rate: ~260 MiB/s
```

Router steady-state được tính từ đoạn 5 triệu messages của log, loại bỏ khoảng idle
giữa hai lượt benchmark. Producer mất 38.212 giây cho 5 triệu messages. Output offsets
đã được kiểm tra ở benchmark 100 nghìn trước đó: mỗi sink nhận đúng 100 nghìn.

### CPU và RAM

25 mẫu cách nhau 200 ms trong steady-state:

```text
Router CPU:          average 88.89% of one core
Router CPU range:    82.8%–95.4% of one core
Router RSS warm idle: 155.1 MiB
Router RSS peak sample: 168.3 MiB
Kafka container:     51.64% CPU, 948.2 MiB
ClickHouse container: 3.43% CPU, 531.6 MiB (không nằm trên data path của test này)
```

Một CPU percent trên macOS tương ứng xấp xỉ một logical core; process Tokio vẫn có
thể dùng nhiều core, nhưng workload này chủ yếu tiêu thụ gần một core.

Ước lượng router CPU cost tại điểm đo:

```text
0.8889 core / 133,000 msg/s × 1,000,000
≈ 6.7 CPU-seconds trên mỗi triệu input messages
```

RSS không bằng toàn bộ memory budget. librdkafka có internal consumer/producer
queues và allocator có thể giữ memory sau khi batch đã được giải phóng.

## 4. Memory model

Phần payload do topology giữ có ceiling mềm:

```text
topology payload ≤ max_in_flight_bytes + tối đa một source batch vượt ngưỡng
```

Fanout không nhân payload theo số sink:

```text
1 EventBatch payload + N Arc pointers + N acknowledgement branches
```

Tổng RSS thực tế còn gồm:

```text
Rust runtime + metadata
librdkafka consumer fetch buffers
librdkafka producer queue cho từng Kafka sink
ClickHouse HTTP encoding buffers
allocator retained pages
logging và shared libraries
```

Do đó `max_in_flight_bytes = 256 MiB` không có nghĩa RSS tối đa là 256 MiB. Với
Kafka output, cần bổ sung config cho `queue.buffering.max.kbytes` nếu muốn harden
memory ceiling của từng producer.

## 5. Vertical scaling

Điều chỉnh từng knob một và đo consumer lag, CPU, RSS, p95/p99 latency.

### `batch_size`

- Tăng: ít channel operations, commits và requests hơn.
- Giảm: latency thấp và memory nhỏ hơn.
- Điểm bắt đầu: 500–2,000 events.

### `max_in_flight_batches`

- Tăng khi sink/network latency khiến pipeline không đủ request concurrent.
- Giảm khi RSS hoặc retry amplification quá cao.
- Điểm bắt đầu: 4–16; hiện dùng 8.

### `max_in_flight_bytes`

Chọn theo memory budget của process, không theo RAM toàn máy. Ví dụ process limit
1 GiB có thể dành 256 MiB cho topology, phần còn lại cho librdkafka, ClickHouse,
allocator và headroom.

### Sink batch

Kafka baseline:

```toml
batch_max_events = 10000
batch_max_bytes = 16777216
batch_linger_ms = 5
```

ClickHouse thường hưởng lợi từ batch lớn hơn, chẳng hạn 10k–100k rows hoặc 16–64
MiB, với linger 50–500 ms tùy latency SLO. Phải benchmark với schema/payload thật.

## 6. Horizontal scaling

Kafka partition là đơn vị parallelism. Với `P` partitions và `R` router replicas
trong cùng consumer group, số consumer hoạt động tối đa bị giới hạn bởi `P`.

```text
effective consumers = min(P, R)
```

Ví dụ 24 partitions và 6 replicas cho phép Kafka phân phối khoảng 4 partitions mỗi
replica. Mỗi replica fan-out phần dữ liệu của nó tới mọi sink.

Quy trình scale:

1. Đảm bảo input có đủ partitions.
2. Chạy nhiều rustper instances với cùng `group_id`.
3. Dùng cùng topology/config và credentials.
4. Theo dõi consumer lag và ClickHouse/Kafka downstream capacity.
5. Scale downstream trước nếu sink đã saturated; thêm router lúc đó chỉ tăng queue.

Cross-sink delivery là at-least-once. Khi một sink thành công và sink khác thất bại,
batch sẽ replay. ClickHouse nên dùng `(source_topic, source_partition, source_offset)`
làm deduplication key nếu duplicate không được chấp nhận.

## 7. Bottleneck tiếp theo

Theo profile bằng CPU sampling hiện tại, source normalization và Kafka delivery path
đã dùng gần một core. Các bước tiếp theo nên dựa trên profiler (`samply`, Instruments
hoặc flamegraph), không tiếp tục tối ưu theo trực giác.

Các candidate rõ ràng:

- delivery callback/batch tracker thay cho một `FutureProducer` future mỗi record;
- expose librdkafka queue/fetch limits trong config;
- Prometheus metrics cho batch size, queue depth, bytes in-flight, latency và lag;
- benchmark ClickHouse riêng với 10–100 triệu rows;
- disk buffer/WAL nếu cần durability khi process hoặc host crash;
- multiple source workers hoặc horizontal replicas khi normalization chạm một core.

## 8. Cách tái lập benchmark

```bash
docker compose start

RUST_LOG=rustper=info \
  target/release/rustper config/resource-benchmark.toml

KAFKA_TOPIC=rustper-resource-input \
MESSAGE_COUNT=5000000 \
PAYLOAD_SIZE=1024 \
CONCURRENCY=1000 \
  target/release/kafka-load
```

Để benchmark sạch cần dùng topic và consumer group mới hoặc xóa dữ liệu benchmark
có chủ đích. Chạy ít nhất 5 lần, báo median/p95, và tách broker khỏi router trước khi
dùng kết quả cho production sizing.
