# Architecture, performance và scaling

Tài liệu này mô tả hot path hiện tại, kết quả đo local và cách capacity planning.
Các số liệu là baseline của một máy, không phải cam kết production hay bằng chứng
rustper nhanh hơn Vector, Benthos, Flink hoặc một sản phẩm khác.

Đọc §3 trước §2: phần đo nói rõ mỗi con số chứng minh được cái gì và **không**
chứng minh được cái gì.

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
- Fanout chia sẻ `Arc<EventBatch>`; payload không được clone theo số sink.
- Mỗi sink có bounded channel và tự coalesce theo events, bytes hoặc timeout.
- Batch có thể hoàn thành lệch thứ tự, nhưng Kafka source chỉ commit chuỗi batch
  liên tục đã thành công.
- Khi một sink chậm, cửa sổ in-flight/byte budget đầy và source ngừng gọi `recv`.
  Backpressure vì vậy truyền ngược về Kafka.

Các file tương ứng:

- `src/source/kafka.rs`: consume, event conversion, in-flight window, ordered commit.
- `src/event.rs`: `Event`, `EventBatch`, và `BatchBuffer` (arena copy).
- `src/topology.rs`: fanout và shared acknowledgement.
- `src/sink/mod.rs`: queue consumer, coalescing, retry, write concurrency.
- `src/sink/kafka.rs`, `src/sink/clickhouse.rs`: protocol-specific delivery.

### Event loop của source

Vòng lặp là một `select!` duy nhất, mọi arm luôn sống:

| Arm | Ý nghĩa |
| --- | --- |
| `shutdown.cancelled()` | abort in-flight; offsets chưa commit sẽ được replay |
| `in_flight.join_next()` | thu batch xong, trả byte budget, commit contiguous prefix |
| linger deadline | đóng batch đang gom khi hết thời gian chờ |
| `consumer.recv()` | nhận thêm một message vào batch đang gom |

Điểm quan trọng: việc gom batch **không** chặn arm nào khác. Một thiết kế gom
message trong vòng lặp con (dạng `timeout_at(deadline, recv()).await` lồng nhau)
sẽ khiến commit và việc giải phóng byte budget bị treo suốt cửa sổ linger, đồng
thời đăng ký một timer entry cho **mỗi** message thay vì mỗi batch.

## 2. Vì sao hot path có overhead thấp

### Không có garbage collector

Rust dùng ownership và deterministic drop. Router không có GC pause và không cần
heap tracing. Điều này có lợi cho tail latency, nhưng không tự động làm mọi chương
trình Rust nhanh: allocation, copy và lock vẫn phải được thiết kế cẩn thận. §3.6
cho thấy chỉ riêng việc đổi allocator đã thay đổi throughput normalization 65%.

### Arena copy tại transport boundary

Record được copy một lần từ buffer mượn của librdkafka. Thay vì một `malloc` cho
key và một cho payload mỗi message, `BatchBuffer` copy vào một chunk `BytesMut`
1 MiB và phát ra các `Bytes` view chia sẻ chunk đó. Chi phí allocation vì thế là
`tổng bytes / chunk` thay vì `2N`.

Đánh đổi: một chunk chỉ được giải phóng khi `Bytes` cuối cùng cắt từ nó bị drop.
Ở đây chấp nhận được vì mọi event trong một batch được ack và drop cùng lúc.

`component_id` và `topic` dùng `Arc<str>`. `TopicCache` giữ thêm một slot "topic
gần nhất" nên lookup thường chỉ là một phép so sánh, không hash lại tên topic cho
từng message.

### Bounded concurrency thay vì spawn vô hạn

Router giữ một cửa sổ batch cố định, không spawn task vô hạn theo từng message.
Điều này giảm scheduler overhead và tạo memory ceiling có thể cấu hình.

### Một acknowledgement cho mỗi write, không phải mỗi record

Kafka sink dùng `ThreadedProducer` với `ProducerContext` riêng. Mọi record của một
write chia sẻ một `BatchTracker`; delivery callback chỉ làm một atomic decrement,
và cả write được đánh thức đúng một lần.

Thiết kế trước đó `await` một `FutureProducer` future cho mỗi record rồi gom bằng
`try_join_all`. Vì `flat_map` không cho `size_hint` cận trên, `try_join_all` luôn
rơi vào nhánh `FuturesOrdered`, tức là với `batch_max_events = 10000` thì mỗi write
tốn 10.000 oneshot channel cộng 10.000 node `Arc` — và thứ tự output mà
`FuturesOrdered` bảo toàn hoàn toàn vô nghĩa vì mọi output đều là `Ok(())`.

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

## 3. Kết quả đo

### 3.1 Môi trường

```text
Machine:  Apple M1 Pro, 10 CPU cores, 32 GiB RAM
OS:       macOS 15.5
Rust:     1.98.0
Build:    release (lto = "thin", codegen-units = 1), jemalloc global allocator
Payload:  1 KiB
```

macOS chạy broker trong một Linux VM với network ảo hoá, nên **mọi số end-to-end
dưới đây không chuyển được sang Linux bare metal.**

### 3.2 In-process pipeline (`cargo run --release --bin benchmark`)

Đo `BatchBuffer` → `EventBatch` → `Fanout` → bounded queue → sink coalescing →
acknowledgement, với sink chỉ đếm. Không broker, không network. Đây là **overhead
của riêng router**.

```text
Input:             20,000,000 messages, 1 KiB
Outputs:           3 counting sinks
Source batch:      1,000 events, in-flight window 8 batches
Sink batch:        10,000 events / 16 MiB / 5 ms linger

Throughput:        1,028,164 input msg/s  (19.452 s)
Logical fanout:    3,084,493 deliveries/s
Router CPU:        median 53.7% of one core  (15 mẫu 1 s; range 38.6% – 63.3%)
Router RSS:        18 – 19 MiB
```

Quy ra chi phí CPU:

```text
0.537 core / 1,028,164 msg/s × 1,000,000
≈ 0.52 CPU-seconds trên mỗi triệu input messages
```

Với `MESSAGE_COUNT=1000000` (mặc định), 7 runs cho median 1,105,652 msg/s, range
1,061,302 – 1,147,231.

Cảnh báo về phép đo này: cửa sổ in-flight 8 batch là bắt buộc. Một phiên bản gửi
tuần tự (chờ ack từng batch) cho 141,062 msg/s — nó đo sink linger 5 ms chứ không
đo overhead. Con số nào không nói rõ cửa sổ in-flight thì vô nghĩa.

### 3.3 Normalization tách riêng (`cargo run --release --example normalize_ab`)

5 triệu message, best-of-5, chỉ đo vòng chuẩn hóa event:

| Copy strategy | Allocator | Throughput |
| --- | --- | --- |
| `BatchBuffer` arena | jemalloc | 18.1M msg/s |
| `BatchBuffer` arena | system | 18.1M msg/s |
| `Bytes::copy_from_slice` mỗi field | jemalloc | 14.2M msg/s |
| `Bytes::copy_from_slice` mỗi field | system | 8.7M msg/s |

### 3.4 End-to-end qua Kafka — drain rate, đo 2026-08-29

Đo theo quy trình hai pha ở §3.6: nạp backlog trước, **producer đã dừng hẳn** rồi
mới khởi động router. Topology là `config/resource-benchmark.toml` — 1 input topic
6 partitions, fan-out ra 2 Kafka topic, `max_concurrent_writes = 1`.

**Run A — backlog 5,000,000, không rebalance:**

```text
Drain:              5,000,000 messages in 17.683 s
Average:            282,761 input msg/s
Steady interval:    306,620 – 406,765 msg/s
Logical fanout:     ~565,000 deliveries/s
Input payload:      ~276 MiB/s in, ~552 MiB/s out
```

**Run B — backlog 15,000,000, có 4 rebalance:**

```text
Overall:            15,002,257 messages in 108.285 s = 138,544 msg/s
Steady interval:    median 343,473 msg/s, p95 457,082, max 485,367
Router RSS:         median 141.0 MiB (min 132, peak 308)
Kafka container:    median 23.9% CPU, 925 MiB
```

Run B chứa **một stall 25 giây** (t=16 s → 41 s) kéo trung bình từ ~343k xuống
138k. Nguyên nhân nằm trong broker log:

```text
Preparing to rebalance group rustper-drain-v3 ... (reason: Adding new member ...)
```

Consumer bị fence rồi rejoin **4 lần** trong một lần chạy. `session_timeout_ms`
mặc định của rustper khi đó là `10000`, trong khi default của Kafka là `45000`:
chỉ cần một heartbeat trễ vì connection bão hoà là broker đá consumer ra, và mỗi
rebalance tốn vài giây ngừng tiêu thụ. Default đã được sửa thành `45000`, và
`max_poll_interval_ms` giờ cấu hình được.

**Bản sửa đó chưa được kiểm chứng end-to-end**: Docker daemon chết trước khi chạy
lại được. Đây là gap đã biết — xem §7.

Đo CPU của router trong Run B không dùng được: cửa sổ lấy mẫu rơi trúng stall nên
median chỉ 2.0% (max 165.3%). Con số CPU end-to-end đáng tin duy nhất hiện có là
số cũ ở §3.5.

### 3.5 Số cũ 2026-08-26 — vì sao thấp hơn 2×

```text
Load producer:      130,847 input msg/s (chạy ĐỒNG THỜI với router)
Router:            ~133,000 input msg/s
Router CPU:         88.89% of one core (25 mẫu, 200 ms)
Router RSS:         155.1 MiB warm idle, 168.3 MiB peak
```

Phép đo đó **producer-bound**: router báo ~133k trong khi producer chỉ ghi 130.8k,
tức là router chỉ đang theo kịp producer. Nó chứng minh một cận dưới, không phải
capacity. §3.4 tách hai pha ra và cho 282,761 msg/s — **gấp 2.1 lần** — trên cùng
máy, cùng topology.

Ba lỗi phương pháp còn lại của số cũ: n = 1; producer, broker và router tranh CPU
trên cùng một máy 10 core; và không có số latency ở bất kỳ percentile nào.

### 3.6 Ba kết luận từ số liệu trên

1. **Router không phải bottleneck, và không gần bottleneck.** In-process 1.03M
   msg/s ở 0.54 core; end-to-end 283k msg/s. Overhead của chính router là
   0.52 CPU-s/triệu message so với 6.7 CPU-s/triệu đo end-to-end — tức là **~8%**.
   92% còn lại là librdkafka, syscall và broker.
2. **Arena thắng 2.1× khi cô lập nhưng không đổi được số pipeline.** Normalization
   tốn ~55 ns/message, pipeline tốn ~900 ns/message. Chi phí nằm ở channel, task
   scheduling và acknowledgement, không nằm ở allocation. Đổi allocator sang
   jemalloc cho +65% trên đường alloc-heavy — nhưng cũng không đổi số pipeline.
3. **`lto = "thin"` + `codegen-units = 1` không tạo khác biệt đo được.** Median
   1.111M (không LTO) so với 1.113M (có LTO), phương sai run-to-run ±5%. Giữ
   setting vì vô hại, đừng ghi công cho nó.

### 3.7 Cách tái lập

Producer và router **không được chạy đồng thời** — đó chính là lỗi của số cũ.

```bash
docker compose up -d kafka

# Pha 1 — nạp backlog, router CHƯA chạy.
KAFKA_TOPIC=rustper-resource-input \
MESSAGE_COUNT=5000000 PAYLOAD_SIZE=1024 CONCURRENCY=1000 \
  target/release/kafka-load

# Đợi broker rảnh (docker stats), rồi đo drain rate thuần.
RUST_LOG=rustper=info \
  target/release/rustper config/resource-benchmark.toml
```

Log của source báo `interval_messages_per_second` bên cạnh
`average_messages_per_second`. **Dùng cột interval**: trung bình tích luỹ bị kéo
xuống bởi ramp-up và che mất stall — chính nó đã giấu stall 25 giây ở Run B.

Lấy mẫu CPU bằng `top -l N -s 1 -pid <pid> -stats cpu,mem`, không dùng `ps %cpu`
(là trung bình vòng đời process, sẽ báo thấp hơn thực tế).

Mỗi lần chạy cần topic và consumer group mới. Chạy ít nhất 5 lần, báo median và
p95, kiểm tra broker log xem có rebalance không, và tách broker sang máy khác
trước khi dùng kết quả cho production sizing.

## 4. Memory model

Phần payload do topology giữ có ceiling mềm:

```text
topology payload ≤ max_in_flight_bytes + tối đa một source batch vượt ngưỡng
```

`Event::estimated_size` tính cả `size_of::<Event>()` chứ không chỉ key + payload.
Với message nhỏ, các header `Bytes`/`Arc<str>` lấn át payload, nên nếu chỉ đếm
payload thì budget sẽ nói dối vài lần.

Fanout không nhân payload theo số sink:

```text
1 EventBatch payload + N Arc pointers + N acknowledgement branches
```

### Buffer của librdkafka thường lớn hơn budget của router

Đây là số hạng bị bỏ sót dễ gây bất ngờ nhất. Mặc định của librdkafka, **cho mỗi
sink**:

| Property | Default | Ý nghĩa |
| --- | --- | --- |
| `queue.buffering.max.messages` | 100,000 | ~100 MiB với payload 1 KiB |
| `queue.buffering.max.kbytes` | 1,048,576 | 1 GiB |

Và cho consumer:

| Property | Default | Ý nghĩa |
| --- | --- | --- |
| `queued.max.messages.kbytes` | 65,536 | 64 MiB fetch buffer |
| `queued.min.messages` | 100,000 | ngưỡng prefetch |

Với hai Kafka sink, trần buffer client-side mặc định vượt xa
`max_in_flight_bytes = 256 MiB`. Cả bốn property giờ đều có trong config
(`queue_buffering_max_messages`, `queue_buffering_max_kbytes`,
`queued_max_messages_kbytes`, `queued_min_messages`); muốn có memory ceiling thật
thì phải set chúng, không chỉ set `max_in_flight_bytes`.

Tổng RSS thực tế còn gồm Rust runtime, ClickHouse HTTP encoding buffers, arena
chunk đang giữ, allocator retained pages, logging và shared libraries.

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

Chọn theo memory budget của process, không theo RAM toàn máy — và nhớ cộng thêm
buffer librdkafka ở §4.

### `max_concurrent_writes` (mỗi sink)

Mặc định `1`, nghĩa là sink ghi tuần tự: gom → ghi → ack → gom. Trần thông lượng
khi đó là `1 / (linger + write latency)`, và **không có batch nào được gom trong
lúc write đang bay**. Với ClickHouse (linger 50 ms cộng HTTP round-trip) đây thường
là bottleneck lớn nhất của cả pipeline.

Đặt `> 1` để write kế tiếp gom song song với write đang bay. Đánh đổi: batch có thể
tới đích lệch thứ tự. Với ClickHouse dùng `(source_topic, source_partition,
source_offset)` làm dedup key thì điều này vô hại; với Kafka sink cần ordering theo
partition thì giữ nguyên `1`.

### Retry

`retry_max_attempts` (mặc định 5) với backoff `retry_initial_backoff_ms` →
`retry_max_backoff_ms`. Một lỗi ClickHouse 5xx thoáng qua hay delivery timeout
không còn giết process.

Khi hết retry thì router **vẫn** dừng, và đó là chủ đích: offsets của batch đó chưa
được commit, nên restart sẽ replay. Bỏ qua batch mới là thứ phá vỡ at-least-once.

### Durability của Kafka sink

`acks`, `enable_idempotence`, `compression_type`, `producer_linger_ms`,
`producer_batch_num_messages` đều cấu hình được; mặc định là
`acks = "all"`, `enable_idempotence = true`, `compression_type = "lz4"`.

Cần biết: `enable_idempotence = true` khiến librdkafka giới hạn
`max.in.flight.requests.per.connection` ở 5 và ép `acks = all`. Đó là một trần
throughput có thật, đổi lấy đảm bảo không trùng lặp phía producer. Tắt nó (cùng
`acks = "1"`) sẽ nhanh hơn nhưng nới lỏng durability.

Mọi property librdkafka khác đặt qua `client_config`, ví dụ:

```toml
[sinks.output_a.client_config]
"socket.nagle.disable" = "true"
```

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

Ưu tiên theo dữ liệu ở §3, không theo trực giác:

1. **Xác nhận bản sửa `session_timeout_ms`.** Default đã đổi từ 10000 sang 45000
   nhưng chưa chạy lại được end-to-end (§3.4). Chạy lại Run B và kiểm tra broker
   log không còn dòng `Preparing to rebalance`. Đây là việc cần làm đầu tiên.
2. **Lấy một phép đo CPU end-to-end hợp lệ.** Cửa sổ lấy mẫu ở Run B rơi trúng
   stall. Cần backlog đủ lớn để có ≥60 s steady state, hoặc lấy mẫu sau khi đã
   xác nhận không có rebalance.
3. **Prometheus metrics**: batch size, queue depth, bytes in-flight, retry count,
   rebalance count, consumer lag, latency p95/p99. Không có nhóm này thì stall 25
   giây ở Run B đã không thể phát hiện từ trong process — phải đi đọc broker log.
4. **Đo `max_concurrent_writes` ở 1 so với 2–4.** Cả hai run đều chạy ở `1`, tức
   là sink ghi tuần tự. §5 giải thích vì sao đây là trần thông lượng có thật.
5. **Profile delivery path, không phải normalization.** §3.6 cho thấy router chỉ
   chiếm ~8% CPU end-to-end; 92% nằm ở librdkafka và syscall. Dùng `samply`,
   Instruments hoặc flamegraph trên phần đó.
6. **Benchmark ClickHouse riêng** với 10–100 triệu rows.
7. **Disk buffer/WAL** nếu cần durability khi process hoặc host crash.
8. **Multiple source workers** — chỉ khi profiler chứng minh một source task đã
   chạm trần một core. Dữ liệu hiện tại **không** ủng hộ việc này.

## 8. Những gì tài liệu này không trả lời

Ghi rõ để không ai đọc nhầm:

- Không có số latency, ở bất kỳ percentile nào.
- Không có phép đo CPU end-to-end hợp lệ sau khi sửa `session_timeout_ms`.
- Không có số nào đo trên Linux, trên bare metal, hay với broker ở máy khác. Broker
  chạy trong Linux VM của Docker Desktop/OrbStack với network và disk ảo hoá.
- Số end-to-end là n = 1 mỗi cấu hình, không phải median của 5 lần chạy như §3.7
  yêu cầu.
- Không có so sánh với Vector, Benthos, Flink hay bất kỳ công cụ nào khác.
- Không có phép đo nào ở nhiều replica hoặc trong lúc rebalance có chủ đích.
