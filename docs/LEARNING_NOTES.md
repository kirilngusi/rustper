# Ghi chú học Rust qua rustper

## Kiến trúc Vector-lite

```text
KafkaSource task
      │ EventBatch
      ▼
    Fanout
   ├── bounded mpsc ──► KafkaSink task
   └── bounded mpsc ──► ClickhouseSink task
```

`Config` chứa map component theo ID. Mỗi sink khai báo `inputs`, giống pipeline
model của Vector. `topology::run` build component, tạo bounded channel và nối graph.

## Luồng acknowledgement

1. `StreamConsumer::recv()` mượn message từ Kafka consumer.
2. `.detach()` đổi nó thành `OwnedMessage` để giữ qua nhiều lần `recv` trong batch.
3. Mỗi batch tối đa `batch_size`, hoặc đóng sau `batch_linger_ms`.
4. Kafka source chuyển record thành `Event` trung lập rồi gọi `Fanout::send`.
5. Fanout gửi concurrent cùng `Arc<Vec<Event>>` tới queue riêng của mọi sink.
6. Mỗi branch giữ một `BranchAck`. Source chờ toàn bộ branch hoàn thành.
7. Source giữ tối đa `max_in_flight_batches` batch cùng lúc. Batch có thể hoàn thành
   lệch thứ tự, nhưng source chỉ commit chuỗi batch liên tục đã thành công.
8. `max_in_flight_bytes` đặt trần mềm cho tổng payload đang được giữ trong pipeline.

Nếu process chết trước commit, Kafka phát lại message: có thể duplicate nhưng không
mất dữ liệu. Đó là at-least-once, chưa phải exactly-once.

## Rust tương ứng với Go

| Go | Rust |
| --- | --- |
| `(T, error)` | `Result<T, E>` |
| `defer close(...)` | resource tự cleanup khi hết scope (`Drop`) |
| goroutine | Tokio task/future |
| `select` | `tokio::select!` |
| slice | `Vec<T>` hoặc `&[T]` |
| interface | `trait` |
| pointer đang được mượn | `&T` / `&mut T` |
| object có ownership | `T` |

`BorrowedMessage<'_>` chứa lifetime: compiler đảm bảo nó không sống lâu hơn Kafka
consumer đang cho mượn buffer. `OwnedMessage` sở hữu dữ liệu nên có thể nằm trong
`Vec` của micro-batch.

`Source` và `Sink` là Rust trait, tương ứng interface trong Go. `async-trait` làm
chúng dùng được dưới dạng `Box<dyn Source>`/`Box<dyn Sink>`, cho phép một topology
chứa nhiều implementation khác loại.

`async fn` không tự chạy. Nó trả về `Future`; `.await` cho Tokio quyền tạm chạy task
khác khi chờ network. Mỗi component chạy như một task độc lập.

## Vì sao dùng micro-batch

Bản đầu đợi delivery acknowledgement từng message. Cách đó phá batching của Kafka
producer và chỉ xử lý được rất ít message mỗi giây khi `linger.ms = 5`.

Micro-batch tạo nhiều request in-flight nhưng vẫn giữ commit an toàn. Batching được
chia hai tầng: source tạo `EventBatch`; sink gom tiếp nhiều batch theo
`batch_max_events`, `batch_max_bytes` hoặc `batch_linger_ms`.

Kafka record chỉ được copy một lần từ buffer librdkafka vào `Bytes`. Component ID và
topic dùng `Arc<str>`, nên clone metadata chỉ tăng reference count thay vì cấp phát
`String` cho từng event.

## ClickHouse schema local

```sql
CREATE TABLE default.rustper_events
(
    timestamp_ms Int64,
    source_component String,
    source_topic String,
    source_partition Int32,
    source_offset Int64,
    key String,
    payload String
)
ENGINE = MergeTree
ORDER BY (source_topic, source_partition, source_offset);
```

`ClickhouseSink::write_batch` chỉ acknowledge sau `insert.end()`, tức ClickHouse đã
trả HTTP success cho toàn insert.

## At-least-once và partial success

Nếu Kafka sink ghi thành công nhưng ClickHouse thất bại, source không commit. Khi
restart, cả batch được replay tới cả hai sink. Kafka output có thể nhận duplicate.
Integration test thực tế đã quan sát đúng hành vi này: batch 1.000 record được replay
sau khi sửa credential ClickHouse.

## Kết quả local ngày 2026-08-26

Apple Silicon, Kafka 4.1.0 container, 6 partitions, payload 1 KiB, batch 1.000,
hai output:

```text
Load producer:       78,183 input msg/s
Vector-lite router:  92,684 input msg/s
Fan-out deliveries: 185,368 deliveries/s
Correctness:        100,000 / 100,000 messages ở mỗi Kafka output
ClickHouse test:     10,000 rows
```

Sau vòng tối ưu pipelining, allocation và sink-side batching:

```text
Optimized router:   129,732 input msg/s
Fan-out deliveries: 259,465 deliveries/s
Improvement:         ~40% so với Vector-lite ban đầu
Correctness:        100,000 / 100,000 mỗi Kafka output
ClickHouse:          thêm đúng 10,000 rows trong integration test
```

Con số local không đại diện production. Benchmark production cần broker riêng,
network thật, replication factor, TLS/SASL, retention và workload thực tế.

## Những việc tiếp theo

- Copy Kafka headers và timestamp sang output.
- Metrics Prometheus: message count, latency, batch size, retry và consumer lag.
- DLQ và phân loại lỗi retryable/non-retryable.
- Xử lý rebalance trong lúc còn batch in-flight.
- Transform trait và transform nodes trong topology graph.
- Disk buffer/WAL; hiện tại queue chỉ nằm trong RAM.
- Nếu cần exactly-once Kafka-to-Kafka, dùng Kafka transactions thay vì tự suy diễn
  từ idempotent producer.
