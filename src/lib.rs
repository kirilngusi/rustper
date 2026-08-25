use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub mod config;
pub mod event;
pub mod sink;
pub mod source;
pub mod topology;

/// Dữ liệu trung lập với transport. Sau này Kafka adapter sẽ chuyển Kafka record
/// thành kiểu này.
#[derive(Debug)]
pub struct Message {
    pub key: Bytes,
    pub payload: Bytes,
}

impl Message {
    pub fn new(key: impl Into<Bytes>, payload: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            payload: payload.into(),
        }
    }
}

/// Thống kê trả về từ một output worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkStats {
    pub messages: u64,
    pub bytes: u64,
}

/// Sender tương tự `chan<- *Message` trong Go.
/// `Arc<Message>` tương tự một shared pointer an toàn giữa nhiều goroutine/task.
pub type MessageSender = mpsc::Sender<Arc<Message>>;

/// Tạo một output worker với queue có giới hạn.
/// Khi queue đầy, `send().await` sẽ tạo backpressure thay vì dùng RAM vô hạn.
pub fn spawn_sink(capacity: usize) -> (MessageSender, JoinHandle<SinkStats>) {
    assert!(capacity > 0, "channel capacity must be greater than zero");

    let (sender, mut receiver) = mpsc::channel::<Arc<Message>>(capacity);

    let worker = tokio::spawn(async move {
        let mut stats = SinkStats {
            messages: 0,
            bytes: 0,
        };

        while let Some(message) = receiver.recv().await {
            stats.messages += 1;
            stats.bytes += message.payload.len() as u64;
        }

        stats
    });

    (sender, worker)
}

/// Fan-out mọi message đến mọi output.
/// Clone `Arc` không clone payload; nó chỉ tăng reference count.
pub async fn fan_out(
    messages: impl IntoIterator<Item = Message>,
    outputs: &[MessageSender],
) -> Result<u64, mpsc::error::SendError<Arc<Message>>> {
    let mut input_count = 0;

    for message in messages {
        let shared = Arc::new(message);

        for output in outputs {
            output.send(Arc::clone(&shared)).await?;
        }

        input_count += 1;
    }

    Ok(input_count)
}
