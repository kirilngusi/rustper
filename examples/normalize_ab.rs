//! Isolates event normalisation: arena copies vs one allocation per field.
use std::{sync::Arc, time::Instant};

use bytes::Bytes;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use rustper::event::{BatchBuffer, Event, EventBatch, SourceMetadata};

const MESSAGES: usize = 5_000_000;
const BATCH: usize = 1_000;

fn main() {
    let payload = vec![42_u8; 1024];
    let component: Arc<str> = Arc::from("source");
    let topic: Arc<str> = Arc::from("input");

    for label in ["arena", "per-message"] {
        let mut best = f64::MAX;
        for _ in 0..5 {
            let mut buffer = BatchBuffer::default();
            let started = Instant::now();
            let mut sunk = 0_usize;
            for batch_index in 0..MESSAGES / BATCH {
                let mut events = Vec::with_capacity(BATCH);
                for i in 0..BATCH {
                    let key = ((batch_index * BATCH + i) as u64).to_be_bytes();
                    let (key, body) = if label == "arena" {
                        (buffer.copy(&key), buffer.copy(&payload))
                    } else {
                        (
                            Bytes::copy_from_slice(&key),
                            Bytes::copy_from_slice(&payload),
                        )
                    };
                    events.push(Event {
                        key: Some(key),
                        payload: body,
                        timestamp_ms: Some(1),
                        source: SourceMetadata {
                            component_id: Arc::clone(&component),
                            topic: Arc::clone(&topic),
                            partition: 0,
                            offset: (batch_index * BATCH + i) as i64,
                        },
                    });
                }
                let batch = EventBatch::new(events);
                sunk += batch.len();
                std::hint::black_box(&batch);
            }
            let seconds = started.elapsed().as_secs_f64();
            assert_eq!(sunk, MESSAGES);
            best = best.min(seconds);
        }
        println!(
            "{label:<12} best {best:.3} s  -> {:.0} msg/s",
            MESSAGES as f64 / best
        );
    }
}
