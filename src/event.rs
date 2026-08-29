use std::{mem::size_of, sync::Arc};

use bytes::{Bytes, BytesMut};

#[derive(Debug, Clone)]
pub struct Event {
    pub key: Option<Bytes>,
    pub payload: Bytes,
    pub timestamp_ms: Option<i64>,
    pub source: SourceMetadata,
}

#[derive(Debug, Clone)]
pub struct SourceMetadata {
    pub component_id: Arc<str>,
    pub topic: Arc<str>,
    pub partition: i32,
    pub offset: i64,
}

impl Event {
    /// Approximate heap + struct footprint of this event.
    ///
    /// The struct itself is counted because `max_in_flight_bytes` and the sink
    /// `batch_max_bytes` budgets are memory ceilings, not payload counters: for
    /// small messages the `Bytes`/`Arc<str>` headers dominate the payload.
    pub fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.key.as_ref().map_or(0, Bytes::len) + self.payload.len()
    }
}

/// A batch of events plus its precomputed size.
///
/// The byte count is computed once, where the batch is built, instead of being
/// recomputed by the source and again by every sink.
#[derive(Debug)]
pub struct EventBatch {
    events: Vec<Event>,
    bytes: usize,
}

impl EventBatch {
    pub fn new(events: Vec<Event>) -> Self {
        let bytes = events.iter().map(Event::estimated_size).sum();
        Self { events, bytes }
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Default arena chunk for [`BatchBuffer`].
const DEFAULT_CHUNK_BYTES: usize = 1024 * 1024;

/// An arena that turns per-message allocations into per-chunk allocations.
///
/// Copying a Kafka record out of librdkafka's borrowed buffer normally costs one
/// `malloc` per key and one per payload. `BatchBuffer` instead copies into a
/// chunk of `BytesMut` and hands out `Bytes` views that share it, so a batch of
/// N messages costs roughly `total_bytes / chunk` allocations instead of 2N.
///
/// Trade-off: a chunk stays resident until every `Bytes` carved from it drops.
/// That is fine here because all events in a batch are acknowledged and dropped
/// together, but it makes the type unsuitable for values with mixed lifetimes.
pub struct BatchBuffer {
    chunk: usize,
    buffer: BytesMut,
}

impl Default for BatchBuffer {
    fn default() -> Self {
        Self::with_chunk(DEFAULT_CHUNK_BYTES)
    }
}

impl BatchBuffer {
    pub fn with_chunk(chunk: usize) -> Self {
        Self {
            chunk: chunk.max(1),
            buffer: BytesMut::new(),
        }
    }

    /// Copies `source` into the arena and returns a zero-copy view of it.
    pub fn copy(&mut self, source: &[u8]) -> Bytes {
        if source.is_empty() {
            return Bytes::new();
        }
        // After a `split_to` the buffer is empty and `capacity` is whatever is
        // left in the current chunk, so this is a pure remaining-space check.
        if self.buffer.capacity() < source.len() {
            self.buffer = BytesMut::with_capacity(self.chunk.max(source.len()));
        }
        self.buffer.extend_from_slice(source);
        self.buffer.split_to(source.len()).freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchBuffer, Event, EventBatch, SourceMetadata};
    use std::sync::Arc;

    fn event(payload: &[u8]) -> Event {
        Event {
            key: None,
            payload: bytes::Bytes::copy_from_slice(payload),
            timestamp_ms: None,
            source: SourceMetadata {
                component_id: Arc::from("source"),
                topic: Arc::from("topic"),
                partition: 0,
                offset: 0,
            },
        }
    }

    #[test]
    fn arena_views_keep_their_own_contents() {
        let mut buffer = BatchBuffer::with_chunk(16);
        let views: Vec<_> = (0..8u8).map(|i| buffer.copy(&[i; 5])).collect();
        for (i, view) in views.iter().enumerate() {
            assert_eq!(view.as_ref(), &[i as u8; 5], "view {i} was corrupted");
        }
    }

    #[test]
    fn arena_handles_values_larger_than_a_chunk() {
        let mut buffer = BatchBuffer::with_chunk(8);
        let big = vec![7_u8; 100];
        assert_eq!(buffer.copy(&big).as_ref(), big.as_slice());
        assert!(buffer.copy(&[]).is_empty());
    }

    #[test]
    fn batch_size_counts_payload_and_struct_overhead() {
        let batch = EventBatch::new(vec![event(b"abcd"), event(b"ef")]);
        assert_eq!(batch.len(), 2);
        assert_eq!(
            batch.bytes(),
            batch
                .events()
                .iter()
                .map(Event::estimated_size)
                .sum::<usize>()
        );
        assert!(batch.bytes() > 6, "struct overhead must be accounted for");
    }
}
