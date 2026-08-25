use std::sync::Arc;

use bytes::Bytes;

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
    pub fn estimated_size(&self) -> usize {
        self.key.as_ref().map_or(0, Bytes::len) + self.payload.len()
    }
}
