//! A Kafka fan-out router: one source normalises records into transport-neutral
//! [`event::Event`]s, [`topology::Fanout`] shares each batch with every sink,
//! and the source commits only the contiguous prefix of batches every sink has
//! acknowledged.
//!
//! Delivery is at-least-once. A batch that any sink rejects is never committed,
//! so Kafka replays it; sinks that already accepted it will see it again.

pub mod config;
pub mod event;
pub mod sink;
pub mod source;
pub mod topology;
