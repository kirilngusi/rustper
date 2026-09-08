//! Building `JSONEachRow` lines from raw Kafka payloads.
//!
//! Deliberately does not parse JSON. ClickHouse performs every type conversion,
//! so the router never needs to know that a column is `Decimal(18, 4)`; all it
//! must do is hand over a syntactically valid object per row. Parsing every
//! message into a `serde_json::Value` to achieve the same result would put a
//! tree allocation on the hot path at several hundred thousand messages a
//! second, for no gain.

use crate::event::Event;

/// Names for the Kafka coordinates, which a JSON payload cannot carry itself.
///
/// Only the fields set here are injected; the rest of the row is the payload,
/// untouched.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataColumns {
    pub topic: Option<String>,
    pub partition: Option<String>,
    pub offset: Option<String>,
    pub timestamp: Option<String>,
    pub key: Option<String>,
}

impl MetadataColumns {
    pub fn is_empty(&self) -> bool {
        self.topic.is_none()
            && self.partition.is_none()
            && self.offset.is_none()
            && self.timestamp.is_none()
            && self.key.is_none()
    }
}

/// Why a row was not written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    Empty,
    NotAnObject,
}

impl Rejection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "payload is empty",
            Self::NotAnObject => "payload is not a JSON object",
        }
    }
}

/// The payload's body: everything between the outer braces.
///
/// This is the whole of the validation done in Rust, and it is O(1) at the
/// front and back of the slice. Anything subtler — a field whose value does not
/// fit its column — is left to ClickHouse, which can skip bad rows itself and
/// reports how many it skipped.
fn object_body(payload: &[u8]) -> Result<&[u8], Rejection> {
    let trimmed = trim_ascii(payload);
    if trimmed.is_empty() {
        return Err(Rejection::Empty);
    }
    match (trimmed.first(), trimmed.last()) {
        (Some(b'{'), Some(b'}')) => Ok(trim_ascii(&trimmed[1..trimmed.len() - 1])),
        _ => Err(Rejection::NotAnObject),
    }
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes.iter().position(|b| !b.is_ascii_whitespace());
    let Some(start) = start else { return &[] };
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .expect("a non-whitespace byte exists");
    &bytes[start..=end]
}

/// Appends one `JSONEachRow` line for `event` to `out`.
///
/// With no metadata columns configured the payload is copied through verbatim.
pub fn write_row(
    out: &mut Vec<u8>,
    event: &Event,
    metadata: &MetadataColumns,
) -> Result<(), Rejection> {
    let body = object_body(&event.payload)?;

    if metadata.is_empty() {
        out.extend_from_slice(b"{");
        out.extend_from_slice(body);
        out.extend_from_slice(b"}\n");
        return Ok(());
    }

    out.push(b'{');
    let mut first = true;
    let mut separate = |out: &mut Vec<u8>| {
        if first {
            first = false;
        } else {
            out.push(b',');
        }
    };
    if let Some(name) = &metadata.topic {
        separate(out);
        write_string_field(out, name, event.source.topic.as_bytes());
    }
    if let Some(name) = &metadata.partition {
        separate(out);
        write_number_field(out, name, event.source.partition as i64);
    }
    if let Some(name) = &metadata.offset {
        separate(out);
        write_number_field(out, name, event.source.offset);
    }
    if let Some(name) = &metadata.timestamp {
        separate(out);
        write_number_field(out, name, event.timestamp_ms.unwrap_or_default());
    }
    if let Some(name) = &metadata.key {
        separate(out);
        write_string_field(out, name, event.key.as_deref().unwrap_or_default());
    }
    // An empty payload object means there is nothing to separate from, and
    // emitting a comma here would produce invalid JSON.
    if !body.is_empty() {
        separate(out);
        out.extend_from_slice(body);
    }
    out.extend_from_slice(b"}\n");
    Ok(())
}

fn write_number_field(out: &mut Vec<u8>, name: &str, value: i64) {
    write_key(out, name);
    out.extend_from_slice(itoa(value).as_bytes());
}

fn write_string_field(out: &mut Vec<u8>, name: &str, value: &[u8]) {
    write_key(out, name);
    write_json_string(out, value);
}

fn write_key(out: &mut Vec<u8>, name: &str) {
    write_json_string(out, name.as_bytes());
    out.push(b':');
}

fn itoa(value: i64) -> String {
    value.to_string()
}

/// Escapes per RFC 8259.
///
/// Kafka keys are arbitrary bytes, so anything that is not valid UTF-8 is
/// replaced rather than emitted raw, which would produce a JSON document
/// ClickHouse rejects for the whole batch.
fn write_json_string(out: &mut Vec<u8>, value: &[u8]) {
    out.push(b'"');
    let text = String::from_utf8_lossy(value);
    for character in text.chars() {
        match character {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\t' => out.extend_from_slice(b"\\t"),
            c if (c as u32) < 0x20 => {
                out.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes());
            }
            c => {
                let mut buffer = [0_u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buffer).as_bytes());
            }
        }
    }
    out.push(b'"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;

    use crate::event::SourceMetadata;

    fn event(payload: &str) -> Event {
        Event {
            key: Some(Bytes::from_static(b"k1")),
            payload: Bytes::copy_from_slice(payload.as_bytes()),
            timestamp_ms: Some(1_700_000_000_123),
            source: SourceMetadata {
                component_id: Arc::from("src"),
                topic: Arc::from("orders"),
                partition: 3,
                offset: 42,
            },
        }
    }

    fn row(payload: &str, metadata: &MetadataColumns) -> Result<String, Rejection> {
        let mut out = Vec::new();
        write_row(&mut out, &event(payload), metadata)?;
        Ok(String::from_utf8(out).expect("valid UTF-8"))
    }

    fn all_metadata() -> MetadataColumns {
        MetadataColumns {
            topic: Some("_topic".into()),
            partition: Some("_partition".into()),
            offset: Some("_offset".into()),
            timestamp: Some("_ts".into()),
            key: Some("_key".into()),
        }
    }

    /// Every emitted row must be a valid JSON object, or ClickHouse rejects the
    /// entire batch rather than one row.
    fn assert_valid_json(line: &str) -> serde_json::Value {
        let trimmed = line
            .strip_suffix('\n')
            .expect("rows are newline terminated");
        serde_json::from_str(trimmed).unwrap_or_else(|e| panic!("invalid JSON {trimmed:?}: {e}"))
    }

    #[test]
    fn without_metadata_the_payload_passes_through() {
        let line = row(r#"{"a":1,"b":"x"}"#, &MetadataColumns::default()).unwrap();
        assert_eq!(line, "{\"a\":1,\"b\":\"x\"}\n");
        assert_valid_json(&line);
    }

    #[test]
    fn metadata_is_spliced_in_front_of_the_payload() {
        let line = row(r#"{"a":1}"#, &all_metadata()).unwrap();
        let parsed = assert_valid_json(&line);
        assert_eq!(parsed["_topic"], "orders");
        assert_eq!(parsed["_partition"], 3);
        assert_eq!(parsed["_offset"], 42);
        assert_eq!(parsed["_ts"], 1_700_000_000_123_i64);
        assert_eq!(parsed["_key"], "k1");
        assert_eq!(
            parsed["a"], 1,
            "the payload survives alongside the metadata"
        );
    }

    #[test]
    fn an_empty_payload_object_does_not_produce_a_trailing_comma() {
        // The naive splice would emit {"_topic":"orders",} which is invalid.
        let line = row("{}", &all_metadata()).unwrap();
        let parsed = assert_valid_json(&line);
        assert_eq!(parsed["_topic"], "orders");
        assert_eq!(parsed.as_object().unwrap().len(), 5);

        let bare = row("{}", &MetadataColumns::default()).unwrap();
        assert_eq!(bare, "{}\n");
    }

    #[test]
    fn whitespace_around_the_payload_is_tolerated() {
        let line = row("  \n {\"a\": 1}\t ", &all_metadata()).unwrap();
        assert_eq!(assert_valid_json(&line)["a"], 1);

        let empty_with_space = row("{   }", &all_metadata()).unwrap();
        assert_valid_json(&empty_with_space);
    }

    #[test]
    fn only_one_metadata_column_still_produces_valid_json() {
        let metadata = MetadataColumns {
            offset: Some("_offset".into()),
            ..MetadataColumns::default()
        };
        let parsed = assert_valid_json(&row(r#"{"a":1}"#, &metadata).unwrap());
        assert_eq!(parsed["_offset"], 42);
        assert_eq!(parsed.as_object().unwrap().len(), 2);
    }

    #[test]
    fn non_objects_are_rejected_rather_than_corrupting_the_batch() {
        assert_eq!(row("", &all_metadata()), Err(Rejection::Empty));
        assert_eq!(row("   \n ", &all_metadata()), Err(Rejection::Empty));
        assert_eq!(
            row("not json", &all_metadata()),
            Err(Rejection::NotAnObject)
        );
        assert_eq!(row("[1,2,3]", &all_metadata()), Err(Rejection::NotAnObject));
        assert_eq!(
            row("\"a string\"", &all_metadata()),
            Err(Rejection::NotAnObject)
        );
        assert_eq!(
            row("{\"a\":1", &all_metadata()),
            Err(Rejection::NotAnObject)
        );
    }

    #[test]
    fn column_names_and_keys_are_escaped() {
        let metadata = MetadataColumns {
            topic: Some("odd\"name".into()),
            key: Some("_key".into()),
            ..MetadataColumns::default()
        };
        let mut event = event(r#"{"a":1}"#);
        event.key = Some(Bytes::from_static(b"line\nbreak\"quote\\slash"));
        let mut out = Vec::new();
        write_row(&mut out, &event, &metadata).unwrap();
        let line = String::from_utf8(out).unwrap();
        let parsed = assert_valid_json(&line);
        assert_eq!(parsed["odd\"name"], "orders");
        assert_eq!(parsed["_key"], "line\nbreak\"quote\\slash");
    }

    #[test]
    fn invalid_utf8_in_a_key_does_not_produce_invalid_json() {
        // A Kafka key is arbitrary bytes. Emitting them raw would break the
        // whole batch, so they are replaced instead.
        let metadata = MetadataColumns {
            key: Some("_key".into()),
            ..MetadataColumns::default()
        };
        let mut event = event(r#"{"a":1}"#);
        event.key = Some(Bytes::from_static(&[0xff, 0xfe, b'o', b'k']));
        let mut out = Vec::new();
        write_row(&mut out, &event, &metadata).unwrap();
        assert_valid_json(&String::from_utf8(out).unwrap());
    }

    #[test]
    fn control_characters_in_a_key_are_escaped() {
        let metadata = MetadataColumns {
            key: Some("_key".into()),
            ..MetadataColumns::default()
        };
        let mut event = event(r#"{"a":1}"#);
        event.key = Some(Bytes::from_static(&[0x01, b'a']));
        let mut out = Vec::new();
        write_row(&mut out, &event, &metadata).unwrap();
        let line = String::from_utf8(out).unwrap();
        assert!(line.contains("\\u0001"), "{line}");
        assert_valid_json(&line);
    }

    #[test]
    fn a_missing_timestamp_becomes_zero_rather_than_null() {
        let metadata = MetadataColumns {
            timestamp: Some("_ts".into()),
            ..MetadataColumns::default()
        };
        let mut event = event(r#"{"a":1}"#);
        event.timestamp_ms = None;
        let mut out = Vec::new();
        write_row(&mut out, &event, &metadata).unwrap();
        assert_eq!(
            assert_valid_json(&String::from_utf8(out).unwrap())["_ts"],
            0
        );
    }

    #[test]
    fn rows_accumulate_into_one_buffer() {
        let mut out = Vec::new();
        write_row(&mut out, &event(r#"{"a":1}"#), &MetadataColumns::default()).unwrap();
        write_row(&mut out, &event(r#"{"a":2}"#), &MetadataColumns::default()).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert_eq!(text.lines().count(), 2);
        for line in text.lines() {
            serde_json::from_str::<serde_json::Value>(line).expect("each line stands alone");
        }
    }
}
