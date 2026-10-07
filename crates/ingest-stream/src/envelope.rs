//! The stream entry envelope (spec §7.2).
//!
//! An [`Envelope`] is the logical entry: envelope version, schema, producer,
//! idempotency key, `produced_at`, an optional batch part, and the JSON
//! payload. On the wire it is a flat list of Redis stream fields, so
//! `XRANGE` and the dead-letter runbook stay readable:
//!
//! | field | example |
//! |---|---|
//! | `v` | `1` |
//! | `schema` | `station-samples/1` |
//! | `producer` | `poller-ldbws/distant-signal-poller-ldbws-6d…` |
//! | `key` | `station-samples:2026-10-06T20:21:00Z:3/6` |
//! | `produced_at` | `2026-10-06T20:21:00.123Z` |
//! | `enc` | `json`, or `json+gzip` when the payload exceeds [`GZIP_THRESHOLD`] |
//! | `batch`, `part`, `parts` | only for a chunked snapshot |
//! | `body` | the payload bytes |
//!
//! [`Envelope::encode`] refuses a body over [`MAX_ENCODED_BODY`] after
//! encoding with [`EnvelopeError::TooLarge`]; [`split_snapshot`] splits a
//! large snapshot into parts that fit. [`Envelope::decode`] refuses anything
//! over [`MAX_ACCEPTED_BODY`] on the wire or [`MAX_DECODED_BODY`] after
//! gunzip, so a misbehaving producer cannot hand the writer a huge
//! allocation (the 2026-09-26 failure).
//!
//! Compatibility: unknown fields are ignored (a v1 writer reads a v1 entry
//! with extra fields), an unknown `v` is [`DecodeError::UnsupportedVersion`]
//! (left pending, not dead-lettered: the writer is older than the
//! producer). The golden fixtures in `tests/fixtures/` pin both the JSON
//! form and the wire form.

use std::fmt;
use std::io::{Read, Write};
use std::str::FromStr;

use chrono::{DateTime, SecondsFormat, Utc};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

/// The envelope version this crate writes and reads.
pub const ENVELOPE_VERSION: u32 = 1;

/// A payload longer than this is gzipped (`enc = json+gzip`).
pub const GZIP_THRESHOLD: usize = 8 * 1024;

/// The largest `body` a producer writes, after encoding.
pub const MAX_ENCODED_BODY: usize = 512 * 1024;

/// The largest `body` the writer accepts on the wire; anything larger is
/// poison. Twice [`MAX_ENCODED_BODY`], so a producer one release ahead with
/// a slightly larger limit is not dead-lettered.
pub const MAX_ACCEPTED_BODY: usize = 1024 * 1024;

/// The largest payload the writer will gunzip to. A real 512 KiB gzip part
/// is a few MB of JSON at most; a zip bomb stops here.
pub const MAX_DECODED_BODY: usize = 16 * 1024 * 1024;

/// The wire field names, in the order [`Envelope::encode`] writes them.
pub mod field {
    pub const VERSION: &str = "v";
    pub const SCHEMA: &str = "schema";
    pub const PRODUCER: &str = "producer";
    pub const KEY: &str = "key";
    pub const PRODUCED_AT: &str = "produced_at";
    pub const ENCODING: &str = "enc";
    pub const BATCH: &str = "batch";
    pub const PART: &str = "part";
    pub const PARTS: &str = "parts";
    pub const BODY: &str = "body";
}

/// `name/version`, for example `station-samples/1`. The name is lower-case
/// ASCII letters, digits and `-`; the version a positive integer.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchemaId {
    name: String,
    version: u32,
}

impl SchemaId {
    pub fn new(name: &str, version: u32) -> Result<Self, EnvelopeError> {
        let valid_name = !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if !valid_name || version == 0 {
            return Err(EnvelopeError::InvalidSchema(format!("{name}/{version}")));
        }
        Ok(Self {
            name: name.to_owned(),
            version,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> u32 {
        self.version
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.name, self.version)
    }
}

impl FromStr for SchemaId {
    type Err = EnvelopeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (name, version) = s
            .split_once('/')
            .ok_or_else(|| EnvelopeError::InvalidSchema(s.to_owned()))?;
        let version = version
            .parse::<u32>()
            .map_err(|_| EnvelopeError::InvalidSchema(s.to_owned()))?;
        Self::new(name, version)
    }
}

impl Serialize for SchemaId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SchemaId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// One part of a chunked snapshot. `part` is 1-based; each part is applied
/// independently, `parts` lets the writer count incomplete batches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchPart {
    pub batch: String,
    pub part: u32,
    pub parts: u32,
}

/// The logical stream entry. See the module docs for the wire form.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub v: u32,
    pub schema: SchemaId,
    /// `<component>/<pod name>`.
    pub producer: String,
    /// The idempotency key: chosen by the producer, stable across a retry of
    /// the same entry (the writer's `ingest_dedup` key).
    pub key: String,
    /// When the snapshot was taken, at millisecond precision.
    #[serde(with = "millis_rfc3339")]
    pub produced_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch: Option<BatchPart>,
    pub payload: Box<RawValue>,
}

impl PartialEq for Envelope {
    fn eq(&self, other: &Self) -> bool {
        self.v == other.v
            && self.schema == other.schema
            && self.producer == other.producer
            && self.key == other.key
            && self.produced_at == other.produced_at
            && self.batch == other.batch
            && self.payload.get() == other.payload.get()
    }
}

impl Envelope {
    /// A v1 envelope carrying `payload` serialised as JSON. `produced_at` is
    /// truncated to milliseconds, the wire precision.
    pub fn new<T: Serialize + ?Sized>(
        schema: SchemaId,
        producer: impl Into<String>,
        key: impl Into<String>,
        produced_at: DateTime<Utc>,
        payload: &T,
    ) -> Result<Self, EnvelopeError> {
        let payload = serde_json::value::to_raw_value(payload)
            .map_err(|e| EnvelopeError::Serialize(e.to_string()))?;
        Ok(Self::from_raw(schema, producer, key, produced_at, payload))
    }

    /// [`Envelope::new`] for a payload that is already JSON.
    pub fn from_raw(
        schema: SchemaId,
        producer: impl Into<String>,
        key: impl Into<String>,
        produced_at: DateTime<Utc>,
        payload: Box<RawValue>,
    ) -> Self {
        Self {
            v: ENVELOPE_VERSION,
            schema,
            producer: producer.into(),
            key: key.into(),
            produced_at: truncate_to_millis(produced_at),
            batch: None,
            payload,
        }
    }

    #[must_use]
    pub fn with_batch(mut self, batch: BatchPart) -> Self {
        self.batch = Some(batch);
        self
    }

    /// The payload, deserialised.
    pub fn payload_as<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(self.payload.get())
    }

    /// The conventional key of part `part` of `parts` of snapshot `batch`:
    /// `<schema name>:<batch>:<part>/<parts>`. A retry re-sends the same
    /// encoded entry, so the key is stable across retries by construction.
    pub fn snapshot_key(schema: &SchemaId, batch: &str, part: u32, parts: u32) -> String {
        format!("{}:{batch}:{part}/{parts}", schema.name())
    }

    /// The wire fields. Gzips a payload over [`GZIP_THRESHOLD`]; refuses a
    /// body over [`MAX_ENCODED_BODY`] after encoding.
    pub fn encode(&self) -> Result<EncodedEntry, EnvelopeError> {
        let raw = self.payload.get().as_bytes();
        let (encoding, body) = if raw.len() > GZIP_THRESHOLD {
            (Encoding::JsonGzip, gzip(raw)?)
        } else {
            (Encoding::Json, raw.to_vec())
        };
        if body.len() > MAX_ENCODED_BODY {
            return Err(EnvelopeError::TooLarge {
                encoded: body.len(),
                limit: MAX_ENCODED_BODY,
            });
        }
        let mut fields: Vec<(&'static str, Vec<u8>)> = vec![
            (field::VERSION, self.v.to_string().into_bytes()),
            (field::SCHEMA, self.schema.to_string().into_bytes()),
            (field::PRODUCER, self.producer.clone().into_bytes()),
            (field::KEY, self.key.clone().into_bytes()),
            (
                field::PRODUCED_AT,
                format_produced_at(self.produced_at).into_bytes(),
            ),
            (field::ENCODING, encoding.as_str().as_bytes().to_vec()),
        ];
        if let Some(batch) = &self.batch {
            fields.push((field::BATCH, batch.batch.clone().into_bytes()));
            fields.push((field::PART, batch.part.to_string().into_bytes()));
            fields.push((field::PARTS, batch.parts.to_string().into_bytes()));
        }
        let body_len = body.len();
        fields.push((field::BODY, body));
        Ok(EncodedEntry {
            schema: self.schema.clone(),
            key: self.key.clone(),
            raw_len: raw.len(),
            body_len,
            encoding,
            fields,
        })
    }

    /// Parses wire fields (in any order; unknown fields are ignored).
    pub fn decode<K: AsRef<[u8]>, V: AsRef<[u8]>>(fields: &[(K, V)]) -> Result<Self, DecodeError> {
        let get = |name: &str| {
            fields
                .iter()
                .find(|(k, _)| k.as_ref() == name.as_bytes())
                .map(|(_, v)| v.as_ref())
        };
        let text = |name: &'static str| -> Result<&str, DecodeError> {
            let bytes = get(name).ok_or(DecodeError::MissingField(name))?;
            std::str::from_utf8(bytes).map_err(|_| DecodeError::BadField(name))
        };
        let v: u32 = text(field::VERSION)?
            .parse()
            .map_err(|_| DecodeError::BadField(field::VERSION))?;
        if v != ENVELOPE_VERSION {
            return Err(DecodeError::UnsupportedVersion(v));
        }
        let schema: SchemaId = text(field::SCHEMA)?
            .parse()
            .map_err(|_| DecodeError::BadField(field::SCHEMA))?;
        let producer = text(field::PRODUCER)?.to_owned();
        let key = text(field::KEY)?.to_owned();
        if key.is_empty() {
            return Err(DecodeError::BadField(field::KEY));
        }
        let produced_at = parse_produced_at(text(field::PRODUCED_AT)?)
            .ok_or(DecodeError::BadField(field::PRODUCED_AT))?;
        let encoding = match text(field::ENCODING)? {
            "json" => Encoding::Json,
            "json+gzip" => Encoding::JsonGzip,
            _ => return Err(DecodeError::BadField(field::ENCODING)),
        };
        let batch = match (get(field::BATCH), get(field::PART), get(field::PARTS)) {
            (None, None, None) => None,
            (Some(_), Some(_), Some(_)) => {
                let part: u32 = text(field::PART)?
                    .parse()
                    .map_err(|_| DecodeError::BadField(field::PART))?;
                let parts: u32 = text(field::PARTS)?
                    .parse()
                    .map_err(|_| DecodeError::BadField(field::PARTS))?;
                if part == 0 || part > parts {
                    return Err(DecodeError::BadField(field::PART));
                }
                Some(BatchPart {
                    batch: text(field::BATCH)?.to_owned(),
                    part,
                    parts,
                })
            }
            _ => return Err(DecodeError::BadField(field::BATCH)),
        };
        let body = get(field::BODY).ok_or(DecodeError::MissingField(field::BODY))?;
        if body.len() > MAX_ACCEPTED_BODY {
            return Err(DecodeError::TooLarge {
                bytes: body.len(),
                limit: MAX_ACCEPTED_BODY,
            });
        }
        let json = match encoding {
            Encoding::Json => body.to_vec(),
            Encoding::JsonGzip => gunzip_bounded(body, MAX_DECODED_BODY)?,
        };
        let json = String::from_utf8(json).map_err(|_| DecodeError::BadField(field::BODY))?;
        let payload =
            RawValue::from_string(json).map_err(|_| DecodeError::BadField(field::BODY))?;
        Ok(Self {
            v,
            schema,
            producer,
            key,
            produced_at,
            batch,
            payload,
        })
    }
}

/// `enc`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    Json,
    JsonGzip,
}

impl Encoding {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::JsonGzip => "json+gzip",
        }
    }
}

/// An envelope ready for `XADD`: its wire fields, plus what the producer
/// needs for metrics and logs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedEntry {
    pub schema: SchemaId,
    pub key: String,
    /// The payload's JSON length before encoding.
    pub raw_len: usize,
    /// The `body` field's length.
    pub body_len: usize,
    pub encoding: Encoding,
    pub fields: Vec<(&'static str, Vec<u8>)>,
}

impl EncodedEntry {
    /// The sum of the field names and values: what the entry adds to the
    /// stream, before Redis's own per-entry overhead.
    pub fn wire_len(&self) -> usize {
        self.fields.iter().map(|(k, v)| k.len() + v.len()).sum()
    }
}

/// Splits `rows` into parts of at most `max_rows_per_part` rows, wraps each
/// part with `wrap` (today's HTTP body shape for that chunk, as JSON:
/// `|chunk| serde_json::value::to_raw_value(&Body { rows: chunk })`) and
/// encodes it
/// as part `i/n` of `batch`, keyed by [`Envelope::snapshot_key`].
///
/// If a part still encodes over [`MAX_ENCODED_BODY`], the rows per part are
/// halved and the whole snapshot is re-split, so every part has the same
/// row bound. A single row that does not fit is [`EnvelopeError::TooLarge`].
/// An empty `rows` yields one empty part, so "the snapshot is empty" still
/// reaches the writer.
pub fn split_snapshot<T, F>(
    schema: &SchemaId,
    producer: &str,
    produced_at: DateTime<Utc>,
    batch: &str,
    rows: &[T],
    max_rows_per_part: usize,
    wrap: F,
) -> Result<Vec<EncodedEntry>, EnvelopeError>
where
    F: Fn(&[T]) -> Result<Box<RawValue>, serde_json::Error>,
{
    let mut per_part = max_rows_per_part.max(1);
    loop {
        let chunks: Vec<&[T]> = if rows.is_empty() {
            vec![rows]
        } else {
            rows.chunks(per_part).collect()
        };
        let parts = u32::try_from(chunks.len()).map_err(|_| EnvelopeError::TooManyParts)?;
        let mut encoded = Vec::with_capacity(chunks.len());
        let mut too_large = None;
        for (index, chunk) in (1..).zip(&chunks) {
            let key = Envelope::snapshot_key(schema, batch, index, parts);
            let payload = wrap(chunk).map_err(|e| EnvelopeError::Serialize(e.to_string()))?;
            let envelope = Envelope::from_raw(schema.clone(), producer, key, produced_at, payload)
                .with_batch(BatchPart {
                    batch: batch.to_owned(),
                    part: index,
                    parts,
                });
            match envelope.encode() {
                Ok(entry) => encoded.push(entry),
                Err(err @ EnvelopeError::TooLarge { .. }) => {
                    too_large = Some(err);
                    break;
                }
                Err(other) => return Err(other),
            }
        }
        match too_large {
            None => return Ok(encoded),
            Some(err) if per_part == 1 => return Err(err),
            Some(_) => per_part = per_part.div_ceil(2),
        }
    }
}

/// Why an envelope could not be built or encoded.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EnvelopeError {
    #[error("invalid schema id {0:?} (want name/version, name in [a-z0-9-])")]
    InvalidSchema(String),
    #[error("payload does not serialise: {0}")]
    Serialize(String),
    #[error("encoded body is {encoded} bytes, over the {limit}-byte limit; split the snapshot")]
    TooLarge { encoded: usize, limit: usize },
    #[error("snapshot needs more than u32::MAX parts")]
    TooManyParts,
    #[error("gzip failed: {0}")]
    Gzip(String),
}

/// Why wire fields are not a usable envelope.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("missing field {0:?}")]
    MissingField(&'static str),
    #[error("malformed field {0:?}")]
    BadField(&'static str),
    #[error("unsupported envelope version {0}")]
    UnsupportedVersion(u32),
    #[error("body is {bytes} bytes, over the {limit}-byte limit")]
    TooLarge { bytes: usize, limit: usize },
    #[error("body does not gunzip: {0}")]
    Gzip(String),
}

impl DecodeError {
    /// Whether the writer dead-letters this entry or leaves it pending.
    /// Only an unknown envelope version is left pending (the writer is older
    /// than the producer; rolling it forward fixes it, spec §7.3).
    pub fn is_poison(&self) -> bool {
        !matches!(self, Self::UnsupportedVersion(_))
    }

    /// A short, stable label for metrics and the dead-letter `error` field.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::MissingField(_) | Self::BadField(_) | Self::Gzip(_) => "undecodable",
            Self::UnsupportedVersion(_) => "unsupported_envelope",
            Self::TooLarge { .. } => "oversize",
        }
    }
}

fn gzip(raw: &[u8]) -> Result<Vec<u8>, EnvelopeError> {
    let mut encoder = GzEncoder::new(Vec::with_capacity(raw.len() / 4), Compression::default());
    encoder
        .write_all(raw)
        .map_err(|e| EnvelopeError::Gzip(e.to_string()))?;
    encoder
        .finish()
        .map_err(|e| EnvelopeError::Gzip(e.to_string()))
}

fn gunzip_bounded(body: &[u8], limit: usize) -> Result<Vec<u8>, DecodeError> {
    let mut out = Vec::new();
    // One byte past the limit tells "exactly at" from "over".
    let cap = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    GzDecoder::new(body)
        .take(cap)
        .read_to_end(&mut out)
        .map_err(|e| DecodeError::Gzip(e.to_string()))?;
    if out.len() > limit {
        return Err(DecodeError::TooLarge {
            bytes: out.len(),
            limit,
        });
    }
    Ok(out)
}

fn truncate_to_millis(at: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(at.timestamp_millis()).unwrap_or(at)
}

/// `2026-10-06T20:21:00.123Z`.
pub fn format_produced_at(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Parses any RFC 3339 time, normalised to UTC milliseconds.
pub fn parse_produced_at(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|t| truncate_to_millis(t.with_timezone(&Utc)))
}

mod millis_rfc3339 {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(at: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&super::format_produced_at(*at))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        let text = String::deserialize(d)?;
        super::parse_produced_at(&text)
            .ok_or_else(|| serde::de::Error::custom(format!("not an RFC 3339 time: {text:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 6, 20, 21, 0).unwrap()
            + chrono::Duration::microseconds(123_456)
    }

    fn schema() -> SchemaId {
        SchemaId::new("station-samples", 1).unwrap()
    }

    fn envelope(payload: &serde_json::Value) -> Envelope {
        Envelope::new(schema(), "poller-ldbws/pod-1", "k1", at(), payload).unwrap()
    }

    fn owned(entry: &EncodedEntry) -> Vec<(String, Vec<u8>)> {
        entry
            .fields
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    }

    fn field_value<'a>(entry: &'a EncodedEntry, name: &str) -> &'a [u8] {
        &entry.fields.iter().find(|(k, _)| *k == name).unwrap().1
    }

    #[test]
    fn schema_ids_parse_and_print() {
        let id: SchemaId = "full-coverage-window-stats/12".parse().unwrap();
        assert_eq!(id.name(), "full-coverage-window-stats");
        assert_eq!(id.version(), 12);
        assert_eq!(id.to_string(), "full-coverage-window-stats/12");
        for bad in ["", "x", "x/", "x/0", "/1", "X/1", "a b/1", "a/1/2", "a/-1"] {
            assert!(bad.parse::<SchemaId>().is_err(), "{bad:?} accepted");
        }
    }

    #[test]
    fn a_small_payload_is_plain_json_and_round_trips() {
        let original = envelope(&serde_json::json!({"rows": [1, 2, 3]}));
        let entry = original.encode().unwrap();
        assert_eq!(entry.encoding, Encoding::Json);
        assert_eq!(field_value(&entry, "body"), br#"{"rows":[1,2,3]}"#);
        assert_eq!(
            field_value(&entry, "produced_at"),
            b"2026-10-06T20:21:00.123Z"
        );
        let decoded = Envelope::decode(&owned(&entry)).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn the_gzip_threshold_is_strictly_above_8_kib() {
        // A JSON string of n chars is n + 2 bytes.
        let at_threshold = serde_json::Value::String("a".repeat(GZIP_THRESHOLD - 2));
        let entry = envelope(&at_threshold).encode().unwrap();
        assert_eq!(entry.raw_len, GZIP_THRESHOLD);
        assert_eq!(entry.encoding, Encoding::Json);

        let over = serde_json::Value::String("a".repeat(GZIP_THRESHOLD - 1));
        let original = envelope(&over);
        let entry = original.encode().unwrap();
        assert_eq!(entry.raw_len, GZIP_THRESHOLD + 1);
        assert_eq!(entry.encoding, Encoding::JsonGzip);
        assert!(
            entry.body_len < 200,
            "compressible body: {}",
            entry.body_len
        );
        assert_eq!(Envelope::decode(&owned(&entry)).unwrap(), original);
    }

    /// Incompressible text (a cheap xorshift over hex digits) of `len` bytes.
    fn noise(seed: u64, len: usize) -> String {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15 ^ seed.wrapping_mul(0x0100_0000_01B3);
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                char::from(b"0123456789abcdef"[usize::try_from(x % 16).unwrap()])
            })
            .collect()
    }

    #[test]
    fn a_body_over_512_kib_after_gzip_is_a_typed_error() {
        // Hex noise compresses to about half, so 1.2 MB stays over 512 KiB.
        let payload = serde_json::Value::String(noise(0, 1_200_000));
        let err = envelope(&payload).encode().unwrap_err();
        match err {
            EnvelopeError::TooLarge { encoded, limit } => {
                assert_eq!(limit, MAX_ENCODED_BODY);
                assert!(encoded > MAX_ENCODED_BODY);
            }
            other => panic!("{other:?}"),
        }
        // A large but compressible payload is fine: gzip brings it under.
        let compressible = serde_json::Value::String("ab".repeat(600_000));
        assert!(envelope(&compressible).encode().is_ok());
    }

    #[test]
    fn the_batch_fields_round_trip_and_must_come_together() {
        let original = envelope(&serde_json::json!([])).with_batch(BatchPart {
            batch: "2026-10-06T20:21:00Z".into(),
            part: 3,
            parts: 6,
        });
        let entry = original.encode().unwrap();
        let fields = owned(&entry);
        assert_eq!(Envelope::decode(&fields).unwrap(), original);

        let without_parts: Vec<_> = fields
            .iter()
            .filter(|(k, _)| k != "parts")
            .cloned()
            .collect();
        assert_eq!(
            Envelope::decode(&without_parts).unwrap_err(),
            DecodeError::BadField("batch")
        );
        let mut part_zero = fields.clone();
        for (k, v) in &mut part_zero {
            if k == "part" {
                *v = b"0".to_vec();
            }
        }
        assert_eq!(
            Envelope::decode(&part_zero).unwrap_err(),
            DecodeError::BadField("part")
        );
    }

    #[test]
    fn decode_refuses_unknown_versions_and_malformed_fields() {
        let entry = envelope(&serde_json::json!({"a": 1})).encode().unwrap();
        let fields = owned(&entry);
        let with = |name: &str, value: &[u8]| -> Vec<(String, Vec<u8>)> {
            fields
                .iter()
                .map(|(k, v)| {
                    if k == name {
                        (k.clone(), value.to_vec())
                    } else {
                        (k.clone(), v.clone())
                    }
                })
                .collect()
        };
        let err = Envelope::decode(&with("v", b"2")).unwrap_err();
        assert_eq!(err, DecodeError::UnsupportedVersion(2));
        assert!(!err.is_poison());

        for (name, value) in [
            ("v", &b"one"[..]),
            ("schema", b"station-samples"),
            ("produced_at", b"yesterday"),
            ("enc", b"json+zstd"),
            ("body", b"{not json"),
            ("key", b""),
        ] {
            let err = Envelope::decode(&with(name, value)).unwrap_err();
            assert!(err.is_poison(), "{name}: {err:?}");
            assert_eq!(err.reason(), "undecodable", "{name}");
        }
        let missing: Vec<_> = fields.iter().filter(|(k, _)| k != "key").cloned().collect();
        assert_eq!(
            Envelope::decode(&missing).unwrap_err(),
            DecodeError::MissingField("key")
        );
        // Unknown extra fields are ignored (forward compatible within v1).
        let mut extra = fields.clone();
        extra.push(("trace_id".into(), b"abc".to_vec()));
        assert!(Envelope::decode(&extra).is_ok());
    }

    #[test]
    fn decode_refuses_an_oversize_body_and_a_gzip_bomb() {
        let entry = envelope(&serde_json::json!(1)).encode().unwrap();
        let mut fields = owned(&entry);
        let body = fields.iter_mut().find(|(k, _)| k == "body").unwrap();
        body.1 = vec![b' '; MAX_ACCEPTED_BODY + 1];
        let err = Envelope::decode(&fields).unwrap_err();
        assert_eq!(err.reason(), "oversize");
        assert!(err.is_poison());

        // 17 MiB of zeros gzips to about 17 KB.
        let bomb = gzip(&vec![b'0'; MAX_DECODED_BODY + 1024 * 1024]).unwrap();
        assert!(bomb.len() < MAX_ACCEPTED_BODY);
        let mut fields = owned(&entry);
        for (k, v) in &mut fields {
            match k.as_str() {
                "body" => *v = bomb.clone(),
                "enc" => *v = b"json+gzip".to_vec(),
                _ => {}
            }
        }
        let err = Envelope::decode(&fields).unwrap_err();
        assert!(
            matches!(err, DecodeError::TooLarge { limit, .. } if limit == MAX_DECODED_BODY),
            "{err:?}"
        );
    }

    #[test]
    fn serde_round_trip_of_the_json_form() {
        let original =
            envelope(&serde_json::json!({"x": [1, {"y": null}]})).with_batch(BatchPart {
                batch: "b".into(),
                part: 1,
                parts: 1,
            });
        let json = serde_json::to_string(&original).unwrap();
        let back: Envelope = serde_json::from_str(&json).unwrap();
        assert_eq!(back, original);
        assert!(
            json.contains(r#""produced_at":"2026-10-06T20:21:00.123Z""#),
            "{json}"
        );
        assert!(json.contains(r#""schema":"station-samples/1""#), "{json}");
    }

    #[test]
    fn split_keeps_parts_under_the_limit_and_keys_stable() {
        #[derive(Serialize)]
        struct Body<'a> {
            stations: &'a [String],
        }
        // 2000 rows of 1 KB noise: about 2 MB of JSON, about 1 MB gzipped.
        let rows: Vec<String> = (0..2000u64)
            .map(|i| noise(i + 1, 1000 + usize::try_from(i % 7).unwrap()))
            .collect();
        // Asked for one part of all 2000 rows; that is about 1 MB gzipped,
        // so it must be halved at least once.
        let split = || {
            split_snapshot(
                &schema(),
                "poller-ldbws/p",
                at(),
                "2026-10-06T20:21:00Z",
                &rows,
                2000,
                |chunk| serde_json::value::to_raw_value(&Body { stations: chunk }),
            )
            .unwrap()
        };
        let parts = split();
        assert!(parts.len() >= 2, "halved at least once: {}", parts.len());
        let n = parts.len();
        let mut total_rows = 0;
        for (i, entry) in parts.iter().enumerate() {
            assert!(entry.body_len <= MAX_ENCODED_BODY);
            assert_eq!(
                entry.key,
                format!("station-samples:2026-10-06T20:21:00Z:{}/{n}", i + 1)
            );
            let decoded = Envelope::decode(&owned(entry)).unwrap();
            let batch = decoded.batch.clone().unwrap();
            assert_eq!(
                (batch.part, batch.parts),
                (u32::try_from(i + 1).unwrap(), u32::try_from(n).unwrap())
            );
            let body: serde_json::Value = decoded.payload_as().unwrap();
            total_rows += body["stations"].as_array().unwrap().len();
        }
        assert_eq!(total_rows, rows.len());
        // Same input, same keys and bytes: a re-split on retry is identical.
        assert_eq!(split(), parts);
    }

    #[test]
    fn split_of_nothing_is_one_empty_part_and_a_huge_row_is_refused() {
        let parts = split_snapshot::<u8, _>(&schema(), "p", at(), "b", &[], 100, |c| {
            serde_json::value::to_raw_value(c)
        })
        .unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].key, "station-samples:b:1/1");
        assert_eq!(field_value(&parts[0], "body"), b"[]");

        let huge = vec![noise(0, 1_200_000)];
        let err = split_snapshot(&schema(), "p", at(), "b", &huge, 100, |c| {
            serde_json::value::to_raw_value(c)
        })
        .unwrap_err();
        assert!(matches!(err, EnvelopeError::TooLarge { .. }), "{err:?}");
    }
}
