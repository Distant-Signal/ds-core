//! The Redis Streams ingest runtime (ingest architecture spec §7; plan 3a;
//! the API and metrics are in `docs/ingest-stream-runtime.md`).
//!
//! - [`envelope`]: the versioned entry envelope, gzip above 8 KiB, the
//!   512 KiB cap and the snapshot split.
//! - [`producer`]: XADD with `MAXLEN ~`, the latest-snapshot and event
//!   policies while Redis is unavailable, backoff and metrics.
//! - [`consumer`]: the writer's consumer-group runtime: PEL first,
//!   `XAUTOCLAIM`, dead letters, graceful shutdown and metrics.
//! - [`snapshot`]: the producers' snapshot sink helpers (one item per
//!   snapshot, row counts, the stream cursor).
//! - [`budget`]: per-stream `MAXLEN` from rates, sizes and the 2-hour
//!   outage target, checked against the 512 MB budget.
//!
//! It has no database dependency, so the stream producers stay light.

pub mod budget;
pub mod consumer;
pub mod envelope;
pub mod metrics;
pub mod producer;
pub mod snapshot;

pub use consumer::{
    ConsumerConfig, Handled, Handler, HandlerError, RetryReason, Step, StreamConsumer, StreamEntry,
    StreamStats,
};
pub use envelope::{
    BatchPart, DecodeError, EncodedEntry, Encoding, Envelope, EnvelopeError, SchemaId,
    split_snapshot,
};
pub use producer::{
    NotWritten, ProducePolicy, Producer, ProducerConfig, Receipt, last_produced_at, xadd_entry,
};

/// The writer's consumer group on every ingest stream.
pub const WRITER_GROUP: &str = "ingest-writer";

/// The ingest stream names (spec §7.1). D1: no `train-events` stream.
pub mod streams {
    pub const STATION_SAMPLES: &str = "ds:ingest:station-samples";
    pub const FULL_COVERAGE: &str = "ds:ingest:full-coverage";
    pub const TFL: &str = "ds:ingest:tfl";
    /// tocs (`tocs/1`).
    pub const REFERENCE: &str = "ds:ingest:reference";
    /// The three island-of-Ireland pollers (disabled).
    pub const ISLAND_OF_IRELAND: &str = "ds:ingest:island-of-ireland";
}

/// The dead-letter stream of `stream`: `ds:ingest:<domain>` →
/// `ds:dlq:<domain>` (spec §7.1, and the `ingest-writer` ACL user's
/// `~ds:dlq:*`). Any prefix before `ds:ingest:` is kept (the tests' random
/// key prefixes); a stream outside `ds:ingest:` gets `<stream>:dlq`.
pub fn dead_letter_stream(stream: &str) -> String {
    match stream.rfind("ds:ingest:") {
        Some(at) => format!(
            "{}ds:dlq:{}",
            &stream[..at],
            &stream[at + "ds:ingest:".len()..]
        ),
        None => format!("{stream}:dlq"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_letter_streams_follow_the_spec_naming() {
        assert_eq!(
            dead_letter_stream(streams::STATION_SAMPLES),
            "ds:dlq:station-samples"
        );
        assert_eq!(dead_letter_stream("t1:ds:ingest:tfl"), "t1:ds:dlq:tfl");
        assert_eq!(dead_letter_stream("other"), "other:dlq");
    }

    #[test]
    fn every_declared_stream_is_an_ingest_stream() {
        for d in budget::INGEST_STREAMS {
            assert!(d.stream.starts_with("ds:ingest:"), "{}", d.stream);
        }
    }
}
