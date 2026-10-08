//! Producer-side helpers for the snapshot sinks (`INGEST_SINK=http+shadow`
//! or `stream`; spec §13.1, plan 3a.7 and 3a.8): encode a snapshot's rows
//! into parts, submit them to a [`Producer`] as ONE item, count the rows
//! once they are written, and read the stream cursor.
//!
//! **One item per snapshot.** Under [`crate::ProducePolicy::LatestSnapshot`]
//! a newer item replaces an unsent older one, so everything one snapshot
//! carries goes in one item: every part of every schema (full-coverage's
//! three outputs share `ds:ingest:full-coverage` and one item per stats
//! write). Two items would supersede each other.
//!
//! **`produced_at` is fixed at encode time.** The parts are encoded once,
//! with the snapshot's fetch time, and the producer re-sends the same bytes
//! on every retry (same keys, same `produced_at`, spec §7.8): nothing here
//! re-stamps at XADD.
//!
//! **The payload is today's HTTP body.** A part's body is the JSON array of
//! its rows, so `/1` is byte for byte the body of the api route (spec
//! §7.2), just split into parts of [`ROWS_PER_PART`].

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::oneshot;

use crate::envelope::{EncodedEntry, EnvelopeError, SchemaId, format_produced_at, split_snapshot};
use crate::metrics;
use crate::producer::{NotWritten, Producer, last_produced_at};

/// A stream producer's `INGEST_SINK` (spec §13.1): where its snapshots go.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SinkMode {
    /// POST to the api only (today; the default).
    #[default]
    Http,
    /// POST to the api (authoritative: its result is the cycle's), plus a
    /// copy XADDed to the stream for the writer's `shadow` mode to check.
    HttpShadow,
    /// XADD only; the api is no longer written.
    Stream,
}

impl SinkMode {
    /// Whether the api is POSTed.
    pub fn posts_http(self) -> bool {
        matches!(self, Self::Http | Self::HttpShadow)
    }

    /// Whether snapshots are XADDed (so Redis is needed).
    pub fn produces(self) -> bool {
        matches!(self, Self::HttpShadow | Self::Stream)
    }
}

impl std::str::FromStr for SinkMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "http" => Ok(Self::Http),
            "http+shadow" => Ok(Self::HttpShadow),
            "stream" => Ok(Self::Stream),
            other => Err(format!(
                "unknown INGEST_SINK {other:?} (http, http+shadow or stream)"
            )),
        }
    }
}

impl std::fmt::Display for SinkMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Http => "http",
            Self::HttpShadow => "http+shadow",
            Self::Stream => "stream",
        })
    }
}

/// Rows per part (spec §7.1: station samples in chunks of 100 stations);
/// [`split_snapshot`] halves it for a part that would exceed 512 KiB.
pub const ROWS_PER_PART: usize = 100;

/// The envelope's `producer` field: `<component>/<pod name>` (`POD_NAME`,
/// else `HOSTNAME`, which Kubernetes sets to the pod name).
pub fn producer_id(component: &str) -> String {
    let pod = std::env::var("POD_NAME")
        .ok()
        .filter(|pod| !pod.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty()))
        .unwrap_or_else(|| "unknown".to_owned());
    format!("{component}/{pod}")
}

/// One snapshot's encoded parts, possibly of several schemas, and the rows
/// each schema carries (for [`metrics::SINK_ROWS_TOTAL`]).
#[derive(Debug, Default)]
pub struct Snapshot {
    parts: Vec<EncodedEntry>,
    rows: Vec<(String, usize)>,
}

impl Snapshot {
    pub fn new() -> Self {
        Self::default()
    }

    /// Encodes `rows` as `schema` parts of this snapshot: batch
    /// `<produced_at>`, keys `<schema name>:<produced_at>:<i>/<n>`, at most
    /// `rows_per_part` rows a part. A part that cannot fit 512 KiB even at
    /// one row is [`EnvelopeError::TooLarge`], counted as
    /// `ingest_stream_produce_dropped_total{reason="oversize"}` for
    /// `stream`; the caller logs it and sends the snapshot without it.
    ///
    /// An empty `rows` adds nothing (the api routes ignore an empty batch
    /// too).
    ///
    /// # Errors
    ///
    /// [`EnvelopeError`] when a row does not serialize or does not fit.
    pub fn add<T: Serialize>(
        &mut self,
        stream: &str,
        schema: &SchemaId,
        producer: &str,
        produced_at: DateTime<Utc>,
        rows: &[T],
        rows_per_part: usize,
    ) -> Result<(), EnvelopeError> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = format_produced_at(produced_at);
        let parts = split_snapshot(
            schema,
            producer,
            produced_at,
            &batch,
            rows,
            rows_per_part,
            serde_json::value::to_raw_value,
        )
        .inspect_err(|err| {
            if matches!(err, EnvelopeError::TooLarge { .. }) {
                metrics::record_oversize(stream);
            }
        })?;
        self.parts.extend(parts);
        self.rows.push((schema.name().to_owned(), rows.len()));
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    pub fn parts(&self) -> &[EncodedEntry] {
        &self.parts
    }

    /// The rows per schema name.
    pub fn rows(&self) -> &[(String, usize)] {
        &self.rows
    }
}

/// Resolves when every part of a submitted [`Snapshot`] is written, or it
/// never will be (superseded by a newer snapshot, or the producer closed).
/// Dropping it changes nothing: the rows are counted either way.
#[derive(Debug)]
pub struct SnapshotReceipt(oneshot::Receiver<Result<Vec<String>, NotWritten>>);

impl SnapshotReceipt {
    /// The stream ids of the snapshot's parts once all are written.
    ///
    /// # Errors
    ///
    /// [`NotWritten`] when it never will be.
    pub async fn written(self) -> Result<Vec<String>, NotWritten> {
        self.0.await.unwrap_or(Err(NotWritten::Closed))
    }
}

/// Submits `snapshot` to `producer` as one item (never waits under
/// [`crate::ProducePolicy::LatestSnapshot`]), and counts its rows in
/// `ingest_stream_sink_rows_total{sink="stream"}` once it is written. An
/// empty snapshot resolves at once and counts nothing.
///
/// # Errors
///
/// [`NotWritten::Closed`] after [`Producer::close`].
pub async fn submit(
    producer: &Producer,
    snapshot: Snapshot,
) -> Result<SnapshotReceipt, NotWritten> {
    let Snapshot { parts, rows } = snapshot;
    let receipt = producer.submit(parts).await?;
    let (done, rx) = oneshot::channel();
    let stream = producer.stream().to_owned();
    tokio::spawn(async move {
        let result = receipt.written().await;
        if result.is_ok() {
            for (schema, count) in &rows {
                metrics::sink_rows(&stream, schema, "stream", *count);
            }
        }
        let _ = done.send(result);
    });
    Ok(SnapshotReceipt(rx))
}

/// One snapshot stream's producer, for a poller or consumer that XADDs
/// (`INGEST_SINK=http+shadow` or `stream`): a
/// [`crate::ProducePolicy::LatestSnapshot`] [`Producer`] with the stream's
/// `MAXLEN` from [`crate::budget`], spawned on first use, so after the
/// caller's metrics recorder is installed (its series registered at 0 are
/// then exported) and inside its runtime. The task is detached: an unsent
/// snapshot at exit is lost, as a POST in flight is, and the next process
/// sends a fresh one.
pub struct SnapshotProducer {
    client: redis::Client,
    stream: &'static str,
    schemas: Vec<SchemaId>,
    producer_id: String,
    producer: std::sync::OnceLock<Producer>,
}

impl SnapshotProducer {
    /// For `stream` (one of [`crate::streams`]) carrying `schemas`, as
    /// `component` (the envelope's `producer` is `<component>/<pod>`),
    /// over `client` (the component's own ACL user, D6).
    pub fn new(
        client: redis::Client,
        stream: &'static str,
        component: &str,
        schemas: Vec<SchemaId>,
    ) -> Self {
        Self {
            client,
            stream,
            schemas,
            producer_id: producer_id(component),
            producer: std::sync::OnceLock::new(),
        }
    }

    pub fn stream(&self) -> &'static str {
        self.stream
    }

    /// The envelope's `producer` field.
    pub fn producer_id(&self) -> &str {
        &self.producer_id
    }

    /// The producer, spawned on the first call (needs a Tokio runtime).
    pub fn producer(&self) -> &Producer {
        self.producer.get_or_init(|| {
            // Every stream is declared (`budget`'s own tests); the fallback
            // is the station-samples cap, the largest.
            let maxlen = crate::budget::decl(self.stream).map_or(720, |decl| decl.maxlen());
            let (producer, _task) = Producer::spawn(
                self.client.clone(),
                crate::ProducerConfig::new(
                    self.stream,
                    maxlen,
                    crate::ProducePolicy::LatestSnapshot,
                ),
            );
            register_sink(self.stream, &self.schemas.iter().collect::<Vec<_>>());
            producer
        })
    }

    /// Whether the last XADD succeeded (readiness `stream_unavailable`
    /// when not; spec §7.5). True before the first one.
    pub fn is_available(&self) -> bool {
        self.producer.get().is_none_or(Producer::is_available)
    }

    /// [`submit`] to this stream.
    ///
    /// # Errors
    ///
    /// [`NotWritten::Closed`] after the producer was closed.
    pub async fn submit(&self, snapshot: Snapshot) -> Result<SnapshotReceipt, NotWritten> {
        submit(self.producer(), snapshot).await
    }

    /// [`cursor`] of this stream.
    ///
    /// # Errors
    ///
    /// The Redis error when it cannot connect or read.
    pub async fn cursor(&self) -> redis::RedisResult<Option<DateTime<Utc>>> {
        cursor(&self.client, self.stream).await
    }
}

/// Registers `ingest_stream_sink_rows_total` at 0 for each of `schemas`
/// and both sinks, so the compare step's `increase()` sees the first rows.
pub fn register_sink(stream: &str, schemas: &[&SchemaId]) {
    for schema in schemas {
        for sink in metrics::SINKS {
            metrics::sink_rows(stream, schema.name(), sink, 0);
        }
    }
}

/// The stream producer's "last fetched" cursor (spec §11.3): the newest
/// entry's `produced_at` on `stream`, over one bounded connection. `None`
/// for an empty or missing stream ("poll now").
///
/// # Errors
///
/// The Redis error when it cannot connect or read.
pub async fn cursor(
    client: &redis::Client,
    stream: &str,
) -> redis::RedisResult<Option<DateTime<Utc>>> {
    let mut conn = common::redis_conn::connect(client).await?;
    last_produced_at(&mut conn, stream).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{Envelope, field};

    fn decode(entry: &EncodedEntry) -> Envelope {
        Envelope::decode(&entry.fields).unwrap()
    }

    #[test]
    fn a_snapshot_is_split_into_parts_of_today_body_shape_with_one_batch() {
        let produced_at = "2026-10-08T12:00:00.123Z".parse().unwrap();
        let stats = SchemaId::new("full-coverage-stats", 1).unwrap();
        let samples = SchemaId::new("station-full-coverage-samples", 1).unwrap();
        let mut snapshot = Snapshot::new();
        let rows: Vec<u32> = (0..250).collect();
        snapshot
            .add("s", &stats, "p/pod", produced_at, &rows, ROWS_PER_PART)
            .unwrap();
        snapshot
            .add("s", &samples, "p/pod", produced_at, &[7_u32], ROWS_PER_PART)
            .unwrap();
        // Nothing for an empty schema.
        snapshot
            .add::<u32>("s", &samples, "p/pod", produced_at, &[], ROWS_PER_PART)
            .unwrap();

        assert_eq!(snapshot.parts().len(), 4);
        assert_eq!(
            snapshot.rows(),
            [
                ("full-coverage-stats".to_owned(), 250),
                ("station-full-coverage-samples".to_owned(), 1)
            ]
        );
        let first = decode(&snapshot.parts()[0]);
        assert_eq!(
            first.key,
            "full-coverage-stats:2026-10-08T12:00:00.123Z:1/3"
        );
        assert_eq!(first.produced_at, produced_at);
        let body: Vec<u32> = first.payload_as().unwrap();
        assert_eq!(body, (0..100).collect::<Vec<_>>());
        let last = decode(&snapshot.parts()[3]);
        assert_eq!(
            last.key,
            "station-full-coverage-samples:2026-10-08T12:00:00.123Z:1/1"
        );
        assert_eq!(last.payload.get(), "[7]");
        // Encoding is deterministic: a retry re-sends the same bytes.
        let mut again = Snapshot::new();
        again
            .add("s", &stats, "p/pod", produced_at, &rows, ROWS_PER_PART)
            .unwrap();
        assert_eq!(again.parts()[0].fields, snapshot.parts()[0].fields);
        assert!(
            snapshot.parts()[0]
                .fields
                .iter()
                .any(|(name, value)| *name == field::PRODUCED_AT
                    && value.as_slice() == b"2026-10-08T12:00:00.123Z")
        );
    }

    #[test]
    fn sink_modes_round_trip_and_default_to_http() {
        for (text, mode) in [
            ("http", SinkMode::Http),
            ("http+shadow", SinkMode::HttpShadow),
            ("stream", SinkMode::Stream),
        ] {
            assert_eq!(text.parse::<SinkMode>().unwrap(), mode);
            assert_eq!(mode.to_string(), text);
        }
        assert_eq!(SinkMode::default(), SinkMode::Http);
        assert!("db".parse::<SinkMode>().is_err());
        assert!(SinkMode::HttpShadow.posts_http() && SinkMode::HttpShadow.produces());
        assert!(!SinkMode::Stream.posts_http() && !SinkMode::Http.produces());
    }

    #[test]
    fn the_producer_id_names_the_component() {
        assert!(producer_id("poller-ldbws").starts_with("poller-ldbws/"));
    }
}
