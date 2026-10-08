//! The producer side of every snapshot sink (`INGEST_SINK=http+shadow` or
//! `stream`; spec §9.1, §11.3, §13.1; plans 3a.7, 3a.8 and 3c.2): the
//! [`SinkMode`], a producer's Redis settings ([`RedisArgs`]), encoding a
//! snapshot's rows into parts ([`Snapshot`]), submitting them to a
//! latest-snapshot [`Producer`] as ONE item ([`SnapshotProducer`]), counting
//! the rows once they are written, and reading the stream cursor.
//! [`SnapshotStream`] is the one-schema form the `TfL`, tocs and
//! island-of-Ireland pollers use; poller-ldbws and full-coverage-consumer
//! drive [`SnapshotProducer`] directly.
//!
//! | `INGEST_SINK` | The api's `POST` | The stream | Startup cursor |
//! |---|---|---|---|
//! | `http` (default) | authoritative | – | the api's `GET` |
//! | `http+shadow` | authoritative | a copy of every snapshot, best effort (the writer's `shadow` mode checks it) | the api's `GET` |
//! | `stream` | – | authoritative | the stream's newest `produced_at` |
//!
//! The island-of-Ireland pollers (decision D8) have only `stream`.
//!
//! **One item per snapshot.** Under [`crate::ProducePolicy::LatestSnapshot`]
//! a newer item replaces an unsent older one, so everything one snapshot
//! carries goes in one item: every part of every schema (full-coverage's
//! three outputs share `ds:ingest:full-coverage` and one item per stats
//! write). Two items would supersede each other. While Redis is
//! unavailable the newest unsent snapshot is kept and retried with backoff
//! (`ingest_stream_produce_dropped_total{reason="superseded"}` counts the
//! replaced ones).
//!
//! **`produced_at` is fixed at encode time.** The parts are encoded once,
//! with the snapshot's fetch time, and the producer re-sends the same bytes
//! on every retry (same keys, same `produced_at`, spec §7.8): nothing here
//! re-stamps at XADD.
//!
//! **The payload is today's HTTP body.** A part's body is the JSON array of
//! its rows, so `/1` is byte for byte the body of the api route (spec
//! §7.2), just split into parts of [`ROWS_PER_PART`].

use std::fmt;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::secret::Secret;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::envelope::{
    EncodedEntry, EnvelopeError, SchemaId, field, format_produced_at, parse_produced_at,
    split_snapshot,
};
use crate::producer::{NotWritten, Producer, last_produced_at};
use crate::{budget, metrics};

/// A stream producer's `INGEST_SINK` (spec §13.1): where its snapshots go.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SinkMode {
    /// POST to the api only (today; the default).
    #[default]
    Http,
    /// POST to the api (authoritative: its result is the cycle's), plus a
    /// copy `XADD`ed to the stream for the writer's `shadow` mode to check.
    HttpShadow,
    /// XADD only; the api is no longer written.
    Stream,
}

impl SinkMode {
    /// Whether the api gets a `POST`.
    pub fn posts_http(self) -> bool {
        matches!(self, Self::Http | Self::HttpShadow)
    }

    /// Whether snapshots are `XADD`ed (so Redis is needed).
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

impl fmt::Display for SinkMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        self.add_parts(stream, schema, producer, produced_at, rows, rows_per_part)
    }

    /// [`Snapshot::add`] without its empty-rows shortcut: an empty `rows`
    /// is one part with the body `[]` (a whole, empty snapshot, which a
    /// writer handler may act on), as [`SnapshotStream::publish`] sends.
    fn add_parts<T: Serialize>(
        &mut self,
        stream: &str,
        schema: &SchemaId,
        producer: &str,
        produced_at: DateTime<Utc>,
        rows: &[T],
        rows_per_part: usize,
    ) -> Result<(), EnvelopeError> {
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
/// `MAXLEN` from [`crate::budget`], spawned on first use (or by
/// [`SnapshotProducer::start`]), so after the caller's metrics recorder is
/// installed (its series registered at 0 are then exported) and inside its
/// runtime. Without a [`SnapshotProducer::shutdown`] an unsent snapshot at
/// exit is lost, as a POST in flight is, and the next process sends a
/// fresh one.
pub struct SnapshotProducer {
    client: redis::Client,
    stream: String,
    schemas: Vec<SchemaId>,
    producer_id: String,
    producer: OnceLock<Producer>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl SnapshotProducer {
    /// For `stream` (one of [`crate::streams`], or a test's prefixed key)
    /// carrying `schemas`, as `component` (the envelope's `producer` is
    /// `<component>/<pod>`), over `client` (the component's own ACL user,
    /// D6).
    pub fn new(
        client: redis::Client,
        stream: impl Into<String>,
        component: &str,
        schemas: Vec<SchemaId>,
    ) -> Self {
        Self {
            client,
            stream: stream.into(),
            schemas,
            producer_id: producer_id(component),
            producer: OnceLock::new(),
            task: Mutex::new(None),
        }
    }

    pub fn stream(&self) -> &str {
        &self.stream
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
            let maxlen = stream_decl(&self.stream).map_or(720, budget::StreamDecl::maxlen);
            let (producer, task) = Producer::spawn(
                self.client.clone(),
                crate::ProducerConfig::new(
                    &self.stream,
                    maxlen,
                    crate::ProducePolicy::LatestSnapshot,
                ),
            );
            if let Ok(mut slot) = self.task.lock() {
                *slot = Some(task);
            }
            register_sink(&self.stream, &self.schemas.iter().collect::<Vec<_>>());
            producer
        })
    }

    /// Spawns the producer now rather than on the first snapshot.
    pub fn start(&self) {
        self.producer();
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

    /// [`cursor`] of this stream: its newest entry's `produced_at`.
    ///
    /// # Errors
    ///
    /// The Redis error when it cannot connect or read.
    pub async fn cursor(&self) -> redis::RedisResult<Option<DateTime<Utc>>> {
        cursor(&self.client, &self.stream).await
    }

    /// Closes the producer and waits up to `grace` for what is queued.
    /// True when nothing was left (or it never started).
    pub async fn shutdown(&self, grace: Duration) -> bool {
        let task = self.task.lock().ok().and_then(|mut slot| slot.take());
        match (self.producer.get(), task) {
            (Some(producer), Some(task)) => producer.shutdown(task, grace).await,
            _ => true,
        }
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

/// The declaration of `stream`, matched by suffix so a test's key prefix
/// (`<prefix>ds:ingest:tfl`) finds its stream's.
fn stream_decl(stream: &str) -> Option<&'static budget::StreamDecl> {
    budget::INGEST_STREAMS
        .iter()
        .find(|decl| stream.ends_with(decl.stream))
}

/// A stream producer's Redis connection settings (`REDIS_URL`,
/// `REDIS_USERNAME`, `REDIS_PASSWORD`; chart `redis.acl.clients.<poller>`
/// for the producer's own ACL user, decision D6).
#[derive(Clone, clap::Args)]
pub struct RedisArgs {
    /// Redis, for the ingest stream. Needed by `INGEST_SINK=http+shadow`
    /// and `stream`. May carry a password (`redis://:pw@host`).
    #[arg(long, env, hide_env_values = true)]
    pub redis_url: Option<Secret>,

    /// Redis AUTH password (chart `redis.auth`, or the producer's ACL
    /// user's own). Applied by `common::redis_auth::redis_url_with_credentials`,
    /// never logged.
    #[arg(long, env, hide_env_values = true)]
    pub redis_password: Option<Secret>,

    /// Redis ACL user (`poller-tfl`, `poller-tocs`, …). Unset or empty: the
    /// `default` user.
    #[arg(long, env)]
    pub redis_username: Option<String>,
}

impl fmt::Debug for RedisArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisArgs")
            .field("redis_url", &self.redis_url)
            .field("redis_password", &self.redis_password)
            .field("redis_username", &self.redis_username)
            .finish()
    }
}

impl RedisArgs {
    /// The client for `REDIS_URL` with the ACL user's credentials applied.
    /// `Err` (a message) when `REDIS_URL` is unset or the credentials do
    /// not combine; `why` names what needs it (`"INGEST_SINK=stream"`).
    ///
    /// # Errors
    ///
    /// As above.
    pub fn client(&self, why: &str) -> Result<redis::Client, String> {
        let Some(url) = &self.redis_url else {
            return Err(format!("{why} needs REDIS_URL"));
        };
        let url = common::redis_auth::redis_url_with_credentials(
            url.expose(),
            self.redis_username.as_deref(),
            self.redis_password.as_ref(),
        )
        .map_err(|err| format!("{err:#}"))?;
        redis::Client::open(url.expose()).map_err(|err| format!("REDIS_URL: {err}"))
    }
}

/// Why a snapshot was not queued.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// It could not be encoded (a single row over the part limit, counted
    /// as `reason="oversize"`, or a row that does not serialize).
    #[error("encoding the snapshot: {0}")]
    Encode(#[from] EnvelopeError),
    /// The producer was shut down.
    #[error(transparent)]
    Closed(#[from] NotWritten),
}

/// One poller's latest-snapshot producer on one stream and ONE schema
/// (plan 3c.2: `TfL`, tocs and the island-of-Ireland pollers): a
/// [`SnapshotProducer`] that [`SnapshotStream::publish`]es a whole snapshot
/// without waiting for Redis, and whose cursor is the newest entry of its
/// own schema.
pub struct SnapshotStream {
    inner: SnapshotProducer,
    schema: SchemaId,
    max_rows_per_part: usize,
}

impl SnapshotStream {
    /// Starts the producer task for `stream` (its `MAXLEN` from
    /// [`budget::INGEST_STREAMS`]), writing `schema` entries as
    /// `component/<pod>`, at most `max_rows_per_part` rows per part. A
    /// `TfL` or tocs snapshot fits one part; a part is also halved until it
    /// fits 512 KiB ([`split_snapshot`]).
    pub fn spawn(
        client: redis::Client,
        stream: &str,
        schema: SchemaId,
        component: &str,
        max_rows_per_part: usize,
    ) -> Self {
        let inner = SnapshotProducer::new(client, stream, component, vec![schema.clone()]);
        inner.start();
        Self {
            inner,
            schema,
            max_rows_per_part: max_rows_per_part.max(1),
        }
    }

    pub fn stream(&self) -> &str {
        self.inner.stream()
    }

    /// Queues `rows`, fetched at `produced_at`, as one snapshot: its parts
    /// share the batch `produced_at` and keep their keys across retries;
    /// an empty `rows` is one empty part (a whole, empty snapshot). Never
    /// waits for Redis. The rows are counted in
    /// `ingest_stream_sink_rows_total{sink="stream"}` once written, and the
    /// write's outcome is logged when known (a newer snapshot superseding
    /// this one is normal while Redis is down).
    ///
    /// # Errors
    ///
    /// [`PublishError`]; nothing is queued then.
    pub async fn publish<T: Serialize>(
        &self,
        rows: &[T],
        produced_at: DateTime<Utc>,
    ) -> Result<(), PublishError> {
        let mut snapshot = Snapshot::new();
        snapshot.add_parts(
            self.stream(),
            &self.schema,
            self.inner.producer_id(),
            produced_at,
            rows,
            self.max_rows_per_part,
        )?;
        let count = snapshot.parts().len();
        let receipt = self.inner.submit(snapshot).await?;
        let stream = self.stream().to_owned();
        let schema = self.schema.to_string();
        let batch = format_produced_at(produced_at);
        tokio::spawn(async move {
            match receipt.written().await {
                Ok(ids) => {
                    tracing::debug!(%stream, %schema, %batch, parts = count, ?ids, "snapshot written to the ingest stream");
                }
                Err(NotWritten::Superseded) => {
                    tracing::info!(%stream, %schema, %batch, "snapshot superseded by a newer one before Redis took it");
                }
                Err(NotWritten::Closed) => {
                    tracing::warn!(%stream, %schema, %batch, "producer closed before the snapshot was written");
                }
            }
        });
        Ok(())
    }

    /// Whether the last XADD succeeded (readiness `stream_unavailable` when
    /// false).
    pub fn is_available(&self) -> bool {
        self.inner.is_available()
    }

    /// The `produced_at` of the newest entry of this producer's schema: the
    /// poller's "last fetched" cursor under `INGEST_SINK=stream` (spec
    /// §11.3). The stream is read backwards (`XREVRANGE`) a page at a time
    /// until an entry of this schema turns up, bounded by the stream's
    /// `MAXLEN`. On a one-schema stream (`tfl`, `reference`) that is the
    /// newest entry; the island-of-Ireland stream is shared by three
    /// pollers and five schemas, and a daily poller must not take the
    /// five-minute live poller's entries for its own.
    ///
    /// # Errors
    ///
    /// A Redis error (unreachable, NOPERM).
    pub async fn last_produced_at(&self) -> redis::RedisResult<Option<DateTime<Utc>>> {
        const PAGE: usize = 50;
        let mut conn = common::redis_conn::connect(&self.inner.client).await?;
        let stream = self.stream();
        let wanted = self.schema.to_string();
        let limit = stream_decl(stream)
            .map_or(2000, budget::StreamDecl::maxlen)
            .saturating_add(100);
        let mut end = "+".to_owned();
        let mut seen: u64 = 0;
        loop {
            let reply: redis::streams::StreamRangeReply = redis::cmd("XREVRANGE")
                .arg(stream)
                .arg(&end)
                .arg("-")
                .arg("COUNT")
                .arg(PAGE)
                .query_async(&mut conn)
                .await?;
            for entry in &reply.ids {
                if entry.get::<String>(field::SCHEMA).as_deref() == Some(wanted.as_str()) {
                    return Ok(entry
                        .get::<String>(field::PRODUCED_AT)
                        .and_then(|text| parse_produced_at(&text)));
                }
            }
            seen = seen.saturating_add(u64::try_from(reply.ids.len()).unwrap_or(u64::MAX));
            match reply.ids.last() {
                // Exclusive start (Redis 6.2+): the page before this one.
                Some(last) if reply.ids.len() == PAGE && seen < limit => {
                    end = format!("({}", last.id);
                }
                _ => return Ok(None),
            }
        }
    }

    /// Closes the producer and waits up to `grace` for what is queued.
    pub async fn shutdown(self, grace: Duration) -> bool {
        self.inner.shutdown(grace).await
    }
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

    #[test]
    fn redis_args_need_a_url_and_apply_the_acl_user() {
        let none = RedisArgs {
            redis_url: None,
            redis_password: None,
            redis_username: None,
        };
        let err = none.client("INGEST_SINK=stream").unwrap_err();
        assert!(err.contains("INGEST_SINK=stream needs REDIS_URL"), "{err}");
        let user = RedisArgs {
            redis_url: Some("redis://redis:6379".into()),
            redis_password: Some("pw".into()),
            redis_username: Some("poller-tfl".into()),
        };
        let client = user.client("x").unwrap();
        let info = client.get_connection_info();
        assert_eq!(info.redis.username.as_deref(), Some("poller-tfl"));
        assert!(
            !format!("{user:?}").contains("pw\""),
            "the password is redacted"
        );
    }

    #[test]
    fn an_empty_stream_snapshot_is_one_empty_part() {
        let schema = SchemaId::new("tocs", 1).unwrap();
        let produced_at = "2026-10-08T12:00:00Z".parse().unwrap();
        let mut snapshot = Snapshot::new();
        snapshot
            .add_parts::<u32>("s", &schema, "p/pod", produced_at, &[], ROWS_PER_PART)
            .unwrap();
        assert_eq!(snapshot.parts().len(), 1);
        assert_eq!(decode(&snapshot.parts()[0]).payload.get(), "[]");
    }
}
