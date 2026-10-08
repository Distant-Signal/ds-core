//! The writer's stream consumer runtime (spec §7.3).
//!
//! One [`StreamConsumer`] per stream task. It:
//!
//! - creates the group if missing (`XGROUP CREATE <stream> <group> 0
//!   MKSTREAM`, `BUSYGROUP` ignored, so entries produced before the writer's
//!   first start are kept), and again after a `NOGROUP`;
//! - reads its **own pending entries first** (`XREADGROUP … 0`): at startup
//!   (a crash left them pending), after `XAUTOCLAIM` moved a dead consumer's
//!   entries to it, and after any retry. Only when its PEL is empty does it
//!   read new entries (`>`, `COUNT batch BLOCK 5000`). That keeps id order
//!   and keeps lag honest;
//! - every `claim_interval` runs `XAUTOCLAIM … <claim_min_idle> 0-0 JUSTID`
//!   for entries a dead pod left pending, and removes consumers that have
//!   nothing pending and have been idle for `delete_idle_consumers_after`;
//! - applies entries **one at a time in id order** through a [`Handler`]:
//!   - `Ok(..)`: ACK (after the handler's own transaction committed);
//!   - [`HandlerError::Poison`] (and an envelope that does not decode): the
//!     entry goes to the dead-letter stream with the reason, then is acked;
//!   - [`HandlerError::Transient`] / [`HandlerError::UnsupportedSchema`]
//!     (and an unknown envelope version): not acked; the batch stops, the
//!     task backs off (1 s doubling to 60 s, jittered) and re-reads its PEL.
//!     An outage never empties a stream into its dead-letter stream;
//!   - except that an unsupported entry older than
//!     [`ConsumerConfig::unsupported_deadline`] (by its id's time, or by
//!     how long this consumer has been retrying it, whichever is longer) is
//!     dead-lettered with reason [`UNSUPPORTED_EXPIRED`] and acked, so one
//!     entry from a producer newer than the writer cannot block its stream
//!     forever (security review L6);
//! - exports the [`crate::metrics`] consumer series, sampling lag, pending,
//!   oldest pending age, dead-letter length and oldest age, and memory every
//!   `gauge_interval`;
//! - stops between entries on shutdown: a running handler is never
//!   cancelled.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use chrono::Utc;
use common::backoff::{Backoff, FailureStreak};
use common::progress::Progress;
use common::redis_conn::RedisConn;
use redis::{ErrorKind, RedisError, Value};
use serde_json::value::RawValue;

use crate::envelope::{DecodeError, Envelope, field};
use crate::metrics;

/// `XREADGROUP … BLOCK` for new entries, milliseconds.
pub const READ_BLOCK_MS: u64 = 5000;

// A blocked read must finish well inside the shared per-command timeout.
const _: () =
    assert!((READ_BLOCK_MS as u128) * 2 < common::redis_conn::RESPONSE_TIMEOUT.as_millis());

/// The consumer's retry schedule after a transient failure (spec §7.3).
pub const CONSUMER_BACKOFF: Backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));

#[derive(Clone, Debug)]
pub struct ConsumerConfig {
    pub stream: String,
    pub group: String,
    /// This consumer's name in the group: the pod name.
    pub consumer: String,
    pub dead_letter_stream: String,
    /// `XADD <dead-letter stream> MAXLEN ~ <n>`; see
    /// [`crate::budget::StreamDecl::dead_letter_maxlen`].
    pub dead_letter_maxlen: u64,
    /// `COUNT` per read (16).
    pub batch_size: usize,
    /// `BLOCK` for new entries; at most [`READ_BLOCK_MS`].
    pub block: Duration,
    /// `XAUTOCLAIM` only entries idle this long (5 min).
    pub claim_min_idle: Duration,
    /// How often to run `XAUTOCLAIM` and consumer hygiene (60 s).
    pub claim_interval: Duration,
    /// `XGROUP DELCONSUMER` consumers with nothing pending idle this long
    /// (1 h).
    pub delete_idle_consumers_after: Duration,
    /// How often to sample the gauges (30 s).
    pub gauge_interval: Duration,
    pub backoff: Backoff,
    /// An entry left pending as unsupported (a newer schema version or
    /// envelope) for this long is dead-lettered ([`UNSUPPORTED_EXPIRED`])
    /// instead of blocking its stream for good. `None`: never (1 h by
    /// default, [`DEFAULT_UNSUPPORTED_DEADLINE`]).
    pub unsupported_deadline: Option<Duration>,
}

/// [`ConsumerConfig::unsupported_deadline`]'s default: long enough to roll
/// the writer forward after a producer, short enough that a stream is not
/// held for a day.
pub const DEFAULT_UNSUPPORTED_DEADLINE: Duration = Duration::from_secs(3600);

/// The dead-letter `reason` of an entry dropped by
/// [`ConsumerConfig::unsupported_deadline`].
pub const UNSUPPORTED_EXPIRED: &str = "unsupported_expired";

impl ConsumerConfig {
    /// The spec's defaults for `stream` read by `consumer` in group
    /// [`crate::WRITER_GROUP`], dead-lettering to
    /// [`crate::dead_letter_stream`]`(stream)`.
    pub fn new(
        stream: impl Into<String>,
        consumer: impl Into<String>,
        dead_letter_maxlen: u64,
    ) -> Self {
        let stream = stream.into();
        Self {
            dead_letter_stream: crate::dead_letter_stream(&stream),
            stream,
            group: crate::WRITER_GROUP.to_owned(),
            consumer: consumer.into(),
            dead_letter_maxlen,
            batch_size: 16,
            block: Duration::from_millis(READ_BLOCK_MS),
            claim_min_idle: Duration::from_secs(300),
            claim_interval: Duration::from_secs(60),
            delete_idle_consumers_after: Duration::from_secs(3600),
            gauge_interval: Duration::from_secs(30),
            backoff: CONSUMER_BACKOFF,
            unsupported_deadline: Some(DEFAULT_UNSUPPORTED_DEADLINE),
        }
    }
}

/// One decoded entry handed to a [`Handler`].
#[derive(Clone, Debug)]
pub struct StreamEntry {
    pub stream: String,
    pub id: String,
    pub envelope: Envelope,
}

/// What a handler did with an entry it accepted. Every variant is acked.
#[derive(Clone, Debug)]
pub enum Handled {
    Applied,
    /// The idempotency key was already applied (`ingest_dedup`).
    Duplicate,
    /// Decoded and validated, nothing written (shadow mode).
    Skipped,
    /// Applied, except some rows the database refused for a data error;
    /// those rows (a JSON payload in the entry's schema) go to the
    /// dead-letter stream with `reason`.
    PartiallyRejected {
        reason: String,
        rejected: Box<RawValue>,
    },
}

/// Why a handler did not accept an entry.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HandlerError {
    /// Retry later; not acked (connection, pool timeout, serialization,
    /// deadlock, lock or statement timeout).
    #[error("transient: {0}")]
    Transient(String),
    /// Never going to work: dead-lettered with this reason, then acked (an
    /// unknown schema *name*, an undecodable payload, a data error for the
    /// whole entry).
    #[error("poison: {0}")]
    Poison(String),
    /// A schema *version* this writer does not know: the producer is newer.
    /// Left pending (like [`HandlerError::Transient`]) and counted as
    /// `unsupported_schema`, which alerts; rolling the writer forward fixes
    /// it.
    #[error("unsupported schema: {0}")]
    UnsupportedSchema(String),
}

/// Applies one entry. Called one entry at a time, in id order; never
/// cancelled mid-call by shutdown.
pub trait Handler: Send + Sync {
    fn handle(
        &self,
        entry: &StreamEntry,
    ) -> impl Future<Output = Result<Handled, HandlerError>> + Send;
}

/// What one [`StreamConsumer::step`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// The blocking read returned nothing.
    Idle,
    /// The PEL is empty; the next step reads new entries.
    PelDrained,
    /// Entries were handled and acked (dead-lettered ones included).
    Processed(usize),
    /// An entry must be retried: back off, then re-read the PEL. Carries
    /// the number handled before it.
    Retry {
        processed: usize,
        reason: RetryReason,
    },
    /// Shutdown fired while waiting for entries.
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryReason {
    Transient,
    Unsupported,
}

/// A point-in-time sample of the stream's gauges.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamStats {
    pub lag: Option<u64>,
    pub pending: u64,
    pub oldest_pending_age: Option<Duration>,
    pub dead_letter_length: u64,
    /// Age of the oldest dead-letter entry, from its id.
    pub dead_letter_oldest_age: Option<Duration>,
    pub bytes: Option<u64>,
}

/// The stream consumer. See the module docs.
pub struct StreamConsumer {
    conn: RedisConn,
    config: ConsumerConfig,
    progress: Option<Progress>,
    pel_first: bool,
    group_ready: bool,
    next_claim: Instant,
    next_gauges: Instant,
    streak: FailureStreak,
    /// The unsupported entry this consumer is stuck on and when it first
    /// saw it (the batch stops there, so there is at most one).
    unsupported_since: Option<(String, Instant)>,
}

impl StreamConsumer {
    pub fn new(conn: RedisConn, config: ConsumerConfig) -> Self {
        metrics::register_consumer(&config.stream);
        let now = Instant::now();
        let streak = FailureStreak::new(config.backoff);
        Self {
            conn,
            config,
            progress: None,
            // A previous run of this consumer may have left entries pending.
            pel_first: true,
            group_ready: false,
            next_claim: now,
            next_gauges: now,
            streak,
            unsupported_since: None,
        }
    }

    /// Beats `progress` after every completed step (a retry included: a
    /// writer waiting out a database outage is alive).
    #[must_use]
    pub fn with_progress(mut self, progress: Progress) -> Self {
        self.progress = Some(progress);
        self
    }

    pub fn config(&self) -> &ConsumerConfig {
        &self.config
    }

    /// `XGROUP CREATE <stream> <group> 0 MKSTREAM`, `BUSYGROUP` ignored.
    pub async fn ensure_group(&mut self) -> Result<(), RedisError> {
        let created: Result<(), RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.config.stream)
            .arg(&self.config.group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut self.conn)
            .await;
        match created {
            Ok(()) => {}
            Err(err) if err.code() == Some("BUSYGROUP") => {}
            Err(err) => return Err(err),
        }
        self.group_ready = true;
        Ok(())
    }

    /// Runs until `shutdown` resolves. Redis errors are logged and retried
    /// with backoff; nothing here returns early except shutdown. Shutdown is
    /// honoured while waiting (the blocking read, a backoff), never in the
    /// middle of a handler.
    pub async fn run<H: Handler, F: Future<Output = ()>>(&mut self, handler: &H, shutdown: F) {
        let mut shutdown = std::pin::pin!(shutdown);
        loop {
            if fired(shutdown.as_mut()).await {
                return;
            }
            let delay = match self.step(handler, shutdown.as_mut()).await {
                Ok(Step::Shutdown) => return,
                Ok(Step::Retry { .. }) => Some(self.streak.failed(None)),
                Ok(_) => None,
                Err(err) => {
                    let delay = self.streak.failed(None);
                    tracing::warn!(
                        stream = %self.config.stream,
                        error = %err,
                        failures = self.streak.failures(),
                        retry_in_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                        "ingest stream consumer: Redis error; retrying"
                    );
                    Some(delay)
                }
            };
            if let Some(delay) = delay {
                tokio::select! {
                    biased;
                    () = shutdown.as_mut() => return,
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    }

    /// One iteration: housekeeping when due, then one read and the entries
    /// it returned. `shutdown` interrupts only the blocking read.
    pub async fn step<H: Handler, F: Future<Output = ()>>(
        &mut self,
        handler: &H,
        shutdown: Pin<&mut F>,
    ) -> Result<Step, RedisError> {
        let result = self.step_inner(handler, shutdown).await;
        match &result {
            Ok(Step::Retry { .. }) | Err(_) => self.pel_first = true,
            Ok(Step::Processed(_) | Step::PelDrained) => self.streak.succeeded(),
            Ok(_) => {}
        }
        if let Err(err) = &result
            && err.code() == Some("NOGROUP")
        {
            self.group_ready = false;
        }
        if result.is_ok()
            && let Some(progress) = &self.progress
        {
            progress.beat();
        }
        result
    }

    async fn step_inner<H: Handler, F: Future<Output = ()>>(
        &mut self,
        handler: &H,
        shutdown: Pin<&mut F>,
    ) -> Result<Step, RedisError> {
        if !self.group_ready {
            self.ensure_group().await?;
        }
        let now = Instant::now();
        if now >= self.next_claim {
            self.next_claim = now + self.config.claim_interval;
            if self.reclaim().await? > 0 {
                self.pel_first = true;
            }
            if let Err(err) = self.delete_idle_consumers().await {
                tracing::warn!(stream = %self.config.stream, error = %err, "XGROUP DELCONSUMER sweep failed");
            }
        }
        if now >= self.next_gauges {
            self.next_gauges = now + self.config.gauge_interval;
            if let Err(err) = self.sample_gauges().await {
                tracing::warn!(stream = %self.config.stream, error = %err, "ingest stream gauges failed");
            }
        }

        let entries = if self.pel_first {
            let entries = self.read("0", None).await?;
            if entries.is_empty() {
                self.pel_first = false;
                return Ok(Step::PelDrained);
            }
            entries
        } else {
            let read = self.read(">", Some(self.config.block));
            tokio::select! {
                biased;
                () = shutdown => return Ok(Step::Shutdown),
                entries = read => entries?,
            }
        };
        if entries.is_empty() {
            return Ok(Step::Idle);
        }
        let mut processed = 0;
        for entry in entries {
            match self.process(handler, entry).await? {
                None => processed += 1,
                Some(reason) => return Ok(Step::Retry { processed, reason }),
            }
        }
        Ok(Step::Processed(processed))
    }

    /// Handles one raw entry. `Some(reason)` means stop and retry it.
    async fn process<H: Handler>(
        &mut self,
        handler: &H,
        raw: RawEntry,
    ) -> Result<Option<RetryReason>, RedisError> {
        let stream = self.config.stream.clone();
        let Some(fields) = raw.fields else {
            // Pending, but `MAXLEN` trimmed it away before it was applied.
            self.ack(&raw.id).await?;
            metrics::consumed(&stream, "unknown", "trimmed");
            tracing::warn!(%stream, id = %raw.id, "pending ingest entry was trimmed before it was applied");
            return Ok(None);
        };
        let envelope = match Envelope::decode(&fields) {
            Ok(envelope) => envelope,
            Err(err) => return self.undecodable(&raw.id, &fields, &err).await,
        };
        let schema = envelope.schema.to_string();
        let entry = StreamEntry {
            stream: stream.clone(),
            id: raw.id,
            envelope,
        };
        let started = Instant::now();
        let result = handler.handle(&entry).await;
        metrics::handler_seconds(&stream, &schema, started.elapsed().as_secs_f64());
        let outcome = match result {
            Ok(Handled::Applied) => "applied",
            Ok(Handled::Duplicate) => "duplicate",
            Ok(Handled::Skipped) => "skipped",
            Ok(Handled::PartiallyRejected { reason, rejected }) => {
                let fields = rejected_fields(&entry.envelope, rejected).unwrap_or(fields);
                self.dead_letter(&entry.id, fields, "rejected_rows", &reason)
                    .await?;
                "rejected"
            }
            Err(HandlerError::Poison(reason)) => {
                self.dead_letter(&entry.id, fields, "poison", &reason)
                    .await?;
                "dead_lettered"
            }
            Err(HandlerError::Transient(reason)) => {
                metrics::consumed(&stream, &schema, "transient_error");
                tracing::warn!(%stream, id = %entry.id, %schema, %reason, "ingest entry failed transiently; left pending");
                return Ok(Some(RetryReason::Transient));
            }
            Err(HandlerError::UnsupportedSchema(reason)) => {
                if !self.unsupported_expired(&entry.id) {
                    metrics::consumed(&stream, &schema, "unsupported_schema");
                    tracing::error!(%stream, id = %entry.id, %schema, %reason, "unsupported ingest schema version; left pending (roll the writer forward)");
                    return Ok(Some(RetryReason::Unsupported));
                }
                self.dead_letter(&entry.id, fields, UNSUPPORTED_EXPIRED, &reason)
                    .await?;
                "dead_lettered"
            }
        };
        self.ack(&entry.id).await?;
        metrics::consumed(&stream, &schema, outcome);
        if outcome != "dead_lettered" {
            #[expect(clippy::cast_precision_loss, reason = "Unix seconds fit f64 exactly")]
            let now = Utc::now().timestamp() as f64;
            metrics::gauge(metrics::LAST_APPLIED_TIMESTAMP_SECONDS, &stream, now);
        }
        Ok(None)
    }

    async fn undecodable(
        &mut self,
        id: &str,
        fields: &[(Vec<u8>, Vec<u8>)],
        err: &DecodeError,
    ) -> Result<Option<RetryReason>, RedisError> {
        let stream = self.config.stream.clone();
        let schema = fields
            .iter()
            .find(|(k, _)| k == field::SCHEMA.as_bytes())
            .and_then(|(_, v)| std::str::from_utf8(v).ok())
            .and_then(|s| s.parse::<crate::envelope::SchemaId>().ok())
            .map_or_else(|| "unknown".to_owned(), |s| s.to_string());
        let reason = if err.is_poison() {
            err.reason()
        } else if self.unsupported_expired(id) {
            UNSUPPORTED_EXPIRED
        } else {
            metrics::consumed(&stream, &schema, "unsupported_schema");
            tracing::error!(%stream, %id, error = %err, "unsupported ingest envelope; left pending (roll the writer forward)");
            return Ok(Some(RetryReason::Unsupported));
        };
        self.dead_letter(id, fields.to_vec(), reason, &err.to_string())
            .await?;
        self.ack(id).await?;
        metrics::consumed(&stream, &schema, "dead_lettered");
        Ok(None)
    }

    /// Whether the unsupported entry `id` has waited past
    /// [`ConsumerConfig::unsupported_deadline`]: by its id's time (when it
    /// was added; survives a restart) or by how long this consumer has been
    /// retrying it (a producer-chosen id in the future cannot extend it),
    /// whichever is longer. Remembers `id` as the entry it is stuck on.
    fn unsupported_expired(&mut self, id: &str) -> bool {
        let first_seen = match &self.unsupported_since {
            Some((stuck, since)) if stuck == id => *since,
            _ => {
                let now = Instant::now();
                self.unsupported_since = Some((id.to_owned(), now));
                now
            }
        };
        let Some(deadline) = self.config.unsupported_deadline else {
            return false;
        };
        let now_ms = u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0);
        let by_id = id_millis(id).map_or(Duration::ZERO, |ms| {
            Duration::from_millis(now_ms.saturating_sub(ms))
        });
        let expired = by_id.max(first_seen.elapsed()) >= deadline;
        if expired {
            self.unsupported_since = None;
        }
        expired
    }

    /// `XADD <dead-letter stream> MAXLEN ~ n *` the entry's fields plus
    /// `error`, `reason`, `failed_at`, `deliveries`, `source_stream` and
    /// `source_id`. The caller ACKs afterwards; a crash in between
    /// dead-letters it twice, which is harmless.
    async fn dead_letter(
        &mut self,
        id: &str,
        mut fields: Vec<(Vec<u8>, Vec<u8>)>,
        reason: &str,
        error: &str,
    ) -> Result<(), RedisError> {
        let deliveries = self.delivery_count(id).await.unwrap_or(None);
        fields.retain(|(k, _)| !DEAD_LETTER_FIELDS.contains(&k.as_slice()));
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&self.config.dead_letter_stream)
            .arg("MAXLEN")
            .arg("~")
            .arg(self.config.dead_letter_maxlen)
            .arg("*");
        for (k, v) in &fields {
            cmd.arg(k.as_slice()).arg(v.as_slice());
        }
        cmd.arg("error")
            .arg(error)
            .arg("reason")
            .arg(reason)
            .arg("failed_at")
            .arg(crate::envelope::format_produced_at(Utc::now()))
            .arg("deliveries")
            .arg(deliveries.map_or_else(String::new, |d| d.to_string()))
            .arg("source_stream")
            .arg(&self.config.stream)
            .arg("source_id")
            .arg(id);
        let _: String = cmd.query_async(&mut self.conn).await?;
        metrics::dead_lettered(&self.config.stream, reason);
        tracing::warn!(
            stream = %self.config.stream,
            dead_letter_stream = %self.config.dead_letter_stream,
            %id,
            reason,
            error,
            "ingest entry dead-lettered"
        );
        Ok(())
    }

    async fn delivery_count(&mut self, id: &str) -> Result<Option<u64>, RedisError> {
        let reply: Value = redis::cmd("XPENDING")
            .arg(&self.config.stream)
            .arg(&self.config.group)
            .arg(id)
            .arg(id)
            .arg(1)
            .query_async(&mut self.conn)
            .await?;
        // [[id, consumer, idle ms, deliveries]]
        Ok(match reply {
            Value::Array(rows) => rows.first().and_then(|row| match row {
                Value::Array(cols) => cols.get(3).and_then(as_u64),
                _ => None,
            }),
            _ => None,
        })
    }

    async fn ack(&mut self, id: &str) -> Result<(), RedisError> {
        let _: i64 = redis::cmd("XACK")
            .arg(&self.config.stream)
            .arg(&self.config.group)
            .arg(id)
            .query_async(&mut self.conn)
            .await?;
        Ok(())
    }

    async fn read(
        &mut self,
        id: &str,
        block: Option<Duration>,
    ) -> Result<Vec<RawEntry>, RedisError> {
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP")
            .arg(&self.config.group)
            .arg(&self.config.consumer)
            .arg("COUNT")
            .arg(self.config.batch_size);
        if let Some(block) = block {
            let ms = u64::try_from(block.as_millis())
                .unwrap_or(READ_BLOCK_MS)
                .min(READ_BLOCK_MS);
            cmd.arg("BLOCK").arg(ms);
        }
        cmd.arg("STREAMS").arg(&self.config.stream).arg(id);
        let reply: Value = cmd.query_async(&mut self.conn).await?;
        parse_read_reply(reply)
    }

    /// `XAUTOCLAIM <stream> <group> <me> <min idle> 0-0 COUNT 100 JUSTID`,
    /// following the cursor. Returns how many entries this consumer now owns
    /// that it did not before.
    pub async fn reclaim(&mut self) -> Result<usize, RedisError> {
        let min_idle = u64::try_from(self.config.claim_min_idle.as_millis()).unwrap_or(u64::MAX);
        let mut cursor = "0-0".to_owned();
        let mut claimed = 0;
        let mut trimmed = 0;
        // Bounded: each pass moves up to 100; a huge PEL is finished next
        // sweep.
        for _ in 0..100 {
            let reply: Value = redis::cmd("XAUTOCLAIM")
                .arg(&self.config.stream)
                .arg(&self.config.group)
                .arg(&self.config.consumer)
                .arg(min_idle)
                .arg(&cursor)
                .arg("COUNT")
                .arg(100)
                .arg("JUSTID")
                .query_async(&mut self.conn)
                .await?;
            let Value::Array(parts) = reply else {
                return Err(unexpected("XAUTOCLAIM"));
            };
            let next = parts
                .first()
                .and_then(as_string)
                .ok_or_else(|| unexpected("XAUTOCLAIM"))?;
            if let Some(Value::Array(ids)) = parts.get(1) {
                claimed += ids.len();
            }
            // Redis 7+ also drops pending entries that `MAXLEN` trimmed away
            // from the PEL, and lists them here (valkey does so whatever
            // their idle time).
            if let Some(Value::Array(deleted)) = parts.get(2) {
                trimmed += deleted.len();
            }
            if next == "0-0" {
                break;
            }
            cursor = next;
        }
        if claimed > 0 {
            tracing::info!(stream = %self.config.stream, claimed, "reclaimed idle pending ingest entries");
        }
        if trimmed > 0 {
            for _ in 0..trimmed {
                metrics::consumed(&self.config.stream, "unknown", "trimmed");
            }
            tracing::warn!(
                stream = %self.config.stream,
                trimmed,
                "pending ingest entries were trimmed before they were applied"
            );
        }
        Ok(claimed)
    }

    /// `XGROUP DELCONSUMER` every other consumer with nothing pending, idle
    /// longer than `delete_idle_consumers_after` (pod names change on every
    /// restart). Returns the names removed.
    pub async fn delete_idle_consumers(&mut self) -> Result<Vec<String>, RedisError> {
        let reply: Value = redis::cmd("XINFO")
            .arg("CONSUMERS")
            .arg(&self.config.stream)
            .arg(&self.config.group)
            .query_async(&mut self.conn)
            .await?;
        let limit =
            u64::try_from(self.config.delete_idle_consumers_after.as_millis()).unwrap_or(u64::MAX);
        let mut removed = Vec::new();
        for consumer in list(&reply) {
            let fields = map_fields(consumer);
            let name = fields.get("name").and_then(|v| as_string(v));
            let pending = fields.get("pending").and_then(|v| as_u64(v));
            // `inactive` (Redis 7.2+) is since the last successful read;
            // `idle` since the last attempt. Use the larger signal of the two.
            let idle = fields
                .get("inactive")
                .and_then(|v| as_i64(v))
                .filter(|&ms| ms >= 0)
                .and_then(|ms| u64::try_from(ms).ok())
                .or_else(|| fields.get("idle").and_then(|v| as_u64(v)));
            if let (Some(name), Some(0), Some(idle)) = (name, pending, idle)
                && name != self.config.consumer
                && idle > limit
            {
                let _: i64 = redis::cmd("XGROUP")
                    .arg("DELCONSUMER")
                    .arg(&self.config.stream)
                    .arg(&self.config.group)
                    .arg(&name)
                    .query_async(&mut self.conn)
                    .await?;
                removed.push(name);
            }
        }
        if !removed.is_empty() {
            tracing::info!(stream = %self.config.stream, ?removed, "removed idle ingest stream consumers");
        }
        Ok(removed)
    }

    /// Samples lag and pending (`XINFO GROUPS`), the oldest pending entry's
    /// age (`XPENDING` summary, from its id), the dead-letter length
    /// (`XLEN`) and memory (`MEMORY USAGE` of both streams), and sets the
    /// gauges.
    #[expect(clippy::cast_precision_loss, reason = "gauge values far below 2^53")]
    pub async fn sample_gauges(&mut self) -> Result<StreamStats, RedisError> {
        let stream = self.config.stream.clone();
        let mut stats = StreamStats::default();

        let groups: Value = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(&stream)
            .query_async(&mut self.conn)
            .await?;
        for group in list(&groups) {
            let fields = map_fields(group);
            if fields.get("name").and_then(|v| as_string(v)).as_deref()
                == Some(self.config.group.as_str())
            {
                stats.lag = fields.get("lag").and_then(|v| as_u64(v));
                stats.pending = fields.get("pending").and_then(|v| as_u64(v)).unwrap_or(0);
            }
        }

        let summary: Value = redis::cmd("XPENDING")
            .arg(&stream)
            .arg(&self.config.group)
            .query_async(&mut self.conn)
            .await?;
        let now = u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0);
        if let Value::Array(parts) = &summary
            && let Some(min_id) = parts.get(1).and_then(as_string)
            && let Some(ms) = id_millis(&min_id)
        {
            stats.oldest_pending_age = Some(Duration::from_millis(now.saturating_sub(ms)));
        }

        stats.dead_letter_length = redis::cmd("XLEN")
            .arg(&self.config.dead_letter_stream)
            .query_async(&mut self.conn)
            .await?;
        if stats.dead_letter_length > 0 {
            let oldest: Value = redis::cmd("XRANGE")
                .arg(&self.config.dead_letter_stream)
                .arg("-")
                .arg("+")
                .arg("COUNT")
                .arg(1)
                .query_async(&mut self.conn)
                .await?;
            stats.dead_letter_oldest_age = list(&oldest)
                .first()
                .and_then(|entry| list(entry).first().and_then(as_string))
                .and_then(|id| id_millis(&id))
                .map(|ms| Duration::from_millis(now.saturating_sub(ms)));
        }

        let mut bytes = None;
        for key in [&stream, &self.config.dead_letter_stream] {
            let usage: Option<u64> = redis::cmd("MEMORY")
                .arg("USAGE")
                .arg(key)
                .query_async(&mut self.conn)
                .await?;
            if let Some(usage) = usage {
                bytes = Some(bytes.unwrap_or(0) + usage);
            }
        }
        stats.bytes = bytes;

        if let Some(lag) = stats.lag {
            metrics::gauge(metrics::LAG, &stream, lag as f64);
        }
        metrics::gauge(metrics::PENDING, &stream, stats.pending as f64);
        metrics::gauge(
            metrics::OLDEST_PENDING_AGE_SECONDS,
            &stream,
            stats.oldest_pending_age.map_or(0.0, |a| a.as_secs_f64()),
        );
        metrics::gauge(
            metrics::DLQ_LENGTH,
            &stream,
            stats.dead_letter_length as f64,
        );
        metrics::gauge(
            metrics::DLQ_OLDEST_AGE_SECONDS,
            &stream,
            stats
                .dead_letter_oldest_age
                .map_or(0.0, |a| a.as_secs_f64()),
        );
        metrics::gauge(
            metrics::STREAM_BYTES,
            &stream,
            stats.bytes.unwrap_or(0) as f64,
        );
        Ok(stats)
    }
}

/// Fields a dead-letter entry sets itself; a copy from the source entry is
/// dropped so a re-dead-lettered entry carries one of each.
const DEAD_LETTER_FIELDS: [&[u8]; 6] = [
    b"error",
    b"reason",
    b"failed_at",
    b"deliveries",
    b"source_stream",
    b"source_id",
];

/// The entry's fields with its payload replaced by the rejected rows, and
/// `rejected_rows = true`.
fn rejected_fields(
    envelope: &Envelope,
    rejected: Box<RawValue>,
) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut copy = envelope.clone();
    copy.payload = rejected;
    let encoded = copy.encode().ok()?;
    let mut fields: Vec<(Vec<u8>, Vec<u8>)> = encoded
        .fields
        .into_iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v))
        .collect();
    fields.push((b"rejected_rows".to_vec(), b"true".to_vec()));
    Some(fields)
}

/// Resolves to whether `shutdown` has already fired, without waiting.
async fn fired<F: Future<Output = ()>>(shutdown: Pin<&mut F>) -> bool {
    tokio::select! {
        biased;
        () = shutdown => true,
        () = std::future::ready(()) => false,
    }
}

/// One entry of an `XREADGROUP` reply. `fields` is `None` for a pending
/// entry that has since been deleted (trimmed).
#[derive(Debug, PartialEq, Eq)]
struct RawEntry {
    id: String,
    fields: Option<Vec<(Vec<u8>, Vec<u8>)>>,
}

fn unexpected(what: &str) -> RedisError {
    RedisError::from((
        ErrorKind::TypeError,
        "unexpected reply shape",
        what.to_owned(),
    ))
}

/// Parses `XREADGROUP`'s reply for one stream (RESP2 arrays or RESP3 maps).
fn parse_read_reply(reply: Value) -> Result<Vec<RawEntry>, RedisError> {
    let streams: Vec<Value> = match reply {
        Value::Nil => return Ok(Vec::new()),
        Value::Array(streams) => streams
            .into_iter()
            .filter_map(|s| match s {
                Value::Array(mut pair) if pair.len() == 2 => pair.pop(),
                _ => None,
            })
            .collect(),
        Value::Map(pairs) => pairs.into_iter().map(|(_, entries)| entries).collect(),
        _ => return Err(unexpected("XREADGROUP")),
    };
    let mut out = Vec::new();
    for entries in streams {
        let Value::Array(entries) = entries else {
            return Err(unexpected("XREADGROUP entries"));
        };
        for entry in entries {
            let Value::Array(mut parts) = entry else {
                return Err(unexpected("XREADGROUP entry"));
            };
            if parts.len() != 2 {
                return Err(unexpected("XREADGROUP entry"));
            }
            let body = parts.pop().unwrap_or(Value::Nil);
            let id = parts
                .first()
                .and_then(as_string)
                .ok_or_else(|| unexpected("XREADGROUP id"))?;
            let fields = match body {
                Value::Nil => None,
                Value::Array(flat) => {
                    let mut pairs = Vec::with_capacity(flat.len() / 2);
                    let mut it = flat.into_iter();
                    while let (Some(k), Some(v)) = (it.next(), it.next()) {
                        pairs.push((as_bytes(k), as_bytes(v)));
                    }
                    Some(pairs)
                }
                Value::Map(map) => Some(
                    map.into_iter()
                        .map(|(k, v)| (as_bytes(k), as_bytes(v)))
                        .collect(),
                ),
                _ => return Err(unexpected("XREADGROUP fields")),
            };
            out.push(RawEntry { id, fields });
        }
    }
    Ok(out)
}

fn as_bytes(value: Value) -> Vec<u8> {
    match value {
        Value::BulkString(b) => b,
        Value::SimpleString(s) => s.into_bytes(),
        Value::Int(i) => i.to_string().into_bytes(),
        _ => Vec::new(),
    }
}

fn as_string(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(b) => String::from_utf8(b.clone()).ok(),
        Value::SimpleString(s) => Some(s.clone()),
        Value::Int(i) => Some(i.to_string()),
        _ => None,
    }
}

fn as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int(i) => Some(*i),
        other => as_string(other)?.parse().ok(),
    }
}

fn as_u64(value: &Value) -> Option<u64> {
    as_i64(value).and_then(|i| u64::try_from(i).ok())
}

fn list(value: &Value) -> &[Value] {
    match value {
        Value::Array(items) => items,
        _ => &[],
    }
}

/// A flat `[k, v, k, v…]` array (or a RESP3 map) as a name → value map.
fn map_fields(value: &Value) -> std::collections::HashMap<String, &Value> {
    let mut out = std::collections::HashMap::new();
    match value {
        Value::Array(flat) => {
            for pair in flat.chunks(2) {
                if let [k, v] = pair
                    && let Some(k) = as_string(k)
                {
                    out.insert(k, v);
                }
            }
        }
        Value::Map(pairs) => {
            for (k, v) in pairs {
                if let Some(k) = as_string(k) {
                    out.insert(k, v);
                }
            }
        }
        _ => {}
    }
    out
}

/// The millisecond part of a stream id (`<ms>-<seq>`).
pub fn id_millis(id: &str) -> Option<u64> {
    id.split_once('-').map_or(id, |(ms, _)| ms).parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> Value {
        Value::BulkString(s.as_bytes().to_vec())
    }

    #[test]
    fn a_read_reply_with_a_trimmed_pending_entry_parses() {
        let reply = Value::Array(vec![Value::Array(vec![
            bulk("ds:ingest:x"),
            Value::Array(vec![
                Value::Array(vec![bulk("1-0"), Value::Array(vec![bulk("v"), bulk("1")])]),
                Value::Array(vec![bulk("2-0"), Value::Nil]),
            ]),
        ])]);
        let entries = parse_read_reply(reply).unwrap();
        assert_eq!(
            entries,
            vec![
                RawEntry {
                    id: "1-0".into(),
                    fields: Some(vec![(b"v".to_vec(), b"1".to_vec())]),
                },
                RawEntry {
                    id: "2-0".into(),
                    fields: None,
                },
            ]
        );
        assert!(parse_read_reply(Value::Nil).unwrap().is_empty());
        assert!(parse_read_reply(Value::Int(3)).is_err());
    }

    #[test]
    fn ids_give_their_milliseconds() {
        assert_eq!(id_millis("1791216135409-3"), Some(1_791_216_135_409));
        assert_eq!(id_millis("17"), Some(17));
        assert_eq!(id_millis("x-1"), None);
    }

    #[test]
    fn the_default_config_follows_the_spec() {
        let c = ConsumerConfig::new("ds:ingest:tfl", "pod-a", 288);
        assert_eq!(c.group, "ingest-writer");
        assert_eq!(c.dead_letter_stream, "ds:dlq:tfl");
        assert_eq!(c.batch_size, 16);
        assert_eq!(c.block, Duration::from_millis(5000));
        assert_eq!(c.claim_min_idle, Duration::from_secs(300));
        assert_eq!(c.claim_interval, Duration::from_secs(60));
    }

    #[tokio::test]
    async fn fired_does_not_wait() {
        let pending = std::future::pending::<()>();
        tokio::pin!(pending);
        assert!(!fired(pending.as_mut()).await);
        let ready = std::future::ready(());
        tokio::pin!(ready);
        assert!(fired(ready.as_mut()).await);
    }
}
