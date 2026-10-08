//! The stream producer (spec §7.5).
//!
//! [`Producer::spawn`] starts one background task per stream that owns the
//! Redis connection and XADDs, in order, what callers [`Producer::submit`].
//! Every XADD carries `MAXLEN ~ <cap>` (producers never XTRIM, so their ACL
//! user needs only `+xadd`, plus `+xrevrange` for [`last_produced_at`]).
//!
//! **When Redis is unavailable** (down, NOAUTH/NOPERM, OOM under
//! `noeviction`, MISCONF after a failed AOF write: all treated alike) the
//! task retries with the shared backoff (1 s doubling to 60 s, jittered;
//! `common::backoff`) and [`Producer::is_available`] turns false, which the
//! caller's readiness reports as `stream_unavailable` (its `/livez` stays
//! 200). What happens to new items depends on the [`ProducePolicy`]:
//!
//! - [`ProducePolicy::LatestSnapshot`]: only the newest unsent item is
//!   kept; a newer one replaces it (`superseded`). [`Producer::submit`]
//!   never waits. For snapshot domains, where a newer snapshot makes the
//!   older ones redundant.
//! - [`ProducePolicy::Event`]: a bounded FIFO. When it is full,
//!   [`Producer::submit`] waits for room (backpressure), and a caller that
//!   must not lose events ACKs its upstream only after
//!   [`Receipt::written`]. Nothing is dropped.
//!
//! No producer spills to disk.
//!
//! A snapshot may be several parts ([`crate::envelope::split_snapshot`]);
//! an item is written part by part, in order, and its receipt resolves when
//! the last part is in. Each part is written with the bytes and key it was
//! encoded with, so a retry after an ambiguous failure (a timeout after
//! Redis applied the XADD) writes a duplicate with the same key, which the
//! writer's `ingest_dedup` absorbs.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::backoff::{Backoff, FailureStreak};
use common::redis_conn::RedisConn;
use redis::RedisError;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

use crate::envelope::{EncodedEntry, field, parse_produced_at};
use crate::metrics;

/// The producers' retry schedule when an XADD fails (spec §7.5).
pub const PRODUCER_BACKOFF: Backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));

/// What a producer does with new items while Redis is unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProducePolicy {
    /// Keep only the newest unsent item.
    LatestSnapshot,
    /// Keep up to `max_buffered` items in order, then make
    /// [`Producer::submit`] wait.
    Event { max_buffered: NonZeroUsize },
}

#[derive(Clone, Debug)]
pub struct ProducerConfig {
    pub stream: String,
    /// `XADD … MAXLEN ~ <maxlen>`; see [`crate::budget`].
    pub maxlen: u64,
    pub policy: ProducePolicy,
    pub backoff: Backoff,
}

impl ProducerConfig {
    pub fn new(stream: impl Into<String>, maxlen: u64, policy: ProducePolicy) -> Self {
        Self {
            stream: stream.into(),
            maxlen,
            policy,
            backoff: PRODUCER_BACKOFF,
        }
    }
}

/// Why an item will never be written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum NotWritten {
    #[error("a newer snapshot replaced it before it was written")]
    Superseded,
    #[error("the producer was closed before it was written")]
    Closed,
}

/// Resolves when the item is fully written (to its stream ids) or never
/// will be.
#[derive(Debug)]
pub struct Receipt(oneshot::Receiver<Result<Vec<String>, NotWritten>>);

impl Receipt {
    pub async fn written(self) -> Result<Vec<String>, NotWritten> {
        self.0.await.unwrap_or(Err(NotWritten::Closed))
    }
}

/// A handle to a stream's producer task. Clones share the task.
#[derive(Clone)]
pub struct Producer {
    shared: Arc<Shared>,
}

struct Shared {
    stream: String,
    maxlen: u64,
    policy: ProducePolicy,
    backoff: Backoff,
    state: Mutex<State>,
    /// The task waits on this for work.
    work: Notify,
    /// [`ProducePolicy::Event`] submitters wait on this for room.
    space: Notify,
    available: AtomicBool,
}

#[derive(Default)]
struct State {
    queue: VecDeque<Item>,
    next_seq: u64,
    closed: bool,
}

struct Item {
    seq: u64,
    parts: Arc<[EncodedEntry]>,
    sent: usize,
    ids: Vec<String>,
    done: oneshot::Sender<Result<Vec<String>, NotWritten>>,
}

impl Producer {
    /// Starts the producer task. It connects lazily (one bounded attempt per
    /// try, `common::redis_conn::connect`) and runs until [`Producer::close`]
    /// and an empty queue, or until the handle is aborted.
    pub fn spawn(client: redis::Client, config: ProducerConfig) -> (Self, JoinHandle<()>) {
        metrics::register_producer(&config.stream);
        let shared = Arc::new(Shared {
            stream: config.stream,
            maxlen: config.maxlen,
            policy: config.policy,
            backoff: config.backoff,
            state: Mutex::new(State::default()),
            work: Notify::new(),
            space: Notify::new(),
            available: AtomicBool::new(true),
        });
        let task = tokio::spawn(worker(client, Arc::clone(&shared)));
        (Self { shared }, task)
    }

    pub fn stream(&self) -> &str {
        &self.shared.stream
    }

    /// Queues `parts` (one snapshot, or one event) for writing. Under
    /// [`ProducePolicy::LatestSnapshot`] this replaces any unsent item and
    /// never waits; under [`ProducePolicy::Event`] it waits while the buffer
    /// is full. `Err(Closed)` after [`Producer::close`].
    pub async fn submit(&self, parts: Vec<EncodedEntry>) -> Result<Receipt, NotWritten> {
        let (tx, rx) = oneshot::channel();
        if parts.is_empty() {
            let _ = tx.send(Ok(Vec::new()));
            return Ok(Receipt(rx));
        }
        let mut item = Some((parts, tx));
        loop {
            let room = self.shared.space.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            {
                let mut state = self.shared.lock();
                if state.closed {
                    return Err(NotWritten::Closed);
                }
                let fits = match self.shared.policy {
                    ProducePolicy::LatestSnapshot => {
                        let superseded = state.queue.len();
                        for old in state.queue.drain(..) {
                            let _ = old.done.send(Err(NotWritten::Superseded));
                        }
                        if superseded > 0 {
                            metrics::dropped(&self.shared.stream, "superseded", superseded);
                        }
                        true
                    }
                    ProducePolicy::Event { max_buffered } => state.queue.len() < max_buffered.get(),
                };
                if fits && let Some((parts, done)) = item.take() {
                    let seq = state.next_seq;
                    state.next_seq += 1;
                    state.queue.push_back(Item {
                        seq,
                        parts: parts.into(),
                        sent: 0,
                        ids: Vec::new(),
                        done,
                    });
                    metrics::buffered(&self.shared.stream, state.queue.len());
                    drop(state);
                    self.shared.work.notify_one();
                    return Ok(Receipt(rx));
                }
            }
            room.await;
        }
    }

    /// False from a failed XADD until the next successful one: readiness
    /// reports `stream_unavailable`.
    pub fn is_available(&self) -> bool {
        self.shared.available.load(Ordering::Relaxed)
    }

    /// Items queued and not yet fully written.
    pub fn buffered(&self) -> usize {
        self.shared.lock().queue.len()
    }

    /// Stops accepting items. The task writes what is queued and then
    /// exits; [`Producer::shutdown`] bounds how long that may take.
    pub fn close(&self) {
        self.shared.lock().closed = true;
        self.shared.work.notify_one();
        self.shared.space.notify_waiters();
    }

    /// [`Producer::close`], then waits up to `grace` for the queue to drain
    /// and aborts the task after that (its unwritten receipts resolve to
    /// [`NotWritten::Closed`]). Returns whether everything was written.
    pub async fn shutdown(&self, task: JoinHandle<()>, grace: Duration) -> bool {
        self.close();
        let abort = task.abort_handle();
        if tokio::time::timeout(grace, task).await.is_ok() {
            return true;
        }
        abort.abort();
        let left = std::mem::take(&mut self.shared.lock().queue);
        if !left.is_empty() {
            tracing::warn!(
                stream = %self.shared.stream,
                items = left.len(),
                "ingest producer shut down with unwritten items"
            );
        }
        metrics::buffered(&self.shared.stream, 0);
        false
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        // Nothing panics while holding the lock; recover regardless.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

async fn worker(client: redis::Client, shared: Arc<Shared>) {
    let mut conn: Option<RedisConn> = None;
    let mut streak = FailureStreak::new(shared.backoff);
    loop {
        let next = {
            let state = shared.lock();
            match state.queue.front() {
                Some(item) => Some((item.seq, item.sent, Arc::clone(&item.parts))),
                None if state.closed => return,
                None => None,
            }
        };
        let Some((seq, index, parts)) = next else {
            shared.work.notified().await;
            continue;
        };
        let entry = &parts[index];
        let result = match conn.as_mut() {
            Some(c) => xadd_entry(c, &shared.stream, shared.maxlen, entry).await,
            None => match common::redis_conn::connect(&client).await {
                Ok(mut c) => {
                    let r = xadd_entry(&mut c, &shared.stream, shared.maxlen, entry).await;
                    conn = Some(c);
                    r
                }
                Err(err) => Err(err),
            },
        };
        match result {
            Ok(id) => {
                streak.succeeded();
                if !shared.available.swap(true, Ordering::Relaxed) {
                    tracing::info!(stream = %shared.stream, "ingest stream XADD recovered");
                }
                metrics::produce(&shared.stream, "ok");
                metrics::produce_bytes(&shared.stream, entry.body_len);
                let mut state = shared.lock();
                let finished = match state.queue.front_mut() {
                    // Still the same item and part (a newer snapshot may
                    // have superseded it while the XADD was in flight).
                    Some(front) if front.seq == seq && front.sent == index => {
                        front.sent += 1;
                        front.ids.push(id);
                        front.sent == front.parts.len()
                    }
                    _ => false,
                };
                if finished && let Some(done) = state.queue.pop_front() {
                    let _ = done.done.send(Ok(done.ids));
                    metrics::buffered(&shared.stream, state.queue.len());
                    drop(state);
                    shared.space.notify_waiters();
                }
            }
            Err(err) => {
                let outcome = classify(&err);
                metrics::produce(&shared.stream, outcome);
                shared.available.store(false, Ordering::Relaxed);
                let delay = streak.failed(None);
                tracing::warn!(
                    stream = %shared.stream,
                    outcome,
                    error = %err,
                    failures = streak.failures(),
                    retry_in_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                    "ingest stream XADD failed; retrying"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// `XADD <stream> MAXLEN ~ <maxlen> * <fields…>`, returning the new id.
pub async fn xadd_entry<C: redis::aio::ConnectionLike + Send>(
    conn: &mut C,
    stream: &str,
    maxlen: u64,
    entry: &EncodedEntry,
) -> Result<String, RedisError> {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(stream).arg("MAXLEN").arg("~").arg(maxlen).arg("*");
    for (name, value) in &entry.fields {
        cmd.arg(*name).arg(value.as_slice());
    }
    cmd.query_async(conn).await
}

/// The `produced_at` of the stream's newest entry (`XREVRANGE <stream> + -
/// COUNT 1`): a stream producer's "last fetched" cursor (spec §11.3).
/// `None` for an empty or missing stream, or an entry without a parseable
/// `produced_at`.
pub async fn last_produced_at<C: redis::aio::ConnectionLike + Send>(
    conn: &mut C,
    stream: &str,
) -> Result<Option<DateTime<Utc>>, RedisError> {
    let reply: redis::streams::StreamRangeReply = redis::cmd("XREVRANGE")
        .arg(stream)
        .arg("+")
        .arg("-")
        .arg("COUNT")
        .arg(1)
        .query_async(conn)
        .await?;
    Ok(reply.ids.first().and_then(|entry| {
        let text: String = entry.get(field::PRODUCED_AT)?;
        parse_produced_at(&text)
    }))
}

/// A stream producer's startup cursor as a `common::ingest::CursorSource`
/// (`Stream`, plan 4.6): [`last_produced_at`] of `stream`, read on a clone
/// of `conn` (a `ConnectionManager` clone shares its connection). Pass it
/// to `common::poller_loop::run_poll_loop_with_source`. The producer's ACL
/// user needs `+xrevrange` on its own stream (spec §8.2).
pub fn stream_cursor<C>(conn: C, stream: impl Into<String>) -> common::ingest::CursorSource<'static>
where
    C: redis::aio::ConnectionLike + Clone + Send + Sync + 'static,
{
    let stream: String = stream.into();
    common::ingest::CursorSource::stream(move || {
        let mut conn = conn.clone();
        let stream = stream.clone();
        async move { Ok(last_produced_at(&mut conn, &stream).await?) }
    })
}

/// The `outcome` label of a failed command: see
/// [`crate::metrics::PRODUCE_OUTCOMES`].
pub fn classify(err: &RedisError) -> &'static str {
    if let Some(outcome) = err.code().and_then(outcome_for_code) {
        outcome
    } else if err.kind() == redis::ErrorKind::AuthenticationFailed {
        "noauth"
    } else if err.is_io_error()
        || err.is_timeout()
        || err.is_connection_dropped()
        || err.is_connection_refusal()
    {
        "down"
    } else {
        "error"
    }
}

/// The outcome of a server error code that has one of its own.
fn outcome_for_code(code: &str) -> Option<&'static str> {
    match code {
        "OOM" => Some("oom"),
        "NOPERM" => Some("noperm"),
        "NOAUTH" | "WRONGPASS" => Some("noauth"),
        "MISCONF" => Some("misconf"),
        "LOADING" => Some("down"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_are_classified_by_their_server_code_or_kind() {
        for (code, outcome) in [
            ("OOM", Some("oom")),
            ("NOPERM", Some("noperm")),
            ("NOAUTH", Some("noauth")),
            ("WRONGPASS", Some("noauth")),
            ("MISCONF", Some("misconf")),
            ("LOADING", Some("down")),
            ("ERR", None),
        ] {
            assert_eq!(outcome_for_code(code), outcome, "{code}");
        }
        let io = RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        assert_eq!(classify(&io), "down");
        let auth = RedisError::from((redis::ErrorKind::AuthenticationFailed, "bad"));
        assert_eq!(classify(&auth), "noauth");
        let other = RedisError::from((redis::ErrorKind::TypeError, "bad"));
        assert_eq!(classify(&other), "error");
        for outcome in ["oom", "noperm", "noauth", "misconf", "down", "error"] {
            assert!(metrics::PRODUCE_OUTCOMES.contains(&outcome), "{outcome}");
        }
    }
}
