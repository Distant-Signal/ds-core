//! The Redis connection settings every long-running worker uses
//! (movement-relay, the three movement-stream consumers via
//! `movement-feed`, enricher; api for its per-ingest text-changed publish),
//! and a startup connect that waits for Redis without hiding the wait.
//!
//! **Why not redis-rs's defaults.** `Client::get_connection_manager()`
//! retries a failed (re)connect 6 more times on a 1s-then-60s (capped)
//! jittered backoff, with no connect timeout and nothing logged. A worker
//! that starts (or loses its connection) while the Redis pod is being
//! recreated sits inside one `await` for minutes: no log line says why, its
//! own loop cannot retry or beat its `/livez` progress, and a stall watchdog
//! shorter than that wait (trust-consumer's is 300s) restarts a pod that was
//! only waiting. It also never gets a response timeout, so a half-open TCP
//! connection (the Redis node gone without a FIN/RST) hangs a command for as
//! long as the kernel keeps retransmitting (~15 minutes).
//!
//! [`connection_config`] instead makes every (re)connect ONE attempt,
//! bounded by [`CONNECT_TIMEOUT`], and every command bounded by
//! [`RESPONSE_TIMEOUT`]. A command during an outage therefore fails within
//! seconds (and the `ConnectionManager` starts one background reconnect,
//! which the next command awaits), and the retrying lives in the worker:
//!
//! - at startup, [`connect_until_ready`] (or the worker's own
//!   `common::startup::retry_until_ready` around a larger connect step):
//!   logged per attempt, beating liveness progress;
//! - afterwards, the worker loop's own error backoff.
//!
//! **Half-open connections.** A response timeout does NOT make redis-rs
//! 0.27's `ConnectionManager` reconnect: it only reconnects on an error
//! that says the socket is gone (reset, EOF, broken pipe, ...). When the
//! peer vanishes without a FIN/RST (Redis pod or node gone, a conntrack
//! entry dropped), the kernel keeps the socket up while it retransmits
//! (`tcp_retries2`, ~15 minutes), so every command on it would time out,
//! one [`RESPONSE_TIMEOUT`] at a time, for that long. redis-rs 0.27 offers
//! no way to shorten that at the socket (its `keep-alive` feature, on by
//! default, uses the system keepalive defaults -- 2 hours idle -- and does
//! not fire while unacknowledged data is outstanding anyway; there is no
//! `TCP_USER_TIMEOUT` setting). So [`connect`] returns a [`RedisConn`],
//! which drops its connection on the FIRST timed-out command and makes the
//! next command open a fresh one (one bounded [`connect`] attempt). A
//! half-open connection therefore costs one timed-out command, not ~30.
//! No well-behaved command gets near the timeout: the longest server-side
//! wait any caller asks for is a 5s `XREADGROUP ... BLOCK`, which every
//! caller checks at compile time against [`RESPONSE_TIMEOUT`].

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use redis::aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig};
use redis::{Cmd, Pipeline, RedisFuture, RedisResult, Value};

use crate::backoff::Backoff;
use crate::progress::Progress;

/// Upper bound on one TCP connect + handshake (`AUTH`, `CLIENT SETINFO`)
/// attempt, so a blackholed address fails the attempt instead of hanging on
/// the kernel's own ~2-minute SYN timeout.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound on one command's reply. Must stay well above the longest
/// server-side block any caller asks for: the stream consumers'
/// `XREADGROUP ... BLOCK 5000` (movement-feed, enricher; each checks this at
/// compile time against its own block).
///
/// A timed-out command returns an error (`io::ErrorKind::TimedOut`) to the
/// caller, whose loop logs it, backs off, beats progress and retries; and
/// [`RedisConn`] replaces the connection before that retry (see the module
/// docs: redis-rs 0.27's `ConnectionManager` would keep the half-open one).
pub const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// The `ConnectionManager` settings every worker's Redis connection uses:
/// no internal retries, [`CONNECT_TIMEOUT`] per (re)connect attempt and
/// [`RESPONSE_TIMEOUT`] per command. See the module docs.
pub fn connection_config() -> ConnectionManagerConfig {
    connection_config_with(CONNECT_TIMEOUT, RESPONSE_TIMEOUT)
}

fn connection_config_with(connect: Duration, response: Duration) -> ConnectionManagerConfig {
    ConnectionManagerConfig::new()
        .set_number_of_retries(0)
        .set_connection_timeout(connect)
        .set_response_timeout(response)
}

/// One bounded connect attempt with [`connection_config`]. Errors once
/// `CONNECT_TIMEOUT` has passed (or at once, if refused); never retries.
pub async fn connect(client: &redis::Client) -> RedisResult<RedisConn> {
    RedisConn::connect_with(client, connection_config()).await
}

/// [`connect`], retried on `backoff` until Redis is reachable (INF-5: an
/// unreachable Redis at startup -- its pod being recreated by the same
/// rollout, or still loading its AOF -- is waited for, not exited on). Each
/// failed attempt is logged as `what` and beats `progress`, so `/livez`
/// stays 200 while this waits. Pass [`crate::startup::CONNECT_BACKOFF`]
/// outside tests.
pub async fn connect_until_ready(
    what: &str,
    client: &redis::Client,
    backoff: Backoff,
    progress: Option<&Progress>,
) -> RedisConn {
    crate::startup::retry_until_ready(what, backoff, progress, || connect(client)).await
}

/// A Redis connection that is replaced after a command times out.
///
/// Wraps a [`ConnectionManager`] built with [`connection_config`] and
/// behaves like one (it implements [`ConnectionLike`], so `redis::cmd(..)
/// .query_async(&mut conn)`, pipelines and `AsyncCommands` work unchanged),
/// with one addition: a command that fails with a timeout
/// ([`redis::RedisError::is_timeout`]) drops the connection it ran on, and
/// the next command first opens a new one -- one bounded [`connect`]
/// attempt, whose failure is that command's error (the next command tries
/// again). Every other error is left to the `ConnectionManager`, which
/// already reconnects on a closed or reset socket.
///
/// Why: a half-open connection (peer gone without a FIN/RST) is not
/// replaced by redis-rs 0.27 until the kernel gives up on it (~15 minutes);
/// see the module docs. With this, it costs one [`RESPONSE_TIMEOUT`].
///
/// Clones share one connection, and replace it together: a timeout seen by
/// several clones at once (commands in flight on the same dead socket)
/// drops it once, and never drops a connection opened after the timed-out
/// command started.
#[derive(Clone)]
pub struct RedisConn {
    shared: Arc<Shared>,
}

struct Shared {
    client: redis::Client,
    config: ConnectionManagerConfig,
    slot: Mutex<Slot>,
}

/// The current connection. `generation` counts the connections opened so
/// far, so a timeout on an older one cannot drop a newer one.
struct Slot {
    generation: u64,
    conn: Option<ConnectionManager>,
}

impl RedisConn {
    async fn connect_with(
        client: &redis::Client,
        config: ConnectionManagerConfig,
    ) -> RedisResult<Self> {
        let conn = client
            .get_connection_manager_with_config(config.clone())
            .await?;
        Ok(Self {
            shared: Arc::new(Shared {
                client: client.clone(),
                config,
                slot: Mutex::new(Slot {
                    generation: 1,
                    conn: Some(conn),
                }),
            }),
        })
    }

    fn slot(&self) -> std::sync::MutexGuard<'_, Slot> {
        // Nothing panics while holding the lock; recover rather than
        // propagate a poison regardless.
        self.shared
            .slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The live connection and its generation, opening a new one if a
    /// timeout dropped the last. The lock is never held across an await:
    /// concurrent callers may each open one; the first to finish is kept.
    async fn current(&self) -> RedisResult<(u64, ConnectionManager)> {
        {
            let slot = self.slot();
            if let Some(conn) = &slot.conn {
                return Ok((slot.generation, conn.clone()));
            }
        }
        let started = std::time::Instant::now();
        let fresh = self
            .shared
            .client
            .get_connection_manager_with_config(self.shared.config.clone())
            .await
            .inspect_err(|err| {
                tracing::warn!(
                    error = %err,
                    "Redis reconnect after a timed-out command failed; the next command retries"
                );
            })?;
        let mut slot = self.slot();
        if let Some(conn) = &slot.conn {
            // Another caller reconnected first; use theirs (ours is dropped).
            return Ok((slot.generation, conn.clone()));
        }
        slot.generation += 1;
        slot.conn = Some(fresh.clone());
        tracing::info!(
            took_ms = started.elapsed().as_millis() as u64,
            "Redis connection replaced after a timed-out command"
        );
        Ok((slot.generation, fresh))
    }

    /// Drops connection `generation` if `result` timed out on it.
    fn after<T>(&self, generation: u64, result: RedisResult<T>) -> RedisResult<T> {
        if let Err(err) = &result
            && err.is_timeout()
        {
            let mut slot = self.slot();
            if slot.generation == generation && slot.conn.take().is_some() {
                tracing::warn!(
                    error = %err,
                    "Redis command timed out; dropping the connection (possibly half-open), \
                     the next command opens a new one"
                );
            }
        }
        result
    }
}

impl ConnectionLike for RedisConn {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            let (generation, mut conn) = self.current().await?;
            let result = conn.send_packed_command(cmd).await;
            self.after(generation, result)
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            let (generation, mut conn) = self.current().await?;
            let result = conn.send_packed_commands(cmd, offset, count).await;
            self.after(generation, result)
        })
    }

    fn get_db(&self) -> i64 {
        self.shared.client.get_connection_info().redis.db
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    /// A local port with nothing listening on it (bound, then released).
    fn closed_local_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// A fake Redis that accepts connections and, when `answer_handshake`,
    /// replies `+OK` to the connection-setup pipeline (redis-rs 0.27 sends
    /// `CLIENT SETINFO LIB-NAME` and `LIB-VER`), then never answers again.
    /// Without `answer_handshake` it answers nothing at all.
    fn silent_redis(answer_handshake: bool) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                std::thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    let mut answered = !answer_handshake;
                    while let Ok(n) = stream.read(&mut buf) {
                        if n == 0 {
                            return;
                        }
                        if !answered {
                            answered = true;
                            let _ = stream.write_all(b"+OK\r\n+OK\r\n");
                        }
                    }
                });
            }
        });
        port
    }

    fn client(port: u16) -> redis::Client {
        redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap()
    }

    #[test]
    fn production_config_has_no_retries_and_both_timeouts() {
        let config = format!("{:?}", connection_config());
        assert!(config.contains("number_of_retries: 0"), "{config}");
        assert!(
            config.contains(&format!("connection_timeout: Some({CONNECT_TIMEOUT:?})")),
            "{config}"
        );
        assert!(
            config.contains(&format!("response_timeout: Some({RESPONSE_TIMEOUT:?})")),
            "{config}"
        );
    }

    /// A refused connect is one attempt, returned at once -- not redis-rs's
    /// default 6 retries on a growing backoff.
    #[tokio::test]
    async fn a_refused_connect_fails_at_once_without_internal_retries() {
        let started = std::time::Instant::now();
        let Err(err) = connect(&client(closed_local_port())).await else {
            panic!("nothing is listening");
        };
        assert!(err.is_io_error(), "{err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "took {:?}",
            started.elapsed()
        );
    }

    /// A server that accepts but never completes the handshake (blackholed
    /// or wedged) fails the attempt after the connect timeout.
    #[tokio::test]
    async fn a_connect_that_never_completes_times_out() {
        let port = silent_redis(false);
        let started = std::time::Instant::now();
        let result = client(port)
            .get_connection_manager_with_config(connection_config_with(
                Duration::from_millis(300),
                RESPONSE_TIMEOUT,
            ))
            .await;
        assert!(result.is_err());
        let took = started.elapsed();
        assert!(
            took >= Duration::from_millis(300) && took < Duration::from_secs(5),
            "took {took:?}"
        );
    }

    /// A connected server that stops answering (a half-open connection)
    /// fails each command after the response timeout instead of hanging it.
    #[tokio::test]
    async fn a_command_without_a_reply_times_out() {
        let port = silent_redis(true);
        let mut conn = client(port)
            .get_connection_manager_with_config(connection_config_with(
                CONNECT_TIMEOUT,
                Duration::from_millis(300),
            ))
            .await
            .expect("handshake is answered");
        let started = std::time::Instant::now();
        let result: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
        let err = result.unwrap_err();
        assert!(err.is_timeout(), "{err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "took {:?}",
            started.elapsed()
        );
    }

    /// Unreachable Redis at startup: retried (logged), beating progress on
    /// every failure; returns once Redis answers.
    #[tokio::test]
    async fn connect_until_ready_waits_and_beats_progress_until_redis_is_up() {
        // Bound now, but not listening until later: attempts are refused.
        let port = closed_local_port();
        let progress = Progress::new(Duration::from_millis(400));
        let client = client(port);
        let waiting = connect_until_ready(
            "Redis",
            &client,
            Backoff::new(Duration::from_millis(20), Duration::from_millis(50)),
            Some(&progress),
        );
        let bring_up = async {
            // 3x the stall window with nothing reachable.
            for _ in 0..12 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert!(!progress.is_stalled(), "failed attempts must beat progress");
            }
            // Bring "Redis" up on that port: a fake answering the handshake.
            let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { return };
                    std::thread::spawn(move || {
                        let mut buf = [0u8; 4096];
                        if let Ok(n) = stream.read(&mut buf)
                            && n > 0
                        {
                            let _ = stream.write_all(b"+OK\r\n+OK\r\n");
                        }
                        while matches!(stream.read(&mut buf), Ok(n) if n > 0) {}
                    });
                }
            });
        };
        let (_conn, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(waiting, bring_up)
        })
        .await
        .expect("connects once Redis is up");
    }

    /// How one connection to [`scripted_redis`] behaves.
    #[derive(Clone, Copy, Debug)]
    enum Behaviour {
        /// Accepts, then answers nothing: the connect times out.
        Silent,
        /// Answers the connection-setup pipeline, then nothing: a half-open
        /// connection, as seen from the client.
        HandshakeThenSilent,
        /// Answers the setup pipeline, then `reply` to every read, `delay`
        /// after it arrives.
        Answer {
            reply: &'static [u8],
            delay: Duration,
        },
    }

    /// A fake Redis whose `n`th accepted connection behaves as
    /// `script[n]` (the last entry repeats). Returns its port and a count
    /// of the connections accepted so far.
    fn scripted_redis(script: Vec<Behaviour>) -> (u16, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepted);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let index = counter.fetch_add(1, Ordering::SeqCst);
                let behaviour = script[index.min(script.len() - 1)];
                std::thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    let mut handshaken = false;
                    while let Ok(n) = stream.read(&mut buf) {
                        if n == 0 {
                            return;
                        }
                        match behaviour {
                            Behaviour::Silent => {}
                            Behaviour::HandshakeThenSilent | Behaviour::Answer { .. }
                                if !handshaken =>
                            {
                                handshaken = true;
                                let _ = stream.write_all(b"+OK\r\n+OK\r\n");
                            }
                            Behaviour::HandshakeThenSilent => {}
                            Behaviour::Answer { reply, delay } => {
                                std::thread::sleep(delay);
                                let _ = stream.write_all(reply);
                            }
                        }
                    }
                });
            }
        });
        (port, accepted)
    }

    const PONG: Behaviour = Behaviour::Answer {
        reply: b"+PONG\r\n",
        delay: Duration::ZERO,
    };

    /// Short timeouts, so the fakes answer (or don't) quickly.
    fn test_config() -> ConnectionManagerConfig {
        connection_config_with(Duration::from_millis(300), Duration::from_millis(300))
    }

    async fn ping(conn: &mut RedisConn) -> RedisResult<String> {
        redis::cmd("PING").query_async(conn).await
    }

    fn accepted(counter: &AtomicUsize) -> usize {
        counter.load(Ordering::SeqCst)
    }

    /// The fix: a half-open connection costs ONE timed-out command. The
    /// next command opens a new connection and succeeds -- where a bare
    /// `ConnectionManager` (see `a_command_without_a_reply_times_out`)
    /// keeps the dead one until the kernel gives up on it.
    #[tokio::test]
    async fn a_timed_out_connection_is_replaced_by_the_next_command() {
        let (port, accepted_count) = scripted_redis(vec![Behaviour::HandshakeThenSilent, PONG]);
        let mut conn = RedisConn::connect_with(&client(port), test_config())
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let err = ping(&mut conn).await.unwrap_err();
        assert!(err.is_timeout(), "{err:?}");
        assert_eq!(ping(&mut conn).await.unwrap(), "PONG");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "took {:?}",
            started.elapsed()
        );
        assert_eq!(accepted(&accepted_count), 2);
        // And the new connection is kept.
        assert_eq!(ping(&mut conn).await.unwrap(), "PONG");
        assert_eq!(accepted(&accepted_count), 2);
    }

    /// Successes, server error replies, and replies that are slow but
    /// inside the timeout (like a blocking `XREADGROUP ... BLOCK` returning
    /// nil) never replace the connection.
    #[tokio::test]
    async fn only_a_timeout_replaces_the_connection() {
        for (reply, delay) in [
            (&b"+PONG\r\n"[..], Duration::ZERO),
            (&b"-ERR nope\r\n"[..], Duration::ZERO),
            (&b"+PONG\r\n"[..], Duration::from_millis(150)),
            // The nil a `BLOCK` read returns when nothing arrived.
            (&b"*-1\r\n"[..], Duration::from_millis(150)),
        ] {
            let (port, accepted_count) = scripted_redis(vec![Behaviour::Answer { reply, delay }]);
            let mut conn = RedisConn::connect_with(&client(port), test_config())
                .await
                .unwrap();
            for _ in 0..3 {
                let result: RedisResult<redis::Value> =
                    redis::cmd("PING").query_async(&mut conn).await;
                if let Err(err) = result {
                    assert!(!err.is_timeout(), "{err:?}");
                }
            }
            assert_eq!(
                accepted(&accepted_count),
                1,
                "reply {reply:?} after {delay:?}"
            );
        }
    }

    /// Redis still unreachable when the replacement is attempted: that
    /// command fails (bounded by the connect timeout), and the one after it
    /// tries again.
    #[tokio::test]
    async fn a_failed_reconnect_is_retried_by_the_next_command() {
        let (port, accepted_count) = scripted_redis(vec![
            Behaviour::HandshakeThenSilent,
            Behaviour::Silent,
            PONG,
        ]);
        let mut conn = RedisConn::connect_with(&client(port), test_config())
            .await
            .unwrap();
        assert!(ping(&mut conn).await.unwrap_err().is_timeout());
        let started = std::time::Instant::now();
        assert!(
            ping(&mut conn).await.is_err(),
            "the reconnect is not answered"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "took {:?}",
            started.elapsed()
        );
        assert_eq!(ping(&mut conn).await.unwrap(), "PONG");
        assert_eq!(accepted(&accepted_count), 3);
    }

    /// Clones share the connection: commands in flight on the same dead
    /// socket all time out, but it is replaced once, and a timeout from an
    /// older connection never drops a newer one.
    #[tokio::test]
    async fn clones_replace_a_shared_connection_once() {
        let (port, accepted_count) = scripted_redis(vec![Behaviour::HandshakeThenSilent, PONG]);
        let mut a = RedisConn::connect_with(&client(port), test_config())
            .await
            .unwrap();
        let mut b = a.clone();
        let (ra, rb) = tokio::join!(ping(&mut a), ping(&mut b));
        assert!(ra.unwrap_err().is_timeout());
        assert!(rb.unwrap_err().is_timeout());
        assert_eq!(ping(&mut a).await.unwrap(), "PONG");
        assert_eq!(ping(&mut b).await.unwrap(), "PONG");
        assert_eq!(accepted(&accepted_count), 2);

        // A late timeout from generation 1 leaves generation 2 alone.
        let stale: RedisResult<()> = Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
        assert!(a.after(1, stale).is_err());
        assert_eq!(a.slot().generation, 2);
        assert!(a.slot().conn.is_some());
        assert_eq!(ping(&mut b).await.unwrap(), "PONG");
        assert_eq!(accepted(&accepted_count), 2);
    }

    /// A TCP proxy that can "freeze" every connection open so far: it
    /// keeps them open and keeps reading (so the client's writes are
    /// acknowledged) but relays nothing in either direction -- a half-open
    /// connection as the client sees it: no reply, and no FIN/RST ever.
    /// Connections accepted after the freeze relay normally, like a Redis
    /// that came back (a new pod behind the same Service).
    struct FreezingProxy {
        port: u16,
        flags: Arc<Mutex<Vec<Arc<AtomicBool>>>>,
    }

    impl FreezingProxy {
        async fn start(upstream: String) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let flags: Arc<Mutex<Vec<Arc<AtomicBool>>>> = Arc::default();
            let registry = Arc::clone(&flags);
            tokio::spawn(async move {
                loop {
                    let Ok((client, _)) = listener.accept().await else {
                        return;
                    };
                    let Ok(server) = tokio::net::TcpStream::connect(&upstream).await else {
                        continue;
                    };
                    let frozen = Arc::new(AtomicBool::new(false));
                    registry.lock().unwrap().push(Arc::clone(&frozen));
                    let (client_read, client_write) = client.into_split();
                    let (server_read, server_write) = server.into_split();
                    tokio::spawn(relay(client_read, server_write, Arc::clone(&frozen)));
                    tokio::spawn(relay(server_read, client_write, frozen));
                }
            });
            Self { port, flags }
        }

        fn freeze_existing(&self) {
            for frozen in self.flags.lock().unwrap().iter() {
                frozen.store(true, Ordering::SeqCst);
            }
        }
    }

    /// Copies `from` to `to` until `frozen`, then keeps reading and
    /// discarding. Never closes `to`.
    async fn relay(
        mut from: tokio::net::tcp::OwnedReadHalf,
        mut to: tokio::net::tcp::OwnedWriteHalf,
        frozen: Arc<AtomicBool>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = vec![0u8; 16 * 1024];
        while let Ok(n) = from.read(&mut buf).await {
            if n == 0 {
                break;
            }
            if !frozen.load(Ordering::SeqCst) {
                let _ = to.write_all(&buf[..n]).await;
            }
        }
        std::future::pending::<()>().await;
    }

    fn local_redis_addr() -> String {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let client = redis::Client::open(url).unwrap();
        match &client.get_connection_info().addr {
            redis::ConnectionAddr::Tcp(host, port) => format!("{host}:{port}"),
            other => panic!("needs a plain TCP Redis, got {other:?}"),
        }
    }

    /// A worker loop as the services run one: a command, and on an error
    /// a 1s backoff and a retry. Returns how long after `since` the first
    /// command succeeded (`None` if none had by `give_up_after`) and the
    /// number of failed commands.
    async fn time_to_recovery<C: ConnectionLike + Send>(
        conn: &mut C,
        since: std::time::Instant,
        give_up_after: Duration,
    ) -> (Option<Duration>, u32) {
        let mut failures = 0;
        while since.elapsed() < give_up_after {
            let result: RedisResult<String> = redis::cmd("PING").query_async(conn).await;
            match result {
                Ok(_) => return (Some(since.elapsed()), failures),
                Err(err) => {
                    eprintln!(
                        "  {:>6.1}s  PING failed: {err}",
                        since.elapsed().as_secs_f64()
                    );
                    failures += 1;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        (None, failures)
    }

    /// Half-open recovery against a real Redis with the PRODUCTION
    /// timeouts, before (a bare `ConnectionManager`, as every worker had)
    /// and after (`RedisConn`). The Redis itself is not disturbed: the
    /// half-open connection is a proxy in front of it going silent.
    ///
    /// ```text
    /// REDIS_URL=redis://127.0.0.1:6379 cargo test -p common --features redis \
    ///   -- --ignored half_open --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "needs a local Redis (REDIS_URL); takes ~2.5 minutes"]
    async fn half_open_connection_recovery_against_real_redis() {
        let proxy = FreezingProxy::start(local_redis_addr()).await;
        let client = client(proxy.port);
        // The bare manager is watched for 3 response timeouts.
        let give_up = RESPONSE_TIMEOUT * 3 + Duration::from_secs(5);

        let mut before = client
            .get_connection_manager_with_config(connection_config())
            .await
            .unwrap();
        let _: String = redis::cmd("PING").query_async(&mut before).await.unwrap();
        proxy.freeze_existing();
        eprintln!("before (bare ConnectionManager):");
        let (recovered, failures) =
            time_to_recovery(&mut before, std::time::Instant::now(), give_up).await;
        eprintln!("  -> recovered after {recovered:?}, {failures} failed commands");
        assert_eq!(
            recovered, None,
            "a bare ConnectionManager keeps the half-open connection"
        );

        let mut after = connect(&client).await.unwrap();
        let _: String = redis::cmd("PING").query_async(&mut after).await.unwrap();
        proxy.freeze_existing();
        eprintln!("after (RedisConn):");
        let (recovered, failures) =
            time_to_recovery(&mut after, std::time::Instant::now(), give_up).await;
        eprintln!("  -> recovered after {recovered:?}, {failures} failed commands");
        let recovered = recovered.expect("RedisConn replaces the half-open connection");
        assert_eq!(failures, 1);
        assert!(
            recovered < RESPONSE_TIMEOUT + Duration::from_secs(5),
            "took {recovered:?}"
        );
    }
}
