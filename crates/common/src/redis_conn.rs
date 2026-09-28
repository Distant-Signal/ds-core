//! The Redis connection settings every long-running worker uses
//! (movement-relay, the three movement-stream consumers via
//! `movement-feed`, enricher), and a startup connect that waits for Redis
//! without hiding the wait.
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

use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};

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
/// A timed-out command returns an error (`io::ErrorKind::TimedOut`) but, in
/// redis-rs 0.27, does NOT by itself make the `ConnectionManager`
/// reconnect; that happens once the socket errors out. What it buys is that
/// no worker waits on a dead connection for longer than this per command:
/// its loop logs the error, backs off, beats progress and retries.
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
pub async fn connect(client: &redis::Client) -> redis::RedisResult<ConnectionManager> {
    client
        .get_connection_manager_with_config(connection_config())
        .await
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
) -> ConnectionManager {
    crate::startup::retry_until_ready(what, backoff, progress, || connect(client)).await
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;

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
}
