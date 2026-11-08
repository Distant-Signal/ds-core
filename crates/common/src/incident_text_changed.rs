//! The `incident-text-changed` Redis Stream's producer side: one entry per
//! incident whose summary or description changed (or that is new), read by
//! the enricher (`crates/enricher/src/stream.rs`, group `enricher`).
//!
//! Called by the incident-snapshot writer, poller-incidents' DB sink
//! (ingest architecture plan 2c.2), after every chunk of
//! `ds_store::incidents::apply_snapshot` has committed and before the
//! removal inference. Moved here from `api/src/data/queries.rs` unchanged;
//! the api's `POST /private/incidents` called it too until release B
//! (ingest phase 5), and the api has had no Redis client since step 5.4.

/// The stream's key.
pub const STREAM: &str = "incident-text-changed";

/// Approximate cap on the `incident-text-changed` stream (API-8). The
/// snapshot writer is its only producer and nothing else trims it, so an
/// enricher that is down, or never catches up, would otherwise grow it
/// without bound in the same Redis that runs `maxmemory` for the movement
/// streams. Text changes are rare (tens a day), so 10,000 entries is weeks
/// of backlog; anything trimmed unprocessed is caught by the enricher's
/// hourly sweep.
pub const INCIDENT_TEXT_CHANGED_MAXLEN: usize = 10_000;

/// `XADD incident-text-changed MAXLEN ~ <cap> * incident_id <id>`. `~` lets
/// Redis trim whole macro-nodes only, so the cap costs nothing per write.
pub fn xadd(incident_id: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(STREAM)
        .arg("MAXLEN")
        .arg("~")
        .arg(INCIDENT_TEXT_CHANGED_MAXLEN)
        .arg("*")
        .arg("incident_id")
        .arg(incident_id);
    cmd
}

/// Waits before each retry of the ids whose publish failed: three
/// attempts in about 2.5 s plus their bounded connects. A Redis blip (the
/// pod being recreated, a dropped connection) then costs no enrichment
/// delay, where it used to leave the ids to the enricher's hourly sweep;
/// small, because the snapshot writer waits for it.
pub const PUBLISH_RETRY_BACKOFF: [std::time::Duration; 2] = [
    std::time::Duration::from_millis(500),
    std::time::Duration::from_secs(2),
];

/// XADDs one `incident-text-changed` entry per id, best effort: the ids
/// whose XADD (or connect) failed are retried on [`PUBLISH_RETRY_BACKOFF`],
/// and any still failing after that are logged and skipped, never returned
/// (the enricher's hourly sweep is the backstop for a missed publish),
/// because a publish failure must not fail the ingest. A retried XADD that
/// had in fact landed adds a duplicate entry, which the enricher absorbs
/// (it re-reads the incident's current text).
///
/// Connecting happens HERE, per snapshot: `redis` is a lazy
/// `redis::Client` that has never opened a socket. A Redis that is down
/// therefore surfaces as a failed publish instead of failing the writer's
/// startup.
///
/// The connection is [`ds_runtime::redis_conn::connect`]'s (INF-5): ONE connect
/// attempt per round, bounded by `CONNECT_TIMEOUT`, every command bounded by
/// `RESPONSE_TIMEOUT`. It used to be redis-rs's default
/// `get_connection_manager()`, which retries a failed connect 6 more times
/// on a 1s-then-60s backoff with no connect or response timeout: about
/// five minutes inside the poller's ingest request whenever the Redis pod
/// was being recreated, and no bound at all on a half-open connection.
pub async fn publish(redis: &redis::Client, incident_ids: Vec<String>) {
    publish_with(redis, incident_ids, &PUBLISH_RETRY_BACKOFF).await;
}

/// [`publish`] with its retry waits.
async fn publish_with(
    redis: &redis::Client,
    incident_ids: Vec<String>,
    backoff: &[std::time::Duration],
) {
    let mut pending = publish_once(redis, incident_ids).await;
    for wait in backoff {
        if pending.is_empty() {
            return;
        }
        tracing::info!(
            pending = pending.len(),
            retry_in_ms = wait.as_millis(),
            "retrying the text-changed events that failed to publish"
        );
        tokio::time::sleep(*wait).await;
        pending = publish_once(redis, pending).await;
    }
    if !pending.is_empty() {
        tracing::warn!(
            pending = pending.len(),
            incident_ids = ?pending,
            "could not publish text-changed events after retrying; the hourly sweep will catch them"
        );
    }
}

/// One round: connects and XADDs each id, returning the ids that failed.
async fn publish_once(redis: &redis::Client, incident_ids: Vec<String>) -> Vec<String> {
    let mut conn = match ds_runtime::redis_conn::connect(redis).await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::warn!(
                error = ?err,
                pending = incident_ids.len(),
                "could not connect to redis to publish text-changed events"
            );
            return incident_ids;
        }
    };
    let mut failed = Vec::new();
    for incident_id in incident_ids {
        let result: redis::RedisResult<String> = xadd(&incident_id).query_async(&mut conn).await;
        if let Err(err) = result {
            tracing::warn!(error = ?err, incident_id, "failed to publish text-changed event");
            failed.push(incident_id);
        }
    }
    failed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unreachable_redis() -> redis::Client {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap()
    }

    /// INF-5: an unreachable Redis costs the ingest one bounded connect
    /// attempt per retry round, not redis-rs's default retry schedule
    /// (seven attempts on a 1s-then-60s backoff, about five minutes).
    #[tokio::test]
    async fn publishing_to_an_unreachable_redis_gives_up_after_bounded_attempts() {
        let rounds = u32::try_from(PUBLISH_RETRY_BACKOFF.len() + 1).unwrap();
        let waits: std::time::Duration = PUBLISH_RETRY_BACKOFF.iter().sum();
        tokio::time::timeout(
            ds_runtime::redis_conn::CONNECT_TIMEOUT * 2 * rounds + waits,
            publish(&unreachable_redis(), vec!["INC-1".to_string()]),
        )
        .await
        .expect("a refused connection must fail the publish within its few retries, not minutes");
    }

    /// The retry rounds are the backoff's, then the publish gives up.
    #[tokio::test(start_paused = true)]
    async fn a_failed_publish_is_retried_on_the_backoff_then_left_to_the_sweep() {
        let backoff = [
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(4),
        ];
        let started = tokio::time::Instant::now();
        publish_with(&unreachable_redis(), vec!["INC-1".to_string()], &backoff).await;
        assert!(started.elapsed() >= std::time::Duration::from_secs(5));
    }

    /// API-8: the text-changed publish carries an approximate MAXLEN cap.
    #[test]
    fn xadd_caps_the_stream() {
        let packed = String::from_utf8(xadd("INC-1").get_packed_command()).unwrap();
        // RESP: `*<n>` then a `$<len>`, `<value>` pair per argument.
        let parts: Vec<&str> = packed.trim_end().split("\r\n").collect();
        let args: Vec<&str> = parts[1..].chunks(2).map(|pair| pair[1]).collect();
        assert_eq!(
            args,
            [
                "XADD",
                "incident-text-changed",
                "MAXLEN",
                "~",
                "10000",
                "*",
                "incident_id",
                "INC-1"
            ]
        );
    }
}
