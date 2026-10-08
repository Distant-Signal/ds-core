//! The `incident-text-changed` Redis Stream's producer side: one entry per
//! incident whose summary or description changed (or that is new), read by
//! the enricher (`crates/enricher/src/stream.rs`, group `enricher`).
//!
//! Shared by both incident-snapshot writers (ingest architecture plan
//! 2c.2): the api's `POST /private/incidents` and poller-incidents' DB
//! sink. Both call [`publish`] after every chunk of
//! `ds_store::incidents::apply_snapshot` has committed and before the
//! removal inference. Moved here from `api/src/data/queries.rs` unchanged.

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

/// XADDs one `incident-text-changed` entry per id, best effort: every
/// failure is logged and skipped, never returned (the enricher's hourly
/// sweep is the backstop for a missed publish), because a publish failure
/// must not fail the ingest.
///
/// Connecting happens HERE, per snapshot: `redis` is a lazy
/// `redis::Client` that has never opened a socket. A Redis that is down
/// therefore surfaces as a failed publish instead of failing the writer's
/// startup.
///
/// The connection is [`crate::redis_conn::connect`]'s (INF-5): ONE connect
/// attempt bounded by `CONNECT_TIMEOUT`, every command bounded by
/// `RESPONSE_TIMEOUT`. It used to be redis-rs's default
/// `get_connection_manager()`, which retries a failed connect 6 more times
/// on a 1s-then-60s backoff with no connect or response timeout: about
/// five minutes inside the poller's ingest request whenever the Redis pod
/// was being recreated, and no bound at all on a half-open connection.
pub async fn publish(redis: &redis::Client, incident_ids: Vec<String>) {
    let mut conn = match crate::redis_conn::connect(redis).await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::warn!(
                error = ?err,
                pending = incident_ids.len(),
                "could not connect to redis to publish text-changed events; hourly sweep will catch them"
            );
            return;
        }
    };
    for incident_id in incident_ids {
        let result: redis::RedisResult<String> = xadd(&incident_id).query_async(&mut conn).await;
        if let Err(err) = result {
            tracing::warn!(error = ?err, incident_id, "failed to publish text-changed event; hourly sweep will catch it");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// INF-5: an unreachable Redis costs the ingest one bounded connect
    /// attempt, not redis-rs's default retry schedule (seven attempts on a
    /// 1s-then-60s backoff, about five minutes).
    #[tokio::test]
    async fn publishing_to_an_unreachable_redis_gives_up_after_one_bounded_attempt() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();

        tokio::time::timeout(
            crate::redis_conn::CONNECT_TIMEOUT * 2,
            publish(&client, vec!["INC-1".to_string()]),
        )
        .await
        .expect("a refused connection must fail the publish at once, not be retried for minutes");
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
