//! The stream runtime against a real Redis or valkey (ignored; CI's
//! rust-db-test job runs them against its Redis service):
//!
//! ```text
//! cargo test -p ingest-stream --test redis_stream -- --ignored --test-threads=1
//! ```
//!
//! `REDIS_URL` (default `redis://localhost:6379`) and `REDIS_PASSWORD`, if
//! set, name an admin connection (the default user, allowed `ACL SETUSER`
//! for the ACL test). Every test uses its own random key prefix, so the
//! real streams are never touched, and deletes its keys (and users)
//! afterwards.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code: a panic is the right failure"
)]

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use chrono::Utc;
use common::backoff::Backoff;
use common::redis_auth::{redis_url_with_credentials, redis_url_with_password};
use common::redis_conn::RedisConn;
use common::secret::Secret;
use ingest_stream::{
    ConsumerConfig, EncodedEntry, Envelope, Handled, Handler, HandlerError, NotWritten,
    ProducePolicy, Producer, ProducerConfig, RetryReason, SchemaId, Step, StreamConsumer,
    StreamEntry, last_produced_at,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

// ---------------------------------------------------------------------------
// Harness.

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into())
}

fn admin_url() -> String {
    let password = std::env::var("REDIS_PASSWORD").ok().map(Secret::from);
    redis_url_with_password(&redis_url(), password.as_ref())
        .unwrap()
        .expose()
        .to_owned()
}

/// One process-wide recorder, so tests can read the runtime's metrics
/// (each test's series carry its own unique stream label).
fn metrics_handle() -> &'static PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE.get_or_init(|| PrometheusBuilder::new().install_recorder().unwrap())
}

/// The value of the rendered series that starts with `series` (`name{...}`
/// with the labels in emission order), or 0.
fn metric(series: &str) -> u64 {
    metrics_handle()
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(series)?.trim().parse().ok())
        .unwrap_or(0)
}

fn rand_u32() -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    );
    #[expect(clippy::cast_possible_truncation, reason = "any 32 bits will do")]
    let value = hasher.finish() as u32;
    value
}

/// A random key prefix plus cleanup of its keys and users.
struct Scope {
    prefix: String,
    admin: redis::Connection,
    users: Vec<String>,
}

impl Scope {
    fn new() -> Self {
        metrics_handle();
        let admin = redis::Client::open(admin_url())
            .unwrap()
            .get_connection()
            .expect("admin connection to REDIS_URL");
        Self {
            prefix: format!("ingtest{:08x}:", rand_u32()),
            admin,
            users: Vec::new(),
        }
    }

    fn stream(&self, domain: &str) -> String {
        format!("{}ds:ingest:{domain}", self.prefix)
    }

    fn cmd<T: redis::FromRedisValue>(&mut self, args: &[&str]) -> T {
        let mut cmd = redis::cmd(args[0]);
        for arg in &args[1..] {
            cmd.arg(*arg);
        }
        cmd.query(&mut self.admin)
            .unwrap_or_else(|e| panic!("{args:?}: {e}"))
    }

    fn xlen(&mut self, key: &str) -> u64 {
        self.cmd(&["XLEN", key])
    }

    /// `XRANGE key - +` as id → field map.
    fn range(&mut self, key: &str) -> Vec<(String, HashMap<String, String>)> {
        let reply: redis::streams::StreamRangeReply = self.cmd(&["XRANGE", key, "-", "+"]);
        reply
            .ids
            .into_iter()
            .map(|e| {
                let fields = e
                    .map
                    .iter()
                    .map(|(k, v)| {
                        let bytes: Vec<u8> = redis::from_redis_value(v).unwrap();
                        (k.clone(), String::from_utf8_lossy(&bytes).into_owned())
                    })
                    .collect();
                (e.id, fields)
            })
            .collect()
    }

    fn pending(&mut self, stream: &str) -> u64 {
        let reply: redis::Value = self.cmd(&["XPENDING", stream, ingest_stream::WRITER_GROUP]);
        match reply {
            redis::Value::Array(parts) => redis::from_redis_value(&parts[0]).unwrap(),
            other => panic!("{other:?}"),
        }
    }

    /// Reads up to `count` new entries as `consumer` without acking them: a
    /// consumer that crashes right after its read.
    fn read_and_crash(&mut self, stream: &str, consumer: &str, count: usize) {
        let _: redis::Value = self.cmd(&[
            "XREADGROUP",
            "GROUP",
            ingest_stream::WRITER_GROUP,
            consumer,
            "COUNT",
            &count.to_string(),
            "STREAMS",
            stream,
            ">",
        ]);
    }

    fn create_group(&mut self, stream: &str) {
        let _: () = self.cmd(&[
            "XGROUP",
            "CREATE",
            stream,
            ingest_stream::WRITER_GROUP,
            "0",
            "MKSTREAM",
        ]);
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        if !self.users.is_empty() {
            let _: redis::RedisResult<i64> = redis::cmd("ACL")
                .arg("DELUSER")
                .arg(&self.users)
                .query(&mut self.admin);
        }
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{}*", self.prefix))
            .query(&mut self.admin)
            .unwrap_or_default();
        if !keys.is_empty() {
            let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(&keys).query(&mut self.admin);
        }
    }
}

async fn connect(url: &str) -> RedisConn {
    common::redis_conn::connect(&redis::Client::open(url).unwrap())
        .await
        .unwrap()
}

fn schema() -> SchemaId {
    SchemaId::new("test-rows", 1).unwrap()
}

fn entry(key: &str) -> EncodedEntry {
    Envelope::new(
        schema(),
        "test/pod",
        key,
        Utc::now(),
        &serde_json::json!({ "key": key }),
    )
    .unwrap()
    .encode()
    .unwrap()
}

const FAST: Backoff = Backoff::new(Duration::from_millis(10), Duration::from_millis(50));

fn producer_config(stream: &str, maxlen: u64, policy: ProducePolicy) -> ProducerConfig {
    let mut config = ProducerConfig::new(stream, maxlen, policy);
    config.backoff = FAST;
    config
}

fn consumer_config(stream: &str, consumer: &str) -> ConsumerConfig {
    let mut config = ConsumerConfig::new(stream, consumer, 1000);
    config.block = Duration::from_millis(100);
    config.backoff = FAST;
    config
}

/// A handler that records what it saw and answers from a per-key script
/// (default: applied).
#[derive(Default)]
struct Scripted {
    seen: Mutex<Vec<String>>,
    script: Mutex<HashMap<String, VecDeque<Result<Handled, HandlerError>>>>,
}

impl Scripted {
    fn answer(&self, key: &str, answers: Vec<Result<Handled, HandlerError>>) {
        self.script
            .lock()
            .unwrap()
            .insert(key.to_owned(), answers.into());
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

impl Handler for Scripted {
    fn handle(
        &self,
        entry: &StreamEntry,
    ) -> impl Future<Output = Result<Handled, HandlerError>> + Send {
        let key = entry.envelope.key.clone();
        self.seen.lock().unwrap().push(key.clone());
        let answer = self
            .script
            .lock()
            .unwrap()
            .get_mut(&key)
            .and_then(VecDeque::pop_front)
            .unwrap_or(Ok(Handled::Applied));
        std::future::ready(answer)
    }
}

async fn step(consumer: &mut StreamConsumer, handler: &Scripted) -> Step {
    let never = std::future::pending::<()>();
    tokio::pin!(never);
    consumer.step(handler, never.as_mut()).await.unwrap()
}

/// Steps until a step returns `Idle` (nothing new, PEL drained), at most
/// `max` steps. Returns the steps taken.
async fn drain(consumer: &mut StreamConsumer, handler: &Scripted, max: usize) -> Vec<Step> {
    let mut steps = Vec::new();
    for _ in 0..max {
        let s = step(consumer, handler).await;
        let idle = s == Step::Idle;
        steps.push(s);
        if idle {
            return steps;
        }
    }
    panic!("not idle after {max} steps: {steps:?}");
}

async fn produce_all(producer: &Producer, keys: &[&str]) -> Vec<String> {
    let mut ids = Vec::new();
    for key in keys {
        let receipt = producer.submit(vec![entry(key)]).await.unwrap();
        ids.extend(receipt.written().await.unwrap());
    }
    ids
}

// ---------------------------------------------------------------------------
// Tests.

#[tokio::test]
#[ignore = "needs Redis (REDIS_URL)"]
async fn produce_and_consume_round_trip_in_order() {
    let mut scope = Scope::new();
    let stream = scope.stream("roundtrip");
    let (producer, task) = Producer::spawn(
        redis::Client::open(admin_url()).unwrap(),
        producer_config(
            &stream,
            100,
            ProducePolicy::Event {
                max_buffered: NonZeroUsize::new(8).unwrap(),
            },
        ),
    );
    let mut consumer = StreamConsumer::new(
        connect(&admin_url()).await,
        consumer_config(&stream, "pod-a"),
    );
    consumer.ensure_group().await.unwrap();
    let ids = produce_all(&producer, &["k1", "k2", "k3"]).await;
    assert_eq!(ids.len(), 3);

    // A three-part snapshot is written part by part, in order.
    let rows: Vec<u32> = (0..30).collect();
    let parts =
        ingest_stream::split_snapshot(&schema(), "test/pod", Utc::now(), "b1", &rows, 10, |c| {
            serde_json::value::to_raw_value(c)
        })
        .unwrap();
    let written = producer
        .submit(parts)
        .await
        .unwrap()
        .written()
        .await
        .unwrap();
    assert_eq!(written.len(), 3);

    let handler = Scripted::default();
    let steps = drain(&mut consumer, &handler, 10).await;
    assert!(steps.contains(&Step::Processed(6)), "{steps:?}");
    assert_eq!(
        handler.seen(),
        vec![
            "k1",
            "k2",
            "k3",
            "test-rows:b1:1/3",
            "test-rows:b1:2/3",
            "test-rows:b1:3/3"
        ]
    );
    assert_eq!(scope.pending(&stream), 0);
    let entries = scope.range(&stream);
    assert_eq!(entries[0].1["schema"], "test-rows/1");
    assert_eq!(entries[0].1["enc"], "json");
    assert_eq!(entries[3].1["part"], "1");

    // The producer cursor is the newest entry's produced_at.
    let mut conn = connect(&admin_url()).await;
    let cursor = last_produced_at(&mut conn, &stream).await.unwrap().unwrap();
    assert_eq!(
        ingest_stream::envelope::format_produced_at(cursor),
        entries[5].1["produced_at"]
    );
    assert!(
        last_produced_at(&mut conn, &scope.stream("missing"))
            .await
            .unwrap()
            .is_none()
    );

    let label = format!("stream=\"{stream}\"");
    assert_eq!(
        metric(&format!(
            "distant_signal_ingest_stream_produce_total{{{label},outcome=\"ok\"}}"
        )),
        6
    );
    assert_eq!(
        metric(&format!(
            "distant_signal_ingest_stream_consumed_total{{{label},schema=\"test-rows/1\",outcome=\"applied\"}}"
        )),
        6
    );
    let stats = consumer.sample_gauges().await.unwrap();
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.lag, Some(0));
    assert!(stats.bytes.unwrap() > 0);
    assert!(producer.shutdown(task, Duration::from_secs(5)).await);
}

#[tokio::test]
#[ignore = "needs Redis (REDIS_URL)"]
async fn entries_a_crashed_consumer_left_pending_are_reclaimed_and_applied_first() {
    let mut scope = Scope::new();
    let stream = scope.stream("reclaim");
    scope.create_group(&stream);
    let mut conn = connect(&admin_url()).await;
    for key in ["a1", "a2", "b1"] {
        ingest_stream::xadd_entry(&mut conn, &stream, 100, &entry(key))
            .await
            .unwrap();
    }
    // pod-a reads a1 and crashes; pod-old reads a2 and is never seen again.
    scope.read_and_crash(&stream, "pod-a", 1);
    scope.read_and_crash(&stream, "pod-old", 1);
    assert_eq!(scope.pending(&stream), 2);

    // pod-a restarts under the same name: its own PEL (a1) comes first,
    // then new entries (b1). pod-old's a2 is only claimed once idle.
    let mut config = consumer_config(&stream, "pod-a");
    config.claim_min_idle = Duration::from_millis(300);
    config.claim_interval = Duration::from_millis(300);
    let mut consumer = StreamConsumer::new(connect(&admin_url()).await, config);
    let handler = Scripted::default();
    assert_eq!(step(&mut consumer, &handler).await, Step::Processed(1));
    assert_eq!(step(&mut consumer, &handler).await, Step::PelDrained);
    assert_eq!(step(&mut consumer, &handler).await, Step::Processed(1));
    assert_eq!(handler.seen(), vec!["a1", "b1"]);
    assert_eq!(scope.pending(&stream), 1);

    tokio::time::sleep(Duration::from_millis(400)).await;
    drain(&mut consumer, &handler, 10).await;
    assert_eq!(handler.seen(), vec!["a1", "b1", "a2"]);
    assert_eq!(scope.pending(&stream), 0);

    // pod-old has nothing pending now; once idle long enough it is removed.
    let mut hygiene = consumer_config(&stream, "pod-a");
    hygiene.delete_idle_consumers_after = Duration::from_millis(1);
    let mut sweeper = StreamConsumer::new(connect(&admin_url()).await, hygiene);
    let removed = sweeper.delete_idle_consumers().await.unwrap();
    assert_eq!(removed, vec!["pod-old".to_owned()]);
}

#[tokio::test]
#[ignore = "needs Redis (REDIS_URL)"]
async fn poison_and_undecodable_entries_are_dead_lettered_then_acked() {
    let mut scope = Scope::new();
    let stream = scope.stream("poison");
    let dlq = ingest_stream::dead_letter_stream(&stream);
    let mut conn = connect(&admin_url()).await;
    let mut consumer = StreamConsumer::new(
        connect(&admin_url()).await,
        consumer_config(&stream, "pod-a"),
    );
    consumer.ensure_group().await.unwrap();
    for key in ["good1", "bad", "good2"] {
        ingest_stream::xadd_entry(&mut conn, &stream, 100, &entry(key))
            .await
            .unwrap();
    }
    // No envelope at all, and a body over the 1 MiB accept limit.
    let _: String = scope.cmd(&["XADD", &stream, "*", "hello", "world"]);
    let big = "x".repeat(1024 * 1024 + 1);
    let _: String = scope.cmd(&[
        "XADD",
        &stream,
        "*",
        "v",
        "1",
        "schema",
        "test-rows/1",
        "producer",
        "p",
        "key",
        "big",
        "produced_at",
        "2026-10-06T20:21:00Z",
        "enc",
        "json",
        "body",
        &big,
    ]);

    let handler = Scripted::default();
    handler.answer(
        "bad",
        vec![Err(HandlerError::Poison("unknown station".into()))],
    );
    drain(&mut consumer, &handler, 10).await;
    assert_eq!(handler.seen(), vec!["good1", "bad", "good2"]);
    assert_eq!(scope.pending(&stream), 0);

    let dead = scope.range(&dlq);
    assert_eq!(dead.len(), 3, "{dead:?}");
    let source_ids: Vec<String> = scope.range(&stream).into_iter().map(|(id, _)| id).collect();
    let (_, bad) = &dead[0];
    assert_eq!(bad["key"], "bad");
    assert_eq!(bad["reason"], "poison");
    assert_eq!(bad["error"], "unknown station");
    assert_eq!(bad["deliveries"], "1");
    assert_eq!(bad["source_stream"], stream);
    assert_eq!(bad["source_id"], source_ids[1]);
    assert_eq!(bad["schema"], "test-rows/1");
    assert!(bad.contains_key("body") && bad.contains_key("failed_at"));
    assert_eq!(dead[1].1["reason"], "undecodable");
    assert_eq!(dead[1].1["hello"], "world");
    assert_eq!(dead[2].1["reason"], "oversize");

    let label = format!("stream=\"{stream}\"");
    assert_eq!(
        metric(&format!(
            "distant_signal_ingest_stream_consumed_total{{{label},schema=\"test-rows/1\",outcome=\"dead_lettered\"}}"
        )),
        2
    );
    assert_eq!(
        metric(&format!(
            "distant_signal_ingest_stream_dead_lettered_total{{{label},reason=\"poison\"}}"
        )),
        1
    );
    let stats = consumer.sample_gauges().await.unwrap();
    assert_eq!(stats.dead_letter_length, 3);
    assert!(stats.dead_letter_oldest_age.unwrap() < Duration::from_secs(60));
}

#[tokio::test]
#[ignore = "needs Redis (REDIS_URL)"]
async fn a_transient_failure_is_retried_in_order_and_never_acked_or_dead_lettered() {
    let mut scope = Scope::new();
    let stream = scope.stream("transient");
    let mut conn = connect(&admin_url()).await;
    let mut consumer = StreamConsumer::new(
        connect(&admin_url()).await,
        consumer_config(&stream, "pod-a"),
    );
    consumer.ensure_group().await.unwrap();
    for key in ["x", "y"] {
        ingest_stream::xadd_entry(&mut conn, &stream, 100, &entry(key))
            .await
            .unwrap();
    }
    let handler = Scripted::default();
    handler.answer(
        "x",
        vec![
            Err(HandlerError::Transient("pool timed out".into())),
            Err(HandlerError::Transient("pool timed out".into())),
        ],
    );
    assert_eq!(step(&mut consumer, &handler).await, Step::PelDrained);
    let retry = Step::Retry {
        processed: 0,
        reason: RetryReason::Transient,
    };
    assert_eq!(step(&mut consumer, &handler).await, retry);
    // Both were delivered (one read of COUNT 16); neither is acked, and y
    // was not handled out of order.
    assert_eq!(scope.pending(&stream), 2);
    assert_eq!(handler.seen(), vec!["x"]);
    // The retry re-reads the PEL from the start.
    assert_eq!(step(&mut consumer, &handler).await, retry);
    assert_eq!(step(&mut consumer, &handler).await, Step::Processed(2));
    assert_eq!(handler.seen(), vec!["x", "x", "x", "y"]);
    assert_eq!(scope.pending(&stream), 0);
    assert_eq!(scope.xlen(&ingest_stream::dead_letter_stream(&stream)), 0);
    let label = format!("stream=\"{stream}\"");
    assert_eq!(
        metric(&format!(
            "distant_signal_ingest_stream_consumed_total{{{label},schema=\"test-rows/1\",outcome=\"transient_error\"}}"
        )),
        2
    );
}

#[tokio::test]
#[ignore = "needs Redis (REDIS_URL)"]
async fn unsupported_schemas_and_envelopes_stay_pending() {
    let mut scope = Scope::new();
    let stream = scope.stream("unsupported");
    let mut conn = connect(&admin_url()).await;
    let mut consumer = StreamConsumer::new(
        connect(&admin_url()).await,
        consumer_config(&stream, "pod-a"),
    );
    consumer.ensure_group().await.unwrap();
    ingest_stream::xadd_entry(&mut conn, &stream, 100, &entry("future-schema"))
        .await
        .unwrap();
    let handler = Scripted::default();
    handler.answer(
        "future-schema",
        vec![Err(HandlerError::UnsupportedSchema("test-rows/2".into()))],
    );
    let unsupported = Step::Retry {
        processed: 0,
        reason: RetryReason::Unsupported,
    };
    drain_until(&mut consumer, &handler, &unsupported).await;
    assert_eq!(scope.pending(&stream), 1);
    // The next attempt succeeds (as after the writer is rolled forward).
    drain(&mut consumer, &handler, 10).await;
    assert_eq!(handler.seen(), vec!["future-schema", "future-schema"]);
    assert_eq!(scope.pending(&stream), 0);

    // Envelope v2 from a newer producer: left pending, never handed to the
    // handler, never dead-lettered, retried.
    let mut v2 = entry("v2");
    v2.fields[0].1 = b"2".to_vec();
    ingest_stream::xadd_entry(&mut conn, &stream, 100, &v2)
        .await
        .unwrap();
    drain_until(&mut consumer, &handler, &unsupported).await;
    drain_until(&mut consumer, &handler, &unsupported).await;
    assert_eq!(handler.seen().len(), 2);
    assert_eq!(scope.pending(&stream), 1);
    assert_eq!(scope.xlen(&ingest_stream::dead_letter_stream(&stream)), 0);
    assert!(
        metric(&format!(
            "distant_signal_ingest_stream_consumed_total{{stream=\"{stream}\",schema=\"test-rows/1\",outcome=\"unsupported_schema\"}}"
        )) >= 3
    );
}

/// Steps until `want` comes back (at most 10 steps).
async fn drain_until(consumer: &mut StreamConsumer, handler: &Scripted, want: &Step) {
    for _ in 0..10 {
        if &step(consumer, handler).await == want {
            return;
        }
    }
    panic!("never saw {want:?}");
}

#[tokio::test]
#[ignore = "needs Redis (REDIS_URL)"]
async fn maxlen_trims_the_stream_and_a_trimmed_pending_entry_is_acked() {
    let mut scope = Scope::new();
    let stream = scope.stream("trim");
    scope.create_group(&stream);
    let mut conn = connect(&admin_url()).await;
    ingest_stream::xadd_entry(&mut conn, &stream, 10, &entry("first"))
        .await
        .unwrap();
    scope.read_and_crash(&stream, "pod-a", 1);
    for i in 0..1000 {
        ingest_stream::xadd_entry(&mut conn, &stream, 10, &entry(&format!("e{i}")))
            .await
            .unwrap();
    }
    // `MAXLEN ~ 10` trims whole nodes (100 entries by default).
    let len = scope.xlen(&stream);
    assert!((10..=110).contains(&len), "XLEN {len}");
    assert!(
        !scope
            .range(&stream)
            .iter()
            .any(|(_, f)| f["key"] == "first")
    );

    // pod-a comes back: "first" is still in its PEL but its body is gone.
    // Either its startup XAUTOCLAIM drops it from the PEL (valkey, and
    // Redis 7+ once it is idle long enough) or its PEL read returns it with
    // no fields and it is acked; either way it is counted as trimmed.
    let mut consumer = StreamConsumer::new(
        connect(&admin_url()).await,
        consumer_config(&stream, "pod-a"),
    );
    let handler = Scripted::default();
    let first = step(&mut consumer, &handler).await;
    assert!(
        first == Step::Processed(1) || first == Step::PelDrained,
        "{first:?}"
    );
    assert!(handler.seen().is_empty());
    assert_eq!(scope.pending(&stream), 0);
    assert_eq!(
        metric(&format!(
            "distant_signal_ingest_stream_consumed_total{{stream=\"{stream}\",schema=\"unknown\",outcome=\"trimmed\"}}"
        )),
        1
    );
}

/// A free local port: bound, then released.
async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// Forwards `port` to the real Redis: "Redis comes back".
fn start_forwarder(listener: tokio::net::TcpListener) -> tokio::task::JoinHandle<()> {
    let target = match redis::Client::open(redis_url())
        .unwrap()
        .get_connection_info()
        .addr
        .clone()
    {
        redis::ConnectionAddr::Tcp(host, port) => format!("{host}:{port}"),
        other => panic!("REDIS_URL must be TCP: {other:?}"),
    };
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                return;
            };
            let target = target.clone();
            tokio::spawn(async move {
                if let Ok(mut outbound) = tokio::net::TcpStream::connect(&target).await {
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                }
            });
        }
    })
}

fn url_on_port(port: u16) -> String {
    let password = std::env::var("REDIS_PASSWORD").ok().map(Secret::from);
    redis_url_with_password(&format!("redis://127.0.0.1:{port}"), password.as_ref())
        .unwrap()
        .expose()
        .to_owned()
}

async fn wait_until(what: &str, mut ok: impl FnMut() -> bool) {
    for _ in 0..200 {
        if ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
#[ignore = "needs Redis (REDIS_URL)"]
async fn latest_snapshot_keeps_only_the_newest_item_while_redis_is_down() {
    let mut scope = Scope::new();
    let stream = scope.stream("latest");
    let port = free_port().await;
    let (producer, task) = Producer::spawn(
        redis::Client::open(url_on_port(port)).unwrap(),
        producer_config(&stream, 100, ProducePolicy::LatestSnapshot),
    );
    let first = producer.submit(vec![entry("s1")]).await.unwrap();
    wait_until("the producer to notice Redis is down", || {
        !producer.is_available()
    })
    .await;
    let second = producer.submit(vec![entry("s2")]).await.unwrap();
    // A multi-part snapshot is one item.
    let third = producer
        .submit(vec![entry("s3-part1"), entry("s3-part2")])
        .await
        .unwrap();
    assert_eq!(first.written().await, Err(NotWritten::Superseded));
    assert_eq!(second.written().await, Err(NotWritten::Superseded));
    assert_eq!(producer.buffered(), 1);
    assert_eq!(
        metric(&format!(
            "distant_signal_ingest_stream_produce_dropped_total{{stream=\"{stream}\",reason=\"superseded\"}}"
        )),
        2
    );
    assert!(
        metric(&format!(
            "distant_signal_ingest_stream_produce_total{{stream=\"{stream}\",outcome=\"down\"}}"
        )) >= 1
    );

    // Redis comes back: only the newest snapshot is written.
    let forwarder = start_forwarder(
        tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap(),
    );
    let ids = tokio::time::timeout(Duration::from_secs(10), third.written())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ids.len(), 2);
    assert!(producer.is_available());
    let keys: Vec<String> = scope
        .range(&stream)
        .into_iter()
        .map(|(_, f)| f["key"].clone())
        .collect();
    assert_eq!(keys, vec!["s3-part1", "s3-part2"]);
    assert!(producer.shutdown(task, Duration::from_secs(5)).await);
    forwarder.abort();
}

#[tokio::test]
#[ignore = "needs Redis (REDIS_URL)"]
async fn event_policy_buffers_then_backpressures_while_redis_is_down() {
    let mut scope = Scope::new();
    let stream = scope.stream("event");
    let port = free_port().await;
    let (producer, task) = Producer::spawn(
        redis::Client::open(url_on_port(port)).unwrap(),
        producer_config(
            &stream,
            100,
            ProducePolicy::Event {
                max_buffered: NonZeroUsize::new(2).unwrap(),
            },
        ),
    );
    let e1 = producer.submit(vec![entry("e1")]).await.unwrap();
    let e2 = producer.submit(vec![entry("e2")]).await.unwrap();
    wait_until("the producer to notice Redis is down", || {
        !producer.is_available()
    })
    .await;
    // The buffer is full: the third submit waits (the caller does not ack
    // its upstream).
    let third = {
        let producer = producer.clone();
        tokio::spawn(async move { producer.submit(vec![entry("e3")]).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !third.is_finished(),
        "submit must wait while the buffer is full"
    );
    assert_eq!(producer.buffered(), 2);

    let forwarder = start_forwarder(
        tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap(),
    );
    let e3 = tokio::time::timeout(Duration::from_secs(10), third)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for receipt in [e1, e2, e3] {
        tokio::time::timeout(Duration::from_secs(10), receipt.written())
            .await
            .unwrap()
            .unwrap();
    }
    let keys: Vec<String> = scope
        .range(&stream)
        .into_iter()
        .map(|(_, f)| f["key"].clone())
        .collect();
    assert_eq!(keys, vec!["e1", "e2", "e3"], "nothing dropped, in order");
    assert_eq!(
        metric(&format!(
            "distant_signal_ingest_stream_produce_dropped_total{{stream=\"{stream}\",reason=\"superseded\"}}"
        )),
        0
    );
    assert!(producer.shutdown(task, Duration::from_secs(5)).await);
    forwarder.abort();
}

#[tokio::test]
#[ignore = "needs Redis (REDIS_URL)"]
async fn shutdown_while_down_gives_up_after_the_grace_period() {
    let scope = Scope::new();
    let stream = scope.stream("shutdown");
    let port = free_port().await;
    let (producer, task) = Producer::spawn(
        redis::Client::open(url_on_port(port)).unwrap(),
        producer_config(&stream, 100, ProducePolicy::LatestSnapshot),
    );
    let receipt = producer.submit(vec![entry("never")]).await.unwrap();
    assert!(!producer.shutdown(task, Duration::from_millis(200)).await);
    assert_eq!(receipt.written().await, Err(NotWritten::Closed));
    assert_eq!(
        producer.submit(vec![entry("late")]).await.unwrap_err(),
        NotWritten::Closed
    );
}

// ---------------------------------------------------------------------------
// Restricted ACL users, from the chart's template (phase 0c).

const TEMPLATE: &str = "../../charts/distant-signal/files/redis-users.acl.tpl";

/// The template's rules for `user`, with `prefix` before every key pattern.
fn acl_rules(user: &str, prefix: &str) -> Vec<String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(TEMPLATE);
    let text = std::fs::read_to_string(&path).unwrap();
    let line = text
        .lines()
        .find(|l| l.split_whitespace().next() == Some(user))
        .unwrap_or_else(|| panic!("{user} not in {}", path.display()));
    let mut parts = line.split_whitespace().skip(2);
    let mut rules = Vec::new();
    for token in parts.by_ref() {
        assert!(
            !token.starts_with('('),
            "selectors are not handled here: {line}"
        );
        rules.push(match token.find('~') {
            Some(at) => format!("{}{prefix}{}", &token[..=at], &token[at + 1..]),
            None => token.to_owned(),
        });
    }
    rules
}

impl Scope {
    /// Creates `<prefix><user>` with the template's rules; returns its URL.
    fn acl_user(&mut self, user: &str) -> String {
        let name = format!("{}{user}", self.prefix);
        let password = format!("{:08x}{:08x}", rand_u32(), rand_u32());
        let mut cmd = redis::cmd("ACL");
        cmd.arg("SETUSER")
            .arg(&name)
            .arg("reset")
            .arg("on")
            .arg(format!(">{password}"));
        for rule in acl_rules(user, &self.prefix) {
            cmd.arg(rule);
        }
        let () = cmd.query(&mut self.admin).unwrap();
        self.users.push(name.clone());
        redis_url_with_credentials(&redis_url(), Some(&name), Some(&Secret::new(password)))
            .unwrap()
            .expose()
            .to_owned()
    }
}

#[tokio::test]
#[ignore = "needs Redis 7+ (REDIS_URL) whose default user may run ACL SETUSER"]
async fn producers_and_the_writer_run_under_their_restricted_acl_users() {
    let mut scope = Scope::new();
    let samples = scope.stream("station-samples");
    let tfl = scope.stream("tfl");
    let producer_url = scope.acl_user("poller-ldbws");
    let writer_url = scope.acl_user("ingest-writer");

    // poller-ldbws may XADD (and XREVRANGE) its own stream only.
    let (producer, task) = Producer::spawn(
        redis::Client::open(producer_url.as_str()).unwrap(),
        producer_config(&samples, 720, ProducePolicy::LatestSnapshot),
    );
    let written = producer
        .submit(vec![entry("ok"), entry("poison"), entry("ok2")])
        .await
        .unwrap()
        .written()
        .await
        .unwrap();
    assert_eq!(written.len(), 3);
    let mut as_producer = connect(&producer_url).await;
    assert!(
        last_produced_at(&mut as_producer, &samples)
            .await
            .unwrap()
            .is_some()
    );
    let denied = ingest_stream::xadd_entry(&mut as_producer, &tfl, 10, &entry("x")).await;
    assert_eq!(
        denied.map_err(|e| ingest_stream::producer::classify(&e)),
        Err("noperm")
    );
    let denied: redis::RedisResult<i64> = redis::cmd("XLEN")
        .arg(&samples)
        .query_async(&mut as_producer)
        .await;
    assert!(denied.is_err(), "XLEN is not in its rights");
    assert!(producer.shutdown(task, Duration::from_secs(5)).await);

    // A producer pointed at another stream reports noperm and goes
    // unavailable.
    let (wrong, wrong_task) = Producer::spawn(
        redis::Client::open(producer_url.as_str()).unwrap(),
        producer_config(&tfl, 288, ProducePolicy::LatestSnapshot),
    );
    let _receipt = wrong.submit(vec![entry("x")]).await.unwrap();
    wait_until("noperm", || !wrong.is_available()).await;
    assert!(
        metric(&format!(
            "distant_signal_ingest_stream_produce_total{{stream=\"{tfl}\",outcome=\"noperm\"}}"
        )) >= 1
    );
    assert!(!wrong.shutdown(wrong_task, Duration::from_millis(50)).await);

    // ingest-writer runs the whole consumer runtime: group create, PEL,
    // XAUTOCLAIM, XPENDING, dead letters, gauges (XINFO, MEMORY USAGE) and
    // consumer hygiene.
    let mut config = consumer_config(&samples, "writer-pod");
    config.delete_idle_consumers_after = Duration::from_millis(1);
    let mut consumer = StreamConsumer::new(connect(&writer_url).await, config);
    consumer.ensure_group().await.unwrap();
    let handler = Scripted::default();
    handler.answer("poison", vec![Err(HandlerError::Poison("bad row".into()))]);
    drain(&mut consumer, &handler, 10).await;
    assert_eq!(handler.seen(), vec!["ok", "poison", "ok2"]);
    assert_eq!(scope.pending(&samples), 0);
    assert_eq!(scope.xlen(&ingest_stream::dead_letter_stream(&samples)), 1);
    let stats = consumer.sample_gauges().await.unwrap();
    assert_eq!(stats.dead_letter_length, 1);
    assert!(stats.bytes.is_some());
    consumer.reclaim().await.unwrap();
    consumer.delete_idle_consumers().await.unwrap();
}

/// `run` returns promptly on shutdown while blocked on a read, and does not
/// cancel a handler that is running.
#[tokio::test]
#[ignore = "needs Redis (REDIS_URL)"]
async fn run_stops_on_shutdown_between_entries() {
    struct Slow(Mutex<Vec<String>>);
    impl Handler for Slow {
        fn handle(
            &self,
            entry: &StreamEntry,
        ) -> impl Future<Output = Result<Handled, HandlerError>> + Send {
            let key = entry.envelope.key.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                self.0.lock().unwrap().push(key);
                Ok(Handled::Applied)
            }
        }
    }
    let mut scope = Scope::new();
    let stream = scope.stream("shutdown-run");
    scope.create_group(&stream);
    let mut conn = connect(&admin_url()).await;
    ingest_stream::xadd_entry(&mut conn, &stream, 100, &entry("slow"))
        .await
        .unwrap();
    let mut consumer = StreamConsumer::new(
        connect(&admin_url()).await,
        consumer_config(&stream, "pod-a"),
    );
    let handler = Slow(Mutex::new(Vec::new()));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let shutdown: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move {
        let _ = rx.await;
    });
    let started = std::time::Instant::now();
    let run = consumer.run(&handler, shutdown);
    let fire = async {
        // Fires while the slow handler is running.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = tx.send(());
    };
    tokio::join!(run, fire);
    assert_eq!(*handler.0.lock().unwrap(), vec!["slow".to_owned()]);
    assert_eq!(
        scope.pending(&stream),
        0,
        "the running entry finished and was acked"
    );
    assert!(started.elapsed() < Duration::from_secs(3));
}
