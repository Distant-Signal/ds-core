//! Redis ACL conformance (ingest architecture phase 0c; docs/redis-acl.md).
//!
//! Every user in `charts/distant-signal/files/redis-users.acl.tpl` is created
//! with its `narrow` rights (step 3 of the rollout) on a real Redis or valkey,
//! and then:
//!
//! - connects with `redis://<user>:<password>@...` built by
//!   `common::redis_auth::redis_url_with_credentials` (so the redis crate's
//!   own handshake, AUTH plus CLIENT SETINFO, runs as that user);
//! - runs its client's real command sequence, mirrored from the code that
//!   issues it (comments name the source), all of which must be allowed;
//! - must NOT be allowed a list of commands, checked with `ACL DRYRUN` (Redis
//!   7+) so a wrongly granted FLUSHALL is reported, never executed.
//!
//! Isolation on a shared server: user names and every key pattern get a
//! random prefix, so the real `movement-events` and friends are never
//! touched, and the users and keys are deleted afterwards. Passwords are
//! generated per run.
//!
//! Ignored (needs Redis 7+). CI's rust-test job runs it against its Redis
//! service:
//!
//! ```text
//! cargo test -p common --test redis_acl -- --ignored
//! ```
//!
//! `REDIS_URL` (default `redis://localhost:6379`) and `REDIS_PASSWORD`, if
//! set, name an admin connection: the `default` user, allowed `ACL SETUSER`.

#![expect(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::too_many_lines,
    reason = "test code: a panic is the right failure, and one test walks every user in turn"
)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use common::redis_auth::{redis_url_with_credentials, redis_url_with_password};
use common::secret::Secret;

const TEMPLATE: &str = "../../charts/distant-signal/files/redis-users.acl.tpl";

/// One line of the template: `<user> <kind> <rules...>`.
struct User {
    name: String,
    kind: String,
    rules: String,
}

fn users() -> Vec<User> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(TEMPLATE);
    let text = std::fs::read_to_string(&path).expect("read redis-users.acl.tpl");
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let mut parts = line.splitn(3, char::is_whitespace);
            User {
                name: parts.next().unwrap().to_owned(),
                kind: parts.next().expect("a kind").to_owned(),
                rules: parts.next().unwrap_or("").trim().to_owned(),
            }
        })
        .collect()
}

/// `rules` with `prefix` in front of every key pattern (`~k`, `%R~k`,
/// `%W~k`, `%RW~k`), split into ACL SETUSER arguments (a parenthesised
/// selector is one argument).
fn prefixed_rules(rules: &str, prefix: &str) -> Vec<String> {
    let mut args: Vec<String> = Vec::new();
    let mut selector: Option<String> = None;
    for token in rules.split_whitespace() {
        let token = match token.find('~') {
            Some(tilde)
                if token[..tilde]
                    .trim_start_matches('(')
                    .chars()
                    .all(|c| "%RW".contains(c)) =>
            {
                format!("{}{prefix}{}", &token[..=tilde], &token[tilde + 1..])
            }
            _ => token.to_owned(),
        };
        match selector.as_mut() {
            Some(open) => {
                open.push(' ');
                open.push_str(&token);
                if token.ends_with(')') {
                    args.push(selector.take().unwrap());
                }
            }
            None if token.starts_with('(') && !token.ends_with(')') => selector = Some(token),
            None => args.push(token),
        }
    }
    assert!(selector.is_none(), "unclosed selector in {rules:?}");
    args
}

struct Harness {
    admin: redis::Connection,
    url: String,
    prefix: String,
    passwords: BTreeMap<String, String>,
}

impl Harness {
    fn new() -> Self {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
        let password = std::env::var("REDIS_PASSWORD").ok().map(Secret::from);
        let admin_url = redis_url_with_password(&url, password.as_ref()).unwrap();
        let admin = redis::Client::open(admin_url.expose())
            .unwrap()
            .get_connection()
            .expect("admin connection to REDIS_URL");
        let prefix = format!("acltest{:08x}:", rand_u32());
        Self {
            admin,
            url,
            prefix,
            passwords: BTreeMap::new(),
        }
    }

    fn user(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    fn create(&mut self, user: &User) {
        let rights = match user.kind.as_str() {
            "client" | "final" => prefixed_rules(&user.rules, &self.prefix),
            "admin" => vec!["~*".into(), "&*".into(), "+@all".into()],
            other => panic!("user {} has unknown kind {other}", user.name),
        };
        let password = format!("{:016x}{:016x}", rand_u32(), rand_u32());
        let mut cmd = redis::cmd("ACL");
        cmd.arg("SETUSER")
            .arg(self.user(&user.name))
            .arg("reset")
            .arg("on")
            .arg(format!(">{password}"));
        for right in &rights {
            cmd.arg(right);
        }
        let result: redis::RedisResult<()> = cmd.query(&mut self.admin);
        result.unwrap_or_else(|e| panic!("ACL SETUSER {} {rights:?}: {e}", user.name));
        self.passwords.insert(user.name.clone(), password);
    }

    /// A connection as `user`, through the services' own URL builder.
    fn connect(&self, user: &str) -> redis::Connection {
        let password = Secret::new(self.passwords[user].clone());
        let url =
            redis_url_with_credentials(&self.url, Some(&self.user(user)), Some(&password)).unwrap();
        let mut conn = redis::Client::open(url.expose())
            .unwrap()
            .get_connection()
            .unwrap_or_else(|e| panic!("{user} cannot connect: {e}"));
        // The rest of the handshake the services' connections use.
        let _: String = redis::cmd("PING").query(&mut conn).unwrap();
        let _: () = redis::cmd("CLIENT")
            .arg("SETNAME")
            .arg("acl-test")
            .query(&mut conn)
            .unwrap_or_else(|e| panic!("{user}: CLIENT SETNAME: {e}"));
        let _: i64 = redis::cmd("CLIENT").arg("ID").query(&mut conn).unwrap();
        conn
    }

    /// `ACL DRYRUN` must refuse `args` for `user` (nothing is executed).
    fn forbidden(&mut self, user: &str, args: &[&str]) {
        let mut cmd = redis::cmd("ACL");
        cmd.arg("DRYRUN").arg(self.user(user));
        for arg in args {
            cmd.arg(*arg);
        }
        let reply: redis::RedisResult<redis::Value> = cmd.query(&mut self.admin);
        let refused = match &reply {
            Ok(redis::Value::Okay) => false,
            Ok(redis::Value::SimpleString(s)) => s != "OK",
            Ok(redis::Value::BulkString(b)) => b.as_slice() != b"OK",
            Ok(_) | Err(_) => true,
        };
        assert!(
            refused,
            "{user} must NOT be allowed {args:?}, but ACL DRYRUN said {reply:?}"
        );
    }

    fn cleanup(&mut self) {
        let users: Vec<String> = self.passwords.keys().map(|u| self.user(u)).collect();
        if !users.is_empty() {
            let _: redis::RedisResult<i64> = redis::cmd("ACL")
                .arg("DELUSER")
                .arg(&users)
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

impl Drop for Harness {
    fn drop(&mut self) {
        self.cleanup();
    }
}

/// Run `args` as `conn`; anything but NOPERM/NOAUTH counts as allowed
/// (BUSYGROUP, an empty reply, a missing group...).
fn allowed(user: &str, conn: &mut redis::Connection, args: &[&str]) {
    let mut cmd = redis::cmd(args[0]);
    for arg in &args[1..] {
        cmd.arg(*arg);
    }
    if let Err(e) = cmd.query::<redis::Value>(conn) {
        let text = e.to_string();
        assert!(
            !text.contains("NOPERM") && !text.contains("NOAUTH"),
            "{user} must be allowed {args:?}: {text}"
        );
    }
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

/// The movement-events consumers' sequence (`RedisStreamMovementFeed`,
/// crates/movement-feed/src/redis_stream.rs): group create, the startup
/// checks, read, ack, claim, pending, replay by XRANGE, and the dead-letter
/// write.
fn movement_consumer(h: &mut Harness, user: &str) {
    let mut c = h.connect(user);
    let stream = h.key("movement-events");
    let dlq = h.key("movement-events-deadletter");
    for args in [
        vec!["XGROUP", "CREATE", &stream, user, "$", "MKSTREAM"],
        vec!["XINFO", "GROUPS", &stream],
        vec!["EXISTS", &stream],
        vec!["XINFO", "STREAM", &stream],
        vec![
            "XREADGROUP",
            "GROUP",
            user,
            "c1",
            "COUNT",
            "16",
            "BLOCK",
            "1",
            "STREAMS",
            &stream,
            ">",
        ],
        vec![
            "XREADGROUP",
            "GROUP",
            user,
            "c1",
            "COUNT",
            "16",
            "STREAMS",
            &stream,
            "0",
        ],
        vec!["XACK", &stream, user, "0-1"],
        vec![
            "XAUTOCLAIM",
            &stream,
            user,
            "c1",
            "1000",
            "0-0",
            "COUNT",
            "10",
        ],
        vec!["XCLAIM", &stream, user, "c1", "1000", "0-1"],
        vec!["XPENDING", &stream, user],
        vec!["XPENDING", &stream, user, "-", "+", "10", "c1"],
        vec!["XRANGE", &stream, "-", "+", "COUNT", "10"],
        vec!["XLEN", &stream],
        vec!["XLEN", &dlq],
        vec!["XADD", &dlq, "*", "group", user, "reason", "test"],
    ] {
        allowed(user, &mut c, &args);
    }
    for args in [
        vec!["XADD", stream.as_str(), "*", "type", "x"],
        vec!["XTRIM", stream.as_str(), "MAXLEN", "0"],
        vec!["XTRIM", dlq.as_str(), "MAXLEN", "0"],
        vec!["XGROUP", "DESTROY", stream.as_str(), user],
        vec!["DEL", stream.as_str()],
        vec!["FLUSHALL"],
        vec!["CONFIG", "SET", "maxmemory", "1"],
    ] {
        h.forbidden(user, &args);
    }
    let itc = h.key("incident-text-changed");
    h.forbidden(user, &["XADD", &itc, "*", "a", "b"]);
}

#[test]
#[ignore = "needs a Redis 7+ (REDIS_URL) whose default user may run ACL SETUSER"]
fn every_user_can_run_its_clients_commands_and_nothing_else() {
    let mut h = Harness::new();
    let all = users();
    let names: Vec<&str> = all.iter().map(|u| u.name.as_str()).collect();
    for expected in [
        "movement-relay",
        "trust-consumer",
        "trust-backlog-consumer",
        "full-coverage-consumer",
        "enricher",
        "api",
        "exporter",
        "poller-incidents",
        "poller-ldbws",
        "poller-tfl",
        "poller-tocs",
        "poller-irish-rail-gtfs",
        "poller-irish-rail-live",
        "poller-nir-stations",
        "ingest-writer",
        "ds-admin",
    ] {
        assert!(
            names.contains(&expected),
            "{expected} missing from the template: {names:?}"
        );
    }
    for user in &all {
        h.create(user);
    }

    let stream = h.key("movement-events");
    let dlq = h.key("movement-events-deadletter");
    let itc = h.key("incident-text-changed");

    // movement-relay: crates/movement-relay/src/{event_sink,deadletter,main}.rs.
    {
        let user = "movement-relay";
        let mut c = h.connect(user);
        for args in [
            vec![
                "XGROUP",
                "CREATE",
                &stream,
                "trust-consumer",
                "$",
                "MKSTREAM",
            ],
            vec![
                "XADD", &stream, "MAXLEN", "~", "100", "*", "type", "x", "payload", "y",
            ],
            vec![
                "XADD",
                &stream,
                "NOMKSTREAM",
                "MAXLEN",
                "~",
                "100",
                "*",
                "type",
                "x",
            ],
            vec!["XTRIM", &stream, "MAXLEN", "~", "100"],
            vec!["XLEN", &stream],
            vec!["XLEN", &dlq],
            vec!["XINFO", "GROUPS", &stream],
            vec!["XINFO", "STREAM", &stream],
            vec!["INFO", "persistence"],
            vec!["XTRIM", &dlq, "MINID", "~", "0"],
            vec!["XRANGE", &dlq, "-", "+", "COUNT", "1"],
            vec!["EXISTS", &stream],
        ] {
            allowed(user, &mut c, &args);
        }
        for args in [
            vec![
                "XREADGROUP",
                "GROUP",
                "trust-consumer",
                "c",
                "STREAMS",
                stream.as_str(),
                ">",
            ],
            vec!["XADD", itc.as_str(), "*", "a", "b"],
            vec!["DEL", stream.as_str()],
            vec!["XGROUP", "DESTROY", stream.as_str(), "trust-consumer"],
            vec!["FLUSHALL"],
        ] {
            h.forbidden(user, &args);
        }
    }

    for user in [
        "trust-consumer",
        "trust-backlog-consumer",
        "full-coverage-consumer",
    ] {
        movement_consumer(&mut h, user);
    }
    // Phase 3a: full-coverage-consumer's own ingest stream (spec §7.1). D1:
    // trust-consumer has none.
    {
        let fc = h.key("ds:ingest:full-coverage");
        let mut c = h.connect("full-coverage-consumer");
        allowed(
            "full-coverage-consumer",
            &mut c,
            &["XADD", &fc, "MAXLEN", "~", "360", "*", "v", "1"],
        );
        allowed(
            "full-coverage-consumer",
            &mut c,
            &["XREVRANGE", &fc, "+", "-", "COUNT", "1"],
        );
        h.forbidden("full-coverage-consumer", &["XTRIM", &fc, "MAXLEN", "0"]);
        for user in ["trust-consumer", "trust-backlog-consumer"] {
            h.forbidden(user, &["XADD", &fc, "*", "v", "1"]);
            let te = h.key("ds:ingest:train-events");
            h.forbidden(user, &["XADD", &te, "*", "v", "1"]);
        }
    }

    // enricher: crates/enricher/src/stream.rs.
    {
        let user = "enricher";
        let mut c = h.connect(user);
        for args in [
            vec!["XGROUP", "CREATE", &itc, "enricher", "$", "MKSTREAM"],
            vec!["XINFO", "GROUPS", &itc],
            vec!["EXISTS", &itc],
            vec!["XINFO", "STREAM", &itc],
            vec![
                "XREADGROUP",
                "GROUP",
                "enricher",
                "e1",
                "COUNT",
                "1",
                "BLOCK",
                "1",
                "STREAMS",
                &itc,
                ">",
            ],
            vec!["XACK", &itc, "enricher", "0-1"],
            vec![
                "XAUTOCLAIM",
                &itc,
                "enricher",
                "e1",
                "1000",
                "0-0",
                "COUNT",
                "10",
            ],
        ] {
            allowed(user, &mut c, &args);
        }
        for args in [
            vec!["XADD", itc.as_str(), "*", "a", "b"],
            vec![
                "XREADGROUP",
                "GROUP",
                "x",
                "c",
                "STREAMS",
                stream.as_str(),
                ">",
            ],
            vec!["FLUSHALL"],
        ] {
            h.forbidden(user, &args);
        }
    }

    // api and (phase 2c) poller-incidents: XADD incident-text-changed
    // MAXLEN ~ N (crates/api/src/data/queries.rs). Write only.
    for user in ["api", "poller-incidents"] {
        let mut c = h.connect(user);
        allowed(
            user,
            &mut c,
            &[
                "XADD",
                &itc,
                "MAXLEN",
                "~",
                "10000",
                "*",
                "incident_id",
                "x",
            ],
        );
        for args in [
            vec!["XRANGE", itc.as_str(), "-", "+"],
            vec!["XLEN", itc.as_str()],
            vec!["XADD", stream.as_str(), "*", "a", "b"],
            vec!["DEL", itc.as_str()],
            vec!["FLUSHALL"],
        ] {
            h.forbidden(user, &args);
        }
    }

    // Ranma's redis_exporter: read-only metrics commands.
    {
        let user = "exporter";
        let mut c = h.connect(user);
        let pattern = format!("{}*", h.prefix);
        for args in [
            vec!["INFO", "all"],
            vec!["CONFIG", "GET", "maxmemory"],
            vec!["CLIENT", "LIST"],
            vec!["SLOWLOG", "GET", "1"],
            vec!["SLOWLOG", "LEN"],
            vec!["LATENCY", "LATEST"],
            vec!["XINFO", "STREAM", &stream],
            vec!["XINFO", "GROUPS", &stream],
            vec!["XLEN", &stream],
            vec!["SCAN", "0", "MATCH", &pattern, "COUNT", "10"],
            vec!["TYPE", &stream],
            vec!["MEMORY", "USAGE", &stream],
            vec!["COMMAND", "INFO", "get"],
        ] {
            allowed(user, &mut c, &args);
        }
        for args in [
            vec!["XADD", stream.as_str(), "*", "a", "b"],
            vec!["CONFIG", "SET", "maxmemory", "1"],
            vec!["DEL", stream.as_str()],
            vec!["COMMAND", "DOCS"],
            vec!["FLUSHALL"],
        ] {
            h.forbidden(user, &args);
        }
    }

    // Phase 3 stream producers: XADD to their own stream (MAXLEN on every
    // XADD, spec §7.1) and XREVRANGE COUNT 1 for the startup cursor.
    for (user, own, other) in [
        ("poller-ldbws", "ds:ingest:station-samples", "ds:ingest:tfl"),
        ("poller-tfl", "ds:ingest:tfl", "ds:ingest:station-samples"),
        ("poller-tocs", "ds:ingest:reference", "ds:ingest:tfl"),
        // One stream per island-of-Ireland poller (security review H1):
        // none may write another's.
        (
            "poller-irish-rail-gtfs",
            "ds:ingest:ioi-gtfs",
            "ds:ingest:ioi-nir",
        ),
        (
            "poller-irish-rail-live",
            "ds:ingest:ioi-live",
            "ds:ingest:ioi-gtfs",
        ),
        (
            "poller-nir-stations",
            "ds:ingest:ioi-nir",
            "ds:ingest:ioi-live",
        ),
    ] {
        let own = h.key(own);
        let other = h.key(other);
        let mut c = h.connect(user);
        allowed(
            user,
            &mut c,
            &["XADD", &own, "MAXLEN", "~", "720", "*", "v", "1"],
        );
        allowed(user, &mut c, &["XREVRANGE", &own, "+", "-", "COUNT", "1"]);
        for args in [
            vec!["XADD", other.as_str(), "*", "v", "1"],
            vec!["XTRIM", own.as_str(), "MAXLEN", "0"],
            vec!["XGROUP", "CREATE", own.as_str(), "g", "0"],
            vec!["XADD", stream.as_str(), "*", "v", "1"],
            vec!["FLUSHALL"],
        ] {
            h.forbidden(user, &args);
        }
    }

    // ingest-writer (spec §7.3).
    {
        let user = "ingest-writer";
        let mut c = h.connect(user);
        let s = h.key("ds:ingest:station-samples");
        let d = h.key("ds:dlq:station-samples");
        for args in [
            vec!["XGROUP", "CREATE", &s, "ingest-writer", "0", "MKSTREAM"],
            vec![
                "XREADGROUP",
                "GROUP",
                "ingest-writer",
                "w1",
                "COUNT",
                "16",
                "BLOCK",
                "1",
                "STREAMS",
                &s,
                ">",
            ],
            vec!["XACK", &s, "ingest-writer", "0-1"],
            vec![
                "XAUTOCLAIM",
                &s,
                "ingest-writer",
                "w1",
                "300000",
                "0-0",
                "COUNT",
                "100",
            ],
            vec!["XCLAIM", &s, "ingest-writer", "w1", "1000", "0-1"],
            vec!["XPENDING", &s, "ingest-writer"],
            vec!["XINFO", "STREAM", &s],
            vec!["XINFO", "GROUPS", &s],
            vec!["XINFO", "CONSUMERS", &s, "ingest-writer"],
            vec!["XGROUP", "DELCONSUMER", &s, "ingest-writer", "gone"],
            vec!["XLEN", &s],
            vec!["XRANGE", &d, "-", "+", "COUNT", "10"],
            vec!["XADD", &d, "MAXLEN", "~", "10000", "*", "error", "x"],
            vec!["XTRIM", &d, "MINID", "~", "0"],
            vec!["XLEN", &d],
            vec!["MEMORY", "USAGE", &s],
            vec!["MEMORY", "USAGE", &d],
        ] {
            allowed(user, &mut c, &args);
        }
        // Security review L7: it cannot forge, trim or delete the
        // producers' entries, nor delete dead letters.
        for args in [
            vec!["XADD", s.as_str(), "*", "v", "1"],
            vec!["XTRIM", s.as_str(), "MAXLEN", "0"],
            vec!["XDEL", s.as_str(), "0-1"],
            vec!["XRANGE", s.as_str(), "-", "+"],
            vec!["XDEL", d.as_str(), "0-1"],
            vec!["XGROUP", "DESTROY", s.as_str(), "ingest-writer"],
            vec!["XGROUP", "CREATE", d.as_str(), "g", "0"],
            vec!["XADD", stream.as_str(), "*", "v", "1"],
            vec![
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "STREAMS",
                stream.as_str(),
                ">",
            ],
            vec!["XADD", itc.as_str(), "*", "v", "1"],
            vec!["FLUSHALL"],
        ] {
            h.forbidden(user, &args);
        }
    }

    // ds-admin: everything (checked without running anything destructive).
    {
        let mut c = h.connect("ds-admin");
        allowed("ds-admin", &mut c, &["CONFIG", "GET", "maxmemory"]);
    }
}

#[test]
fn the_template_is_well_formed() {
    let all = users();
    assert!(!all.is_empty());
    let mut seen = std::collections::BTreeSet::new();
    for user in &all {
        assert!(
            seen.insert(user.name.clone()),
            "duplicate user {}",
            user.name
        );
        assert!(
            ["client", "final", "admin"].contains(&user.kind.as_str()),
            "{}: kind {}",
            user.name,
            user.kind
        );
        assert_ne!(
            user.name, "default",
            "default is rendered from redis.acl.defaultUser"
        );
        if user.kind == "admin" {
            assert!(user.rules.is_empty(), "{}: admin takes no rules", user.name);
            continue;
        }
        // Every non-admin user has the handshake and no blanket rights.
        for needed in [
            "+ping",
            "+hello",
            "+auth",
            "+client|setinfo",
            "+client|setname",
        ] {
            assert!(user.rules.contains(needed), "{} lacks {needed}", user.name);
        }
        assert!(!user.rules.contains("+@all"), "{}: +@all", user.name);
        assert!(!user.rules.contains("&*"), "{}: channels", user.name);
        let args = prefixed_rules(&user.rules, "p:");
        for arg in &args {
            if let Some(tilde) = arg.find('~') {
                assert!(
                    arg[tilde + 1..].starts_with("p:"),
                    "{}: unprefixed {arg}",
                    user.name
                );
            }
        }
    }
}

#[test]
fn prefixing_handles_selectors_and_key_permissions() {
    assert_eq!(
        prefixed_rules("~a %W~b +xadd (~c +xlen) (%R~d* +get)", "P:"),
        ["~P:a", "%W~P:b", "+xadd", "(~P:c +xlen)", "(%R~P:d* +get)"]
    );
}
