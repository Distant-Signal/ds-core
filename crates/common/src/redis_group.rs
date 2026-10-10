//! Keeping a Redis Streams consumer group from going backwards (2026-10-09
//! redelivery review).
//!
//! A consumer group's `last-delivered-id` lives in Redis. When Redis
//! restarts and reloads an older copy of its data (AOF `everysec` loses up
//! to the last second of writes; a restored snapshot loses more), the group
//! still exists but stands where it stood then: the next `XREADGROUP ... >`
//! hands out again every entry the consumer already read and acted on.
//! `NOGROUP` recovery (`movement_feed::redis_stream`, `enricher::stream`)
//! does not see this, since nothing is missing.
//!
//! Each consumer remembers the highest id it was handed. After a failed
//! command (a Redis restart breaks the connection, so the in-flight
//! `XREADGROUP` fails), it calls [`restore_group_position`], which compares
//! that id with the group's `last-delivered-id` (`XINFO GROUPS`) and, when
//! the group is behind, moves it forward with `XGROUP SETID`. Not checked
//! on a healthy connection, so an operator's deliberate `XGROUP SETID`
//! rewind of a running consumer is left alone.
//!
//! Not restored: the group's pending-entries list. Entries the consumer
//! `ACKed` after the reloaded copy was written are pending again, and are
//! redelivered once idle (`XAUTOCLAIM`); every consumer's writes are
//! idempotent per entry, so that costs duplicates, not wrong state.
//!
//! Needs the ACL rights `xinfo|groups` and `xgroup|setid` (see
//! `charts/distant-signal/files/redis-users.acl.tpl`).

use std::collections::HashMap;

use redis::aio::ConnectionLike;

/// What [`restore_group_position`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupPosition {
    /// The group was behind `remembered` and was moved forward to it;
    /// `was` is where it stood.
    Restored { was: String },
    /// The group is at or past `remembered`; `at` is its
    /// `last-delivered-id`, which the caller should adopt.
    Current { at: String },
    /// The stream or the group does not exist (left to `NOGROUP`
    /// recovery), or reports no `last-delivered-id`.
    Missing,
}

/// Moves consumer group `group` of `stream` forward to `remembered` if its
/// `last-delivered-id` is behind it. See the module docs.
pub async fn restore_group_position<C: ConnectionLike + Send>(
    conn: &mut C,
    stream: &str,
    group: &str,
    remembered: &str,
) -> anyhow::Result<GroupPosition> {
    let Some(at) = group_last_delivered_id(conn, stream, group).await? else {
        return Ok(GroupPosition::Missing);
    };
    if !stream_id_less_than(&at, remembered) {
        return Ok(GroupPosition::Current { at });
    }
    let () = redis::cmd("XGROUP")
        .arg("SETID")
        .arg(stream)
        .arg(group)
        .arg(remembered)
        .query_async(conn)
        .await?;
    Ok(GroupPosition::Restored { was: at })
}

/// `group`'s `last-delivered-id` from `XINFO GROUPS stream`, `None` when
/// the stream (`ERR no such key`) or the group does not exist.
pub async fn group_last_delivered_id<C: ConnectionLike + Send>(
    conn: &mut C,
    stream: &str,
    group: &str,
) -> anyhow::Result<Option<String>> {
    let reply: redis::RedisResult<Vec<redis::Value>> = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(stream)
        .query_async(conn)
        .await;
    let groups = match reply {
        Ok(groups) => groups,
        Err(err) if err.to_string().contains("no such key") => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    for entry in &groups {
        // RESP2 replies each group as a flat array; RESP3 as a map.
        let fields: HashMap<String, &redis::Value> = match entry {
            redis::Value::Array(fields) => {
                let mut map = HashMap::new();
                let mut it = fields.iter();
                while let (Some(k), Some(v)) = (it.next(), it.next()) {
                    map.insert(redis::from_redis_value::<String>(k)?, v);
                }
                map
            }
            redis::Value::Map(pairs) => {
                let mut map = HashMap::new();
                for (k, v) in pairs {
                    map.insert(redis::from_redis_value::<String>(k)?, v);
                }
                map
            }
            _ => continue,
        };
        let name: Option<String> = fields
            .get("name")
            .and_then(|v| redis::from_redis_value(v).ok());
        if name.as_deref() == Some(group) {
            return Ok(match fields.get("last-delivered-id") {
                Some(value) => redis::from_redis_value::<Option<String>>(value)?,
                None => None,
            });
        }
    }
    Ok(None)
}

/// Stream ids compared as `(ms, seq)` integer pairs, never as strings.
fn stream_id_less_than(a: &str, b: &str) -> bool {
    fn parts(id: &str) -> (u64, u64) {
        let mut it = id.splitn(2, '-');
        let ms = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let seq = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        (ms, seq)
    }
    parts(a) < parts(b)
}

/// A scripted stand-in for a Redis connection, for this module's tests and
/// the stream consumers' own: each command gets the next queued reply, and
/// every command sent is recorded.
#[cfg(any(test, feature = "redis-test-util"))]
pub mod fake {
    use std::collections::VecDeque;

    use redis::aio::ConnectionLike;
    use redis::{Cmd, Pipeline, RedisFuture, RedisResult, Value};

    /// See the module docs.
    #[derive(Default)]
    pub struct FakeConn {
        replies: VecDeque<RedisResult<Value>>,
        /// Every command sent, as its arguments rendered lossily as text.
        pub sent: Vec<Vec<String>>,
    }

    impl FakeConn {
        /// A connection that answers its commands with `replies`, in order.
        pub fn new(replies: impl IntoIterator<Item = RedisResult<Value>>) -> Self {
            Self {
                replies: replies.into_iter().collect(),
                sent: Vec::new(),
            }
        }

        /// One group's entry in an `XINFO GROUPS` reply (RESP2 shape).
        pub fn group_info(name: &str, last_delivered_id: &str) -> Value {
            let bulk = |s: &str| Value::BulkString(s.as_bytes().to_vec());
            Value::Array(vec![
                bulk("name"),
                bulk(name),
                bulk("consumers"),
                Value::Int(1),
                bulk("pending"),
                Value::Int(0),
                bulk("last-delivered-id"),
                bulk(last_delivered_id),
            ])
        }
    }

    impl ConnectionLike for FakeConn {
        fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
            let args = cmd
                .args_iter()
                .map(|arg| match arg {
                    redis::Arg::Simple(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    redis::Arg::Cursor => "<cursor>".to_string(),
                })
                .collect();
            self.sent.push(args);
            let reply = self.replies.pop_front().unwrap_or_else(|| {
                Err(redis::RedisError::from((
                    redis::ErrorKind::ClientError,
                    "FakeConn: no reply queued",
                )))
            });
            Box::pin(async move { reply })
        }

        fn req_packed_commands<'a>(
            &'a mut self,
            _cmd: &'a Pipeline,
            _offset: usize,
            _count: usize,
        ) -> RedisFuture<'a, Vec<Value>> {
            Box::pin(async {
                Err(redis::RedisError::from((
                    redis::ErrorKind::ClientError,
                    "FakeConn: pipelines are not scripted",
                )))
            })
        }

        fn get_db(&self) -> i64 {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use redis::Value;

    use super::fake::FakeConn;
    use super::*;

    #[expect(
        clippy::unnecessary_wraps,
        reason = "FakeConn::new takes RedisResult replies; the helper builds one"
    )]
    fn groups(entries: Vec<Value>) -> redis::RedisResult<Value> {
        Ok(Value::Array(entries))
    }

    #[tokio::test]
    async fn a_group_behind_the_remembered_id_is_moved_forward_to_it() {
        let mut conn = FakeConn::new([
            groups(vec![
                FakeConn::group_info("other", "500-0"),
                FakeConn::group_info("trust-consumer", "100-3"),
            ]),
            Ok(Value::Okay),
        ]);
        let position = restore_group_position(&mut conn, "s", "trust-consumer", "250-1")
            .await
            .unwrap();
        assert_eq!(
            position,
            GroupPosition::Restored {
                was: "100-3".to_string()
            }
        );
        assert_eq!(conn.sent[0], ["XINFO", "GROUPS", "s"]);
        assert_eq!(
            conn.sent[1],
            ["XGROUP", "SETID", "s", "trust-consumer", "250-1"]
        );
    }

    #[tokio::test]
    async fn a_group_at_or_past_the_remembered_id_is_left_alone() {
        for at in ["250-1", "250-2", "1000-0"] {
            let mut conn = FakeConn::new([groups(vec![FakeConn::group_info("g", at)])]);
            let position = restore_group_position(&mut conn, "s", "g", "250-1")
                .await
                .unwrap();
            assert_eq!(position, GroupPosition::Current { at: at.to_string() });
            assert_eq!(conn.sent.len(), 1, "no SETID for a group at {at}");
        }
    }

    /// Numeric, not string, comparison: "99-0" is behind "100-0".
    #[tokio::test]
    async fn ids_compare_numerically() {
        let mut conn = FakeConn::new([
            groups(vec![FakeConn::group_info("g", "99-0")]),
            Ok(Value::Okay),
        ]);
        let position = restore_group_position(&mut conn, "s", "g", "100-0")
            .await
            .unwrap();
        assert!(matches!(position, GroupPosition::Restored { .. }));
    }

    #[tokio::test]
    async fn a_missing_group_or_stream_is_left_to_nogroup_recovery() {
        let mut conn = FakeConn::new([groups(vec![FakeConn::group_info("other", "1-0")])]);
        assert_eq!(
            restore_group_position(&mut conn, "s", "g", "100-0")
                .await
                .unwrap(),
            GroupPosition::Missing
        );
        let mut conn = FakeConn::new([Err(redis::RedisError::from((
            redis::ErrorKind::ResponseError,
            "ERR",
            "no such key".to_string(),
        )))]);
        assert_eq!(
            restore_group_position(&mut conn, "s", "g", "100-0")
                .await
                .unwrap(),
            GroupPosition::Missing
        );
        assert_eq!(conn.sent.len(), 1);
    }

    #[tokio::test]
    async fn any_other_error_is_returned() {
        let mut conn = FakeConn::new([Err(redis::RedisError::from((
            redis::ErrorKind::IoError,
            "connection reset",
        )))]);
        assert!(
            restore_group_position(&mut conn, "s", "g", "100-0")
                .await
                .is_err()
        );
        let mut conn = FakeConn::new([
            groups(vec![FakeConn::group_info("g", "1-0")]),
            Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "NOPERM",
            ))),
        ]);
        assert!(
            restore_group_position(&mut conn, "s", "g", "100-0")
                .await
                .is_err(),
            "a refused SETID is an error"
        );
    }

    #[tokio::test]
    async fn a_resp3_map_reply_is_read_too() {
        let bulk = |s: &str| Value::BulkString(s.as_bytes().to_vec());
        let mut conn = FakeConn::new([groups(vec![Value::Map(vec![
            (bulk("name"), bulk("g")),
            (bulk("last-delivered-id"), bulk("300-0")),
        ])])]);
        assert_eq!(
            group_last_delivered_id(&mut conn, "s", "g").await.unwrap(),
            Some("300-0".to_string())
        );
    }
}
