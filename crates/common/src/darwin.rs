//! The Darwin Push Port (DS-152;
//! docs/superpowers/specs/2026-10-21-pre-departure-prediction-design.md
//! sections 2 and 6.2): the `darwin-pushport/1` payload darwin-relay
//! produces on `ds:ingest:darwin` and ingest-writer applies to
//! `darwin_services`, `darwin_calls` and `darwin_associations`.
//!
//! The relay keeps only what DS uses from the Push Port's update messages
//! (`uR`, and the same shapes in a snapshot's `sR`):
//!
//! - `schedule`: the service's identity (RID, UID, SSD, operator, headcode)
//!   and each location's planned platform and cancelled flag ([`Service`],
//!   [`ScheduleCall`]);
//! - `TS` (train status): each location's current platform with its
//!   confirmed and suppressed flags, and its forecasts and actuals
//!   ([`StatusCall`]);
//! - `association`: live next-working (NP), join (JJ), divide (VV) and
//!   linked (LK) links ([`Association`]);
//! - `deactivated`: Darwin stopped tracking a service ([`Deactivated`]).
//!
//! One [`Batch`] is a few seconds of messages, merged so each key appears
//! once ([`Batch::merge`]), newest Darwin timestamp first: the writer's
//! bulk upserts need unique keys, and the stored rows are guarded by the
//! same timestamps, so a replayed batch changes nothing.

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde::{Deserialize, Serialize};

/// The stream schema's name (`darwin-pushport/1`).
pub const SCHEMA: &str = "darwin-pushport";

/// `ingest_freshness.source` the writer records with each applied batch,
/// at the batch's newest Darwin timestamp.
pub const FRESHNESS_SOURCE: &str = "darwin-pushport";

/// A Darwin service's identity, from a schedule or TS message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Service {
    pub rid: String,
    /// The CIF train UID.
    pub uid: String,
    /// The scheduled start date.
    pub ssd: NaiveDate,
    /// The schedule message's fields; `None` when the service was seen in a
    /// TS message only.
    pub schedule: Option<ScheduleFields>,
    /// The Darwin `ts` of the message.
    pub ts: DateTime<Utc>,
}

/// The parts of a schedule message kept per service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleFields {
    pub toc: Option<String>,
    /// The headcode (Darwin `trainId`).
    pub train_id: Option<String>,
    pub rsid: Option<String>,
    pub is_passenger: Option<bool>,
    pub deleted: bool,
    /// Darwin's cancellation reason code, if any.
    pub cancel_reason: Option<String>,
}

/// How Darwin names a location within a service's schedule: the TIPLOC and
/// its working times as given (`""` when absent). A circular train visits a
/// TIPLOC twice; the times tell the visits apart.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CallKey {
    pub rid: String,
    pub tiploc: String,
    pub wta: String,
    pub wtd: String,
    pub wtp: String,
}

/// One location of a schedule message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleCall {
    pub key: CallKey,
    pub ssd: NaiveDate,
    /// Public times, `HH:MM`.
    pub pta: Option<String>,
    pub ptd: Option<String>,
    /// Darwin's planned platform (`plat`).
    pub planned_platform: Option<String>,
    /// The call is cancelled (`can`).
    pub cancelled: bool,
    pub ts: DateTime<Utc>,
}

/// A location's platform in a TS message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Platform {
    pub number: String,
    /// `conf`: confirmed by the operator.
    pub confirmed: bool,
    /// `platsup`: suppressed from public display.
    pub suppressed: bool,
    /// `cisPlatsup`: suppressed by the station's CIS.
    pub cis_suppressed: bool,
    /// `platsrc`: `P`lanned, `A`utomatic or `M`anual.
    pub source: Option<char>,
}

/// One event (arrival, departure or pass) of a TS location.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// The forecast, `HH:MM`.
    pub et: Option<String>,
    /// The actual, `HH:MM`.
    pub at: Option<String>,
    /// Darwin's "delayed, time unknown".
    pub delayed: bool,
}

/// One location of a TS message. A group that is `None` was not in the
/// message and keeps its stored value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusCall {
    pub key: CallKey,
    pub ssd: NaiveDate,
    pub pta: Option<String>,
    pub ptd: Option<String>,
    pub platform: Option<Platform>,
    pub arrival: Option<Event>,
    pub departure: Option<Event>,
    pub pass: Option<Event>,
    /// `suppr`: the location is suppressed from public display.
    pub suppressed: bool,
    pub ts: DateTime<Utc>,
}

impl StatusCall {
    /// `newer`'s groups over this one's: a group `newer` carries replaces
    /// this one's, a group it lacks is kept.
    fn absorb(&mut self, newer: Self) {
        self.ssd = newer.ssd;
        self.pta = newer.pta.or(self.pta.take());
        self.ptd = newer.ptd.or(self.ptd.take());
        if newer.platform.is_some() {
            self.platform = newer.platform;
        }
        if newer.arrival.is_some() {
            self.arrival = newer.arrival;
        }
        if newer.departure.is_some() {
            self.departure = newer.departure;
        }
        if newer.pass.is_some() {
            self.pass = newer.pass;
        }
        self.suppressed = newer.suppressed;
        self.ts = newer.ts;
    }
}

/// A live association between two services at a location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Association {
    pub main_rid: String,
    pub assoc_rid: String,
    pub tiploc: String,
    /// `NP`, `JJ`, `VV` or `LK`.
    pub category: String,
    pub cancelled: bool,
    pub deleted: bool,
    pub main_wta: Option<NaiveTime>,
    pub main_wtd: Option<NaiveTime>,
    pub main_wtp: Option<NaiveTime>,
    pub assoc_wta: Option<NaiveTime>,
    pub assoc_wtd: Option<NaiveTime>,
    pub assoc_wtp: Option<NaiveTime>,
    /// The main service's scheduled start date: a RID starts with it.
    pub main_ssd: Option<NaiveDate>,
    pub ts: DateTime<Utc>,
}

/// Darwin stopped tracking a service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deactivated {
    pub rid: String,
    pub ts: DateTime<Utc>,
}

/// One `darwin-pushport/1` entry: a few seconds of Push Port messages,
/// with each key once.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Batch {
    pub services: Vec<Service>,
    pub schedule_calls: Vec<ScheduleCall>,
    pub status_calls: Vec<StatusCall>,
    pub associations: Vec<Association>,
    pub deactivated: Vec<Deactivated>,
}

impl Batch {
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
            && self.schedule_calls.is_empty()
            && self.status_calls.is_empty()
            && self.associations.is_empty()
            && self.deactivated.is_empty()
    }

    /// Rows across every kind.
    pub fn len(&self) -> usize {
        self.services.len()
            + self.schedule_calls.len()
            + self.status_calls.len()
            + self.associations.len()
            + self.deactivated.len()
    }

    /// The newest Darwin timestamp in the batch.
    pub fn newest_ts(&self) -> Option<DateTime<Utc>> {
        self.services
            .iter()
            .map(|s| s.ts)
            .chain(self.schedule_calls.iter().map(|c| c.ts))
            .chain(self.status_calls.iter().map(|c| c.ts))
            .chain(self.associations.iter().map(|a| a.ts))
            .chain(self.deactivated.iter().map(|d| d.ts))
            .max()
    }

    /// Appends `other` (later messages) and merges each kind to one row per
    /// key, in timestamp order (a tie goes to the later message):
    ///
    /// - a service keeps its newest schedule fields, and the newest `ts`;
    /// - a schedule call and an association: the newest wins whole;
    /// - a TS call: each group from the newest message that carried it
    ///   ([`StatusCall::absorb`]);
    /// - a deactivation: the newest.
    pub fn merge(&mut self, other: Self) {
        self.services.extend(other.services);
        self.schedule_calls.extend(other.schedule_calls);
        self.status_calls.extend(other.status_calls);
        self.associations.extend(other.associations);
        self.deactivated.extend(other.deactivated);
        self.services = merge_services(std::mem::take(&mut self.services));
        self.schedule_calls = newest_by(std::mem::take(&mut self.schedule_calls), |c| {
            (c.key.clone(), c.ts)
        });
        self.status_calls = merge_status_calls(std::mem::take(&mut self.status_calls));
        self.associations = newest_by(std::mem::take(&mut self.associations), |a| {
            (
                (
                    a.main_rid.clone(),
                    a.assoc_rid.clone(),
                    a.tiploc.clone(),
                    a.category.clone(),
                ),
                a.ts,
            )
        });
        self.deactivated = newest_by(std::mem::take(&mut self.deactivated), |d| {
            (d.rid.clone(), d.ts)
        });
    }
}

/// One row per key: the newest by `ts`, a tie going to the later row.
/// Keeps first-seen key order.
fn newest_by<T, K: std::hash::Hash + Eq + Clone>(
    rows: Vec<T>,
    key_ts: impl Fn(&T) -> (K, DateTime<Utc>),
) -> Vec<T> {
    let mut index: HashMap<K, usize> = HashMap::new();
    let mut out: Vec<T> = Vec::with_capacity(rows.len());
    for row in rows {
        let (key, ts) = key_ts(&row);
        if let Some(&at) = index.get(&key) {
            if ts >= key_ts(&out[at]).1 {
                out[at] = row;
            }
        } else {
            index.insert(key, out.len());
            out.push(row);
        }
    }
    out
}

fn merge_services(rows: Vec<Service>) -> Vec<Service> {
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut out: Vec<Service> = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(&at) = index.get(&row.rid) else {
            index.insert(row.rid.clone(), out.len());
            out.push(row);
            continue;
        };
        let kept = &mut out[at];
        let newer = row.ts >= kept.ts;
        // Schedule fields: the newer message's, when it has them.
        if row.schedule.is_some() && (newer || kept.schedule.is_none()) {
            kept.schedule = row.schedule;
        }
        if newer {
            kept.uid = row.uid;
            kept.ssd = row.ssd;
            kept.ts = row.ts;
        }
    }
    out
}

fn merge_status_calls(mut rows: Vec<StatusCall>) -> Vec<StatusCall> {
    // Oldest first, stable: a tie keeps message order.
    rows.sort_by_key(|row| row.ts);
    let mut index: HashMap<CallKey, usize> = HashMap::new();
    let mut out: Vec<StatusCall> = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(&at) = index.get(&row.key) {
            out[at].absorb(row);
        } else {
            index.insert(row.key.clone(), out.len());
            out.push(row);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(4_102_444_800 + seconds, 0).expect("valid instant")
    }

    fn date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2100, 1, 1).expect("valid date")
    }

    fn key(rid: &str, tiploc: &str) -> CallKey {
        CallKey {
            rid: rid.to_string(),
            tiploc: tiploc.to_string(),
            wta: String::new(),
            wtd: "10:00".to_string(),
            wtp: String::new(),
        }
    }

    fn status(rid: &str, at: i64, platform: Option<&str>, dep_et: Option<&str>) -> StatusCall {
        StatusCall {
            key: key(rid, "YORK"),
            ssd: date(),
            pta: None,
            ptd: Some("10:00".to_string()),
            platform: platform.map(|number| Platform {
                number: number.to_string(),
                confirmed: true,
                suppressed: false,
                cis_suppressed: false,
                source: Some('A'),
            }),
            arrival: None,
            departure: dep_et.map(|et| Event {
                et: Some(et.to_string()),
                ..Event::default()
            }),
            pass: None,
            suppressed: false,
            ts: ts(at),
        }
    }

    #[test]
    fn a_ts_call_keeps_each_group_from_the_newest_message_that_carried_it() {
        let mut batch = Batch {
            status_calls: vec![status("R1", 10, Some("4"), Some("10:02"))],
            ..Batch::default()
        };
        batch.merge(Batch {
            status_calls: vec![status("R1", 20, None, Some("10:05"))],
            ..Batch::default()
        });
        assert_eq!(batch.status_calls.len(), 1);
        let call = &batch.status_calls[0];
        assert_eq!(call.platform.as_ref().map(|p| p.number.as_str()), Some("4"));
        assert_eq!(
            call.departure.as_ref().and_then(|e| e.et.as_deref()),
            Some("10:05")
        );
        assert_eq!(call.ts, ts(20));
    }

    #[test]
    fn an_older_ts_message_does_not_overwrite_a_newer_one() {
        let mut batch = Batch {
            status_calls: vec![status("R1", 20, Some("5"), None)],
            ..Batch::default()
        };
        batch.merge(Batch {
            status_calls: vec![status("R1", 10, Some("4"), None)],
            ..Batch::default()
        });
        assert_eq!(
            batch.status_calls[0]
                .platform
                .as_ref()
                .map(|p| p.number.as_str()),
            Some("5")
        );
    }

    #[test]
    fn a_service_keeps_its_schedule_fields_when_a_later_ts_message_has_none() {
        let fields = ScheduleFields {
            toc: Some("NT".to_string()),
            train_id: Some("2A01".to_string()),
            rsid: None,
            is_passenger: Some(true),
            deleted: false,
            cancel_reason: None,
        };
        let mut batch = Batch {
            services: vec![Service {
                rid: "R1".to_string(),
                uid: "P00001".to_string(),
                ssd: date(),
                schedule: Some(fields.clone()),
                ts: ts(10),
            }],
            ..Batch::default()
        };
        batch.merge(Batch {
            services: vec![Service {
                rid: "R1".to_string(),
                uid: "P00001".to_string(),
                ssd: date(),
                schedule: None,
                ts: ts(20),
            }],
            deactivated: vec![Deactivated {
                rid: "R1".to_string(),
                ts: ts(30),
            }],
            ..Batch::default()
        });
        assert_eq!(batch.services.len(), 1);
        assert_eq!(batch.services[0].schedule, Some(fields));
        assert_eq!(batch.services[0].ts, ts(20));
        assert_eq!(batch.newest_ts(), Some(ts(30)));
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn schedule_calls_and_associations_keep_the_newest_whole() {
        let call = |at: i64, platform: &str| ScheduleCall {
            key: key("R1", "YORK"),
            ssd: date(),
            pta: None,
            ptd: None,
            planned_platform: Some(platform.to_string()),
            cancelled: false,
            ts: ts(at),
        };
        let mut batch = Batch {
            schedule_calls: vec![call(10, "1"), call(30, "3")],
            ..Batch::default()
        };
        batch.merge(Batch {
            schedule_calls: vec![call(20, "2")],
            ..Batch::default()
        });
        assert_eq!(batch.schedule_calls.len(), 1);
        assert_eq!(
            batch.schedule_calls[0].planned_platform.as_deref(),
            Some("3")
        );
    }

    #[test]
    fn the_payload_round_trips_through_json() {
        let batch = Batch {
            status_calls: vec![status("R1", 10, Some("4"), Some("10:02"))],
            ..Batch::default()
        };
        let json = serde_json::to_string(&batch).expect("serializes");
        let back: Batch = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, batch);
    }
}
