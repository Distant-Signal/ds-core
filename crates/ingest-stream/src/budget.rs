//! The ingest streams' memory budget (spec §7.1, §7.6; decision D5).
//!
//! Each stream declares its producer's rate and its worst-case entry size
//! (after gzip) and how long a writer outage its `MAXLEN` must cover. The
//! cover is at least [`OUTAGE_TARGET`] (2 hours); cheap, slow streams
//! cover more (`TfL` a day, tocs 30 days). [`check_budget`] computes every
//! `MAXLEN` and the worst-case memory of each stream **and** its
//! dead-letter stream (capped at the same `MAXLEN`), and fails if the sum
//! does not fit [`BUDGET_BYTES`] (512 MB). The alert fires at
//! [`ALERT_FRACTION`] of it.
//!
//! Worst case per stream is `(MAXLEN + TRIM_SLACK_ENTRIES) * (entry bytes +
//! ENTRY_OVERHEAD_BYTES)`: `MAXLEN ~` trims whole radix-tree nodes, so a
//! stream can run up to one node (`stream-node-max-entries`, default 100)
//! over its cap.

use std::time::Duration;

/// The writer outage every snapshot stream's `MAXLEN` covers at least.
pub const OUTAGE_TARGET: Duration = Duration::from_secs(2 * 3600);

/// All `ds:ingest:*` and `ds:dlq:*` keys together (decision D5,
/// 2026-10-07: 512 MB of the 2 GB Redis `maxmemory`), in Redis's own
/// units (`512mb` is 512 MiB).
pub const BUDGET_BYTES: u64 = 512 * 1024 * 1024;

/// `DistantSignalIngestStreamMemoryHigh` fires at this fraction of
/// [`BUDGET_BYTES`].
pub const ALERT_FRACTION: f64 = 0.75;

/// Per-entry overhead on top of the `body`: the envelope's other fields
/// (about 200 bytes) plus the stream's listpack and radix-tree overhead.
pub const ENTRY_OVERHEAD_BYTES: u64 = 512;

/// How far `MAXLEN ~` may overshoot: one node of Redis's default
/// `stream-node-max-entries`.
pub const TRIM_SLACK_ENTRIES: u64 = 100;

/// How a stream's `MAXLEN` is chosen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bound {
    /// Enough entries for this long at the declared rate (rounded up).
    Covers(Duration),
    /// A fixed cap, for a stream whose rate is not yet known (the disabled
    /// island-of-Ireland producers); it must still cover [`OUTAGE_TARGET`]
    /// at the declared rate.
    Fixed(u64),
}

/// One ingest stream's declaration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamDecl {
    pub stream: &'static str,
    /// Entries the producers write per day, worst case.
    pub entries_per_day: u64,
    /// Worst-case `body` bytes of one entry, after gzip.
    pub entry_bytes: u64,
    pub bound: Bound,
}

const DAY_SECS: u128 = 86_400;

impl StreamDecl {
    /// The stream's `MAXLEN ~`.
    pub fn maxlen(&self) -> u64 {
        match self.bound {
            Bound::Covers(cover) => self.entries_in(cover),
            Bound::Fixed(n) => n,
        }
    }

    /// The dead-letter stream's `MAXLEN ~`: the same as the source, so a
    /// dead-letter flood can hold one full outage window and no more.
    pub fn dead_letter_maxlen(&self) -> u64 {
        self.maxlen()
    }

    /// Entries produced in `window` at the declared rate, rounded up.
    pub fn entries_in(&self, window: Duration) -> u64 {
        let millis = window.as_millis();
        let n = (u128::from(self.entries_per_day) * millis).div_ceil(DAY_SECS * 1000);
        u64::try_from(n).unwrap_or(u64::MAX)
    }

    /// Worst-case bytes of a full stream at its cap.
    pub fn worst_case_bytes(&self) -> u64 {
        stream_bytes(self.maxlen(), self.entry_bytes)
    }

    /// Worst-case bytes of its full dead-letter stream. A dead-letter entry
    /// is the source entry plus about 200 bytes of error fields, covered by
    /// [`ENTRY_OVERHEAD_BYTES`].
    pub fn worst_case_dead_letter_bytes(&self) -> u64 {
        stream_bytes(self.dead_letter_maxlen(), self.entry_bytes)
    }
}

fn stream_bytes(maxlen: u64, entry_bytes: u64) -> u64 {
    maxlen
        .saturating_add(TRIM_SLACK_ENTRIES)
        .saturating_mul(entry_bytes.saturating_add(ENTRY_OVERHEAD_BYTES))
}

const KIB: u64 = 1024;

/// The ingest streams (spec §7.1, with D1: no `train-events` and no
/// `trust-backlog` stream).
pub const INGEST_STREAMS: [StreamDecl; 7] = [
    // poller-ldbws: 1 snapshot a minute in 6 parts of 100 stations, about
    // 60–80 KB gzip each.
    StreamDecl {
        stream: crate::streams::STATION_SAMPLES,
        entries_per_day: 6 * 60 * 24,
        entry_bytes: 80 * KIB,
        bound: Bound::Covers(OUTAGE_TARGET),
    },
    // full-coverage-consumer: 3 entries a minute of 10/40/80 KB gzip;
    // sized at the largest.
    StreamDecl {
        stream: crate::streams::FULL_COVERAGE,
        entries_per_day: 3 * 60 * 24,
        entry_bytes: 80 * KIB,
        bound: Bound::Covers(OUTAGE_TARGET),
    },
    // poller-tfl: every 5 minutes, under 2 KB gzip. A day costs under 1 MB.
    StreamDecl {
        stream: crate::streams::TFL,
        entries_per_day: 12 * 24,
        entry_bytes: 2 * KIB,
        bound: Bound::Covers(Duration::from_secs(24 * 3600)),
    },
    // poller-tocs: daily, about 3 KB (under the gzip threshold).
    StreamDecl {
        stream: crate::streams::REFERENCE,
        entries_per_day: 1,
        entry_bytes: 3 * KIB,
        bound: Bound::Covers(Duration::from_secs(30 * 24 * 3600)),
    },
    // The three island-of-Ireland pollers (disabled), one stream each:
    // live every 5 minutes, the GTFS and NIR stations pollers two snapshots
    // (stations, lines) a cycle, daily by default (sized for hourly).
    // Together the 2,000 entries the shared stream had.
    StreamDecl {
        stream: crate::streams::IOI_GTFS,
        entries_per_day: 2 * 24,
        entry_bytes: 16 * KIB,
        bound: Bound::Fixed(500),
    },
    StreamDecl {
        stream: crate::streams::IOI_NIR,
        entries_per_day: 2 * 24,
        entry_bytes: 16 * KIB,
        bound: Bound::Fixed(500),
    },
    StreamDecl {
        stream: crate::streams::IOI_LIVE,
        entries_per_day: 12 * 24,
        entry_bytes: 16 * KIB,
        bound: Bound::Fixed(1000),
    },
];

/// The declaration of `stream`, if it is one of [`INGEST_STREAMS`].
pub fn decl(stream: &str) -> Option<&'static StreamDecl> {
    INGEST_STREAMS.iter().find(|d| d.stream == stream)
}

/// One stream's line in a [`BudgetReport`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamBudget {
    pub stream: &'static str,
    pub maxlen: u64,
    pub covers: Duration,
    pub bytes: u64,
    pub dead_letter_bytes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BudgetReport {
    pub streams: Vec<StreamBudget>,
    pub total_bytes: u64,
    pub budget_bytes: u64,
    pub alert_bytes: u64,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BudgetError {
    #[error(
        "{stream}: MAXLEN {maxlen} covers {covers:?} at its rate, under the {target:?} outage target"
    )]
    ShortCover {
        stream: &'static str,
        maxlen: u64,
        covers: Duration,
        target: Duration,
    },
    #[error("worst case {total} bytes exceeds the {budget}-byte budget")]
    OverBudget { total: u64, budget: u64 },
}

/// Computes every stream's `MAXLEN` and worst-case bytes, and checks that
/// each covers [`OUTAGE_TARGET`] and that the total fits `budget_bytes`.
pub fn check_budget(decls: &[StreamDecl], budget_bytes: u64) -> Result<BudgetReport, BudgetError> {
    let mut streams = Vec::with_capacity(decls.len());
    for d in decls {
        let maxlen = d.maxlen();
        let covers = covered(d.entries_per_day, maxlen);
        if covers < OUTAGE_TARGET {
            return Err(BudgetError::ShortCover {
                stream: d.stream,
                maxlen,
                covers,
                target: OUTAGE_TARGET,
            });
        }
        streams.push(StreamBudget {
            stream: d.stream,
            maxlen,
            covers,
            bytes: d.worst_case_bytes(),
            dead_letter_bytes: d.worst_case_dead_letter_bytes(),
        });
    }
    let total_bytes = streams
        .iter()
        .map(|s| s.bytes.saturating_add(s.dead_letter_bytes))
        .fold(0u64, u64::saturating_add);
    if total_bytes > budget_bytes {
        return Err(BudgetError::OverBudget {
            total: total_bytes,
            budget: budget_bytes,
        });
    }
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "a positive byte count far below 2^53"
    )]
    let alert_bytes = (budget_bytes as f64 * ALERT_FRACTION) as u64;
    Ok(BudgetReport {
        streams,
        total_bytes,
        budget_bytes,
        alert_bytes,
    })
}

/// How long `maxlen` entries last at `entries_per_day`.
fn covered(entries_per_day: u64, maxlen: u64) -> Duration {
    if entries_per_day == 0 {
        return Duration::MAX;
    }
    let millis = u128::from(maxlen) * DAY_SECS * 1000 / u128::from(entries_per_day);
    Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_spec_stream_table_fits_the_512_mb_budget() {
        let report = check_budget(&INGEST_STREAMS, BUDGET_BYTES).unwrap();
        let maxlens: Vec<(&str, u64)> = report
            .streams
            .iter()
            .map(|s| (s.stream, s.maxlen))
            .collect();
        // Spec §7.1's MAXLEN column.
        assert_eq!(
            maxlens,
            vec![
                ("ds:ingest:station-samples", 720),
                ("ds:ingest:full-coverage", 360),
                ("ds:ingest:tfl", 288),
                ("ds:ingest:reference", 30),
                ("ds:ingest:ioi-gtfs", 500),
                ("ds:ingest:ioi-nir", 500),
                ("ds:ingest:ioi-live", 1000),
            ]
        );
        let station = &report.streams[0];
        assert_eq!(station.covers, OUTAGE_TARGET);
        // (720 + 100) * (80 KiB + 512) = 67.6 MB, and the same again for its
        // dead-letter stream.
        assert_eq!(station.bytes, 820 * (80 * 1024 + 512));
        assert_eq!(station.dead_letter_bytes, station.bytes);
        // About 292 MB in all: under the 75% alert line (384 MiB), let alone 512.
        assert!(report.total_bytes < report.alert_bytes, "{report:#?}");
        assert_eq!(report.alert_bytes, 384 * 1024 * 1024);
        assert!(report.total_bytes > 250_000_000, "{report:#?}");
    }

    #[test]
    fn a_short_cover_or_an_overspent_budget_is_refused() {
        let short = StreamDecl {
            stream: "s",
            entries_per_day: 8640,
            entry_bytes: 1024,
            bound: Bound::Covers(Duration::from_secs(3600)),
        };
        assert!(matches!(
            check_budget(&[short], BUDGET_BYTES),
            Err(BudgetError::ShortCover { maxlen: 360, .. })
        ));
        let fixed_short = StreamDecl {
            bound: Bound::Fixed(100),
            ..short
        };
        assert!(matches!(
            check_budget(&[fixed_short], BUDGET_BYTES),
            Err(BudgetError::ShortCover { .. })
        ));
        assert!(matches!(
            check_budget(&INGEST_STREAMS, 200_000_000),
            Err(BudgetError::OverBudget { .. })
        ));
    }

    #[test]
    fn maxlen_rounds_up() {
        let d = StreamDecl {
            stream: "s",
            entries_per_day: 1,
            entry_bytes: 1,
            bound: Bound::Covers(OUTAGE_TARGET),
        };
        assert_eq!(d.maxlen(), 1);
        assert_eq!(decl("ds:ingest:tfl").unwrap().maxlen(), 288);
        assert!(decl("movement-events").is_none());
    }
}
