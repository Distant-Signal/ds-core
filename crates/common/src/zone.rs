//! Time zones: the one module that uses a time zone library.
//!
//! Every other crate asks this module for "the London date of this instant",
//! "this London wall-clock time as an instant" and so on, using only `chrono`
//! types and the opaque [`Zone`]. The library behind it (`jiff`, with its
//! bundled copy of the IANA database) can then be replaced without touching
//! a caller.
//!
//! The zone data is the database `jiff` bundles (`jiff-tzdb`, the
//! `tzdb-bundle-always` feature, with the host's `/usr/share/zoneinfo`
//! switched off), so behaviour never depends on the machine or the container
//! image.
//!
//! Three rules for wall-clock times that a clock change makes ambiguous or
//! impossible are named here, because callers used to spell them out by hand:
//!
//! - [`Zone::resolve`] reports everything: one instant, two (the repeated
//!   hour in autumn) or none (the skipped hour in spring).
//! - [`Zone::to_utc_earliest`] takes the first reading of a repeated time and
//!   gives `None` for a skipped one.
//! - [`Zone::to_utc_gap_shifted`] is "earliest, or the reading one hour later
//!   if the time does not exist": a skipped time is read as the instant the
//!   clock would show it an hour on (the clock is already on summer time), so
//!   a booked time is never lost.
//!
//! `jiff` only answers "what is the offset at this instant" and "which
//! offsets can this wall-clock time have". Every instant and local value that
//! leaves this module is computed with `chrono` from that offset, so the
//! precision, leap-second and range behaviour callers see is `chrono`'s.
//! `jiff`'s timestamps end in the year 9999: an instant beyond that is read
//! at the nearest end of its range, and a wall-clock time beyond it has no
//! reading ([`LocalTime::Gap`]).

use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{LazyLock, Mutex, PoisonError};

use chrono::{
    DateTime, Datelike, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset,
    Timelike, Utc,
};
use jiff::Timestamp;
use jiff::civil;
use jiff::tz::{AmbiguousOffset, TimeZone};

/// An IANA time zone. Opaque: build one with [`Zone::LONDON`],
/// [`Zone::DUBLIN`] or [`Zone::parse`].
#[derive(Clone, Copy)]
pub struct Zone(Repr);

/// `Europe/London` and `Europe/Dublin` are variants so that their constants
/// need no lookup; every other zone is looked up once by [`Zone::parse`] and
/// kept for the life of the process (at most one entry per database name).
#[derive(Clone, Copy)]
enum Repr {
    London,
    Dublin,
    Other(&'static Entry),
}

struct Entry {
    name: &'static str,
    tz: TimeZone,
}

#[expect(
    clippy::expect_used,
    reason = "the bundled database always has this zone; a test pins it"
)]
fn bundled(name: &str) -> TimeZone {
    TimeZone::get(name).expect("the bundled time zone database has this zone")
}

static LONDON_TZ: LazyLock<TimeZone> = LazyLock::new(|| bundled("Europe/London"));
static DUBLIN_TZ: LazyLock<TimeZone> = LazyLock::new(|| bundled("Europe/Dublin"));

/// The zones [`Zone::parse`] has built, by name.
static PARSED: Mutex<BTreeMap<&'static str, &'static Entry>> = Mutex::new(BTreeMap::new());

/// What a local wall-clock reading means in a [`Zone`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalTime {
    /// Exactly one instant.
    Single(DateTime<Utc>),
    /// A repeated time (the clocks went back): both instants, in order.
    Ambiguous {
        earliest: DateTime<Utc>,
        latest: DateTime<Utc>,
    },
    /// A skipped time (the clocks went forward): no instant.
    Gap,
}

/// `at` as a `jiff` instant, to the whole second (clock changes fall on whole
/// seconds), read at the nearest end of `jiff`'s range if it is outside it.
fn timestamp(at: DateTime<Utc>) -> Timestamp {
    let seconds = at.timestamp();
    Timestamp::from_second(seconds).unwrap_or(if seconds < 0 {
        Timestamp::MIN
    } else {
        Timestamp::MAX
    })
}

/// `local` as a `jiff` civil time, to the whole second (an offset never
/// changes inside a second; a leap second is read as `:59`), or `None`
/// outside `jiff`'s range.
fn civil_time(local: NaiveDateTime) -> Option<civil::DateTime> {
    civil::DateTime::new(
        i16::try_from(local.year()).ok()?,
        i8::try_from(local.month()).ok()?,
        i8::try_from(local.day()).ok()?,
        i8::try_from(local.hour()).ok()?,
        i8::try_from(local.minute()).ok()?,
        i8::try_from(local.second().min(59)).ok()?,
        0,
    )
    .ok()
}

impl Zone {
    /// `Europe/London`.
    pub const LONDON: Self = Self(Repr::London);
    /// `Europe/Dublin`.
    pub const DUBLIN: Self = Self(Repr::Dublin);

    /// The zone with this IANA name, or `None`. Names are matched exactly
    /// (case-sensitive), as the preference check always did: the database
    /// lookup ignores case, so the name it found must be the one given.
    /// Link names (`GB`, `US/Eastern`, `Europe/Kiev`, `UTC`, `Etc/UTC`) are
    /// accepted as themselves.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "Europe/London" => return Some(Self::LONDON),
            "Europe/Dublin" => return Some(Self::DUBLIN),
            // In the database but not a place: never accepted.
            "Factory" => return None,
            _ => {}
        }
        let mut parsed = PARSED.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = parsed.get(name) {
            return Some(Self(Repr::Other(entry)));
        }
        let tz = TimeZone::get(name).ok()?;
        if tz.iana_name() != Some(name) {
            return None;
        }
        let name: &'static str = Box::leak(name.to_owned().into_boxed_str());
        let entry: &'static Entry = Box::leak(Box::new(Entry { name, tz }));
        parsed.insert(name, entry);
        Some(Self(Repr::Other(entry)))
    }

    /// The IANA name, as [`Zone::parse`] accepts it.
    pub fn name(self) -> &'static str {
        match self.0 {
            Repr::London => "Europe/London",
            Repr::Dublin => "Europe/Dublin",
            Repr::Other(entry) => entry.name,
        }
    }

    fn tz(self) -> &'static TimeZone {
        match self.0 {
            Repr::London => &LONDON_TZ,
            Repr::Dublin => &DUBLIN_TZ,
            Repr::Other(entry) => &entry.tz,
        }
    }

    /// `at` as a local date and time with the zone's offset at that instant.
    /// Arithmetic on the result keeps that offset (it is a fixed offset), so
    /// convert back to UTC before adding durations across a clock change.
    pub fn local(self, at: DateTime<Utc>) -> DateTime<FixedOffset> {
        at.with_timezone(&self.offset_at(at))
    }

    /// The zone's offset from UTC at `at`.
    pub fn offset_at(self, at: DateTime<Utc>) -> FixedOffset {
        let seconds = self.tz().to_offset(timestamp(at)).seconds();
        FixedOffset::east_opt(seconds).unwrap_or_else(|| Utc.fix())
    }

    /// The local date and time at `at`.
    pub fn naive_local(self, at: DateTime<Utc>) -> NaiveDateTime {
        self.local(at).naive_local()
    }

    /// The local calendar date at `at`.
    pub fn date(self, at: DateTime<Utc>) -> NaiveDate {
        self.local(at).date_naive()
    }

    /// The local time of day at `at`.
    pub fn time(self, at: DateTime<Utc>) -> NaiveTime {
        self.local(at).time()
    }

    /// What `local` means in this zone ([`LocalTime`]).
    pub fn resolve(self, local: NaiveDateTime) -> LocalTime {
        let Some(civil) = civil_time(local) else {
            return LocalTime::Gap;
        };
        let at = |offset: jiff::tz::Offset| {
            local
                .and_utc()
                .checked_sub_signed(Duration::seconds(i64::from(offset.seconds())))
        };
        match self.tz().to_ambiguous_timestamp(civil).offset() {
            AmbiguousOffset::Unambiguous { offset } => {
                at(offset).map_or(LocalTime::Gap, LocalTime::Single)
            }
            // `before` is the offset of the first reading, `after` the second's.
            AmbiguousOffset::Fold { before, after } => match (at(before), at(after)) {
                (Some(earliest), Some(latest)) => LocalTime::Ambiguous { earliest, latest },
                _ => LocalTime::Gap,
            },
            AmbiguousOffset::Gap { .. } => LocalTime::Gap,
        }
    }

    /// `local` as an instant: the first reading of a repeated time, `None`
    /// for a skipped one.
    pub fn to_utc_earliest(self, local: NaiveDateTime) -> Option<DateTime<Utc>> {
        match self.resolve(local) {
            LocalTime::Single(at) | LocalTime::Ambiguous { earliest: at, .. } => Some(at),
            LocalTime::Gap => None,
        }
    }

    /// `local` as an instant: the first reading of a repeated time, and for
    /// a skipped time the first reading of the time one hour later. `None`
    /// only if neither exists (outside the representable range).
    pub fn to_utc_gap_shifted(self, local: NaiveDateTime) -> Option<DateTime<Utc>> {
        self.to_utc_earliest(local).or_else(|| {
            local
                .checked_add_signed(Duration::hours(1))
                .and_then(|later| self.to_utc_earliest(later))
        })
    }
}

impl PartialEq for Zone {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

impl Eq for Zone {}

impl Hash for Zone {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl fmt::Debug for Zone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Zone").field(&self.name()).finish()
    }
}

impl fmt::Display for Zone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// `at` in London ([`Zone::local`]).
pub fn london_local(at: DateTime<Utc>) -> DateTime<FixedOffset> {
    Zone::LONDON.local(at)
}

/// The London calendar date at `at` (not the service date: see
/// [`crate::rail_day::current_rail_day`] for the 02:00 cutoff).
pub fn london_date(at: DateTime<Utc>) -> NaiveDate {
    Zone::LONDON.date(at)
}

/// The London time of day at `at`.
pub fn london_time(at: DateTime<Utc>) -> NaiveTime {
    Zone::LONDON.time(at)
}

/// The London date and time at `at`.
pub fn london_naive(at: DateTime<Utc>) -> NaiveDateTime {
    Zone::LONDON.naive_local(at)
}

/// What a London wall-clock reading means ([`Zone::resolve`]).
pub fn london_resolve(local: NaiveDateTime) -> LocalTime {
    Zone::LONDON.resolve(local)
}

/// A London wall-clock time as an instant, earliest reading of a repeated
/// time, `None` for a skipped one ([`Zone::to_utc_earliest`]).
pub fn london_to_utc_earliest(local: NaiveDateTime) -> Option<DateTime<Utc>> {
    Zone::LONDON.to_utc_earliest(local)
}

/// A London wall-clock time as an instant, earliest reading of a repeated
/// time, the hour on for a skipped one ([`Zone::to_utc_gap_shifted`]).
pub fn london_to_utc_gap_shifted(local: NaiveDateTime) -> Option<DateTime<Utc>> {
    Zone::LONDON.to_utc_gap_shifted(local)
}

/// A Dublin wall-clock time as an instant: [`Zone::to_utc_earliest`].
pub fn dublin_to_utc_earliest(local: NaiveDateTime) -> Option<DateTime<Utc>> {
    Zone::DUBLIN.to_utc_earliest(local)
}

/// `at` in Dublin ([`Zone::local`]).
pub fn dublin_local(at: DateTime<Utc>) -> DateTime<FixedOffset> {
    Zone::DUBLIN.local(at)
}

#[cfg(test)]
mod tests {
    use super::*;
    use Want::{Ambiguous, Gap, Single};

    /// `(instant, local date and time, offset in seconds)`. London and Dublin
    /// agree on every row (both change at 01:00 UTC on the same days). The
    /// expected values come from the IANA rules via an unrelated
    /// implementation.
    #[rustfmt::skip]
    const OFFSETS: &[(&str, &str, i32)] = &[
        ("2025-03-29T22:00:00Z", "2025-03-29T22:00:00", 0),
        ("2025-03-29T23:00:00Z", "2025-03-29T23:00:00", 0),
        ("2025-03-29T23:30:00Z", "2025-03-29T23:30:00", 0),
        ("2025-03-30T00:00:00Z", "2025-03-30T00:00:00", 0),
        ("2025-03-30T00:30:00Z", "2025-03-30T00:30:00", 0),
        ("2025-03-30T00:59:00Z", "2025-03-30T00:59:00", 0),
        ("2025-03-30T01:00:00Z", "2025-03-30T02:00:00", 3600),
        ("2025-03-30T01:01:00Z", "2025-03-30T02:01:00", 3600),
        ("2025-03-30T01:30:00Z", "2025-03-30T02:30:00", 3600),
        ("2025-03-30T01:59:00Z", "2025-03-30T02:59:00", 3600),
        ("2025-03-30T02:00:00Z", "2025-03-30T03:00:00", 3600),
        ("2025-03-30T02:30:00Z", "2025-03-30T03:30:00", 3600),
        ("2025-03-30T03:00:00Z", "2025-03-30T04:00:00", 3600),
        ("2025-03-30T12:00:00Z", "2025-03-30T13:00:00", 3600),
        ("2025-03-30T22:59:00Z", "2025-03-30T23:59:00", 3600),
        ("2025-03-30T23:00:00Z", "2025-03-31T00:00:00", 3600),
        ("2025-03-30T23:30:00Z", "2025-03-31T00:30:00", 3600),
        ("2025-03-31T00:30:00Z", "2025-03-31T01:30:00", 3600),
        ("2025-10-25T22:00:00Z", "2025-10-25T23:00:00", 3600),
        ("2025-10-25T23:00:00Z", "2025-10-26T00:00:00", 3600),
        ("2025-10-25T23:30:00Z", "2025-10-26T00:30:00", 3600),
        ("2025-10-26T00:00:00Z", "2025-10-26T01:00:00", 3600),
        ("2025-10-26T00:30:00Z", "2025-10-26T01:30:00", 3600),
        ("2025-10-26T00:59:00Z", "2025-10-26T01:59:00", 3600),
        ("2025-10-26T01:00:00Z", "2025-10-26T01:00:00", 0),
        ("2025-10-26T01:01:00Z", "2025-10-26T01:01:00", 0),
        ("2025-10-26T01:30:00Z", "2025-10-26T01:30:00", 0),
        ("2025-10-26T01:59:00Z", "2025-10-26T01:59:00", 0),
        ("2025-10-26T02:00:00Z", "2025-10-26T02:00:00", 0),
        ("2025-10-26T02:30:00Z", "2025-10-26T02:30:00", 0),
        ("2025-10-26T03:00:00Z", "2025-10-26T03:00:00", 0),
        ("2025-10-26T12:00:00Z", "2025-10-26T12:00:00", 0),
        ("2025-10-26T22:59:00Z", "2025-10-26T22:59:00", 0),
        ("2025-10-26T23:00:00Z", "2025-10-26T23:00:00", 0),
        ("2025-10-26T23:30:00Z", "2025-10-26T23:30:00", 0),
        ("2025-10-27T00:30:00Z", "2025-10-27T00:30:00", 0),
        ("2025-12-31T22:00:00Z", "2025-12-31T22:00:00", 0),
        ("2025-12-31T22:59:00Z", "2025-12-31T22:59:00", 0),
        ("2025-12-31T23:00:00Z", "2025-12-31T23:00:00", 0),
        ("2025-12-31T23:30:00Z", "2025-12-31T23:30:00", 0),
        ("2025-12-31T23:59:00Z", "2025-12-31T23:59:00", 0),
        ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00", 0),
        ("2026-01-01T00:30:00Z", "2026-01-01T00:30:00", 0),
        ("2026-01-01T00:59:00Z", "2026-01-01T00:59:00", 0),
        ("2026-01-01T01:00:00Z", "2026-01-01T01:00:00", 0),
        ("2026-01-01T01:30:00Z", "2026-01-01T01:30:00", 0),
        ("2026-01-01T01:59:00Z", "2026-01-01T01:59:00", 0),
        ("2026-01-01T02:00:00Z", "2026-01-01T02:00:00", 0),
        ("2026-01-01T02:30:00Z", "2026-01-01T02:30:00", 0),
        ("2026-01-15T22:00:00Z", "2026-01-15T22:00:00", 0),
        ("2026-01-15T22:59:00Z", "2026-01-15T22:59:00", 0),
        ("2026-01-15T23:00:00Z", "2026-01-15T23:00:00", 0),
        ("2026-01-15T23:30:00Z", "2026-01-15T23:30:00", 0),
        ("2026-01-15T23:59:00Z", "2026-01-15T23:59:00", 0),
        ("2026-01-16T00:00:00Z", "2026-01-16T00:00:00", 0),
        ("2026-01-16T00:30:00Z", "2026-01-16T00:30:00", 0),
        ("2026-01-16T00:59:00Z", "2026-01-16T00:59:00", 0),
        ("2026-01-16T01:00:00Z", "2026-01-16T01:00:00", 0),
        ("2026-01-16T01:30:00Z", "2026-01-16T01:30:00", 0),
        ("2026-01-16T01:59:00Z", "2026-01-16T01:59:00", 0),
        ("2026-01-16T02:00:00Z", "2026-01-16T02:00:00", 0),
        ("2026-01-16T02:30:00Z", "2026-01-16T02:30:00", 0),
        ("2026-03-28T22:00:00Z", "2026-03-28T22:00:00", 0),
        ("2026-03-28T23:00:00Z", "2026-03-28T23:00:00", 0),
        ("2026-03-28T23:30:00Z", "2026-03-28T23:30:00", 0),
        ("2026-03-29T00:00:00Z", "2026-03-29T00:00:00", 0),
        ("2026-03-29T00:30:00Z", "2026-03-29T00:30:00", 0),
        ("2026-03-29T00:59:00Z", "2026-03-29T00:59:00", 0),
        ("2026-03-29T01:00:00Z", "2026-03-29T02:00:00", 3600),
        ("2026-03-29T01:01:00Z", "2026-03-29T02:01:00", 3600),
        ("2026-03-29T01:30:00Z", "2026-03-29T02:30:00", 3600),
        ("2026-03-29T01:59:00Z", "2026-03-29T02:59:00", 3600),
        ("2026-03-29T02:00:00Z", "2026-03-29T03:00:00", 3600),
        ("2026-03-29T02:30:00Z", "2026-03-29T03:30:00", 3600),
        ("2026-03-29T03:00:00Z", "2026-03-29T04:00:00", 3600),
        ("2026-03-29T12:00:00Z", "2026-03-29T13:00:00", 3600),
        ("2026-03-29T22:59:00Z", "2026-03-29T23:59:00", 3600),
        ("2026-03-29T23:00:00Z", "2026-03-30T00:00:00", 3600),
        ("2026-03-29T23:30:00Z", "2026-03-30T00:30:00", 3600),
        ("2026-03-30T00:30:00Z", "2026-03-30T01:30:00", 3600),
        ("2026-06-30T22:00:00Z", "2026-06-30T23:00:00", 3600),
        ("2026-06-30T22:59:00Z", "2026-06-30T23:59:00", 3600),
        ("2026-06-30T23:00:00Z", "2026-07-01T00:00:00", 3600),
        ("2026-06-30T23:30:00Z", "2026-07-01T00:30:00", 3600),
        ("2026-06-30T23:59:00Z", "2026-07-01T00:59:00", 3600),
        ("2026-07-01T00:00:00Z", "2026-07-01T01:00:00", 3600),
        ("2026-07-01T00:30:00Z", "2026-07-01T01:30:00", 3600),
        ("2026-07-01T00:59:00Z", "2026-07-01T01:59:00", 3600),
        ("2026-07-01T01:00:00Z", "2026-07-01T02:00:00", 3600),
        ("2026-07-01T01:30:00Z", "2026-07-01T02:30:00", 3600),
        ("2026-07-01T01:59:00Z", "2026-07-01T02:59:00", 3600),
        ("2026-07-01T02:00:00Z", "2026-07-01T03:00:00", 3600),
        ("2026-07-01T02:30:00Z", "2026-07-01T03:30:00", 3600),
        ("2026-07-15T22:00:00Z", "2026-07-15T23:00:00", 3600),
        ("2026-07-15T22:59:00Z", "2026-07-15T23:59:00", 3600),
        ("2026-07-15T23:00:00Z", "2026-07-16T00:00:00", 3600),
        ("2026-07-15T23:30:00Z", "2026-07-16T00:30:00", 3600),
        ("2026-07-15T23:59:00Z", "2026-07-16T00:59:00", 3600),
        ("2026-07-16T00:00:00Z", "2026-07-16T01:00:00", 3600),
        ("2026-07-16T00:30:00Z", "2026-07-16T01:30:00", 3600),
        ("2026-07-16T00:59:00Z", "2026-07-16T01:59:00", 3600),
        ("2026-07-16T01:00:00Z", "2026-07-16T02:00:00", 3600),
        ("2026-07-16T01:30:00Z", "2026-07-16T02:30:00", 3600),
        ("2026-07-16T01:59:00Z", "2026-07-16T02:59:00", 3600),
        ("2026-07-16T02:00:00Z", "2026-07-16T03:00:00", 3600),
        ("2026-07-16T02:30:00Z", "2026-07-16T03:30:00", 3600),
        ("2026-10-24T22:00:00Z", "2026-10-24T23:00:00", 3600),
        ("2026-10-24T23:00:00Z", "2026-10-25T00:00:00", 3600),
        ("2026-10-24T23:30:00Z", "2026-10-25T00:30:00", 3600),
        ("2026-10-25T00:00:00Z", "2026-10-25T01:00:00", 3600),
        ("2026-10-25T00:30:00Z", "2026-10-25T01:30:00", 3600),
        ("2026-10-25T00:59:00Z", "2026-10-25T01:59:00", 3600),
        ("2026-10-25T01:00:00Z", "2026-10-25T01:00:00", 0),
        ("2026-10-25T01:01:00Z", "2026-10-25T01:01:00", 0),
        ("2026-10-25T01:30:00Z", "2026-10-25T01:30:00", 0),
        ("2026-10-25T01:59:00Z", "2026-10-25T01:59:00", 0),
        ("2026-10-25T02:00:00Z", "2026-10-25T02:00:00", 0),
        ("2026-10-25T02:30:00Z", "2026-10-25T02:30:00", 0),
        ("2026-10-25T03:00:00Z", "2026-10-25T03:00:00", 0),
        ("2026-10-25T12:00:00Z", "2026-10-25T12:00:00", 0),
        ("2026-10-25T22:59:00Z", "2026-10-25T22:59:00", 0),
        ("2026-10-25T23:00:00Z", "2026-10-25T23:00:00", 0),
        ("2026-10-25T23:30:00Z", "2026-10-25T23:30:00", 0),
        ("2026-10-26T00:30:00Z", "2026-10-26T00:30:00", 0),
    ];

    /// `(local date and time, what it means)`; `None` is a skipped time,
    /// one instant a normal time, two a repeated time.
    #[rustfmt::skip]
    const RESOLVE: &[(&str, Want)] = &[
        ("2025-03-30T00:00:00", Single("2025-03-30T00:00:00Z")),
        ("2025-03-30T00:30:00", Single("2025-03-30T00:30:00Z")),
        ("2025-03-30T00:59:00", Single("2025-03-30T00:59:00Z")),
        ("2025-03-30T01:00:00", Gap),
        ("2025-03-30T01:01:00", Gap),
        ("2025-03-30T01:30:00", Gap),
        ("2025-03-30T01:59:00", Gap),
        ("2025-03-30T02:00:00", Single("2025-03-30T01:00:00Z")),
        ("2025-03-30T02:01:00", Single("2025-03-30T01:01:00Z")),
        ("2025-03-30T02:30:00", Single("2025-03-30T01:30:00Z")),
        ("2025-03-30T03:00:00", Single("2025-03-30T02:00:00Z")),
        ("2025-03-29T23:30:00", Single("2025-03-29T23:30:00Z")),
        ("2025-03-29T23:59:00", Single("2025-03-29T23:59:00Z")),
        ("2025-10-26T00:00:00", Single("2025-10-25T23:00:00Z")),
        ("2025-10-26T00:30:00", Single("2025-10-25T23:30:00Z")),
        ("2025-10-26T00:59:00", Single("2025-10-25T23:59:00Z")),
        ("2025-10-26T01:00:00", Ambiguous("2025-10-26T00:00:00Z", "2025-10-26T01:00:00Z")),
        ("2025-10-26T01:01:00", Ambiguous("2025-10-26T00:01:00Z", "2025-10-26T01:01:00Z")),
        ("2025-10-26T01:30:00", Ambiguous("2025-10-26T00:30:00Z", "2025-10-26T01:30:00Z")),
        ("2025-10-26T01:59:00", Ambiguous("2025-10-26T00:59:00Z", "2025-10-26T01:59:00Z")),
        ("2025-10-26T02:00:00", Single("2025-10-26T02:00:00Z")),
        ("2025-10-26T02:01:00", Single("2025-10-26T02:01:00Z")),
        ("2025-10-26T02:30:00", Single("2025-10-26T02:30:00Z")),
        ("2025-10-26T03:00:00", Single("2025-10-26T03:00:00Z")),
        ("2025-10-25T23:30:00", Single("2025-10-25T22:30:00Z")),
        ("2025-10-25T23:59:00", Single("2025-10-25T22:59:00Z")),
        ("2026-03-29T00:00:00", Single("2026-03-29T00:00:00Z")),
        ("2026-03-29T00:30:00", Single("2026-03-29T00:30:00Z")),
        ("2026-03-29T00:59:00", Single("2026-03-29T00:59:00Z")),
        ("2026-03-29T01:00:00", Gap),
        ("2026-03-29T01:01:00", Gap),
        ("2026-03-29T01:30:00", Gap),
        ("2026-03-29T01:59:00", Gap),
        ("2026-03-29T02:00:00", Single("2026-03-29T01:00:00Z")),
        ("2026-03-29T02:01:00", Single("2026-03-29T01:01:00Z")),
        ("2026-03-29T02:30:00", Single("2026-03-29T01:30:00Z")),
        ("2026-03-29T03:00:00", Single("2026-03-29T02:00:00Z")),
        ("2026-03-28T23:30:00", Single("2026-03-28T23:30:00Z")),
        ("2026-03-28T23:59:00", Single("2026-03-28T23:59:00Z")),
        ("2026-10-25T00:00:00", Single("2026-10-24T23:00:00Z")),
        ("2026-10-25T00:30:00", Single("2026-10-24T23:30:00Z")),
        ("2026-10-25T00:59:00", Single("2026-10-24T23:59:00Z")),
        ("2026-10-25T01:00:00", Ambiguous("2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z")),
        ("2026-10-25T01:01:00", Ambiguous("2026-10-25T00:01:00Z", "2026-10-25T01:01:00Z")),
        ("2026-10-25T01:30:00", Ambiguous("2026-10-25T00:30:00Z", "2026-10-25T01:30:00Z")),
        ("2026-10-25T01:59:00", Ambiguous("2026-10-25T00:59:00Z", "2026-10-25T01:59:00Z")),
        ("2026-10-25T02:00:00", Single("2026-10-25T02:00:00Z")),
        ("2026-10-25T02:01:00", Single("2026-10-25T02:01:00Z")),
        ("2026-10-25T02:30:00", Single("2026-10-25T02:30:00Z")),
        ("2026-10-25T03:00:00", Single("2026-10-25T03:00:00Z")),
        ("2026-10-24T23:30:00", Single("2026-10-24T22:30:00Z")),
        ("2026-10-24T23:59:00", Single("2026-10-24T22:59:00Z")),
        ("2026-01-15T00:00:00", Single("2026-01-15T00:00:00Z")),
        ("2026-01-15T00:30:00", Single("2026-01-15T00:30:00Z")),
        ("2026-01-15T01:30:00", Single("2026-01-15T01:30:00Z")),
        ("2026-01-15T02:30:00", Single("2026-01-15T02:30:00Z")),
        ("2026-01-15T08:22:00", Single("2026-01-15T08:22:00Z")),
        ("2026-01-15T23:59:00", Single("2026-01-15T23:59:00Z")),
        ("2026-07-15T00:00:00", Single("2026-07-14T23:00:00Z")),
        ("2026-07-15T00:30:00", Single("2026-07-14T23:30:00Z")),
        ("2026-07-15T01:30:00", Single("2026-07-15T00:30:00Z")),
        ("2026-07-15T02:30:00", Single("2026-07-15T01:30:00Z")),
        ("2026-07-15T08:22:00", Single("2026-07-15T07:22:00Z")),
        ("2026-07-15T23:59:00", Single("2026-07-15T22:59:00Z")),
    ];

    /// `(date, time, instant)` under [`Zone::to_utc_gap_shifted`].
    #[rustfmt::skip]
    const SHIFTED: &[(&str, &str, &str)] = &[
        ("2025-03-30", "00:00:00", "2025-03-30T00:00:00Z"),
        ("2025-03-30", "00:30:00", "2025-03-30T00:30:00Z"),
        ("2025-03-30", "00:59:00", "2025-03-30T00:59:00Z"),
        ("2025-03-30", "01:00:00", "2025-03-30T01:00:00Z"),
        ("2025-03-30", "01:01:00", "2025-03-30T01:01:00Z"),
        ("2025-03-30", "01:30:00", "2025-03-30T01:30:00Z"),
        ("2025-03-30", "01:59:00", "2025-03-30T01:59:00Z"),
        ("2025-03-30", "02:00:00", "2025-03-30T01:00:00Z"),
        ("2025-03-30", "02:01:00", "2025-03-30T01:01:00Z"),
        ("2025-03-30", "02:30:00", "2025-03-30T01:30:00Z"),
        ("2025-03-30", "03:00:00", "2025-03-30T02:00:00Z"),
        ("2025-03-29", "23:30:00", "2025-03-29T23:30:00Z"),
        ("2025-03-29", "23:59:00", "2025-03-29T23:59:00Z"),
        ("2025-10-26", "00:00:00", "2025-10-25T23:00:00Z"),
        ("2025-10-26", "00:30:00", "2025-10-25T23:30:00Z"),
        ("2025-10-26", "00:59:00", "2025-10-25T23:59:00Z"),
        ("2025-10-26", "01:00:00", "2025-10-26T00:00:00Z"),
        ("2025-10-26", "01:01:00", "2025-10-26T00:01:00Z"),
        ("2025-10-26", "01:30:00", "2025-10-26T00:30:00Z"),
        ("2025-10-26", "01:59:00", "2025-10-26T00:59:00Z"),
        ("2025-10-26", "02:00:00", "2025-10-26T02:00:00Z"),
        ("2025-10-26", "02:01:00", "2025-10-26T02:01:00Z"),
        ("2025-10-26", "02:30:00", "2025-10-26T02:30:00Z"),
        ("2025-10-26", "03:00:00", "2025-10-26T03:00:00Z"),
        ("2025-10-25", "23:30:00", "2025-10-25T22:30:00Z"),
        ("2025-10-25", "23:59:00", "2025-10-25T22:59:00Z"),
        ("2026-03-29", "00:00:00", "2026-03-29T00:00:00Z"),
        ("2026-03-29", "00:30:00", "2026-03-29T00:30:00Z"),
        ("2026-03-29", "00:59:00", "2026-03-29T00:59:00Z"),
        ("2026-03-29", "01:00:00", "2026-03-29T01:00:00Z"),
        ("2026-03-29", "01:01:00", "2026-03-29T01:01:00Z"),
        ("2026-03-29", "01:30:00", "2026-03-29T01:30:00Z"),
        ("2026-03-29", "01:59:00", "2026-03-29T01:59:00Z"),
        ("2026-03-29", "02:00:00", "2026-03-29T01:00:00Z"),
        ("2026-03-29", "02:01:00", "2026-03-29T01:01:00Z"),
        ("2026-03-29", "02:30:00", "2026-03-29T01:30:00Z"),
        ("2026-03-29", "03:00:00", "2026-03-29T02:00:00Z"),
        ("2026-03-28", "23:30:00", "2026-03-28T23:30:00Z"),
        ("2026-03-28", "23:59:00", "2026-03-28T23:59:00Z"),
        ("2026-10-25", "00:00:00", "2026-10-24T23:00:00Z"),
        ("2026-10-25", "00:30:00", "2026-10-24T23:30:00Z"),
        ("2026-10-25", "00:59:00", "2026-10-24T23:59:00Z"),
        ("2026-10-25", "01:00:00", "2026-10-25T00:00:00Z"),
        ("2026-10-25", "01:01:00", "2026-10-25T00:01:00Z"),
        ("2026-10-25", "01:30:00", "2026-10-25T00:30:00Z"),
        ("2026-10-25", "01:59:00", "2026-10-25T00:59:00Z"),
        ("2026-10-25", "02:00:00", "2026-10-25T02:00:00Z"),
        ("2026-10-25", "02:01:00", "2026-10-25T02:01:00Z"),
        ("2026-10-25", "02:30:00", "2026-10-25T02:30:00Z"),
        ("2026-10-25", "03:00:00", "2026-10-25T03:00:00Z"),
        ("2026-10-24", "23:30:00", "2026-10-24T22:30:00Z"),
        ("2026-10-24", "23:59:00", "2026-10-24T22:59:00Z"),
        ("2026-01-15", "00:00:00", "2026-01-15T00:00:00Z"),
        ("2026-01-15", "00:30:00", "2026-01-15T00:30:00Z"),
        ("2026-01-15", "01:30:00", "2026-01-15T01:30:00Z"),
        ("2026-01-15", "02:30:00", "2026-01-15T02:30:00Z"),
        ("2026-01-15", "08:22:00", "2026-01-15T08:22:00Z"),
        ("2026-01-15", "23:59:00", "2026-01-15T23:59:00Z"),
        ("2026-07-15", "00:00:00", "2026-07-14T23:00:00Z"),
        ("2026-07-15", "00:30:00", "2026-07-14T23:30:00Z"),
        ("2026-07-15", "01:30:00", "2026-07-15T00:30:00Z"),
        ("2026-07-15", "02:30:00", "2026-07-15T01:30:00Z"),
        ("2026-07-15", "08:22:00", "2026-07-15T07:22:00Z"),
        ("2026-07-15", "23:59:00", "2026-07-15T22:59:00Z"),
    ];

    #[derive(Debug, Clone, Copy)]
    enum Want {
        Single(&'static str),
        Ambiguous(&'static str, &'static str),
        Gap,
    }

    const ZONES: [Zone; 2] = [Zone::LONDON, Zone::DUBLIN];

    fn instant(raw: &str) -> DateTime<Utc> {
        raw.parse().unwrap()
    }

    fn naive(raw: &str) -> NaiveDateTime {
        raw.parse().unwrap()
    }

    #[test]
    fn local_views_match_the_table() {
        for zone in ZONES {
            for &(at, local, offset) in OFFSETS {
                let at_utc = instant(at);
                let local = naive(local);
                assert_eq!(zone.naive_local(at_utc), local, "{zone} at {at}");
                assert_eq!(zone.date(at_utc), local.date(), "{zone} date at {at}");
                assert_eq!(zone.time(at_utc), local.time(), "{zone} time at {at}");
                assert_eq!(
                    zone.offset_at(at_utc).local_minus_utc(),
                    offset,
                    "{zone} offset at {at}"
                );
                let shown = zone.local(at_utc);
                assert_eq!(shown.naive_local(), local, "{zone} local at {at}");
                assert_eq!(shown.with_timezone(&Utc), at_utc, "{zone} instant at {at}");
            }
        }
    }

    #[test]
    fn local_renders_rfc3339_with_the_zone_offset() {
        assert_eq!(
            london_local(instant("2026-07-15T07:22:00Z")).to_rfc3339(),
            "2026-07-15T08:22:00+01:00"
        );
        assert_eq!(
            london_local(instant("2026-01-15T08:22:00Z")).to_rfc3339(),
            "2026-01-15T08:22:00+00:00"
        );
        assert_eq!(
            dublin_local(instant("2026-03-29T01:00:00Z")).to_rfc3339(),
            "2026-03-29T02:00:00+01:00"
        );
        assert_eq!(
            dublin_local(instant("2026-10-25T01:00:00Z")).to_rfc3339(),
            "2026-10-25T01:00:00+00:00"
        );
    }

    #[test]
    fn london_helpers_agree_with_the_zone() {
        for &(at, local, _) in OFFSETS {
            let at_utc = instant(at);
            let local = naive(local);
            assert_eq!(london_naive(at_utc), local, "{at}");
            assert_eq!(london_date(at_utc), local.date(), "{at}");
            assert_eq!(london_time(at_utc), local.time(), "{at}");
        }
    }

    #[test]
    fn resolve_reports_single_repeated_and_skipped_times() {
        for zone in ZONES {
            for &(local, want) in RESOLVE {
                let expected = match want {
                    Single(at) => LocalTime::Single(instant(at)),
                    Ambiguous(earliest, latest) => LocalTime::Ambiguous {
                        earliest: instant(earliest),
                        latest: instant(latest),
                    },
                    Gap => LocalTime::Gap,
                };
                assert_eq!(zone.resolve(naive(local)), expected, "{zone} {local}");
            }
        }
        assert_eq!(london_resolve(naive("2026-03-29T01:30:00")), LocalTime::Gap);
    }

    #[test]
    fn to_utc_earliest_is_the_first_reading_and_none_in_the_gap() {
        for zone in ZONES {
            for &(local, want) in RESOLVE {
                let expected = match want {
                    Single(at) | Ambiguous(at, _) => Some(instant(at)),
                    Gap => None,
                };
                assert_eq!(
                    zone.to_utc_earliest(naive(local)),
                    expected,
                    "{zone} {local}"
                );
            }
        }
        assert_eq!(
            london_to_utc_earliest(naive("2026-10-25T01:30:00")),
            Some(instant("2026-10-25T00:30:00Z"))
        );
        assert_eq!(dublin_to_utc_earliest(naive("2026-03-29T01:30:00")), None);
    }

    #[test]
    fn to_utc_gap_shifted_reads_a_skipped_time_an_hour_on() {
        for zone in ZONES {
            for &(date, time, expected) in SHIFTED {
                let local = naive(&format!("{date}T{time}"));
                assert_eq!(
                    zone.to_utc_gap_shifted(local),
                    Some(instant(expected)),
                    "{zone} {date} {time}"
                );
            }
        }
        assert_eq!(
            london_to_utc_gap_shifted(naive("2026-03-29T01:30:00")),
            Some(instant("2026-03-29T01:30:00Z"))
        );
    }

    #[test]
    fn parse_accepts_exact_iana_names_only() {
        for (name, expected) in [
            ("Europe/London", Some("Europe/London")),
            ("Europe/Dublin", Some("Europe/Dublin")),
            ("America/New_York", Some("America/New_York")),
            ("Asia/Kolkata", Some("Asia/Kolkata")),
            ("GB", Some("GB")),
            ("US/Eastern", Some("US/Eastern")),
            ("Europe/Kyiv", Some("Europe/Kyiv")),
            ("Europe/Kiev", Some("Europe/Kiev")),
            ("UTC", Some("UTC")),
            ("Etc/UTC", Some("Etc/UTC")),
            // Case variants, abbreviations, offsets, junk and empty input.
            ("europe/london", None),
            ("EUROPE/LONDON", None),
            ("Europe/london", None),
            ("BST", None),
            ("Z", None),
            ("+01:00", None),
            ("Mars/Olympus", None),
            ("Europe/London ", None),
            (" Europe/London", None),
            ("", None),
        ] {
            assert_eq!(
                Zone::parse(name).map(Zone::name),
                expected,
                "parse {name:?}"
            );
        }
        assert_eq!(Zone::parse("Europe/London"), Some(Zone::LONDON));
        assert_eq!(Zone::parse("Europe/Dublin"), Some(Zone::DUBLIN));
        assert_eq!(Zone::LONDON.to_string(), "Europe/London");
    }

    /// The database lookup ignores case and has a few names that are not
    /// places; parsing accepts none of them unless it is the exact name.
    #[test]
    fn parse_refuses_what_the_lookup_alone_would_accept() {
        for name in [
            "utc",
            "Utc",
            "etc/utc",
            "ETC/UTC",
            "gb",
            "us/eastern",
            "europe/kyiv",
            "Etc/Unknown",
            "etc/unknown",
            "Factory",
            "UTC+1",
            "UTC+01:00",
            "GMT+1",
            "Europe/London\n",
            "Europe/London\0",
            "../Europe/London",
            "Europe//London",
            "Europe",
            "EST5EDT ",
        ] {
            assert_eq!(Zone::parse(name), None, "parse {name:?}");
        }
        for name in [
            "EST5EDT",
            "Etc/GMT+5",
            "Australia/Lord_Howe",
            "Eire",
            "Zulu",
        ] {
            assert_eq!(
                Zone::parse(name).map(Zone::name),
                Some(name),
                "parse {name:?}"
            );
        }
    }

    #[test]
    fn zones_are_copy_eq_hash_debug_and_display() {
        use std::collections::HashSet;

        let new_york = Zone::parse("America/New_York").unwrap();
        let again = Zone::parse("America/New_York").unwrap();
        let copy = new_york;
        assert_eq!(new_york, copy);
        assert_eq!(new_york, again);
        assert_ne!(new_york, Zone::LONDON);
        assert_ne!(Zone::LONDON, Zone::DUBLIN);
        assert_ne!(Zone::parse("GB").unwrap(), Zone::LONDON);
        let set: HashSet<Zone> = [new_york, again, copy, Zone::LONDON, Zone::LONDON]
            .into_iter()
            .collect();
        assert_eq!(set.len(), 2);
        assert_eq!(new_york.to_string(), "America/New_York");
        assert_eq!(format!("{:?}", Zone::LONDON), r#"Zone("Europe/London")"#);
    }

    /// An instant or wall-clock time outside `jiff`'s range is read at its
    /// edge, not a panic.
    #[test]
    fn times_beyond_the_library_range_do_not_panic() {
        let far_future = NaiveDateTime::MAX.and_utc();
        let far_past = NaiveDateTime::MIN.and_utc();
        assert_eq!(Zone::LONDON.offset_at(far_future).local_minus_utc(), 0);
        let _ = Zone::LONDON.offset_at(far_past);
        assert_eq!(Zone::LONDON.resolve(NaiveDateTime::MAX), LocalTime::Gap);
        assert_eq!(Zone::LONDON.to_utc_gap_shifted(NaiveDateTime::MIN), None);
    }

    #[test]
    fn a_user_zone_converts_with_its_own_rules() {
        let new_york = Zone::parse("America/New_York").unwrap();
        // US clocks change on 2026-03-08 and 2026-11-01, not on the UK dates.
        assert_eq!(
            new_york.naive_local(instant("2026-03-29T01:30:00Z")),
            naive("2026-03-28T21:30:00")
        );
        assert_eq!(
            new_york
                .offset_at(instant("2026-03-07T12:00:00Z"))
                .local_minus_utc(),
            -5 * 3600
        );
        assert_eq!(
            new_york
                .offset_at(instant("2026-03-08T12:00:00Z"))
                .local_minus_utc(),
            -4 * 3600
        );
        assert_eq!(
            new_york.resolve(naive("2026-03-08T02:30:00")),
            LocalTime::Gap
        );
        assert_eq!(
            new_york.resolve(naive("2026-11-01T01:30:00")),
            LocalTime::Ambiguous {
                earliest: instant("2026-11-01T05:30:00Z"),
                latest: instant("2026-11-01T06:30:00Z"),
            }
        );
        let kolkata = Zone::parse("Asia/Kolkata").unwrap();
        assert_eq!(
            kolkata.naive_local(instant("2026-07-15T12:00:00Z")),
            naive("2026-07-15T17:30:00")
        );
    }
}
