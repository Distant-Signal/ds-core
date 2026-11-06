//! Regular journeys' recurrence: the one rule for which dates a template
//! is due on (distant-signal's
//! docs/superpowers/specs/2026-11-06-regular-journeys-design.md, and the
//! scheduling options that extend it).
//!
//! The notifier's mint, the api's `nextDate` and `upcomingDates`, and the
//! frontend's preview (through the api's `POST /JourneyTemplates/preview-dates`)
//! all decide "is this date due" here, so the date a page shows is the date
//! that gets minted. Before this module the rule was written four times.
//!
//! A [`Rule`] is the template's own pattern: its kind, weekdays, interval,
//! start and end, pause and extra dates. [`Exceptions`] are the dates the
//! pattern would run but something else skips: a tombstone left by a
//! skipped or deleted occurrence, a bank holiday in the owner's division,
//! the owner's time away. [`Rule::day`] combines the two for one date, and
//! [`Rule::upcoming`] lists the next scheduled dates with why any is
//! skipped.
//!
//! Every date here is a London service date ([`NaiveDate`]); nothing here
//! reads an instant, so a clock change can't move a date.
//!
//! An unknown `recurrence_kind` ([`Kind::Unknown`]) is never due: a
//! notifier built before a new kind existed must not mint one it can't
//! read.

use std::collections::{BTreeSet, HashMap};

use chrono::{Datelike, Duration, NaiveDate};

/// `journey_templates.recurrence_kind` for the weekdays pattern (every
/// template made before the kinds existed).
pub const KIND_WEEKLY: &str = "weekly";
/// `recurrence_kind` for a list of dates: the template runs only on its
/// extra dates.
pub const KIND_DATES: &str = "dates";
/// `recurrence_kind` reserved for shift rotas. No code reads or writes it
/// yet, so it is [`Kind::Unknown`] here: never due.
pub const KIND_CYCLE: &str = "cycle";

/// The largest `interval_weeks` ("every 4 weeks").
pub const MAX_INTERVAL_WEEKS: i16 = 4;

/// How far ahead [`Rule::upcoming`] and [`Rule::next_due`] look by default,
/// in days. A tombstone can be 366 days ahead, so this reaches past the
/// last one.
pub const DEFAULT_HORIZON_DAYS: u32 = 400;

/// A template's recurrence kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// [`KIND_WEEKLY`].
    Weekly,
    /// [`KIND_DATES`].
    Dates,
    /// Any other value, [`KIND_CYCLE`] included: never due.
    Unknown,
}

impl Kind {
    /// The kind a `recurrence_kind` value names.
    #[must_use]
    pub fn parse(value: &str) -> Self {
        match value {
            KIND_WEEKLY => Self::Weekly,
            KIND_DATES => Self::Dates,
            _ => Self::Unknown,
        }
    }
}

/// Why a scheduled date doesn't get a journey.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SkipReason {
    /// The template is paused until a later date (`paused_until`).
    Paused,
    /// The date falls in one of the owner's time away periods.
    Away,
    /// A tombstone (`journey_template_skipped_dates`): the user skipped the
    /// date, or deleted its journey.
    Skipped,
    /// A bank holiday in the owner's division, and the template skips them.
    BankHoliday,
}

impl SkipReason {
    /// The API's name for the reason (`nextSkips`, `upcomingDates`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Paused => "paused",
            Self::Away => "away",
            Self::Skipped => "skipped",
            Self::BankHoliday => "bank_holiday",
        }
    }
}

/// What a template does on one date ([`Rule::day`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Day {
    /// The pattern doesn't include the date.
    NotScheduled,
    /// A journey is made for it.
    Due,
    /// The pattern includes it, but it is skipped.
    Skipped(SkipReason),
}

/// One date of [`Rule::upcoming`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Upcoming {
    /// The service date.
    pub date: NaiveDate,
    /// `None` when it is due; otherwise why not.
    pub skip: Option<SkipReason>,
}

/// The dates a template's pattern runs on but which are skipped anyway.
/// [`load`] fills them for a date range.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Exceptions {
    /// Tombstones (`journey_template_skipped_dates`).
    pub skipped: BTreeSet<NaiveDate>,
    /// Bank holidays in the owner's division. Empty when the template
    /// doesn't skip them (`skip_bank_holidays` off).
    pub bank_holidays: BTreeSet<NaiveDate>,
    /// The owner's time away, as inclusive `(starts_on, ends_on)` ranges.
    /// Time away applies to every regular journey the user has.
    pub away: Vec<(NaiveDate, NaiveDate)>,
}

impl Exceptions {
    fn away_on(&self, date: NaiveDate) -> bool {
        self.away
            .iter()
            .any(|&(start, end)| start <= date && date <= end)
    }
}

/// A template's recurrence pattern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    /// `recurrence_kind`.
    pub kind: Kind,
    /// `active`: a template paused with no end is never due.
    pub active: bool,
    /// `days_of_week`: Monday is bit 0, Sunday bit 6. `None` for a weekly
    /// template that only runs on its extra dates. Unused by
    /// [`Kind::Dates`].
    pub days_of_week: Option<i16>,
    /// `interval_weeks`, 1 to [`MAX_INTERVAL_WEEKS`]: the weekdays run
    /// every this many weeks, counted from `anchor_week`.
    pub interval_weeks: i16,
    /// `anchor_week`: the Monday of a week that runs. Needed when
    /// `interval_weeks` is above 1; a date in another week is resolved to
    /// its own week's Monday first, so any day of the anchor week works.
    pub anchor_week: Option<NaiveDate>,
    /// `starts_on`: no weekday date before it.
    pub starts_on: Option<NaiveDate>,
    /// `ends_on`: no weekday date after it.
    pub ends_on: Option<NaiveDate>,
    /// `paused_until`: every date before it is skipped
    /// ([`SkipReason::Paused`]); the template runs again on it.
    pub paused_until: Option<NaiveDate>,
    /// `journey_template_extra_dates`: dates the template also runs on. For
    /// [`Kind::Dates`] they are its only dates. They are outside the
    /// weekday pattern, so `starts_on`, `ends_on` and bank holidays don't
    /// apply to them; a tombstone, time away and a pause still do.
    pub extra_dates: BTreeSet<NaiveDate>,
}

impl Rule {
    /// A weekly, every-week, active template on `days_of_week`, with
    /// nothing else set: what every template made before the scheduling
    /// options is.
    #[must_use]
    pub fn weekly(days_of_week: Option<i16>) -> Self {
        Self {
            kind: Kind::Weekly,
            active: true,
            days_of_week,
            interval_weeks: 1,
            anchor_week: None,
            starts_on: None,
            ends_on: None,
            paused_until: None,
            extra_dates: BTreeSet::new(),
        }
    }

    /// Whether the weekday pattern (not the extra dates) includes `date`:
    /// the weekday's bit, inside `starts_on..=ends_on`, in a running week.
    fn weekday_pattern(&self, date: NaiveDate) -> bool {
        if self.kind != Kind::Weekly {
            return false;
        }
        let Some(days) = self.days_of_week else {
            return false;
        };
        let bit = 1_i16 << date.weekday().num_days_from_monday();
        if days & bit == 0 {
            return false;
        }
        if self.starts_on.is_some_and(|start| date < start)
            || self.ends_on.is_some_and(|end| date > end)
        {
            return false;
        }
        self.week_runs(date)
    }

    /// Whether `date`'s week is one the interval runs.
    fn week_runs(&self, date: NaiveDate) -> bool {
        let interval = i64::from(self.interval_weeks);
        if interval <= 1 {
            return true;
        }
        if interval > i64::from(MAX_INTERVAL_WEEKS) {
            return false;
        }
        // The CHECK makes an interval above 1 carry an anchor; without
        // one, refuse rather than guess the week.
        let Some(anchor) = self.anchor_week else {
            return false;
        };
        let weeks = (monday_of(date) - monday_of(anchor)).num_days() / 7;
        weeks.rem_euclid(interval) == 0
    }

    /// What the template does on `date`, given its exceptions.
    ///
    /// The reasons, when more than one applies: [`SkipReason::Paused`],
    /// then [`SkipReason::Away`], then [`SkipReason::Skipped`], then
    /// [`SkipReason::BankHoliday`]. A tombstone shows ahead of a bank
    /// holiday so it can be undone.
    #[must_use]
    pub fn day(&self, exceptions: &Exceptions, date: NaiveDate) -> Day {
        if !self.active || self.kind == Kind::Unknown {
            return Day::NotScheduled;
        }
        let extra = self.extra_dates.contains(&date);
        let pattern = !extra && self.weekday_pattern(date);
        if !extra && !pattern {
            return Day::NotScheduled;
        }
        if self.paused_until.is_some_and(|until| date < until) {
            return Day::Skipped(SkipReason::Paused);
        }
        if exceptions.away_on(date) {
            return Day::Skipped(SkipReason::Away);
        }
        if exceptions.skipped.contains(&date) {
            return Day::Skipped(SkipReason::Skipped);
        }
        if pattern && exceptions.bank_holidays.contains(&date) {
            return Day::Skipped(SkipReason::BankHoliday);
        }
        Day::Due
    }

    /// Whether a journey is made for `date`.
    #[must_use]
    pub fn is_due(&self, exceptions: &Exceptions, date: NaiveDate) -> bool {
        self.day(exceptions, date) == Day::Due
    }

    /// The first `n` dates from `from` (inclusive) the pattern includes,
    /// due or skipped, within `horizon_days` days of `from`.
    #[must_use]
    pub fn upcoming(
        &self,
        exceptions: &Exceptions,
        from: NaiveDate,
        n: usize,
        horizon_days: u32,
    ) -> Vec<Upcoming> {
        let mut out = Vec::new();
        if n == 0 {
            return out;
        }
        for date in horizon(from, horizon_days) {
            match self.day(exceptions, date) {
                Day::NotScheduled => continue,
                Day::Due => out.push(Upcoming { date, skip: None }),
                Day::Skipped(reason) => out.push(Upcoming {
                    date,
                    skip: Some(reason),
                }),
            }
            if out.len() >= n {
                break;
            }
        }
        out
    }

    /// The first due date from `from` (inclusive) within `horizon_days`
    /// days, passing over every skipped one. `None` only when no date in
    /// the horizon is due.
    #[must_use]
    pub fn next_due(
        &self,
        exceptions: &Exceptions,
        from: NaiveDate,
        horizon_days: u32,
    ) -> Option<NaiveDate> {
        horizon(from, horizon_days).find(|&date| self.is_due(exceptions, date))
    }
}

/// A template's rule and its exceptions over a date range ([`load`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schedule {
    /// The template's owner.
    pub user_id: String,
    /// The pattern.
    pub rule: Rule,
    /// The exceptions in the range [`load`] was asked for.
    pub exceptions: Exceptions,
}

/// [`load`]'s row.
#[derive(sqlx::FromRow)]
struct ScheduleRow {
    id: i64,
    user_id: String,
    active: bool,
    days_of_week: Option<i16>,
    starts_on: Option<NaiveDate>,
    ends_on: Option<NaiveDate>,
    paused_until: Option<NaiveDate>,
    skipped: Vec<NaiveDate>,
    bank_holidays: Vec<NaiveDate>,
    away_starts: Vec<NaiveDate>,
    away_ends: Vec<NaiveDate>,
}

/// SQL for the bank holidays from `$from` to `$to` (inclusive, SQL `DATE`
/// expressions) in the division of `user` (an SQL text expression for a
/// `users.id`), as a `DATE[]`: their `bank_holiday_division`, or
/// [`crate::bank_holidays::DEFAULT_DIVISION`] when they haven't chosen.
fn bank_holidays_array_sql(user: &str, from: &str, to: &str) -> String {
    let default = crate::bank_holidays::DEFAULT_DIVISION;
    format!(
        "ARRAY(SELECT rc_h.holiday_date FROM bank_holidays rc_h \
                WHERE rc_h.division = COALESCE( \
                        (SELECT rc_u.bank_holiday_division FROM users rc_u WHERE rc_u.id = {user}), \
                        '{default}') \
                  AND rc_h.holiday_date BETWEEN {from} AND {to} \
                ORDER BY rc_h.holiday_date)"
    )
}

/// SQL for one column (`starts_on` or `ends_on`) of `user`'s time away
/// overlapping `$from..=$to`, as a `DATE[]` in a fixed order, so the two
/// columns' arrays pair up by index.
fn away_array_sql(column: &str, user: &str, from: &str, to: &str) -> String {
    format!(
        "ARRAY(SELECT rc_a.{column} FROM user_away_periods rc_a \
                WHERE rc_a.user_id = {user} \
                  AND rc_a.ends_on >= {from} AND rc_a.starts_on <= {to} \
                ORDER BY rc_a.starts_on, rc_a.id)"
    )
}

/// Each template in `template_ids` with its [`Rule`] and its
/// [`Exceptions`] from `from` to `to` (inclusive), by id. A template that
/// doesn't exist is absent.
///
/// One statement, so it runs on a pool, a connection or inside a
/// transaction. Reads `journey_templates` (`id`, `user_id`, `active`,
/// `days_of_week`, `starts_on`, `ends_on`, `paused_until`,
/// `skip_bank_holidays`), `journey_template_skipped_dates`,
/// `user_away_periods`, `bank_holidays` and `users` (`id`,
/// `bank_holiday_division`) (db-grants.yaml).
///
/// # Errors
///
/// The query's.
pub async fn load<'c, E>(
    executor: E,
    template_ids: &[i64],
    from: NaiveDate,
    to: NaiveDate,
) -> anyhow::Result<HashMap<i64, Schedule>>
where
    E: sqlx::PgExecutor<'c>,
{
    let rows: Vec<ScheduleRow> = sqlx::query_as(&format!(
        "SELECT jt.id, jt.user_id, jt.active, jt.days_of_week, jt.starts_on, jt.ends_on, \
                jt.paused_until, \
                ARRAY(SELECT rc_s.service_date FROM journey_template_skipped_dates rc_s \
                       WHERE rc_s.template_id = jt.id \
                         AND rc_s.service_date BETWEEN $2 AND $3 \
                       ORDER BY rc_s.service_date) AS skipped, \
                CASE WHEN jt.skip_bank_holidays THEN {holidays} \
                     ELSE '{{}}'::date[] END AS bank_holidays, \
                {away_starts} AS away_starts, {away_ends} AS away_ends \
         FROM journey_templates jt WHERE jt.id = ANY($1)",
        holidays = bank_holidays_array_sql("jt.user_id", "$2", "$3"),
        away_starts = away_array_sql("starts_on", "jt.user_id", "$2", "$3"),
        away_ends = away_array_sql("ends_on", "jt.user_id", "$2", "$3"),
    ))
    .bind(template_ids)
    .bind(from)
    .bind(to)
    .fetch_all(executor)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let mut rule = Rule::weekly(row.days_of_week);
            rule.active = row.active;
            rule.starts_on = row.starts_on;
            rule.ends_on = row.ends_on;
            rule.paused_until = row.paused_until;
            let exceptions = Exceptions {
                skipped: row.skipped.into_iter().collect(),
                bank_holidays: row.bank_holidays.into_iter().collect(),
                away: row.away_starts.into_iter().zip(row.away_ends).collect(),
            };
            (
                row.id,
                Schedule {
                    user_id: row.user_id,
                    rule,
                    exceptions,
                },
            )
        })
        .collect())
}

/// The exceptions that come from a user rather than a template, from
/// `from` to `to` (inclusive): their time away, and their bank holidays
/// when `skip_bank_holidays`. For previewing a rule not saved yet, which
/// has no tombstones.
///
/// Reads `user_away_periods`, `bank_holidays` and `users` (`id`,
/// `bank_holiday_division`).
///
/// # Errors
///
/// The query's.
pub async fn user_exceptions<'c, E>(
    executor: E,
    user_id: &str,
    skip_bank_holidays: bool,
    from: NaiveDate,
    to: NaiveDate,
) -> anyhow::Result<Exceptions>
where
    E: sqlx::PgExecutor<'c>,
{
    let (holidays, starts, ends): (Vec<NaiveDate>, Vec<NaiveDate>, Vec<NaiveDate>) =
        sqlx::query_as(&format!(
            "SELECT CASE WHEN $4 THEN {holidays} ELSE '{{}}'::date[] END, {starts}, {ends}",
            holidays = bank_holidays_array_sql("$1", "$2", "$3"),
            starts = away_array_sql("starts_on", "$1", "$2", "$3"),
            ends = away_array_sql("ends_on", "$1", "$2", "$3"),
        ))
        .bind(user_id)
        .bind(from)
        .bind(to)
        .bind(skip_bank_holidays)
        .fetch_one(executor)
        .await?;
    Ok(Exceptions {
        bank_holidays: holidays.into_iter().collect(),
        away: starts.into_iter().zip(ends).collect(),
        ..Exceptions::default()
    })
}

/// SQL boolean: whether template `template` (the outer query's alias for a
/// `journey_templates` row) might be due on `date` (an SQL `DATE`
/// expression). A cheap, indexed-columns-only superset of [`Rule::is_due`]
/// for narrowing a scan; the caller then [`load`]s the candidates and
/// decides with [`Rule::is_due`]. Never false for a date the rule is due.
#[must_use]
pub fn may_be_due_sql(template: &str, date: &str) -> String {
    format!(
        "({template}.active \
          AND {template}.days_of_week IS NOT NULL \
          AND ({template}.days_of_week & (1 << (EXTRACT(ISODOW FROM ({date}))::int - 1))) != 0 \
          AND ({date}) >= COALESCE({template}.starts_on, ({date})) \
          AND ({date}) <= COALESCE({template}.ends_on, ({date})))"
    )
}

/// `from`, then each following date, for `days` dates.
fn horizon(from: NaiveDate, days: u32) -> impl Iterator<Item = NaiveDate> {
    from.iter_days().take(days as usize)
}

/// The Monday of `date`'s ISO week.
#[must_use]
pub fn monday_of(date: NaiveDate) -> NaiveDate {
    date - Duration::days(i64::from(date.weekday().num_days_from_monday()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    const WEEKDAYS: i16 = 0b001_1111;
    const MON: i16 = 1;
    const WED: i16 = 1 << 2;

    fn dates(list: &[NaiveDate]) -> BTreeSet<NaiveDate> {
        list.iter().copied().collect()
    }

    #[test]
    fn kinds_parse_and_unknown_is_never_due() {
        assert_eq!(Kind::parse("weekly"), Kind::Weekly);
        assert_eq!(Kind::parse("dates"), Kind::Dates);
        assert_eq!(Kind::parse(KIND_CYCLE), Kind::Unknown);
        assert_eq!(Kind::parse("rrule"), Kind::Unknown);
        let mut rule = Rule::weekly(Some(127));
        rule.kind = Kind::Unknown;
        rule.extra_dates = dates(&[d(2026, 11, 9)]);
        let ex = Exceptions::default();
        for date in d(2026, 11, 6).iter_days().take(30) {
            assert_eq!(rule.day(&ex, date), Day::NotScheduled, "{date}");
        }
        assert_eq!(rule.upcoming(&ex, d(2026, 11, 6), 8, 400), Vec::new());
        assert_eq!(rule.next_due(&ex, d(2026, 11, 6), 400), None);
    }

    #[test]
    fn weekly_runs_on_its_weekdays_inside_its_range() {
        let mut rule = Rule::weekly(Some(WEEKDAYS));
        rule.starts_on = Some(d(2026, 11, 10));
        rule.ends_on = Some(d(2026, 11, 19));
        let ex = Exceptions::default();
        let got: Vec<_> = rule
            .upcoming(&ex, d(2026, 11, 6), 20, 400)
            .into_iter()
            .map(|u| u.date)
            .collect();
        assert_eq!(
            got,
            vec![
                d(2026, 11, 10),
                d(2026, 11, 11),
                d(2026, 11, 12),
                d(2026, 11, 13),
                d(2026, 11, 16),
                d(2026, 11, 17),
                d(2026, 11, 18),
                d(2026, 11, 19),
            ]
        );
        // Saturday 7 Nov is not a weekday.
        assert_eq!(rule.day(&ex, d(2026, 11, 7)), Day::NotScheduled);
    }

    #[test]
    fn inactive_or_no_days_is_never_due() {
        let ex = Exceptions::default();
        let mut paused = Rule::weekly(Some(127));
        paused.active = false;
        assert_eq!(paused.next_due(&ex, d(2026, 11, 6), 400), None);
        assert_eq!(Rule::weekly(None).next_due(&ex, d(2026, 11, 6), 400), None);
        assert_eq!(
            Rule::weekly(Some(0)).next_due(&ex, d(2026, 11, 6), 400),
            None
        );
    }

    #[test]
    fn every_other_week_counts_from_the_anchor_monday() {
        let mut rule = Rule::weekly(Some(MON | WED));
        rule.interval_weeks = 2;
        // Any day of the anchor week anchors it: Thursday 12 Nov 2026.
        rule.anchor_week = Some(d(2026, 11, 12));
        let ex = Exceptions::default();
        let got: Vec<_> = rule
            .upcoming(&ex, d(2026, 11, 2), 6, 400)
            .into_iter()
            .map(|u| u.date)
            .collect();
        assert_eq!(
            got,
            vec![
                // The week of 2 Nov is the one before the anchor: off.
                d(2026, 11, 9),
                d(2026, 11, 11),
                d(2026, 11, 23),
                d(2026, 11, 25),
                d(2026, 12, 7),
                d(2026, 12, 9),
            ]
        );
        // Weeks before the anchor count backwards too.
        assert!(rule.is_due(&ex, d(2026, 10, 26)));
        assert!(!rule.is_due(&ex, d(2026, 11, 2)));
    }

    #[test]
    fn every_four_weeks_and_out_of_range_intervals() {
        let mut rule = Rule::weekly(Some(MON));
        rule.interval_weeks = 4;
        rule.anchor_week = Some(d(2026, 11, 9));
        let ex = Exceptions::default();
        let got: Vec<_> = rule
            .upcoming(&ex, d(2026, 11, 6), 3, 400)
            .into_iter()
            .map(|u| u.date)
            .collect();
        assert_eq!(got, vec![d(2026, 11, 9), d(2026, 12, 7), d(2027, 1, 4)]);

        rule.interval_weeks = 5;
        assert_eq!(rule.next_due(&ex, d(2026, 11, 6), 400), None);
        rule.interval_weeks = 2;
        rule.anchor_week = None;
        assert_eq!(rule.next_due(&ex, d(2026, 11, 6), 400), None);
        // An interval of 1 needs no anchor.
        rule.interval_weeks = 1;
        assert_eq!(
            rule.next_due(&ex, d(2026, 11, 6), 400),
            Some(d(2026, 11, 9))
        );
    }

    #[test]
    fn dates_kind_runs_only_on_its_dates() {
        let mut rule = Rule::weekly(Some(127));
        rule.kind = Kind::Dates;
        rule.extra_dates = dates(&[d(2026, 12, 1), d(2026, 11, 20), d(2027, 3, 3)]);
        // starts_on and ends_on don't limit a list of dates.
        rule.ends_on = Some(d(2026, 11, 1));
        let ex = Exceptions::default();
        let got: Vec<_> = rule
            .upcoming(&ex, d(2026, 11, 6), 8, 400)
            .into_iter()
            .map(|u| u.date)
            .collect();
        assert_eq!(got, vec![d(2026, 11, 20), d(2026, 12, 1), d(2027, 3, 3)]);
    }

    #[test]
    fn extra_dates_add_to_weekly_and_skip_bank_holidays_only_on_the_pattern() {
        let mut rule = Rule::weekly(Some(WEEKDAYS));
        // A Saturday, and a weekday after ends_on.
        rule.extra_dates = dates(&[d(2026, 11, 7), d(2026, 12, 25)]);
        rule.ends_on = Some(d(2026, 12, 18));
        let ex = Exceptions {
            bank_holidays: dates(&[d(2026, 12, 25), d(2026, 11, 9)]),
            ..Exceptions::default()
        };
        assert_eq!(rule.day(&ex, d(2026, 11, 7)), Day::Due);
        assert_eq!(
            rule.day(&ex, d(2026, 11, 9)),
            Day::Skipped(SkipReason::BankHoliday)
        );
        // Chosen by hand on a bank holiday: it runs.
        assert_eq!(rule.day(&ex, d(2026, 12, 25)), Day::Due);
        assert_eq!(rule.day(&ex, d(2026, 12, 24)), Day::NotScheduled);
    }

    #[test]
    fn reasons_and_their_order() {
        let mut rule = Rule::weekly(Some(127));
        rule.paused_until = Some(d(2026, 11, 10));
        let ex = Exceptions {
            skipped: dates(&[d(2026, 11, 9), d(2026, 11, 12), d(2026, 11, 14)]),
            bank_holidays: dates(&[d(2026, 11, 12), d(2026, 11, 13)]),
            away: vec![(d(2026, 11, 14), d(2026, 11, 15))],
        };
        let got = rule.upcoming(&ex, d(2026, 11, 8), 9, 400);
        let want = [
            (d(2026, 11, 8), Some(SkipReason::Paused)),
            // Paused ahead of the tombstone.
            (d(2026, 11, 9), Some(SkipReason::Paused)),
            // paused_until itself runs.
            (d(2026, 11, 10), None),
            (d(2026, 11, 11), None),
            // The tombstone ahead of the bank holiday.
            (d(2026, 11, 12), Some(SkipReason::Skipped)),
            (d(2026, 11, 13), Some(SkipReason::BankHoliday)),
            // Away ahead of the tombstone.
            (d(2026, 11, 14), Some(SkipReason::Away)),
            (d(2026, 11, 15), Some(SkipReason::Away)),
            (d(2026, 11, 16), None),
        ];
        let got: Vec<_> = got.into_iter().map(|u| (u.date, u.skip)).collect();
        assert_eq!(got, want);
        assert_eq!(SkipReason::BankHoliday.as_str(), "bank_holiday");
        assert_eq!(SkipReason::Skipped.as_str(), "skipped");
        assert_eq!(SkipReason::Away.as_str(), "away");
        assert_eq!(SkipReason::Paused.as_str(), "paused");
    }

    #[test]
    fn exceptions_apply_to_extra_dates_except_bank_holidays() {
        let mut rule = Rule::weekly(None);
        rule.extra_dates = dates(&[d(2026, 11, 20), d(2026, 11, 21), d(2026, 11, 22)]);
        rule.paused_until = Some(d(2026, 11, 21));
        let ex = Exceptions {
            skipped: dates(&[d(2026, 11, 22)]),
            away: vec![(d(2026, 11, 21), d(2026, 11, 21))],
            ..Exceptions::default()
        };
        assert_eq!(
            rule.day(&ex, d(2026, 11, 20)),
            Day::Skipped(SkipReason::Paused)
        );
        assert_eq!(
            rule.day(&ex, d(2026, 11, 21)),
            Day::Skipped(SkipReason::Away)
        );
        assert_eq!(
            rule.day(&ex, d(2026, 11, 22)),
            Day::Skipped(SkipReason::Skipped)
        );
    }

    #[test]
    fn next_due_passes_any_number_of_skips_within_the_horizon() {
        // Every Monday, with the next 52 skipped: the old 7-plus-skips loop
        // found it only by luck; here it is found a year out.
        let rule = Rule::weekly(Some(MON));
        let from = d(2026, 11, 6);
        let mondays: Vec<_> = from
            .iter_days()
            .filter(|day| day.weekday() == chrono::Weekday::Mon)
            .take(52)
            .collect();
        let ex = Exceptions {
            skipped: dates(&mondays),
            ..Exceptions::default()
        };
        let next = rule.next_due(&ex, from, DEFAULT_HORIZON_DAYS).unwrap();
        assert_eq!(next, d(2027, 11, 8));
        assert_eq!(rule.day(&ex, next), Day::Due);
        // And the horizon is a real bound.
        assert_eq!(rule.next_due(&ex, from, 360), None);
    }

    #[test]
    fn a_400_day_horizon_reaches_a_year_and_a_month() {
        let mut rule = Rule::weekly(None);
        rule.kind = Kind::Dates;
        let from = d(2026, 11, 6);
        rule.extra_dates = dates(&[from + Duration::days(399), from + Duration::days(400)]);
        let ex = Exceptions::default();
        assert_eq!(
            rule.next_due(&ex, from, DEFAULT_HORIZON_DAYS),
            Some(from + Duration::days(399))
        );
        assert_eq!(rule.upcoming(&ex, from, 8, DEFAULT_HORIZON_DAYS).len(), 1);
        assert_eq!(
            rule.upcoming(&ex, from, 0, DEFAULT_HORIZON_DAYS),
            Vec::new()
        );
    }

    #[test]
    fn clock_change_sundays_are_ordinary_dates() {
        // 29 Mar 2026 and 25 Oct 2026 are the clock-change Sundays; dates
        // are London service dates, so the change can't move or drop one.
        let rule = Rule::weekly(Some(1 << 6));
        let ex = Exceptions::default();
        assert!(rule.is_due(&ex, d(2026, 3, 29)));
        assert!(rule.is_due(&ex, d(2026, 10, 25)));
        assert_eq!(
            rule.next_due(&ex, d(2026, 3, 23), 400),
            Some(d(2026, 3, 29))
        );
        let mut fortnightly = Rule::weekly(Some(1 << 6));
        fortnightly.interval_weeks = 2;
        fortnightly.anchor_week = Some(d(2026, 3, 23));
        // 31 weeks after 29 Mar is 1 Nov; 30 weeks is 25 Oct.
        assert!(fortnightly.is_due(&ex, d(2026, 10, 25)));
        assert!(!fortnightly.is_due(&ex, d(2026, 11, 1)));
    }

    #[test]
    fn monday_of_handles_sundays_and_year_ends() {
        assert_eq!(monday_of(d(2026, 11, 8)), d(2026, 11, 2));
        assert_eq!(monday_of(d(2026, 11, 2)), d(2026, 11, 2));
        assert_eq!(monday_of(d(2027, 1, 1)), d(2026, 12, 28));
    }
}

#[cfg(test)]
mod db_tests {
    use chrono::NaiveDate;
    use sqlx::PgPool;

    use super::*;

    const USER: &str = "recurrence-test-user";

    fn d(m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2050, m, day).unwrap()
    }

    async fn template(pool: &PgPool, days: i16, skip_bank_holidays: bool) -> i64 {
        crate::test_support::seed_user(pool, USER).await;
        sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, days_of_week, skip_bank_holidays, starts_on) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(USER)
        .bind(days)
        .bind(skip_bank_holidays)
        .bind(d(1, 3))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn holiday(pool: &PgPool, division: &str, date: NaiveDate) {
        sqlx::query(
            "INSERT INTO bank_holidays (division, holiday_date, title, fetched_at) \
             VALUES ($1, $2, 'A holiday', now())",
        )
        .bind(division)
        .bind(date)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn tombstone(pool: &PgPool, template_id: i64, date: NaiveDate) {
        sqlx::query(
            "INSERT INTO journey_template_skipped_dates (template_id, service_date) VALUES ($1, $2)",
        )
        .bind(template_id)
        .bind(date)
        .execute(pool)
        .await
        .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn load_reads_the_rule_tombstones_and_the_owners_bank_holidays(pool: PgPool) {
        use crate::bank_holidays::{ENGLAND_AND_WALES, SCOTLAND};
        // Easter Monday 2050 is 11 Apr; 2 Jan (a Monday holiday in
        // Scotland, observed 3 Jan) only in Scotland.
        let skips = template(&pool, 0b001_1111, true).await;
        let keeps = template(&pool, 0b001_1111, false).await;
        holiday(&pool, ENGLAND_AND_WALES, d(4, 11)).await;
        holiday(&pool, SCOTLAND, d(1, 3)).await;
        tombstone(&pool, skips, d(4, 12)).await;
        // Outside the range: not read.
        tombstone(&pool, skips, d(12, 30)).await;

        let got = load(&pool, &[skips, keeps, -1], d(1, 1), d(6, 30))
            .await
            .unwrap();
        assert_eq!(got.len(), 2);
        let s = &got[&skips];
        assert_eq!(s.user_id, USER);
        assert_eq!(s.rule.days_of_week, Some(0b001_1111));
        assert_eq!(s.rule.starts_on, Some(d(1, 3)));
        assert!(s.rule.active);
        assert_eq!(s.exceptions.skipped, [d(4, 12)].into_iter().collect());
        // No division chosen: England and Wales.
        assert_eq!(s.exceptions.bank_holidays, [d(4, 11)].into_iter().collect());
        assert_eq!(
            s.rule.day(&s.exceptions, d(4, 11)),
            Day::Skipped(SkipReason::BankHoliday)
        );
        assert_eq!(
            s.rule
                .next_due(&s.exceptions, d(4, 9), DEFAULT_HORIZON_DAYS),
            Some(d(4, 13))
        );
        // The switch off reads no bank holidays.
        assert_eq!(got[&keeps].exceptions.bank_holidays, BTreeSet::new());

        sqlx::query("UPDATE users SET bank_holiday_division = $2 WHERE id = $1")
            .bind(USER)
            .bind(SCOTLAND)
            .execute(&pool)
            .await
            .unwrap();
        let got = load(&pool, &[skips], d(1, 1), d(6, 30)).await.unwrap();
        assert_eq!(
            got[&skips].exceptions.bank_holidays,
            [d(1, 3)].into_iter().collect()
        );
        let preview = user_exceptions(&pool, USER, true, d(1, 1), d(6, 30))
            .await
            .unwrap();
        assert_eq!(preview.bank_holidays, [d(1, 3)].into_iter().collect());
        assert_eq!(preview.skipped, BTreeSet::new());
        let off = user_exceptions(&pool, USER, false, d(1, 1), d(6, 30))
            .await
            .unwrap();
        assert_eq!(off, Exceptions::default());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn the_prefilter_never_drops_a_due_date(pool: PgPool) {
        let mut ids = Vec::new();
        for days in [0b001_1111_i16, 1, 1 << 6, 0b101_0101] {
            ids.push(template(&pool, days, true).await);
        }
        sqlx::query("UPDATE journey_templates SET ends_on = $2 WHERE id = $1")
            .bind(ids[2])
            .bind(d(2, 20))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE journey_templates SET active = FALSE WHERE id = $1")
            .bind(ids[3])
            .execute(&pool)
            .await
            .unwrap();
        let schedules = load(&pool, &ids, d(1, 1), d(3, 31)).await.unwrap();
        let sql = format!(
            "SELECT jt.id FROM journey_templates jt WHERE jt.id = ANY($1) AND {}",
            may_be_due_sql("jt", "$2::date")
        );
        for date in d(1, 1).iter_days().take(90) {
            let candidates: Vec<i64> = sqlx::query_scalar(&sql)
                .bind(&ids)
                .bind(date)
                .fetch_all(&pool)
                .await
                .unwrap();
            for (id, s) in &schedules {
                if s.rule.is_due(&s.exceptions, date) {
                    assert!(candidates.contains(id), "{id} due {date} but filtered");
                }
            }
        }
    }
}
