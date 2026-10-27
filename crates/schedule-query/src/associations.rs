//! CIF `AA` (association) records (pre-departure prediction phase 2,
//! DS-152; docs/superpowers/specs/2026-10-21-pre-departure-prediction-design.md
//! section 3a and the phase plan).
//!
//! An association links two schedules at a location: `JJ` (the associated
//! train joins the main one), `VV` (it divides from it) and `NP` (the main
//! train's unit forms the associated one; a full CIF delivery carries none).
//! Like schedules, associations come with a date range, days of the week
//! and an STP indicator, so a short-term record can overlay or cancel a
//! permanent one for some dates. [`AssociationIndex::on_date`] resolves that
//! overlay per `(main UID, associated UID, location)`: of the records valid
//! on the date, the lowest STP letter wins (`C` < `N` < `O` < `P`, the same
//! precedence as schedules), and a winning `C` means no association that
//! day.
//!
//! Record layout (80 columns, 0-based offsets):
//!
//! | Field | Columns |
//! |---|---|
//! | record type `AA` | 0-1 |
//! | transaction type (`N`, `D`, `R`) | 2 |
//! | main train UID | 3-8 |
//! | associated train UID | 9-14 |
//! | start date (`yymmdd`) | 15-20 |
//! | end date (`yymmdd`) | 21-26 |
//! | days run (Monday first, `1`/`0`) | 27-33 |
//! | category (`JJ`, `VV`, `NP`) | 34-35 |
//! | date indicator (`S`, `N`, `P`) | 36 |
//! | location TIPLOC | 37-43 |
//! | base and associated location suffixes | 44, 45 |
//! | diagram type, association type | 46, 47 |
//! | STP indicator (`C`, `N`, `O`, `P`) | 79 |
//!
//! A delete transaction (`D`, only in update extracts) is skipped; a revise
//! (`R`) is read like a new record. A line that does not decode is skipped.

use std::collections::HashMap;

use chrono::{Datelike, Duration, NaiveDate};

/// The association's category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AssociationCategory {
    /// `JJ`: the associated train joins the main train.
    Join,
    /// `VV`: the associated train divides from the main train.
    Divide,
    /// `NP`: the main train's unit forms the associated train next.
    NextWorking,
}

impl AssociationCategory {
    /// The CIF code (`JJ`, `VV`, `NP`).
    pub const fn code(self) -> &'static str {
        match self {
            Self::Join => "JJ",
            Self::Divide => "VV",
            Self::NextWorking => "NP",
        }
    }

    fn parse(code: &str) -> Option<Self> {
        match code {
            "JJ" => Some(Self::Join),
            "VV" => Some(Self::Divide),
            "NP" => Some(Self::NextWorking),
            _ => None,
        }
    }
}

/// One decoded `AA` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssociationRecord {
    pub main_uid: String,
    pub assoc_uid: String,
    pub start: NaiveDate,
    pub end: NaiveDate,
    /// Monday first.
    pub days: [bool; 7],
    /// `None` on a cancellation (`C`) record, which leaves it blank.
    pub category: Option<AssociationCategory>,
    /// Days from the main train's date to the associated train's: 0
    /// (`S`), +1 (`N`) or -1 (`P`).
    pub assoc_day_offset: i64,
    pub tiploc: String,
    /// `C`, `N`, `O` or `P`.
    pub stp: char,
}

/// One association in force on a date (the main train's service date).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveAssociation {
    pub main_uid: String,
    pub assoc_uid: String,
    /// The associated train's service date: the main train's, or the day
    /// after or before it per the record's date indicator.
    pub assoc_service_date: NaiveDate,
    pub category: AssociationCategory,
    pub tiploc: String,
}

fn yymmdd(field: &str) -> Option<NaiveDate> {
    if field.len() != 6 || !field.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // `999999` is "no end date".
    if field == "999999" {
        return NaiveDate::from_ymd_opt(2099, 12, 31);
    }
    let yy: i32 = field[0..2].parse().ok()?;
    let mm: u32 = field[2..4].parse().ok()?;
    let dd: u32 = field[4..6].parse().ok()?;
    // CIF's convention: 60-99 are the 1900s, the rest the 2000s.
    let year = if yy >= 60 { 1900 + yy } else { 2000 + yy };
    NaiveDate::from_ymd_opt(year, mm, dd)
}

/// Decodes one `AA` line, or `None` (not an `AA` line, a delete, or a
/// malformed one).
pub fn parse_association_line(line: &str) -> Option<AssociationRecord> {
    if !line.starts_with("AA") || !line.is_ascii() || line.len() < 80 {
        return None;
    }
    let transaction = &line[2..3];
    if transaction == "D" {
        return None;
    }
    let main_uid = line[3..9].trim();
    let assoc_uid = line[9..15].trim();
    if main_uid.is_empty() || assoc_uid.is_empty() {
        return None;
    }
    let start = yymmdd(&line[15..21])?;
    let stp = line[79..80].chars().next()?;
    if !matches!(stp, 'C' | 'N' | 'O' | 'P') {
        return None;
    }
    // A cancellation may leave the end date and days blank: it then
    // applies to its start date only.
    let end = yymmdd(&line[21..27]).unwrap_or(start);
    let days_field = &line[27..34];
    let mut days = [false; 7];
    if days_field.trim().is_empty() {
        days = [true; 7];
    } else {
        for (slot, b) in days.iter_mut().zip(days_field.bytes()) {
            *slot = b == b'1';
        }
    }
    let category = AssociationCategory::parse(&line[34..36]);
    if category.is_none() && stp != 'C' {
        return None;
    }
    let assoc_day_offset = match &line[36..37] {
        "N" => 1,
        "P" => -1,
        _ => 0,
    };
    let tiploc = line[37..44].trim();
    if tiploc.is_empty() {
        return None;
    }
    Some(AssociationRecord {
        main_uid: main_uid.to_owned(),
        assoc_uid: assoc_uid.to_owned(),
        start,
        end,
        days,
        category,
        assoc_day_offset,
        tiploc: tiploc.to_owned(),
        stp,
    })
}

/// Collects `AA` records line by line (every other line is ignored), so a
/// caller streaming the CIF file can feed it alongside the schedule index.
#[derive(Debug, Default)]
pub struct AssociationIndexBuilder {
    records: Vec<AssociationRecord>,
}

impl AssociationIndexBuilder {
    pub fn push_line(&mut self, line: &str) {
        if let Some(record) = parse_association_line(line) {
            self.records.push(record);
        }
    }

    pub fn finish(self) -> AssociationIndex {
        let mut by_key: HashMap<(String, String, String), Vec<AssociationRecord>> = HashMap::new();
        for record in self.records {
            by_key
                .entry((
                    record.main_uid.clone(),
                    record.assoc_uid.clone(),
                    record.tiploc.clone(),
                ))
                .or_default()
                .push(record);
        }
        AssociationIndex { by_key }
    }
}

/// Every `AA` record of a delivery, grouped by `(main UID, associated UID,
/// location)`.
#[derive(Debug, Default)]
pub struct AssociationIndex {
    by_key: HashMap<(String, String, String), Vec<AssociationRecord>>,
}

/// STP precedence: the lowest wins.
fn stp_rank(stp: char) -> u8 {
    match stp {
        'C' => 0,
        'N' => 1,
        'O' => 2,
        _ => 3,
    }
}

impl AssociationIndex {
    /// How many records the index holds.
    pub fn len(&self) -> usize {
        self.by_key.values().map(Vec::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// The associations in force on `date` (the main train's service date),
    /// sorted by main UID, associated UID and location.
    pub fn on_date(&self, date: NaiveDate) -> Vec<ActiveAssociation> {
        let weekday = date.weekday().num_days_from_monday() as usize;
        let mut active: Vec<ActiveAssociation> = self
            .by_key
            .values()
            .filter_map(|records| {
                let winner = records
                    .iter()
                    .filter(|r| r.start <= date && date <= r.end && r.days[weekday])
                    .min_by_key(|r| stp_rank(r.stp))?;
                if winner.stp == 'C' {
                    return None;
                }
                Some(ActiveAssociation {
                    main_uid: winner.main_uid.clone(),
                    assoc_uid: winner.assoc_uid.clone(),
                    assoc_service_date: date + Duration::days(winner.assoc_day_offset),
                    category: winner.category?,
                    tiploc: winner.tiploc.clone(),
                })
            })
            .collect();
        active.sort_by(|a, b| {
            (&a.main_uid, &a.assoc_uid, &a.tiploc).cmp(&(&b.main_uid, &b.assoc_uid, &b.tiploc))
        });
        active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A synthetic `AA` line padded to 80 columns.
    #[expect(clippy::too_many_arguments, reason = "one record's fields")]
    fn aa(
        tx: char,
        main: &str,
        assoc: &str,
        start: &str,
        end: &str,
        days: &str,
        cat: &str,
        ind: char,
        tiploc: &str,
        stp: char,
    ) -> String {
        let mut line =
            format!("AA{tx}{main:<6}{assoc:<6}{start}{end}{days}{cat}{ind}{tiploc:<7}  TP");
        while line.len() < 79 {
            line.push(' ');
        }
        line.push(stp);
        assert_eq!(line.len(), 80);
        line
    }

    fn day(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).expect("date")
    }

    fn index(lines: &[String]) -> AssociationIndex {
        let mut builder = AssociationIndexBuilder::default();
        for line in lines {
            builder.push_line(line);
        }
        // Other record types are ignored.
        builder.push_line(
            "BSNC123452605172612060000001 POO2A21    121690005 EMU    075D     B            P",
        );
        builder.finish()
    }

    #[test]
    fn decodes_a_join_record() {
        let line = aa(
            'N', "A00001", "B00002", "991001", "991231", "1111100", "JJ", 'S', "YORK", 'P',
        );
        let record = parse_association_line(&line).expect("decodes");
        assert_eq!(record.main_uid, "A00001");
        assert_eq!(record.assoc_uid, "B00002");
        assert_eq!(record.start, day(1999, 10, 1));
        assert_eq!(record.end, day(1999, 12, 31));
        assert_eq!(record.days, [true, true, true, true, true, false, false]);
        assert_eq!(record.category, Some(AssociationCategory::Join));
        assert_eq!(record.assoc_day_offset, 0);
        assert_eq!(record.tiploc, "YORK");
        assert_eq!(record.stp, 'P');
    }

    #[test]
    fn skips_deletes_short_and_malformed_lines() {
        let delete = aa(
            'D', "A00001", "B00002", "991001", "991231", "1111111", "JJ", 'S', "YORK", 'P',
        );
        assert_eq!(parse_association_line(&delete), None);
        assert_eq!(parse_association_line("AANA00001"), None);
        let bad_date = aa(
            'N', "A00001", "B00002", "99xx01", "991231", "1111111", "JJ", 'S', "YORK", 'P',
        );
        assert_eq!(parse_association_line(&bad_date), None);
        let bad_category = aa(
            'N', "A00001", "B00002", "991001", "991231", "1111111", "ZZ", 'S', "YORK", 'P',
        );
        assert_eq!(parse_association_line(&bad_category), None);
    }

    #[test]
    fn a_permanent_record_applies_on_its_running_days_only() {
        // 2026-07-10 is a Friday; Monday to Friday only.
        let idx = index(&[aa(
            'N', "A00001", "B00002", "260701", "260731", "1111100", "VV", 'S', "YORK", 'P',
        )]);
        let friday = idx.on_date(day(2026, 7, 10));
        assert_eq!(
            friday,
            vec![ActiveAssociation {
                main_uid: "A00001".into(),
                assoc_uid: "B00002".into(),
                assoc_service_date: day(2026, 7, 10),
                category: AssociationCategory::Divide,
                tiploc: "YORK".into(),
            }]
        );
        assert!(idx.on_date(day(2026, 7, 11)).is_empty(), "Saturday");
        assert!(idx.on_date(day(2026, 8, 3)).is_empty(), "after its end");
    }

    #[test]
    fn a_cancellation_overlays_the_permanent_record_for_its_dates() {
        let idx = index(&[
            aa(
                'N', "A00001", "B00002", "260701", "260731", "1111111", "JJ", 'S', "YORK", 'P',
            ),
            aa(
                'N', "A00001", "B00002", "260710", "260710", "1111111", "  ", ' ', "YORK", 'C',
            ),
        ]);
        assert!(
            idx.on_date(day(2026, 7, 10)).is_empty(),
            "cancelled that day"
        );
        assert_eq!(idx.on_date(day(2026, 7, 11)).len(), 1);
    }

    #[test]
    fn an_overlay_replaces_the_permanent_record_and_the_date_indicator_moves_the_assoc_date() {
        let idx = index(&[
            aa(
                'N', "A00001", "B00002", "260701", "260731", "1111111", "JJ", 'S', "YORK", 'P',
            ),
            aa(
                'N', "A00001", "B00002", "260710", "260712", "1111111", "VV", 'N', "YORK", 'O',
            ),
        ]);
        let active = idx.on_date(day(2026, 7, 10));
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].category, AssociationCategory::Divide);
        assert_eq!(active[0].assoc_service_date, day(2026, 7, 11));
        assert_eq!(
            idx.on_date(day(2026, 7, 13))[0].category,
            AssociationCategory::Join
        );
    }

    #[test]
    fn different_locations_are_different_associations() {
        let idx = index(&[
            aa(
                'N', "A00001", "B00002", "260701", "260731", "1111111", "JJ", 'S', "YORK", 'P',
            ),
            aa(
                'N', "A00001", "B00002", "260701", "260731", "1111111", "VV", 'P', "LEEDS", 'P',
            ),
        ]);
        let active = idx.on_date(day(2026, 7, 10));
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].tiploc, "LEEDS");
        assert_eq!(active[0].assoc_service_date, day(2026, 7, 9));
        assert_eq!(idx.len(), 2);
    }
}
