//! Curated fixed links the timetable's ALF lacks:
//! `reference-data/supplementary-fixed-links.csv`, compiled in. `api`
//! merges them with the ALF's `fixed_links` rows when it builds the
//! planner's interchange data. The CSV's header comment says what is in
//! it, why, and where its minutes come from.

/// `reference-data/supplementary-fixed-links.csv`, compiled in.
pub const SUPPLEMENTARY_LINKS_CSV: &str =
    include_str!("../../../reference-data/supplementary-fixed-links.csv");

/// The longest link the CSV may give.
pub const MAX_MINUTES: i32 = 120;

/// One curated link, between `from_crs` and `to_crs` both ways.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupplementaryLink {
    pub from_crs: String,
    pub to_crs: String,
    /// As the ALF's `M=`: `TUBE`, `WALK`, ...
    pub mode: String,
    /// The line's name, e.g. `Victoria`.
    pub line: String,
    pub minutes: i32,
    /// `HHMM`, inclusive.
    pub valid_from: String,
    pub valid_to: String,
    /// Monday-first, 7 x `0`/`1`.
    pub days_mask: String,
    pub source: String,
}

/// The links in `csv`
/// (`from_crs,to_crs,mode,line,minutes,valid_from,valid_to,days_mask,source`).
/// Comment (`#`) and blank lines and the header are skipped, and so is a
/// malformed row (the checked-in file's own test proves it has none).
pub fn parse_supplementary_links(csv: &str) -> Vec<SupplementaryLink> {
    csv.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(parse_row)
        .collect()
}

fn parse_row(line: &str) -> Option<SupplementaryLink> {
    let mut fields = line.splitn(9, ',').map(str::trim);
    let mut next = || fields.next().map(str::to_string);
    let from_crs = next()?;
    let to_crs = next()?;
    let mode = next()?;
    let line_name = next()?;
    let minutes: i32 = next()?.parse().ok()?;
    let valid_from = next()?;
    let valid_to = next()?;
    let days_mask = next()?;
    let source = next()?;
    let is_crs = |code: &str| code.len() == 3 && code.bytes().all(|b| b.is_ascii_uppercase());
    let is_hhmm = |time: &str| {
        time.len() == 4
            && time.bytes().all(|b| b.is_ascii_digit())
            && time[..2] <= *"23"
            && time[2..] <= *"59"
    };
    let valid = is_crs(&from_crs)
        && is_crs(&to_crs)
        && from_crs != to_crs
        && !mode.is_empty()
        && mode.bytes().all(|b| b.is_ascii_uppercase())
        && !line_name.is_empty()
        && (1..=MAX_MINUTES).contains(&minutes)
        && is_hhmm(&valid_from)
        && is_hhmm(&valid_to)
        && valid_from <= valid_to
        && days_mask.len() == 7
        && days_mask.bytes().all(|b| b == b'0' || b == b'1')
        && !source.is_empty();
    valid.then_some(SupplementaryLink {
        from_crs,
        to_crs,
        mode,
        line: line_name,
        minutes,
        valid_from,
        valid_to,
        days_mask,
        source,
    })
}

/// The checked-in links.
pub fn supplementary_links() -> Vec<SupplementaryLink> {
    parse_supplementary_links(SUPPLEMENTARY_LINKS_CSV)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_checked_in_csv_parses_completely() {
        let links = supplementary_links();
        let rows = SUPPLEMENTARY_LINKS_CSV
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
            .count();
        // Every data row (all but the header) parsed.
        assert_eq!(links.len(), rows - 1);
        assert!(links.iter().any(|link| link.from_crs == "VXH"
            && link.to_crs == "VIC"
            && link.line == "Victoria"
            && link.minutes == 11
            && link.valid_from == "0700"
            && link.valid_to == "1900"
            && link.days_mask == "1111110"));
    }

    #[test]
    fn malformed_rows_are_skipped() {
        let csv = "\
from_crs,to_crs,mode,line,minutes,valid_from,valid_to,days_mask,source
VXH,VIC,TUBE,Victoria,11,0700,1900,1111110,ok
vxh,VIC,TUBE,Victoria,11,0700,1900,1111110,lower-case CRS
VXH,VXH,TUBE,Victoria,11,0700,1900,1111110,same station
VXH,VIC,TUBE,Victoria,0,0700,1900,1111110,zero minutes
VXH,VIC,TUBE,Victoria,11,0700,2400,1111110,bad time
VXH,VIC,TUBE,Victoria,11,1900,0700,1111110,window backwards
VXH,VIC,TUBE,Victoria,11,0700,1900,111111,short mask
VXH,VIC,TUBE,,11,0700,1900,1111110,no line
VXH,VIC,TUBE,Victoria,11,0700,1900,1111110,
VXH,VIC,TUBE,Victoria,11,0700,1900,1111110,source, with a comma
";
        let links = parse_supplementary_links(csv);
        let sources: Vec<&str> = links.iter().map(|l| l.source.as_str()).collect();
        assert_eq!(sources, ["ok", "source, with a comma"]);
    }
}
