//! **Manual dev tool. Not part of any deployed service** (same posture as
//! `inspect.rs` next to it). Measures the resident-memory cost of building a
//! [`ScheduleIndex`] from a production-sized CIF `MCA` extract, so a change to
//! the per-calling-point representation can be sized before it ships rather
//! than discovered as an `OOMKilled` `reference` container.
//!
//! The extract is synthetic but built from this crate's own real,
//! byte-verified fixture line shapes (`BS_C00573_PERMANENT`, the `BX` ATOC
//! line, `LO_EUSTON`/`LO_WATRLMN`, `LI_CARLILE`, `LT_EUSTON` -- see
//! `src/parse.rs`'s tests), padded to CIF's real 80-byte record width, with
//! only the UID, TIPLOC, times, platform and activity bytes varied. Its
//! proportions match the 2026-09 production delivery that OOMKilled the
//! `reference` container: 427k schedules, ~7.05M `LI`, 427k `LO`, 427k `LT`
//! (~7.9M calling points), ~43% of them carrying a platform, ~700MB of text.
//!
//! Peak RSS is read from the kernel's own high-water mark (`VmHWM` in
//! `/proc/self/status`) -- the same number `/usr/bin/time -v` reports as
//! "Maximum resident set size" -- so it needs no allocator hooks. Linux only.
//!
//! # Usage
//!
//! ```text
//! # Index built from an in-memory copy of the text, text dropped afterwards:
//! cargo run --release -p schedule-query --example index_memory -- inmem
//!
//! # The schedule-reference production path, against a file on disk:
//! cargo run --release -p schedule-query --example index_memory -- gen /tmp/mca.txt
//! cargo run --release -p schedule-query --example index_memory -- file-text /tmp/mca.txt
//! cargo run --release -p schedule-query --example index_memory -- file-stream /tmp/mca.txt
//! ```
//!
//! `file-text` is the pre-2026-09-26 `schedule-reference` shape: read every
//! `BS`/`BX`/`LO`/`LI`/`CR`/`LT` line into one `String`, then
//! `ScheduleIndex::from_text`. `file-stream` is the current one: feed the file
//! line by line into a `ScheduleIndexBuilder`, never holding the text.

use std::io::{BufRead, Write};

use schedule_query::{CallingPoint, RawSchedule, ScheduleIndex, ScheduleIndexBuilder};

const SCHEDULES: usize = 427_000;
const TIPLOC_POOL: usize = 8_000;
const RECORD_WIDTH: usize = 80;

const BS_TEMPLATE: &str =
    "BSNC005732605172612060000001 PXX1S003101121194800 DMU    125      S A T        P";
const BX_TEMPLATE: &str = "BX         SRYSR408800";
const LO_TEMPLATE: &str = "LOEUSTON  0822 08227  C      TB";
const LI_TEMPLATE: &str = "LICARLILE 1202 1213      120212131        T";
const LT_TEMPLATE: &str = "LTEUSTON  0804 08079     TF";

fn padded(template: &str) -> Vec<u8> {
    let mut line = template.as_bytes().to_vec();
    line.resize(RECORD_WIDTH, b' ');
    line
}

fn put(line: &mut [u8], at: usize, value: &str) {
    line[at..at + value.len()].copy_from_slice(value.as_bytes());
}

fn hhmm(minutes: usize) -> String {
    let minutes = minutes % (24 * 60);
    format!("{:02}{:02}", minutes / 60, minutes % 60)
}

fn tiploc(i: usize, j: usize) -> String {
    format!("T{:06}", (i * 7 + j * 13) % TIPLOC_POOL)
}

/// Writes the synthetic extract to `out`, one record per line.
fn generate(out: &mut impl Write) -> std::io::Result<()> {
    let bs = padded(BS_TEMPLATE);
    let bx = padded(BX_TEMPLATE);
    let lo = padded(LO_TEMPLATE);
    let li = padded(LI_TEMPLATE);
    let lt = padded(LT_TEMPLATE);

    for i in 0..SCHEDULES {
        // Every fifth schedule is an STP overlay of the previous UID, so the
        // index sees multi-record UIDs the way a real extract has them.
        let uid_number = if i % 5 == 4 { i - 1 } else { i };
        let mut line = bs.clone();
        put(&mut line, 3, &format!("{:06}", uid_number % 1_000_000));
        put(&mut line, 79, if i % 5 == 4 { "O" } else { "P" });
        put(&mut line, 32, &format!("{}{:03}", 1 + i % 9, i % 1000));
        // One running day per UID (an overlay shares its base's day), so a
        // single service date resolves ~1/7 of the extract -- still roughly
        // twice a real day's share, i.e. conservative for the per-date phase.
        let mut days = *b"0000000";
        days[uid_number % 7] = b'1';
        line[21..28].copy_from_slice(&days);
        out.write_all(&line)?;
        out.write_all(b"\n")?;
        out.write_all(&bx)?;
        out.write_all(b"\n")?;

        let start = (i * 17) % (20 * 60) + 5 * 60;
        // 16.5 intermediates per schedule on average -> ~7.05M `LI`.
        let intermediates = 16 + (i % 2);

        let mut line = lo.clone();
        put(&mut line, 2, &tiploc(i, 0));
        put(&mut line, 10, &hhmm(start));
        put(&mut line, 15, &hhmm(start));
        put(&mut line, 19, if i % 7 < 3 { "1  " } else { "   " });
        out.write_all(&line)?;
        out.write_all(b"\n")?;

        for j in 1..=intermediates {
            let mut line = li.clone();
            put(&mut line, 2, &tiploc(i, j));
            put(&mut line, 10, &hhmm(start + j * 3));
            put(&mut line, 15, &hhmm(start + j * 3 + 1));
            let with_platform = (i + j) % 7 < 3;
            if with_platform {
                put(&mut line, 25, &hhmm(start + j * 3));
                put(&mut line, 29, &hhmm(start + j * 3 + 1));
                put(&mut line, 33, &format!("{:<3}", (i + j) % 12 + 1));
                put(&mut line, 42, "T ");
            } else {
                // A passing/timing point: a pass time (`20..25`) instead of
                // booked arrival/departure, no public times, no platform, no
                // activity.
                put(&mut line, 10, "          ");
                put(&mut line, 20, &format!("{} ", hhmm(start + j * 3)));
                put(&mut line, 25, "        ");
                put(&mut line, 33, "   ");
                put(&mut line, 42, "  ");
            }
            out.write_all(&line)?;
            out.write_all(b"\n")?;
        }

        let mut line = lt.clone();
        put(&mut line, 2, &tiploc(i, intermediates + 1));
        put(&mut line, 10, &hhmm(start + (intermediates + 1) * 3));
        put(&mut line, 15, &hhmm(start + (intermediates + 1) * 3));
        put(&mut line, 19, if i % 7 < 3 { "4  " } else { "   " });
        out.write_all(&line)?;
        out.write_all(b"\n")?;
    }
    Ok(())
}

fn proc_status_kib(field: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap_or(0)
}

fn report(stage: &str) {
    eprintln!(
        "{stage:<40} rss={:>6} MiB  peak(VmHWM)={:>6} MiB",
        proc_status_kib("VmRSS:") / 1024,
        proc_status_kib("VmHWM:") / 1024
    );
}

fn summarize(index: &ScheduleIndex) {
    eprintln!(
        "indexed {} distinct UID(s); size_of::<CallingPoint>() = {}, size_of::<RawSchedule>() = {}",
        index.uids().count(),
        std::mem::size_of::<CallingPoint>(),
        std::mem::size_of::<RawSchedule>()
    );
}

/// The pre-2026-09-26 `schedule-reference::read_prefixed_lines_multi` shape.
fn read_prefixed_text(path: &str) -> String {
    let reader = std::io::BufReader::new(std::fs::File::open(path).expect("open"));
    let mut out = String::new();
    for line in reader.lines() {
        let line = line.expect("read");
        if ["BS", "BX", "LO", "LI", "CR", "LT"]
            .iter()
            .any(|prefix| line.starts_with(prefix))
        {
            out.push_str(&line);
            out.push('\n');
        }
    }
    out
}

/// Optional third argument `rows`: after the index is built, also build one
/// service date's `schedule_destination_departures` rows the way
/// `schedule-reference` does (every row a `serde_json::Value`, all rows for
/// the date collected before the first POST), to size the per-date phase.
fn destination_rows(index: &ScheduleIndex) -> impl Iterator<Item = serde_json::Value> {
    // A Sunday inside the template's 2026-05-17..2026-12-06 validity.
    let date = chrono::NaiveDate::from_ymd_opt(2026, 5, 17).expect("date");
    let tiploc_to_crs: std::collections::HashMap<String, String> = (0..TIPLOC_POOL)
        .map(|i| (format!("T{i:06}"), format!("C{:02}", i % 100)))
        .collect();
    let by_destination = schedule_query::departures_by_destination_crs(
        index,
        date,
        chrono::NaiveTime::MIN,
        &tiploc_to_crs,
    );
    report("destination departures grouped");
    by_destination
        .into_iter()
        .flat_map(move |(destination_crs, departures)| {
            departures.into_iter().map(move |d| {
                serde_json::json!({
                    "service_date": date,
                    "destination_crs": destination_crs,
                    "scheduled": d.scheduled,
                    "day_offset": d.day_offset,
                    "train_uid": d.uid,
                    "origin_crs": d.origin_crs,
                    "true_origin_crs": d.true_origin_crs,
                    "calling_point_arrival": d.calling_point_arrival,
                    "destination_arrival": d.destination_arrival,
                    "destination_arrival_day_offset": d.destination_arrival_day_offset,
                    "operator_atoc": d.operator_atoc,
                    "headcode": d.headcode,
                })
            })
        })
}

/// `rows`: every row for the date collected before posting (the
/// pre-2026-09-26 `schedule-reference` shape). `rows-chunked`: rows pulled
/// 50,000 at a time, each chunk dropped before the next is built (the
/// current `post_date_scoped_row_stream` shape).
fn maybe_rows(index: &ScheduleIndex) {
    match std::env::args().nth(3).as_deref() {
        Some("rows") => {
            let rows: Vec<serde_json::Value> = destination_rows(index).collect();
            eprintln!("built {} destination-departure rows", rows.len());
            report("destination rows built (all, Value)");
        }
        Some("rows-chunked") => {
            let mut rows = destination_rows(index);
            let mut total = 0;
            loop {
                let chunk: Vec<serde_json::Value> = rows.by_ref().take(50_000).collect();
                if chunk.is_empty() {
                    break;
                }
                total += chunk.len();
            }
            eprintln!("built {total} destination-departure rows, 50k at a time");
            report("destination rows built (chunked)");
        }
        _ => {}
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    report("start");
    match (args.first().map(String::as_str), args.get(1)) {
        (Some("inmem") | None, _) => {
            let mut text = Vec::new();
            generate(&mut text).expect("generate");
            let text = String::from_utf8(text).expect("ascii");
            eprintln!("generated {} MiB of CIF text", text.len() / (1024 * 1024));
            report("text generated");
            let index = ScheduleIndex::from_text(&text);
            report("index built (text still alive)");
            drop(text);
            report("text dropped");
            summarize(&index);
            maybe_rows(&index);
        }
        (Some("gen"), Some(path)) => {
            let mut out = std::io::BufWriter::new(std::fs::File::create(path).expect("create"));
            generate(&mut out).expect("generate");
            out.flush().expect("flush");
        }
        (Some("file-text"), Some(path)) => {
            let text = read_prefixed_text(path);
            report("text read");
            let index = ScheduleIndex::from_text(&text);
            report("index built (text still alive)");
            drop(text);
            report("text dropped");
            summarize(&index);
            maybe_rows(&index);
        }
        (Some("file-stream"), Some(path)) => {
            // Mirrors `schedule-reference::build_schedule_index_from_file`.
            let mut reader = std::io::BufReader::new(std::fs::File::open(path).expect("open"));
            let mut builder = ScheduleIndexBuilder::default();
            let mut line = String::new();
            while reader.read_line(&mut line).expect("read") != 0 {
                let trimmed = line.strip_suffix('\n').unwrap_or(&line);
                builder.push_line(trimmed.strip_suffix('\r').unwrap_or(trimmed));
                line.clear();
            }
            let index = builder.finish();
            report("index built (streamed, no text)");
            summarize(&index);
            maybe_rows(&index);
        }
        _ => {
            eprintln!(
                "usage: index_memory [inmem | gen <path> | file-text <path> | file-stream <path>]"
            );
            std::process::exit(2);
        }
    }
}
