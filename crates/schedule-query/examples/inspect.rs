//! **Manual dev tool. Not part of any deployed service.** Lives under
//! Cargo `examples/`, never built into a container image, never referenced
//! by any Helm chart or Dockerfile -- see this crate's `Cargo.toml` (no
//! `[[bin]]`) and `src/lib.rs`'s own "What this crate is not" section.
//!
//! Lets a human manually re-run this crate against the real, local,
//! untracked `timetable_full.zip` to sanity-check the library's byte
//! offsets against the full real extract -- Open Question 2 in
//! `docs/superpowers/plans/2026-09-03-option-b-consumer-first-slice-plan.md`
//! ("byte offsets are pinned against the specific real lines quoted in the
//! findings doc, not against the full extract") -- the same way every
//! validation session's own throwaway scripts already did, without that
//! check being a hard requirement of this crate's own `cargo test` gate.
//!
//! # Usage
//!
//! Query one UID's resolved schedule for a date:
//!
//! ```text
//! unzip -p timetable_full.zip RJTTF942MCA.txt \
//!   | cargo run -p schedule-query --example inspect -- uid <UID> <YYYY-MM-DD>
//! ```
//!
//! Query every schedule touching a set of TIPLOCs on a date:
//!
//! ```text
//! unzip -p timetable_full.zip RJTTF942MCA.txt \
//!   | cargo run -p schedule-query --example inspect -- \
//!       touching <YYYY-MM-DD> EUSTON,MKNSCEN,CREWE,PRSTON,CARLILE
//! ```
//!
//! Either form reads the CIF `MCA` text from stdin, so the real
//! `timetable_full.zip` is never extracted to disk and never leaves this
//! process's own memory -- matching this repo's established
//! `unzip -p ... | ...` streaming convention (see the findings/verification
//! docs' own commands).

use std::io::Read;

use chrono::NaiveDate;
use schedule_query::ScheduleIndex;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut text = String::new();
    if let Err(err) = std::io::stdin().read_to_string(&mut text) {
        eprintln!("failed to read CIF text from stdin: {err}");
        std::process::exit(1);
    }

    let index = ScheduleIndex::from_text(&text);
    eprintln!("parsed {} distinct UID(s)", index.uids().count());

    match args.first().map(String::as_str) {
        Some("uid") => {
            let (Some(uid), Some(date_str)) = (args.get(1), args.get(2)) else {
                usage_and_exit();
            };
            let date = parse_date_or_exit(date_str);
            match index.schedule_for_uid(uid, date) {
                Some(resolved) => println!("{resolved:#?}"),
                None => println!("no schedule found for UID {uid} on {date}"),
            }
        }
        Some("touching") => {
            let (Some(date_str), Some(tiplocs_str)) = (args.get(1), args.get(2)) else {
                usage_and_exit();
            };
            let date = parse_date_or_exit(date_str);
            let tiplocs: Vec<&str> = tiplocs_str.split(',').collect();
            let results = schedule_query::schedules_touching(&index, &tiplocs, date);
            println!(
                "{} schedule(s) touching {tiplocs:?} on {date}:",
                results.len()
            );
            for resolved in &results {
                println!("{resolved:#?}");
            }
        }
        _ => usage_and_exit(),
    }
}

fn parse_date_or_exit(date_str: &str) -> NaiveDate {
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap_or_else(|err| {
        eprintln!("invalid date {date_str:?} (expected YYYY-MM-DD): {err}");
        std::process::exit(1);
    })
}

fn usage_and_exit() -> ! {
    eprintln!(
        "usage:\n  ... | cargo run -p schedule-query --example inspect -- uid <UID> <YYYY-MM-DD>\n  ... | cargo run -p schedule-query --example inspect -- touching <YYYY-MM-DD> <TIPLOC1,TIPLOC2,...>"
    );
    std::process::exit(1);
}
