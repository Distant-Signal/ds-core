//! Coverage-guided fuzz target for [`schedule_query::parse_schedule_records`].
//!
//! Reproduces the harness an external campaign used to find this parser's
//! char-boundary panic class (eight fixed-offset `&line[a..b]` slices
//! guarded only by byte-length checks; see `src/parse.rs`'s
//! `is_fixed_width_decodable`). The parser is total -- it returns
//! `Vec<RawSchedule>`, skipping any malformed line rather than erroring --
//! so the only failure this target can observe is a panic or an ASan
//! finding, which is exactly the property under test.
//!
//! Run with (the second directory is the committed seed corpus of real,
//! byte-verbatim CIF records -- libFuzzer only ever WRITES into the first
//! directory given, so the seeds stay pristine; they make the interesting
//! states cheap to reach, though a cold start from an empty corpus does
//! get to the same edge coverage within a comparable budget):
//!
//! ```text
//! cargo +nightly fuzz run parse_schedule_records \
//!     fuzz/corpus/parse_schedule_records fuzz/seed_corpus \
//!     -- -fork=6 -ignore_crashes=1 -max_total_time=300
//! ```
//!
//! **Read the result, don't read the exit code.** `-ignore_crashes=1` (the
//! flag that makes a fork-mode run keep going past the first crash instead
//! of reporting one and stopping) also makes the run exit 0 whether it
//! crashed or not. The two things to check are the `crash:` counter in the
//! progress lines and, definitively:
//!
//! ```text
//! ls fuzz/artifacts/parse_schedule_records
//! ```
//!
//! -- one file per crashing input, so an empty directory is the pass. Drop
//! `-fork`/`-ignore_crashes` for a single-process run that does stop and
//! exit non-zero on the first crash.
//!
//! Against the pre-fix parser the command above reproduced all eight panic
//! sites in ~3 minutes -- 2,245 artifacts, which triage by panic location
//! to exactly `parse.rs` `54:20`, `113:19`, `117:52`, `118:50`, `121:23`,
//! `155:22`, `156:44`, `164:57`, every one of them `"byte index N is not a
//! char boundary"`. Against the fixed parser: 11.5M executions across 6
//! workers, 0 crashes, 0 artifacts, at cov 493 / ft 3059 -- higher than
//! the cov 462 / ft 2890 an earlier draft of the fix reached, because
//! that draft rejected every non-ASCII line before the record-type
//! dispatch and so made less of the parser reachable, not more.
//!
//! Requires a nightly toolchain (`-Zsanitizer=address`), which the rest of
//! this workspace does not -- see this crate's `fuzz/Cargo.toml` for why
//! the fuzz crate is deliberately its own workspace, and for the price
//! that isolation carries (no CI job builds this file).

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The real caller hands this function the already-read text of a CIF
    // `MCA` extract, so a `&str` is the honest input shape. Feeding the
    // raw bytes through `from_utf8_lossy` keeps every byte sequence
    // libFuzzer produces reachable (invalid UTF-8 arrives as U+FFFD, a
    // three-byte character -- itself a perfectly good char-boundary
    // hazard) instead of discarding most of the corpus.
    let text = String::from_utf8_lossy(data);
    let _ = schedule_query::parse_schedule_records(&text);
});
