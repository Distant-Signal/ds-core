//! The chart's per-stream alert constants mirror [`budget::INGEST_STREAMS`]
//! (plan 3a.4): `metrics.prometheusRule.ingestStreamBacklog.maxlen` must
//! name exactly the budgeted streams with the `MAXLEN` each is trimmed to,
//! and `ingestStreamStalled.stallAfterSecs` must name exactly the same
//! streams. A stream added to (or resized in) `budget.rs` without its
//! values.yaml line would otherwise alert against a stale cap, or not at
//! all.
//!
//! The values file is read with a small indentation-based scan of the two
//! maps rather than a YAML parser: the workspace has no YAML crate, and
//! both maps are flat `"stream": number` lines.

#![expect(
    clippy::unwrap_used,
    reason = "test code: a panic is the right failure"
)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use ingest_stream::budget::INGEST_STREAMS;

fn values_yaml() -> String {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../charts/distant-signal/values.yaml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn indent(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

/// The flat `"key": number` map at `parent.child` (each a key of its own
/// line), e.g. `ingestStreamBacklog` / `maxlen`.
fn number_map(yaml: &str, parent: &str, child: &str) -> BTreeMap<String, u64> {
    let lines: Vec<&str> = yaml.lines().collect();
    let parent_at = lines
        .iter()
        .position(|l| l.trim_start() == format!("{parent}:"))
        .unwrap_or_else(|| panic!("values.yaml has no `{parent}:` line"));
    let parent_indent = indent(lines[parent_at]);
    let child_at = lines[parent_at + 1..]
        .iter()
        .take_while(|l| l.trim().is_empty() || indent(l) > parent_indent)
        .position(|l| l.trim_start() == format!("{child}:"))
        .map(|i| parent_at + 1 + i)
        .unwrap_or_else(|| panic!("values.yaml has no `{parent}.{child}:`"));
    let child_indent = indent(lines[child_at]);
    let mut out = BTreeMap::new();
    for line in &lines[child_at + 1..] {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if indent(line) <= child_indent {
            break;
        }
        // rsplit: the stream names themselves contain colons.
        let (key, value) = trimmed
            .rsplit_once(':')
            .unwrap_or_else(|| panic!("{parent}.{child}: malformed line {line:?}"));
        let key = key.trim().trim_matches('"').to_owned();
        let value: u64 = value
            .trim()
            .parse()
            .unwrap_or_else(|e| panic!("{parent}.{child}.{key}: {value:?}: {e}"));
        assert!(
            out.insert(key.clone(), value).is_none(),
            "{parent}.{child}: {key} listed twice"
        );
    }
    out
}

fn budgeted() -> BTreeMap<String, u64> {
    INGEST_STREAMS
        .iter()
        .map(|d| (d.stream.to_owned(), d.maxlen()))
        .collect()
}

#[test]
fn backlog_maxlen_matches_the_budget() {
    let chart = number_map(&values_yaml(), "ingestStreamBacklog", "maxlen");
    assert_eq!(
        chart,
        budgeted(),
        "metrics.prometheusRule.ingestStreamBacklog.maxlen must list every \
         stream in crates/ingest-stream/src/budget.rs with its MAXLEN"
    );
}

#[test]
fn stall_thresholds_cover_exactly_the_budgeted_streams() {
    let chart = number_map(&values_yaml(), "ingestStreamStalled", "stallAfterSecs");
    assert_eq!(
        chart.keys().collect::<Vec<_>>(),
        budgeted().keys().collect::<Vec<_>>(),
        "metrics.prometheusRule.ingestStreamStalled.stallAfterSecs must list \
         exactly the streams in crates/ingest-stream/src/budget.rs"
    );
    for (stream, secs) in &chart {
        assert!(*secs > 0, "{stream}: stallAfterSecs must be positive");
    }
}
