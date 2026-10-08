//! Metric names (without the `distant_signal_` prefix that
//! [`common::metrics::metric_name`] adds) and label values. The chart's
//! alerts (plan 3a.4) are written against these; spec §14.1 lists them.

use common::metrics::metric_name;

// Producer side.

/// Counter `{stream, outcome}`: one per XADD attempt. Outcomes:
/// [`PRODUCE_OUTCOMES`].
pub const PRODUCE_TOTAL: &str = "ingest_stream_produce_total";
/// Counter `{stream}`: `body` bytes successfully written.
pub const PRODUCE_BYTES_TOTAL: &str = "ingest_stream_produce_bytes_total";
/// Gauge `{stream}`: items (snapshots or events) held in memory, not yet
/// fully written.
pub const PRODUCE_BUFFERED: &str = "ingest_stream_produce_buffered";
/// Counter `{stream, reason}`: items the producer gave up on. Reasons:
/// [`PRODUCE_DROP_REASONS`].
pub const PRODUCE_DROPPED_TOTAL: &str = "ingest_stream_produce_dropped_total";

/// Counter `{stream, schema, sink}`: snapshot rows a producer delivered,
/// by sink: `http` (the api accepted the POST) or `stream` (every part of
/// the snapshot was `XADD`ed). Under `INGEST_SINK=http+shadow` both count the
/// same snapshots, so the two series agree while both paths work; the
/// rollout's compare step (plan 3a, `docs/ingest-stream-runtime.md`) checks
/// them against the writer's [`ROWS_TOTAL`].
pub const SINK_ROWS_TOTAL: &str = "ingest_stream_sink_rows_total";
/// `sink` label values of [`SINK_ROWS_TOTAL`].
pub const SINKS: [&str; 2] = ["http", "stream"];

/// `ok`, or why an XADD failed. `down` covers refused, reset and timed-out
/// connections and `LOADING`; `error` anything unrecognised.
pub const PRODUCE_OUTCOMES: [&str; 7] =
    ["ok", "down", "oom", "noauth", "noperm", "misconf", "error"];
/// `superseded`: a newer snapshot replaced an unsent one (latest-snapshot
/// policy); `oversize`: an entry over 512 KiB refused before XADD.
pub const PRODUCE_DROP_REASONS: [&str; 2] = ["superseded", "oversize"];

// Consumer (writer) side.

/// Counter `{stream, schema, outcome}`. Outcomes: [`CONSUME_OUTCOMES`].
/// `schema` is `unknown` for an entry whose envelope did not decode.
pub const CONSUMED_TOTAL: &str = "ingest_stream_consumed_total";
/// Histogram `{stream, schema}`: handler wall time, seconds.
pub const HANDLER_SECONDS: &str = "ingest_stream_handler_seconds";
/// Counter `{stream, reason}`: entries written to the dead-letter stream.
pub const DEAD_LETTERED_TOTAL: &str = "ingest_stream_dead_lettered_total";
/// Gauge `{stream}`: entries not yet delivered to the group (`XINFO GROUPS`
/// `lag`).
pub const LAG: &str = "ingest_stream_lag";
/// Gauge `{stream}`: delivered but un-acked entries (the group's PEL).
pub const PENDING: &str = "ingest_stream_pending";
/// Gauge `{stream}`: age of the oldest pending entry, from its id.
pub const OLDEST_PENDING_AGE_SECONDS: &str = "ingest_stream_oldest_pending_age_seconds";
/// Gauge `{stream}`: `XLEN` of the stream's dead-letter stream.
pub const DLQ_LENGTH: &str = "ingest_stream_dlq_length";
/// Gauge `{stream}`: age of the oldest dead-letter entry (from its id), or
/// 0. `DistantSignalIngestDeadLetterExpiring` compares it with the
/// retention.
pub const DLQ_OLDEST_AGE_SECONDS: &str = "ingest_stream_dlq_oldest_age_seconds";
/// Gauge `{stream}`: `MEMORY USAGE` of the stream plus its dead-letter
/// stream.
pub const STREAM_BYTES: &str = "ingest_stream_bytes";
/// Gauge `{stream}`: Unix time of the last applied entry.
pub const LAST_APPLIED_TIMESTAMP_SECONDS: &str = "ingest_stream_last_applied_timestamp_seconds";
/// Counter `{stream, schema}`: observed times the writer's guard helpers
/// clamped to its `now() + 2 min` (spec §7.8, D13). Emitted by the
/// ingest-writer (`ingest_writer::observed`), not by this crate's runtime;
/// named here with the rest of the family the alerts use.
pub const OBSERVED_AT_CLAMPED_TOTAL: &str = "ingest_stream_observed_at_clamped_total";
/// Counter `{stream, schema, mode}`: rows the writer's snapshot handlers
/// decoded and validated (`mode="shadow"`) or wrote (`mode="apply"`, rows
/// refused for a data error excluded). Emitted by the ingest-writer's
/// handlers (plan 3a.6). In shadow it should match the producer's
/// `SINK_ROWS_TOTAL{sink="http"}` for the same schema.
pub const ROWS_TOTAL: &str = "ingest_stream_rows_total";
/// Counter `{stream, schema, outcome}`: of the rows counted in
/// `ROWS_TOTAL{mode="apply"}`, those whose upsert `written` a row (inserted
/// or updated) or `skipped` it (unchanged, a duplicate key in the batch, or
/// refused by the ordering guard as older). Emitted by the ingest-writer's
/// snapshot handlers; `INGEST_WRITER_CHANGED_ROWS_ONLY` (plan 3a.9) moves
/// unchanged rows from `written` to `skipped`.
pub const ROW_WRITES_TOTAL: &str = "ingest_stream_row_writes_total";

/// - `applied`, `duplicate` (the handler saw the key already applied),
///   `skipped` (shadow mode), `rejected` (applied, with some rows
///   dead-lettered) are acked;
/// - `dead_lettered` (poison) is dead-lettered, then acked;
/// - `trimmed`: a pending entry `MAXLEN` removed before it was handled,
///   acked;
/// - `transient_error` and `unsupported_schema` stay pending and are
///   retried (an unsupported entry past the consumer's
///   `unsupported_deadline` is then `dead_lettered`, reason
///   `unsupported_expired`).
pub const CONSUME_OUTCOMES: [&str; 8] = [
    "applied",
    "duplicate",
    "skipped",
    "rejected",
    "dead_lettered",
    "trimmed",
    "transient_error",
    "unsupported_schema",
];

pub(crate) fn produce(stream: &str, outcome: &'static str) {
    metrics::counter!(metric_name(PRODUCE_TOTAL), "stream" => stream.to_owned(), "outcome" => outcome)
        .increment(1);
}

pub(crate) fn produce_bytes(stream: &str, bytes: usize) {
    metrics::counter!(metric_name(PRODUCE_BYTES_TOTAL), "stream" => stream.to_owned())
        .increment(u64::try_from(bytes).unwrap_or(u64::MAX));
}

#[expect(
    clippy::cast_precision_loss,
    reason = "a buffer of at most thousands of items"
)]
pub(crate) fn buffered(stream: &str, items: usize) {
    metrics::gauge!(metric_name(PRODUCE_BUFFERED), "stream" => stream.to_owned()).set(items as f64);
}

pub(crate) fn dropped(stream: &str, reason: &'static str, count: usize) {
    metrics::counter!(metric_name(PRODUCE_DROPPED_TOTAL), "stream" => stream.to_owned(), "reason" => reason)
        .increment(u64::try_from(count).unwrap_or(u64::MAX));
}

/// Counts an item the caller could not submit because
/// [`crate::Envelope::encode`] or [`crate::split_snapshot`] returned
/// [`crate::EnvelopeError::TooLarge`] (`reason="oversize"`).
pub fn record_oversize(stream: &str) {
    dropped(stream, "oversize", 1);
}

/// Registers every producer series for `stream` at 0, so an alert's
/// `increase()` sees the first failure.
pub fn register_producer(stream: &str) {
    for outcome in PRODUCE_OUTCOMES {
        metrics::counter!(metric_name(PRODUCE_TOTAL), "stream" => stream.to_owned(), "outcome" => outcome)
            .increment(0);
    }
    for reason in PRODUCE_DROP_REASONS {
        dropped(stream, reason, 0);
    }
    produce_bytes(stream, 0);
    buffered(stream, 0);
}

pub(crate) fn consumed(stream: &str, schema: &str, outcome: &'static str) {
    metrics::counter!(
        metric_name(CONSUMED_TOTAL),
        "stream" => stream.to_owned(),
        "schema" => schema.to_owned(),
        "outcome" => outcome
    )
    .increment(1);
}

/// Counts `count` observed times clamped for `stream`/`schema`
/// ([`OBSERVED_AT_CLAMPED_TOTAL`]); `count` 0 registers the series.
pub fn observed_at_clamped(stream: &str, schema: &str, count: u64) {
    metrics::counter!(
        metric_name(OBSERVED_AT_CLAMPED_TOTAL),
        "stream" => stream.to_owned(),
        "schema" => schema.to_owned()
    )
    .increment(count);
}

/// Counts `count` rows a producer delivered through `sink` (`http` or
/// `stream`; [`SINK_ROWS_TOTAL`]). `count` 0 registers the series.
pub fn sink_rows(stream: &str, schema: &str, sink: &'static str, count: usize) {
    metrics::counter!(
        metric_name(SINK_ROWS_TOTAL),
        "stream" => stream.to_owned(),
        "schema" => schema.to_owned(),
        "sink" => sink
    )
    .increment(u64::try_from(count).unwrap_or(u64::MAX));
}

/// Counts `count` rows the writer handled for `stream`/`schema` in `mode`
/// (`shadow` or `apply`; [`ROWS_TOTAL`]).
pub fn rows(stream: &str, schema: &str, mode: &'static str, count: usize) {
    metrics::counter!(
        metric_name(ROWS_TOTAL),
        "stream" => stream.to_owned(),
        "schema" => schema.to_owned(),
        "mode" => mode
    )
    .increment(u64::try_from(count).unwrap_or(u64::MAX));
}

/// Counts the rows the writer `written` and `skipped` for `stream`/`schema`
/// ([`ROW_WRITES_TOTAL`]).
pub fn row_writes(stream: &str, schema: &str, written: u64, skipped: u64) {
    for (outcome, count) in [("written", written), ("skipped", skipped)] {
        metrics::counter!(
            metric_name(ROW_WRITES_TOTAL),
            "stream" => stream.to_owned(),
            "schema" => schema.to_owned(),
            "outcome" => outcome
        )
        .increment(count);
    }
}

pub(crate) fn handler_seconds(stream: &str, schema: &str, seconds: f64) {
    metrics::histogram!(
        metric_name(HANDLER_SECONDS),
        "stream" => stream.to_owned(),
        "schema" => schema.to_owned()
    )
    .record(seconds);
}

pub(crate) fn dead_lettered(stream: &str, reason: &str) {
    metrics::counter!(
        metric_name(DEAD_LETTERED_TOTAL),
        "stream" => stream.to_owned(),
        "reason" => reason.to_owned()
    )
    .increment(1);
}

pub(crate) fn gauge(name: &str, stream: &str, value: f64) {
    metrics::gauge!(metric_name(name), "stream" => stream.to_owned()).set(value);
}

/// Registers the consumer's gauges and its `schema`-independent counters
/// for `stream` at 0. Per-schema series appear on first use; the
/// dead-letter alert uses `DEAD_LETTERED_TOTAL`, which is registered here
/// for the decode reasons.
pub fn register_consumer(stream: &str) {
    for name in [
        LAG,
        PENDING,
        OLDEST_PENDING_AGE_SECONDS,
        DLQ_LENGTH,
        DLQ_OLDEST_AGE_SECONDS,
    ] {
        gauge(name, stream, 0.0);
    }
    for reason in [
        "poison",
        "undecodable",
        "oversize",
        "rejected_rows",
        crate::consumer::UNSUPPORTED_EXPIRED,
    ] {
        metrics::counter!(
            metric_name(DEAD_LETTERED_TOTAL),
            "stream" => stream.to_owned(),
            "reason" => reason
        )
        .increment(0);
    }
}
