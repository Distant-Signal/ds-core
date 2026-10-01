//! The one tracing setup every binary shares: structured JSON lines by
//! default, human-readable text for local development.
//!
//! # `LOG_FORMAT`
//!
//! - `json` (the default, and what the chart runs): one JSON object per line,
//!   no ANSI colour, no multi-line output. Loki/Alloy parse it.
//! - `pretty`: tracing's human-readable text with colour (the format every
//!   binary logged in before this module existed), for a terminal.
//!
//! Anything else falls back to `json` and says so in the first log line.
//! The level filter is unchanged: `RUST_LOG` (EnvFilter syntax) for every
//! binary, except the callers of [`init_with_filter`] that build their own.
//!
//! # The JSON line schema
//!
//! Shared with the MCP server, and the format for any new service:
//!
//! | key | value |
//! |---|---|
//! | `timestamp` | RFC 3339, UTC, microseconds (`2026-10-01T09:30:00.123456Z`) |
//! | `level` | `TRACE`, `DEBUG`, `INFO`, `WARN` or `ERROR` |
//! | `service` | the binary/workload name, static per process (`api`, `poller-ldbws`) |
//! | `target` | the event's target (its module path unless overridden) |
//! | `message` | the event's message, when it has one |
//! | *(event fields)* | flattened to the top level under their own snake_case names |
//! | `error` | an error's text (`error = ?err` / `error = %err` at the call site) |
//! | `stack` | a backtrace, for panics (and fatal errors, when one was captured) |
//! | `spans` | the enclosing spans, root first, each `{"name": ..., <span fields>}`; omitted outside any span |
//!
//! Events from the `log` crate (rdkafka, sqlx, reqwest, ...) are bridged in
//! with their real target and no `log.*` bookkeeping fields. An event field
//! whose name collides with one of the keys above is written as
//! `field_<name>` instead, so a line never has duplicate keys.
//!
//! Secrets stay out of logs the same way as before: credential fields are
//! [`crate::secret::Secret`], whose `Debug` prints `Secret(***)` and which
//! has no `Display`.
//!
//! # Panics and fatal errors
//!
//! In `json` mode a panic hook replaces the default stderr message with one
//! `ERROR` line (target `panic`, message `panicked at <file:line:col>: <payload>`,
//! plus `stack`). [`exit_code`] does the same for an error returned from
//! `main` (target `fatal`), which would otherwise be printed as multi-line
//! `Error: ...` text by the standard library.

use std::backtrace::{Backtrace, BacktraceStatus};
use std::fmt::{self, Write as _};
use std::process::ExitCode;
use std::sync::OnceLock;

use serde::Serializer as _;
use serde::ser::SerializeMap as _;
use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{JsonFields, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormattedFields, MakeWriter};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt as _;

pub use tracing_subscriber::EnvFilter;

/// The selectable output formats; see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Pretty,
}

impl LogFormat {
    /// Parse a `LOG_FORMAT` value. `None` (unset) and the empty string mean
    /// the default, `json`. An unrecognised value also gives `json`, with
    /// `Err` carrying the value so the caller can warn about it.
    pub fn parse(value: Option<&str>) -> Result<Self, (Self, String)> {
        match value.map(str::trim) {
            None | Some("") => Ok(Self::Json),
            Some(v) if v.eq_ignore_ascii_case("json") => Ok(Self::Json),
            Some(v) if v.eq_ignore_ascii_case("pretty") => Ok(Self::Pretty),
            Some(v) => Err((Self::Json, v.to_string())),
        }
    }
}

/// The format [`init`] installed, if it has run. [`exit_code`] reads it.
static INSTALLED: OnceLock<LogFormat> = OnceLock::new();

/// Install the global subscriber for `service`, filtered by `RUST_LOG`
/// exactly as `EnvFilter::from_default_env()` always has been.
pub fn init(service: &'static str) {
    init_with_filter(service, EnvFilter::from_default_env());
}

/// [`init`], with a caller-built filter (the notifier's `LOG_LEVEL`, the
/// one-off backfill binaries' `info` default).
pub fn init_with_filter(service: &'static str, filter: EnvFilter) {
    let raw = std::env::var("LOG_FORMAT").ok();
    let (format, unrecognised) = match LogFormat::parse(raw.as_deref()) {
        Ok(format) => (format, None),
        Err((format, value)) => (format, Some(value)),
    };
    let installed = match format {
        LogFormat::Json => {
            // `try_init` also installs the `log` -> tracing bridge, as the
            // fmt builder's own `init()` always did.
            json_subscriber(service, filter, std::io::stdout)
                .try_init()
                .is_ok()
        }
        LogFormat::Pretty => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .is_ok(),
    };
    if !installed {
        // Someone else's subscriber is already global (a test harness);
        // keep it, and leave the panic hook and `exit_code` alone.
        return;
    }
    let _ = INSTALLED.set(format);
    if format == LogFormat::Json {
        install_panic_hook();
    }
    if let Some(value) = unrecognised {
        tracing::warn!(log_format = %value, "unrecognised LOG_FORMAT; using json (expected json or pretty)");
    }
}

/// The JSON subscriber, writing to `make_writer`. [`init`] uses stdout; the
/// tests capture it.
pub fn json_subscriber<W>(
    service: &'static str,
    filter: EnvFilter,
    make_writer: W,
) -> impl Subscriber + Send + Sync + for<'a> LookupSpan<'a>
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .fmt_fields(JsonFields::new())
        .event_format(JsonLine { service })
        .with_writer(make_writer)
        .finish()
}

/// Turn `main`'s result into the process exit code. An error is logged as
/// one `ERROR` line (target `fatal`, `error` = the `: `-joined cause chain,
/// `stack` when anyhow captured a backtrace) and gives exit code 1, the
/// same code `fn main() -> anyhow::Result<()>` exits with. Before [`init`]
/// has run, or in `pretty` mode, it prints `Error: {err:?}` to stderr as the
/// standard library would.
pub fn exit_code(result: anyhow::Result<()>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            if INSTALLED.get() == Some(&LogFormat::Json) {
                let backtrace = err.backtrace();
                let stack = (backtrace.status() == BacktraceStatus::Captured)
                    .then(|| backtrace.to_string());
                tracing::error!(
                    target: "fatal",
                    error = %format!("{err:#}"),
                    stack = stack.as_deref(),
                    "exiting after a fatal error"
                );
            } else {
                eprintln!("Error: {err:?}");
            }
            ExitCode::FAILURE
        }
    }
}

fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        // `PanicHookInfo::payload_as_str` is newer than the 1.88 MSRV.
        let payload = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
            .unwrap_or("Box<dyn Any>");
        let location = info
            .location()
            .map_or_else(|| "<unknown>".to_string(), ToString::to_string);
        let thread = std::thread::current();
        let stack = Backtrace::force_capture().to_string();
        tracing::error!(
            target: "panic",
            thread = thread.name().unwrap_or("<unnamed>"),
            stack = %stack,
            "panicked at {location}: {payload}"
        );
    }));
}

/// Keys the line itself owns; an event field with one of these names is
/// renamed `field_<name>`.
const RESERVED: [&str; 5] = ["timestamp", "level", "service", "target", "spans"];

/// The [`FormatEvent`] behind the JSON lines. tracing-subscriber's own
/// `.json().flatten_event(true)` cannot carry a static `service` field and
/// keeps the `log` bridge's `log.*` fields, so the line is written here
/// with serde_json instead; span fields are still recorded by
/// tracing-subscriber's [`JsonFields`].
struct JsonLine {
    service: &'static str,
}

impl<S> FormatEvent<S, JsonFields> for JsonLine
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, JsonFields>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let mut fields = FieldVisitor::default();
        event.record(&mut fields);

        let spans: Vec<Map<String, Value>> = ctx
            .event_scope()
            .into_iter()
            .flat_map(|scope| scope.from_root())
            .map(|span| {
                let mut entry = Map::new();
                entry.insert("name".into(), Value::from(span.name()));
                let extensions = span.extensions();
                if let Some(recorded) = extensions.get::<FormattedFields<JsonFields>>()
                    && let Ok(Value::Object(map)) = serde_json::from_str::<Value>(&recorded.fields)
                {
                    entry.extend(map);
                }
                entry
            })
            .collect();

        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        let target = fields.log_target.as_deref().unwrap_or(meta.target());

        let mut buf = Vec::with_capacity(256);
        self.serialize(
            &mut buf,
            &timestamp,
            meta.level().as_str(),
            target,
            &fields,
            &spans,
        )
        .map_err(|_| fmt::Error)?;
        writer.write_str(std::str::from_utf8(&buf).map_err(|_| fmt::Error)?)?;
        writer.write_char('\n')
    }
}

impl JsonLine {
    fn serialize(
        &self,
        buf: &mut Vec<u8>,
        timestamp: &str,
        level: &str,
        target: &str,
        fields: &FieldVisitor,
        spans: &[Map<String, Value>],
    ) -> serde_json::Result<()> {
        let mut ser = serde_json::Serializer::new(buf);
        let mut map = (&mut ser).serialize_map(None)?;
        map.serialize_entry("timestamp", timestamp)?;
        map.serialize_entry("level", level)?;
        map.serialize_entry("service", self.service)?;
        map.serialize_entry("target", target)?;
        if let Some(message) = &fields.message {
            map.serialize_entry("message", message)?;
        }
        for (name, value) in &fields.fields {
            if RESERVED.contains(&name.as_str()) {
                map.serialize_entry(&format!("field_{name}"), value)?;
            } else {
                map.serialize_entry(name, value)?;
            }
        }
        if !spans.is_empty() {
            map.serialize_entry("spans", spans)?;
        }
        map.end()
    }
}

/// Collects an event's fields in recording order.
#[derive(Default)]
struct FieldVisitor {
    message: Option<String>,
    /// The `log` bridge's real target (`log.target`), used as `target`.
    log_target: Option<String>,
    fields: Vec<(String, Value)>,
}

impl FieldVisitor {
    fn put(&mut self, field: &Field, value: Value) {
        match field.name() {
            "message" => {
                self.message = Some(match value {
                    Value::String(s) => s,
                    other => other.to_string(),
                });
            }
            "log.target" => {
                if let Value::String(s) = value {
                    self.log_target = Some(s);
                }
            }
            // The rest of the `log` bridge's bookkeeping
            // (`log.module_path`, `log.file`, `log.line`).
            name if name.starts_with("log.") => {}
            name => self.fields.push((name.to_string(), value)),
        }
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.put(field, Value::String(format!("{value:?}")));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, Value::from(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, Value::from(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, Value::from(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, Value::from(value));
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        let value =
            i64::try_from(value).map_or_else(|_| Value::from(value.to_string()), Value::from);
        self.put(field, value);
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        let value =
            u64::try_from(value).map_or_else(|_| Value::from(value.to_string()), Value::from);
        self.put(field, value);
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        // NaN and the infinities have no JSON number; keep them as text.
        let value = serde_json::Number::from_f64(value)
            .map_or_else(|| Value::from(value.to_string()), Value::Number);
        self.put(field, value);
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        // The whole cause chain on one line, like anyhow's `{:#}`.
        let mut text = value.to_string();
        let mut source = value.source();
        while let Some(cause) = source {
            let _ = write!(text, ": {cause}");
            source = cause.source();
        }
        self.put(field, Value::String(text));
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::secret::Secret;

    /// A `MakeWriter` collecting everything written into one buffer.
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Capture {
        type Writer = Capture;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl Capture {
        fn lines(&self) -> Vec<Value> {
            let bytes = self.0.lock().unwrap().clone();
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.ends_with('\n'), "{text:?}");
            assert!(!text.contains('\u{1b}'), "ANSI escape in {text:?}");
            text.lines()
                .map(|line| {
                    let value: Value = serde_json::from_str(line)
                        .unwrap_or_else(|e| panic!("not one JSON object per line ({e}): {line:?}"));
                    assert!(value.is_object(), "{line}");
                    value
                })
                .collect()
        }
    }

    fn capture(service: &'static str, f: impl FnOnce()) -> Vec<Value> {
        let out = Capture::default();
        let subscriber = json_subscriber(service, EnvFilter::new("trace"), out.clone());
        tracing::subscriber::with_default(subscriber, f);
        out.lines()
    }

    #[test]
    fn log_format_parses_json_by_default() {
        assert_eq!(LogFormat::parse(None), Ok(LogFormat::Json));
        assert_eq!(LogFormat::parse(Some("")), Ok(LogFormat::Json));
        assert_eq!(LogFormat::parse(Some(" JSON ")), Ok(LogFormat::Json));
        assert_eq!(LogFormat::parse(Some("pretty")), Ok(LogFormat::Pretty));
        assert_eq!(
            LogFormat::parse(Some("logfmt")),
            Err((LogFormat::Json, "logfmt".to_string()))
        );
    }

    #[test]
    fn emits_one_json_object_per_line_with_the_schema_keys() {
        let lines = capture("poller-test", || {
            tracing::info!(
                count = 3,
                crs = "KGX",
                ratio = 0.5,
                ok = true,
                "fetched\nstations"
            );
            let err = anyhow::anyhow!("inner").context("outer");
            tracing::warn!(error = %format!("{err:#}"), "multi-line\nmessage stays one line");
        });
        assert_eq!(lines.len(), 2);

        let line = &lines[0];
        let ts = line["timestamp"].as_str().unwrap();
        let parsed = chrono::DateTime::parse_from_rfc3339(ts).unwrap();
        assert_eq!(parsed.offset().local_minus_utc(), 0, "{ts} is not UTC");
        assert!(ts.ends_with('Z'), "{ts}");
        assert_eq!(line["level"], "INFO");
        assert_eq!(line["service"], "poller-test");
        assert_eq!(line["target"], "common::logging::tests");
        assert_eq!(line["message"], "fetched\nstations");
        assert_eq!(line["count"], 3);
        assert_eq!(line["crs"], "KGX");
        assert_eq!(line["ratio"], 0.5);
        assert_eq!(line["ok"], true);
        assert!(
            line.get("fields").is_none(),
            "fields must be flattened: {line}"
        );
        assert!(line.get("spans").is_none(), "no span, no spans key: {line}");

        assert_eq!(lines[1]["level"], "WARN");
        assert_eq!(lines[1]["error"], "outer: inner");
    }

    #[test]
    fn key_order_is_timestamp_level_service_target_message() {
        let out = Capture::default();
        let subscriber = json_subscriber("api", EnvFilter::new("info"), out.clone());
        tracing::subscriber::with_default(subscriber, || tracing::info!(n = 1, "hi"));
        let text = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
        let keys = [
            "\"timestamp\"",
            "\"level\"",
            "\"service\"",
            "\"target\"",
            "\"message\"",
            "\"n\"",
        ];
        let positions: Vec<usize> = keys.iter().map(|k| text.find(k).unwrap()).collect();
        assert!(positions.windows(2).all(|w| w[0] < w[1]), "{text}");
    }

    #[test]
    fn spans_are_listed_root_first_with_their_fields() {
        let lines = capture("api", || {
            let outer = tracing::info_span!("request", method = "GET", path = "/x");
            let _outer = outer.enter();
            let inner = tracing::info_span!("handler", line_id = "tfl-victoria");
            let _inner = inner.enter();
            tracing::info!("handled");
        });
        let spans = lines[0]["spans"].as_array().unwrap();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0]["name"], "request");
        assert_eq!(spans[0]["method"], "GET");
        assert_eq!(spans[1]["name"], "handler");
        assert_eq!(spans[1]["line_id"], "tfl-victoria");
    }

    #[test]
    fn colliding_field_names_do_not_duplicate_keys() {
        let lines = capture("api", || tracing::info!(service = "tfl", level = 2, "x"));
        assert_eq!(lines[0]["service"], "api");
        assert_eq!(lines[0]["level"], "INFO");
        assert_eq!(lines[0]["field_service"], "tfl");
        assert_eq!(lines[0]["field_level"], 2);
    }

    #[test]
    fn levels_are_uppercase() {
        let lines = capture("api", || {
            tracing::trace!("t");
            tracing::debug!("d");
            tracing::info!("i");
            tracing::warn!("w");
            tracing::error!("e");
        });
        let levels: Vec<&str> = lines.iter().map(|l| l["level"].as_str().unwrap()).collect();
        assert_eq!(levels, ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"]);
    }

    #[test]
    fn secret_fields_are_redacted_in_json_output() {
        let secret: Secret = "postgres://ds:hunter2@db/ds".parse().unwrap();
        let lines = capture("api", || {
            tracing::info!(database_url = ?secret, config = ?Some(secret.clone()), "connecting");
        });
        let line = lines[0].to_string();
        assert!(!line.contains("hunter2"), "{line}");
        assert_eq!(lines[0]["database_url"], "Secret(***)");
        assert_eq!(lines[0]["config"], "Some(Secret(***))");
    }

    #[test]
    fn error_values_record_their_cause_chain() {
        #[derive(Debug)]
        struct Outer(io::Error);
        impl fmt::Display for Outer {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("could not read config")
            }
        }
        impl std::error::Error for Outer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let err = Outer(io::Error::other("disk on fire"));
        let lines = capture("api", || {
            tracing::error!(error = &err as &(dyn std::error::Error + 'static), "failed")
        });
        assert_eq!(lines[0]["error"], "could not read config: disk on fire");
    }

    #[test]
    fn log_crate_events_keep_their_target_without_log_fields() {
        // What `LogTracer` does with a `log::Record`, without installing it
        // globally (which a test cannot undo).
        let lines = capture("trust-consumer", || {
            let record = log::Record::builder()
                .args(format_args!("broker down"))
                .level(log::Level::Warn)
                .target("librdkafka")
                .module_path_static(Some("rdkafka::client"))
                .file_static(Some("client.rs"))
                .line(Some(42))
                .build();
            tracing_log::format_trace(&record).unwrap();
        });
        assert_eq!(lines[0]["target"], "librdkafka");
        assert_eq!(lines[0]["level"], "WARN");
        assert_eq!(lines[0]["message"], "broker down");
        let keys: Vec<&String> = lines[0].as_object().unwrap().keys().collect();
        assert!(keys.iter().all(|k| !k.starts_with("log.")), "{keys:?}");
    }

    #[test]
    fn panic_hook_logs_one_error_line_with_a_stack() {
        let out = Capture::default();
        let subscriber = json_subscriber("api", EnvFilter::new("info"), out.clone());
        tracing::subscriber::with_default(subscriber, || {
            let previous = std::panic::take_hook();
            install_panic_hook();
            let result = std::panic::catch_unwind(|| panic!("boom {}", 7));
            std::panic::set_hook(previous);
            assert!(result.is_err());
        });
        let lines = out.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        let line = &lines[0];
        assert_eq!(line["level"], "ERROR");
        assert_eq!(line["target"], "panic");
        let message = line["message"].as_str().unwrap();
        assert!(message.starts_with("panicked at "), "{message}");
        assert!(message.ends_with(": boom 7"), "{message}");
        assert!(message.contains("logging.rs"), "{message}");
        assert!(!line["stack"].as_str().unwrap().is_empty());
    }
}
