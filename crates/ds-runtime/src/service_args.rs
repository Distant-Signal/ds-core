//! Shared `clap::Args` sub-structs for CLI/env config blocks that are
//! byte-identical (or identical apart from one deliberately-excluded
//! per-crate field) across multiple binaries. See
//! docs/superpowers/specs/2026-09-05-rust-service-deduplication-design.md
//! §3.2 for the full per-field verification.
//!
//! Also `--print-config-defaults` ([`exit_if_print_config_defaults`]): every
//! service binary prints the environment it reads, with each variable's
//! default, as JSON and exits, without connecting to anything. The chart's
//! contract check (`scripts/check-config-defaults-chart.py`, repo split spec
//! §9 phase 3 item 7) compares `values.yaml` and the env the chart renders
//! against it, so no crate test reads chart files.

use std::collections::BTreeMap;
use std::ffi::OsString;

/// The one argument that makes a service print [`ConfigDefaults`] and exit.
/// Not a clap argument: it is checked before the service's own parser runs
/// (which would refuse to start without the required settings), and only
/// when it is the sole argument, so it never mixes with a real start.
pub const PRINT_CONFIG_DEFAULTS_FLAG: &str = "--print-config-defaults";

/// What a service reads from its environment, and the contract constants
/// the chart must agree with, as `--print-config-defaults` prints it.
///
/// ```json
/// {"service": "notifier",
///  "env": {"LOG_LEVEL": {"flag": "--log-level", "default": "info", "required": false}},
///  "contract": {}}
/// ```
///
/// `default` is `null` for a variable with no default (`required` says
/// whether clap insists on it). A variable read outside clap
/// ([`ConfigDefaults::env`]) has no `flag`.
#[derive(Debug, Clone)]
pub struct ConfigDefaults {
    service: String,
    env: BTreeMap<String, serde_json::Value>,
    contract: BTreeMap<String, serde_json::Value>,
}

impl ConfigDefaults {
    /// `service` is the binary's name (the chart check's key for it).
    pub fn new(service: &str) -> Self {
        Self {
            service: service.to_owned(),
            env: BTreeMap::new(),
            contract: BTreeMap::new(),
        }
    }

    /// [`ConfigDefaults::new`] with every env-backed argument of the
    /// service's clap config `C`.
    pub fn for_config<C: clap::CommandFactory>(service: &str) -> Self {
        Self::new(service).args(&C::command())
    }

    /// Every env-backed argument of `command` (and its subcommands).
    #[must_use]
    pub fn args(mut self, command: &clap::Command) -> Self {
        for arg in command.get_arguments() {
            let Some(env) = arg.get_env().and_then(|env| env.to_str()) else {
                continue;
            };
            let defaults: Vec<String> = arg
                .get_default_values()
                .iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect();
            let default = if defaults.is_empty() {
                serde_json::Value::Null
            } else {
                let delimiter = arg.get_value_delimiter().unwrap_or(',');
                serde_json::Value::String(defaults.join(&delimiter.to_string()))
            };
            self.env.insert(
                env.to_owned(),
                serde_json::json!({
                    "flag": arg.get_long().map(|long| format!("--{long}")),
                    "default": default,
                    "required": arg.is_required_set(),
                }),
            );
        }
        for subcommand in command.get_subcommands() {
            self = self.args(subcommand);
        }
        self
    }

    /// A variable the service reads outside clap (`std::env::var`), with
    /// the value it uses when the variable is unset (`None`: no default).
    #[must_use]
    pub fn env(mut self, name: &str, default: Option<&str>) -> Self {
        self.env.insert(
            name.to_owned(),
            serde_json::json!({"flag": null, "default": default, "required": false}),
        );
        self
    }

    /// The `DATABASE_*` pool variables [`crate::pg::PoolSettings::from_env`]
    /// reads, with the defaults it uses for a service whose own default
    /// pool size is `max_connections`.
    #[cfg(feature = "postgres")]
    #[must_use]
    pub fn postgres_pool(self, max_connections: u32) -> Self {
        let defaults = crate::pg::PoolSettings::new("", max_connections);
        let secs = |duration: std::time::Duration| duration.as_secs().to_string();
        self.env(
            crate::pg::MAX_CONNECTIONS_ENV,
            Some(&defaults.max_connections.to_string()),
        )
        .env(
            crate::pg::STATEMENT_TIMEOUT_ENV,
            Some(&secs(defaults.statement_timeout)),
        )
        .env(
            crate::pg::IDLE_IN_TRANSACTION_TIMEOUT_ENV,
            Some(&secs(defaults.idle_in_transaction_timeout)),
        )
        .env(
            crate::pg::ACQUIRE_TIMEOUT_ENV,
            Some(&secs(defaults.acquire_timeout)),
        )
        .env(
            crate::pg::STARTUP_CONNECT_TIMEOUT_ENV,
            Some(&secs(defaults.startup_connect_timeout)),
        )
    }

    /// The service's own Postgres role (its key in `db-grants.yaml`): with
    /// per-service roles on, the chart must connect it as that role.
    #[must_use]
    pub fn db_role(self, key: &str) -> Self {
        self.contract("db_role", key)
    }

    /// A constant the chart must agree with, e.g. the operations an alert
    /// sums or the stream caps an alert compares against.
    #[must_use]
    pub fn contract(mut self, key: &str, value: impl Into<serde_json::Value>) -> Self {
        self.contract.insert(key.to_owned(), value.into());
        self
    }

    /// The JSON `--print-config-defaults` prints.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "service": self.service,
            "env": self.env,
            "contract": self.contract,
        })
    }
}

/// Whether `args` (a process's, program name first) ask for
/// [`PRINT_CONFIG_DEFAULTS_FLAG`]: it must be the only argument.
fn print_config_defaults_requested(args: impl IntoIterator<Item = OsString>) -> bool {
    let mut args = args.into_iter().skip(1);
    args.next()
        .is_some_and(|arg| arg == PRINT_CONFIG_DEFAULTS_FLAG)
        && args.next().is_none()
}

/// The first line of every service's `main`: with `--print-config-defaults`
/// as the only argument, prints `defaults()` as JSON and exits 0 before the
/// service parses its config, sets up logging or connects to anything.
/// Otherwise returns at once without calling `defaults`.
#[expect(
    clippy::print_stdout,
    reason = "the flag's whole output is this JSON on stdout"
)]
pub fn exit_if_print_config_defaults(defaults: impl FnOnce() -> ConfigDefaults) {
    if !print_config_defaults_requested(std::env::args_os()) {
        return;
    }
    let json = serde_json::to_string_pretty(&defaults().to_json())
        .unwrap_or_else(|err| unreachable!("a JSON value always serialises: {err}"));
    println!("{json}");
    std::process::exit(0);
}

/// `metrics_enabled` only -- NOT `metrics_port`, whose default genuinely
/// differs per crate (`9091`/`9092`/`9093`/`9095`) and which
/// `docker-compose.yml` relies on the code-level default for. Every real
/// caller's `metrics_enabled` default is `true`, confirmed identical
/// across all 9.
#[derive(Debug, Clone, clap::Args)]
pub struct MetricsArgs {
    #[arg(long, env, default_value_t = true)]
    pub metrics_enabled: bool,
}

/// 5 of the 6 Kafka connection fields shared by `trust-consumer`,
/// `full-coverage-consumer`, and `movement-relay` -- NOT
/// `kafka_consumer_group`, which stays a per-crate field: two of the three
/// crates default it to a distinct per-deployment string, and the third
/// (`movement-relay`) deliberately has no default at all (a fixed,
/// externally-issued, unforgeable identity -- see
/// `crates/movement-relay/src/config.rs:16-24`'s own comment). All 5
/// fields below are `#[arg(long, env)]` with no default (required) in
/// every one of the 3 real callers today -- flattening changes nothing
/// about defaultedness or requiredness.
///
/// Signal Box Audit, Finding #2 (`common/service_args.rs)`: does NOT derive
/// `Debug` -- `kafka_sasl_password` is a real secret (RDM's "Consumer
/// secret" for this Kafka product), and a future accidental
/// `tracing::debug!("{args:?}")` on a `Config` that flattens this in would
/// otherwise print it verbatim. See the hand-written impl below.
/// The `/livez` + `/healthz` listener every background worker (aggregator,
/// enricher, notifier, the pollers, schedule-ingest, schedule-reference)
/// serves via `health_http::spawn_with_progress` (SVC-08/INF-9). Liveness
/// probes hit `/livez` (503 only once the loop has stalled); workers with a
/// readiness probe hit `/healthz` (also 503 until the initial database/Redis
/// connection is up, INF-5).
#[derive(Debug, Clone, clap::Args)]
pub struct HealthArgs {
    /// Bind address for the health listener. The chart sets it per
    /// container (scheduleFeed's two Rust containers share a pod, so they
    /// need different ports).
    #[arg(long, env, default_value = "0.0.0.0:8090")]
    pub health_bind_url: String,
    /// `/livez` answers 503 "stalled" once one loop iteration has run for
    /// this many seconds without completing. Idle time between iterations
    /// never counts (`ds_runtime::progress::Progress::idle`), so this only needs
    /// to exceed the longest legitimate single iteration.
    #[arg(long, env, default_value_t = 1800)]
    pub progress_stall_secs: u64,
}

impl HealthArgs {
    pub fn stall_after(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.progress_stall_secs)
    }
}

#[derive(Clone, clap::Args)]
pub struct KafkaConnectionArgs {
    /// RDM Kafka broker address(es), comma-separated, e.g.
    /// `kafka.raildata.org.uk:9094`. GAP: unconfirmed hostname.
    #[arg(long, env)]
    pub kafka_brokers: String,
    /// GAP: unconfirmed exact topic name for the Train Movements product.
    #[arg(long, env)]
    pub kafka_topic: String,
    #[arg(long, env)]
    pub kafka_sasl_username: String,
    /// RDM's "Consumer secret" for this product (SASL password).
    #[arg(long, env)]
    pub kafka_sasl_password: String,
    /// GAP: unconfirmed whether RDM's Kafka product uses PLAIN or a SCRAM
    /// variant. PLAIN is `librdkafka`'s simplest, most common default for
    /// managed Kafka-as-a-service offerings, but this is an assumption,
    /// not a confirmed fact.
    #[arg(long, env)]
    pub kafka_sasl_mechanism: String,
}

impl std::fmt::Debug for KafkaConnectionArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaConnectionArgs")
            .field("kafka_brokers", &self.kafka_brokers)
            .field("kafka_topic", &self.kafka_topic)
            .field("kafka_sasl_username", &self.kafka_sasl_username)
            .field("kafka_sasl_password", &"[REDACTED]")
            .field("kafka_sasl_mechanism", &self.kafka_sasl_mechanism)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct TestConfig {
        #[command(flatten)]
        metrics: MetricsArgs,
        #[command(flatten)]
        kafka: KafkaConnectionArgs,
        #[command(flatten)]
        health: HealthArgs,
    }

    #[test]
    fn metrics_args_flatten_preserves_flag_names_and_default() {
        let config = TestConfig::try_parse_from([
            "test",
            "--kafka-brokers",
            "b",
            "--kafka-topic",
            "t",
            "--kafka-sasl-username",
            "u",
            "--kafka-sasl-password",
            "p",
            "--kafka-sasl-mechanism",
            "PLAIN",
        ])
        .expect("only the required Kafka args should be needed");
        assert!(
            config.metrics.metrics_enabled,
            "metrics_enabled's default must stay true when --metrics-enabled is omitted"
        );
        assert_eq!(config.health.health_bind_url, "0.0.0.0:8090");
        assert_eq!(config.health.progress_stall_secs, 1800);
    }

    #[test]
    fn config_defaults_lists_every_env_backed_argument() {
        use clap::CommandFactory;
        let json = ConfigDefaults::new("test")
            .args(&TestConfig::command())
            .env("RAW_SETTING", Some("7"))
            .contract("operations", serde_json::json!(["a", "b"]))
            .to_json();
        assert_eq!(json["service"], "test");
        let env = &json["env"];
        assert_eq!(
            env["METRICS_ENABLED"],
            serde_json::json!({"flag": "--metrics-enabled", "default": "true", "required": false})
        );
        assert_eq!(env["HEALTH_BIND_URL"]["default"], "0.0.0.0:8090");
        assert_eq!(env["PROGRESS_STALL_SECS"]["default"], "1800");
        assert_eq!(
            env["KAFKA_SASL_PASSWORD"],
            serde_json::json!({"flag": "--kafka-sasl-password", "default": null, "required": true})
        );
        assert_eq!(
            env["RAW_SETTING"],
            serde_json::json!({"flag": null, "default": "7", "required": false})
        );
        assert_eq!(
            json["contract"]["operations"],
            serde_json::json!(["a", "b"])
        );
    }

    #[test]
    fn print_config_defaults_only_as_the_sole_argument() {
        let args = |list: &[&str]| list.iter().map(OsString::from).collect::<Vec<_>>();
        assert!(print_config_defaults_requested(args(&[
            "svc",
            "--print-config-defaults"
        ])));
        assert!(!print_config_defaults_requested(args(&["svc"])));
        assert!(!print_config_defaults_requested(args(&[
            "svc",
            "--print-config-defaults",
            "--metrics-enabled"
        ])));
        assert!(!print_config_defaults_requested(args(&[
            "svc",
            "--metrics-enabled",
            "--print-config-defaults"
        ])));
    }

    #[test]
    fn kafka_connection_args_requires_all_five_fields() {
        let result = TestConfig::try_parse_from(["test"]);
        assert!(
            result.is_err(),
            "all 5 Kafka fields are required (no default) -- omitting them must fail to parse"
        );
    }
}
