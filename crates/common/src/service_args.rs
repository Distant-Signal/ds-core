//! Shared `clap::Args` sub-structs for CLI/env config blocks that are
//! byte-identical (or identical apart from one deliberately-excluded
//! per-crate field) across multiple binaries. See
//! docs/superpowers/specs/2026-09-05-rust-service-deduplication-design.md
//! §3.2 for the full per-field verification.

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
#[derive(Debug, Clone, clap::Args)]
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
