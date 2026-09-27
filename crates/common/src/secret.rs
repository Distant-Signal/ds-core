//! [`Secret`]: a string whose `Debug` never prints its value.
//!
//! For configuration fields that hold credentials (database URLs with a
//! password, API keys, signing keys), so a `tracing::info!(?config)` or a
//! panic message cannot leak them (services review SVC-12). Moved here from
//! `aggregator::archive`, which introduced it for the S3 credentials.
//!
//! Use it as a clap field type directly (`FromStr` is infallible), together
//! with `hide_env_values = true` so `--help` does not print the environment
//! value either. Read the value with [`Secret::expose`], at the point of use.

use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Default, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The secret value itself.
    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl FromStr for Secret {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_prints_the_value() {
        let secret: Secret = "postgres://u:hunter2@db/x".parse().unwrap();
        let rendered = format!("{secret:?} {:?}", Some(secret.clone()));
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert_eq!(rendered, "Secret(***) Some(Secret(***))");
        assert_eq!(secret.expose(), "postgres://u:hunter2@db/x");
        assert!(!secret.is_empty());
        assert!(Secret::default().is_empty());
    }
}
