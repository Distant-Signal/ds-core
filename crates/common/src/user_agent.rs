//! Honest, identifying `User-Agent` strings for outbound requests to
//! third-party hosts (LEG-21 / LEG-24).
//!
//! Every service that fetches from someone else's server should say who it
//! is and where to find out more, rather than sending no `User-Agent` at all
//! (reqwest's default) or impersonating a browser. The string is built at
//! the *call site's* compile time by [`user_agent!`](crate::user_agent), so
//! each binary names itself and its own crate version:
//!
//! ```text
//! distant-signal-poller-irish-rail-live/0.1.0 (+https://github.com/FasterSpeeding/Distant-Signal)
//! ```

/// Public project URL carried in every [`user_agent!`](crate::user_agent)
/// string, so an upstream operator seeing our traffic can find the source
/// and a contact route. Kept in sync with the literal inside the macro
/// (a `macro_rules!` `concat!` can only take literals); the unit test below
/// pins the two together.
pub const PROJECT_URL: &str = "https://github.com/FasterSpeeding/Distant-Signal";

/// Expands to a `&'static str` `User-Agent` of the form
/// `distant-signal-<calling crate>/<calling crate version> (+<project URL>)`.
///
/// `env!("CARGO_PKG_NAME")`/`env!("CARGO_PKG_VERSION")` resolve in the crate
/// that invokes the macro, not in `common`, which is the whole reason this
/// is a macro rather than a `const`.
#[macro_export]
macro_rules! user_agent {
    () => {
        concat!(
            "distant-signal-",
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION"),
            " (+https://github.com/FasterSpeeding/Distant-Signal)"
        )
    };
}

#[cfg(test)]
mod tests {
    use super::PROJECT_URL;

    #[test]
    fn user_agent_names_the_calling_crate_version_and_project() {
        let ua = crate::user_agent!();
        assert_eq!(
            ua,
            format!(
                "distant-signal-common/{} (+{PROJECT_URL})",
                env!("CARGO_PKG_VERSION")
            )
        );
        assert!(!ua.contains("Mozilla"));
    }
}
