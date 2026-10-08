//! Guards for the one-off maintenance entry points that moved out of the
//! api image's reach (ingest phase 5 prep, Q2 of
//! docs/ingest-phase5-runbook.md): `ds-migrate backfill-trains` and
//! `backfill-line-train-summaries` (as the schema owner), and the
//! ingest-writer image's `writer-maintenance` (`replay-uidless-movements` as
//! the writer, `backfill-incident-lines` as the `incidents` role).
//!
//! Decided (Q2): these tools never run with the api's credentials, because
//! they write tables the narrowed api role (5.5) cannot. [`refuse_api_role`]
//! makes that a startup error rather than a convention; [`require`] names the
//! missing grant up front instead of failing halfway through a run.

use anyhow::{Result, bail};
use sqlx::PgPool;

/// The api's per-service role (`db-grants.yaml` `roles.api.name`).
pub const API_ROLE: &str = "distant_signal_api";

/// A privilege a tool needs, checked with `has_table_privilege` /
/// `has_column_privilege` as the connected user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Needs {
    /// `privilege` (`SELECT`, `INSERT`, `UPDATE` or `DELETE`) on a table.
    Table {
        table: &'static str,
        privilege: &'static str,
    },
    /// `privilege` on one column of a table.
    Column {
        table: &'static str,
        column: &'static str,
        privilege: &'static str,
    },
}

impl std::fmt::Display for Needs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Table { table, privilege } => write!(f, "{privilege} on {table}"),
            Self::Column {
                table,
                column,
                privilege,
            } => write!(f, "{privilege} ({column}) on {table}"),
        }
    }
}

/// The connected user (`current_user`).
pub async fn current_user(pool: &PgPool) -> Result<String> {
    let (user,): (String,) = sqlx::query_as("SELECT current_user::text")
        .fetch_one(pool)
        .await?;
    Ok(user)
}

/// Fails when connected as [`API_ROLE`]; returns the connected user. `tool`
/// and `use_role` name the command and the role it should run as, for the
/// error.
pub async fn refuse_api_role(pool: &PgPool, tool: &str, use_role: &str) -> Result<String> {
    let user = current_user(pool).await?;
    if user == API_ROLE {
        bail!(
            "{tool} must not run with the api's credentials ({API_ROLE}): connect as {use_role} \
             instead (docs/ingest-phase5-runbook.md, Q2)"
        );
    }
    Ok(user)
}

/// Fails, naming every missing privilege, unless the connected user holds
/// all of `needs`.
pub async fn require(pool: &PgPool, tool: &str, use_role: &str, needs: &[Needs]) -> Result<()> {
    let mut missing = Vec::new();
    for need in needs {
        let (held,): (bool,) = match need {
            Needs::Table { table, privilege } => {
                sqlx::query_as("SELECT has_table_privilege($1, $2)")
                    .bind(format!("public.{table}"))
                    .bind(*privilege)
                    .fetch_one(pool)
                    .await?
            }
            Needs::Column {
                table,
                column,
                privilege,
            } => {
                sqlx::query_as("SELECT has_column_privilege($1, $2, $3)")
                    .bind(format!("public.{table}"))
                    .bind(*column)
                    .bind(*privilege)
                    .fetch_one(pool)
                    .await?
            }
        };
        if !held {
            missing.push(need.to_string());
        }
    }
    if !missing.is_empty() {
        let user = current_user(pool).await?;
        bail!(
            "{tool}: {user} lacks {}; run it as {use_role}",
            missing.join(", ")
        );
    }
    Ok(())
}

#[cfg(test)]
mod db_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires a live database (DATABASE_URL)"]
    async fn require_names_what_is_missing() {
        let pool = crate::test_support::connect().await;
        require(
            &pool,
            "test",
            "any",
            &[Needs::Table {
                table: "_sqlx_migrations",
                privilege: "SELECT",
            }],
        )
        .await
        .expect("every role may read _sqlx_migrations (schema_gate)");
        // No role but the owner may TRUNCATE.
        let user = current_user(&pool).await.unwrap();
        let (owner,): (bool,) = sqlx::query_as(
            "SELECT pg_get_userbyid(relowner) = current_user OR \
                    (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) \
             FROM pg_class WHERE oid = 'public.stations'::regclass",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let result = require(
            &pool,
            "test",
            "the owner",
            &[Needs::Table {
                table: "stations",
                privilege: "TRUNCATE",
            }],
        )
        .await;
        if owner {
            result.unwrap();
        } else {
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains(&format!("{user} lacks TRUNCATE on stations")),
                "{err}"
            );
        }
        assert_eq!(
            refuse_api_role(&pool, "test", "any").await.is_err(),
            user == API_ROLE
        );
    }
}
