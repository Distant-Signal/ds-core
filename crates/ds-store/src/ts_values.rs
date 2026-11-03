//! The value sets of string fields the api serves from this crate's read
//! models, for its generated TypeScript only (repo-split spec §5.3). A
//! field opts in with `#[cfg_attr(feature = "ts", ts(as = "..."))]`; these
//! enums are never built, and serde never sees them.

use ts_rs::TS;

/// How a predicted start found its previous working:
/// `trains::predicted_start::LINK_INFERRED` and `LINK_ASSOCIATION`.
#[derive(TS)]
#[ts(rename_all = "snake_case")]
pub enum PreviousWorkingLink {
    Inferred,
    Association,
}
