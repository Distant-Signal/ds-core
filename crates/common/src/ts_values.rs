//! The value sets of string fields shared with the api, for its generated
//! TypeScript only (repo-split spec §5.3). A field opts in with
//! `#[ts(as = "...")]`; these enums are never built, and serde never sees
//! them. Each names where its values are enforced.

use ts_rs::TS;

/// `tracked_train_tickets.source` (its CHECK constraint).
#[derive(TS)]
#[ts(rename_all = "kebab-case")]
pub enum TicketSource {
    Manual,
    PkpassSemantics,
    PkpassHeuristic,
    PdfHeuristic,
}
