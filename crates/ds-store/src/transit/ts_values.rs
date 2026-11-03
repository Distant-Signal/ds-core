//! The value sets of the transit read models' string fields, for the
//! api's generated TypeScript only (repo-split spec §5.3). A field opts in
//! with `#[cfg_attr(feature = "ts", ts(as = "..."))]`; these enums are never
//! built, and serde never sees them.

use ts_rs::TS;

/// A pattern's direction: `transit_patterns.direction` (its CHECK
/// constraint).
#[derive(TS)]
#[ts(rename_all = "snake_case")]
pub enum TransitDirection {
    Outbound,
    Inbound,
}

/// How a transit stop links to a rail station: `transit_rail_links.kind`
/// (its CHECK constraint).
#[derive(TS)]
#[ts(rename_all = "snake_case")]
pub enum TransitLinkKind {
    Interchange,
    Nearby,
}

/// A frequency band's name: `read::FREQUENCY_BANDS`.
#[derive(TS)]
#[ts(rename_all = "kebab-case")]
pub enum FrequencyBandName {
    Early,
    MorningPeak,
    Daytime,
    EveningPeak,
    Evening,
    Late,
}
