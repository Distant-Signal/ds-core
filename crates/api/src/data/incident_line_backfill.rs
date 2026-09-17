//! Recompute `incidents.affected_lines` for rows already in the table.
//!
//! # Why this exists
//!
//! `queries::upsert_incidents` fills `affected_lines` for every incident it
//! writes, and `poller-incidents` re-sends the whole current feed every
//! cycle -- so any incident still *in* the feed is corrected within one
//! poll of the column being added. Incidents that have dropped out of the
//! feed are never written again, and those are precisely the rows the
//! incident archive exists to serve: at the time of writing, all 1507 rows
//! in production carry `affected_lines = '{}'` inherited from the
//! migration's default, and would stay that way forever.
//!
//! # Why a binary rather than SQL in the migration
//!
//! The line attribution is not derivable in SQL. It is
//! `common::matcher::lines_affected_by` -- substring matching each line's
//! `match_keywords`/`excluded_keywords` against the incident prose, gated
//! by the feed's structured operator list, with a cross-line post-filter
//! over the whole match set. That lives in Rust and reads the `lines/*.toml`
//! catalogue; a migration cannot call it. Same reasoning, and the same
//! shape, as `legacy_backfill`/`backfill_trains`.
//!
//! # Safety
//!
//! Idempotent and re-runnable. It only ever writes `affected_lines`, a
//! column nothing else owns, and only for rows whose recomputed value
//! actually differs from what is stored -- so a second run reports zero
//! updates. Running it again after a `lines/*.toml` change is the supported
//! way to propagate that change to archived incidents.

use anyhow::{Result, ensure};
use common::IncidentMessage;
use common::matcher::LineMatcher;
use sqlx::{PgPool, Row};

/// How many incidents are loaded, matched and written per round trip.
/// Large enough that a table measured in low thousands of rows finishes in
/// a handful of round trips, small enough that one batch's transaction
/// stays short.
const BATCH_SIZE: i64 = 500;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BackfillReport {
    /// Rows read and re-matched.
    pub rows_examined: u64,
    /// Rows whose stored `affected_lines` differed from the recomputed
    /// value and were therefore written.
    pub rows_updated: u64,
    /// Rows that, after recomputation, match no catalogue line at all.
    ///
    /// Not a failure: an incident can genuinely name an operator and a
    /// route this catalogue has no line for (freight, a TOC with no
    /// `lines/*.toml` entry, a station-closure notice with no line
    /// keyword). Reported separately so the operator can tell "the
    /// backfill did nothing" apart from "the backfill ran and these
    /// incidents really are unattributable."
    pub rows_matching_no_line: u64,
    /// Rows found with `affected_lines IS NULL` -- never computed at all,
    /// as opposed to computed-and-empty. On a first run this is every
    /// pre-existing row; on any later run it should be zero, since
    /// `upsert_incidents` always writes a (possibly empty) array.
    pub rows_never_computed: u64,
}

/// One incident's matcher inputs, read back out of the table.
///
/// Only the four fields `lines_affected_by` actually reads are loaded --
/// `summary`, `description`, `operators`, `affected_stations`. The rest of
/// `IncidentMessage` is filled with neutral values it never consults,
/// deliberately: reading `validity_periods` back would mean deserializing
/// JSONB written by every version of this app that ever ran, and one
/// unparseable archived row would abort the whole backfill over a field the
/// matcher does not look at.
struct StoredIncident {
    message: IncidentMessage,
    /// `None` when the column is SQL NULL -- "never computed" (see the
    /// migration), distinct from `Some(vec![])`, "computed, matched
    /// nothing". The write below must fire for the former even though the
    /// two filter identically.
    stored_lines: Option<Vec<String>>,
}

/// One row the walk decided needs writing, with the value it read so the
/// write can be a compare-and-swap against exactly that.
struct PendingWrite<'a> {
    incident_id: &'a str,
    recomputed: Vec<String>,
    /// What the walk read -- `None` for a never-computed row. Matched with
    /// `IS NOT DISTINCT FROM`, so a concurrent poller upsert makes the
    /// write decline rather than clobber.
    previous: &'a Option<Vec<String>>,
}

/// Recompute and persist `affected_lines` for every row in `incidents`.
///
/// Walks the table in `incident_id` order using a keyset cursor rather
/// than `OFFSET`, so a row written concurrently by the poller mid-walk
/// cannot shift the pages underneath us and cause a row to be skipped.
/// (A row the poller writes concurrently is written *with* its
/// `affected_lines`, so missing it would be harmless anyway -- the keyset
/// is belt-and-braces.)
pub async fn run_backfill(pool: &PgPool, matcher: &LineMatcher) -> Result<BackfillReport> {
    // Guarded here rather than only in the binary: an empty catalogue
    // matches nothing, so running with one would *clear* every row's
    // attribution instead of filling it. Every caller, tests included, is
    // protected by having the check live at this level.
    ensure!(
        matcher.line_count() > 0,
        "refusing to run the incident affected_lines backfill against an empty line catalogue: \
         it would clear every row's attribution rather than fill it"
    );

    let mut report = BackfillReport::default();
    let mut after: Option<String> = None;

    loop {
        let batch = load_batch(pool, after.as_deref()).await?;
        if batch.is_empty() {
            break;
        }

        after = Some(batch[batch.len() - 1].message.incident_id.clone());

        let mut changed: Vec<PendingWrite<'_>> = Vec::new();

        for stored in &batch {
            report.rows_examined += 1;
            if stored.stored_lines.is_none() {
                report.rows_never_computed += 1;
            }
            let recomputed = matcher.affected_line_ids(&stored.message);
            if recomputed.is_empty() {
                report.rows_matching_no_line += 1;
            }
            // `affected_line_ids` is sorted and deduped, and
            // `upsert_incidents` stores exactly what it returns, so a plain
            // equality check is a true "nothing to do" test, not an
            // ordering artefact. A stored NULL never compares equal, so a
            // never-computed row is always written, even to `'{}'`.
            if stored.stored_lines.as_ref() != Some(&recomputed) {
                changed.push(PendingWrite {
                    incident_id: stored.message.incident_id.as_str(),
                    recomputed,
                    previous: &stored.stored_lines,
                });
            }
        }

        // One statement per changed row, one transaction per batch.
        // Deliberately not a single set-returning `UPDATE ... FROM
        // (VALUES ...)`: this is a one-shot job over a table measured in
        // low thousands of rows, and a plainly-readable statement is worth
        // more here than shaving round trips off something that runs once.
        //
        // `IS NOT DISTINCT FROM` makes each write a NULL-safe
        // compare-and-swap against the value this walk actually read. The
        // poller can upsert a row between `load_batch` and here; without
        // the clause that fresher attribution would be overwritten by one
        // computed from the older text. With it the write simply does not
        // fire and `rows_updated` does not count it -- the poller's value
        // came from the same matcher over newer text, so it is the one to
        // keep.
        if !changed.is_empty() {
            let mut tx = pool.begin().await?;
            for write in &changed {
                let updated = sqlx::query(
                    "UPDATE incidents SET affected_lines = $2 \
                     WHERE incident_id = $1 AND affected_lines IS NOT DISTINCT FROM $3",
                )
                .bind(write.incident_id)
                .bind(&write.recomputed)
                .bind(write.previous.as_ref())
                .execute(&mut *tx)
                .await?
                .rows_affected();
                report.rows_updated += updated;
            }
            tx.commit().await?;
        }

        if (batch.len() as i64) < BATCH_SIZE {
            break;
        }
    }

    Ok(report)
}

async fn load_batch(pool: &PgPool, after: Option<&str>) -> Result<Vec<StoredIncident>> {
    let rows = sqlx::query(
        "SELECT incident_id, summary, description, operators, affected_stations, \
                affected_lines \
         FROM incidents \
         WHERE ($1::text IS NULL OR incident_id > $1) \
         ORDER BY incident_id \
         LIMIT $2",
    )
    .bind(after)
    .bind(BATCH_SIZE)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(StoredIncident {
                message: IncidentMessage {
                    incident_id: row.try_get("incident_id")?,
                    summary: row.try_get("summary")?,
                    description: row.try_get("description")?,
                    operators: row.try_get("operators")?,
                    affected_stations: row.try_get("affected_stations")?,
                    // Never read by `lines_affected_by` -- see StoredIncident.
                    priority: 0,
                    validity: vec![],
                    is_planned: false,
                    is_cleared: false,
                },
                stored_lines: row.try_get("affected_lines")?,
            })
        })
        .collect()
}
