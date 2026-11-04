//! `--regenerate-crs-tiploc-from-db`: the CORPUS rows the app has loaded
//! (`corpus_locations`, written by `schedule-ingest`'s CORPUS mode through
//! api), read from `DATABASE_URL`, so regenerating `crs-tiploc.csv` no
//! longer needs a hand-downloaded extract. Built only with `--features db`,
//! so the default build (what CI's fast tier runs) has no database client.
//! Read-only: a single `SELECT`.

use anyhow::Result;
use common::corpus_inference::CorpusRow;

/// Every `corpus_locations` row as inference input. Refuses an empty table
/// (no CORPUS loaded yet) rather than writing an empty CSV.
#[cfg(feature = "db")]
#[expect(
    clippy::items_after_statements,
    reason = "a local type or import sits next to its only use"
)]
pub(crate) async fn read_corpus_rows() -> Result<Vec<CorpusRow>> {
    use anyhow::Context;

    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .context("connecting to DATABASE_URL")?;
    type Row = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    // The effective rows (ds_store::corpus::crosswalk::CorpusLayers): with
    // an NLC XML snapshot loaded over a frozen JSON extract, every XML row
    // plus the JSON rows it does not cover (a TIPLOC it lacks, or, for a
    // row without a TIPLOC, an NLC it lacks); otherwise every row.
    let rows: Vec<Row> = sqlx::query_as(
        r"WITH xml AS (
              SELECT nlc, tiploc FROM corpus_locations
              WHERE source_file ~* '^nlc.*\.xml\.gz$'
          )
          SELECT c.nlc, c.stanox, c.tiploc, c.crs, c.nlc_desc FROM corpus_locations c
          WHERE NOT EXISTS (SELECT 1 FROM xml)
             OR c.source_file ~* '^nlc.*\.xml\.gz$'
             OR (c.tiploc IS NOT NULL
                 AND NOT EXISTS (SELECT 1 FROM xml WHERE xml.tiploc = c.tiploc))
             OR (c.tiploc IS NULL
                 AND NOT EXISTS (SELECT 1 FROM xml WHERE xml.nlc = c.nlc))
          ORDER BY c.id",
    )
    .fetch_all(&pool)
    .await
    .context("reading corpus_locations")?;
    anyhow::ensure!(
        !rows.is_empty(),
        "corpus_locations is empty: no CORPUS delivery has been loaded into this database"
    );
    Ok(rows
        .into_iter()
        .map(|(nlc, stanox, tiploc, crs, nlc_desc)| CorpusRow {
            nlc: Some(nlc),
            stanox,
            tiploc,
            crs,
            nlc_desc,
        })
        .collect())
}

#[cfg(not(feature = "db"))]
#[expect(
    clippy::unused_async,
    reason = "matches the async signature of the `db` build, which main awaits"
)]
pub(crate) async fn read_corpus_rows() -> Result<Vec<CorpusRow>> {
    anyhow::bail!(
        "--regenerate-crs-tiploc-from-db needs a build with the `db` feature: \
         cargo run -p line-catalogue-validator --features db -- --regenerate-crs-tiploc-from-db ..."
    )
}
