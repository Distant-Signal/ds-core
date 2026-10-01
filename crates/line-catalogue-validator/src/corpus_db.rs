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
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT nlc, stanox, tiploc, crs, nlc_desc FROM corpus_locations ORDER BY id",
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
