# `crs-tiploc.csv` / `toc-codes.csv` provenance

These two files are the **fast-tier, no-secrets-required** source of truth
used by `crates/line-catalogue-validator` to check every `crs`, `tiploc`
and `operators` value in `lines/*.toml` against real, currently-valid
external data. See that crate's `src/main.rs` module doc for how they're
used; this file is the extraction methodology, mirroring
`reference-data/stanox-crs.md`'s existing precedent for documenting a
vendored snapshot's provenance next to the data itself.

## Why a vendored snapshot, not this app's own CIF-derived `stanox_crs`

This repo already has a CIF-derived reference table two ways:
`reference-data/stanox-crs.csv` (checked-in, STANOX->CRS only, no TIPLOC
column) and the live `stanox_crs` Postgres table `crates/schedule-reference`
populates daily from the real CIF SCHEDULE feed `schedule-ingest` receives
over SFTP from Network Rail Data Feeds. Both are genuinely *external*
ground truth relative to this repo's own line-catalogue authoring (neither
is derived from `lines/*.toml` itself), so using either would not be
circular in principle. In practice, neither fits this validator's fast,
no-secrets, CI-blocking tier:

- `stanox-crs.csv` has no `tiploc` column at all (see its own provenance
  doc's "File format" section -- STANOX is the join key, not TIPLOC), so it
  cannot validate a `[[stations]]` entry's `crs`/`tiploc` *pair*, only a
  bare CRS's plausibility via a STANOX side-channel this validator doesn't
  need.
- The live `stanox_crs` table requires a running Postgres populated by a
  live `schedule-reference` process fed by Network Rail's real CIF feed --
  i.e. a database and Network Rail Data Feeds credentials, neither of which
  a "runs on every PR, no secrets, seconds not minutes" check can assume.

So this validator instead vendors its own snapshot, independently sourced
from a different, genuinely third-party site: **railwaycodes.org.uk**
(the "Railway Codes and other data" reference site maintained by Nigel
Bryan/Paul Smith, widely cited across the openraildata community and by
tools like PyRCS). This is deliberately a *different* source from this
app's own CIF pipeline -- if this app's own catalogue-authoring process and
its own CIF ingestion both happened to share one upstream data quirk, a
single-source check could rubber-stamp a shared mistake. Cross-checking
against an independently-curated site closes that gap, at the cost of that
site itself not being a primary-issuing authority (see "Known limitations"
below).

## Where the data comes from

### `crs-tiploc.csv`

Scraped from `https://www.railwaycodes.org.uk/crs/crs<a-z>.shtm` (26 pages,
one per initial letter of location name), each an HTML table with columns
`Location, CRS, NLC, TIPLOC, STANME, STANOX`. For every row that carries at
least one 3-letter CRS token:

- Every whitespace-separated token in the CRS cell matching `[A-Z]{3}` is
  taken as a CRS this location answers to (a handful of locations carry
  more than one, e.g. historical/superseded codes).
- Every whitespace-separated token in the TIPLOC cell matching `[A-Z0-9]{2,7}`
  is taken as a TIPLOC for that location (a handful of locations, mostly
  Underground/DLR interchanges, carry more than one).
- The final CSV has one row per `(crs, tiploc)` pair actually observed
  (multiple rows share a `crs` where a location has several TIPLOCs, e.g.
  different platform faces); a `crs` with no TIPLOC recorded anywhere in
  the source table gets one row with an empty `tiploc` cell, so its CRS is
  still known-valid even though no TIPLOC pairing can be verified for it.
- `name` is the location name from the first source row that carried this
  `crs` -- informational only (used in this validator's error messages and
  its coverage-gap report), never compared programmatically against a
  station's own inline comment (see "What this deliberately does not
  check" below).

Result: 4,545 distinct CRS codes, 5,305 `(crs, tiploc)` rows, generated
2026-09-21. Every one of the 1,563 distinct CRS codes actually used across
this repo's `lines/*.toml` today was found in this set with zero misses,
confirmed by a one-off cross-check before this file was committed.

### `toc-codes.csv`

Scraped from `https://www.railwaycodes.org.uk/operators/toccodes.shtm`, a
single HTML table of every ATOC/timetable operator code ever issued, each
row giving a code, an operator name, and a date range. Only rows whose date
range column contains the literal string `"to date"` (i.e. still current,
per that page's own convention for an open-ended range) are kept -- this
excludes retired codes like `AN` (Arriva Trains Northern, superseded by
`NT`) or the erroneous historical `ATW` entry, which is exactly the
"currently-valid" list this validator needs (see `lines/SCHEMA.md`'s
curation rule: "Old codes shouldn't silently match new operators").

Result: 33 currently-valid ATOC codes, generated 2026-09-21. All 21 codes
actually used across `lines/*.toml` today are present.

## Known limitations (documented, not silently papered over)

- **Not an official/primary-issuing source.** railwaycodes.org.uk is a
  well-regarded, actively-maintained community reference (independently
  cross-referenced by the Open Rail Data wiki and the PyRCS Python
  package), not ATOC/RSSB/ORR itself. The genuinely authoritative sources
  -- Network Rail's CORPUS/SMART reference data, and RDM's own TOC List
  feed (the same one `crates/poller-tocs` already consumes) -- both need a
  registered account/API key. See `.github/workflows/validate-line-catalogue.yml`
  for the live, credentialed tier that checks against the real thing, kept
  disabled until those credentials exist as repo secrets.
- **A CRS/TIPLOC pair being "real" doesn't mean it's the *right* station
  for a given line.** This snapshot can confirm `WWA` really is a live,
  bookable CRS (it is -- Woolwich Arsenal) but cannot tell you a
  particular `lines/*.toml` entry meant to reference a *different* station
  entirely. That class of bug (the one this session's manual audit mostly
  found, e.g. `WNE` used where `WDM`/Windermere was meant) is only caught
  by this validator when the code used doesn't exist at all, or when it
  exists but pairs with a different TIPLOC than the one the file also
  states -- not when the wrong-but-real code was written with no `tiploc`
  alongside it to contradict it. A human-legible inline comment naming the
  intended station remains the real safety net for that class of mistake.
- **TIPLOC completeness is uneven for multi-TIPLOC stations.** Large
  stations with several platform-specific TIPLOCs (e.g. Clapham Junction:
  `CLPHMJN`, `CLPHMJ1`, `CLPHMJ2`, `CLPHMJC`, ...) often have the CRS
  recorded on only *one* of those rows (the "main" one), with the
  platform-specific TIPLOCs listed as separate rows carrying no CRS of
  their own on this site. That means this snapshot's `crs-tiploc.csv`
  under-counts legitimate `(crs, tiploc)` pairs for those stations --
  confirmed by spot-checking two real, currently-documented
  `lines/*.toml` entries (`CLJ`/`CLPHMJ1` in three SWR files, `BSK`/`BSNGSEB`
  in `swr-south-west-main.toml`) that are legitimate real TIPLOCs but don't
  appear against their CRS in this file. Because of this, and because
  `tiploc` is explicitly documentation-only and non-load-bearing per
  `lines/SCHEMA.md`, **a CRS/TIPLOC mismatch is reported as a non-blocking
  warning, not a hard CI failure** -- only an unknown CRS or an unknown
  operator code fails the build. See `crates/line-catalogue-validator`'s
  own doc comment for where this is enforced in code.

## What this deliberately does not check

An inline `lines/*.toml` comment naming a station (e.g. `# Prestwick
International Airport`) is not fuzzy-matched against this file's `name`
column. TOML comments aren't retained by `common::LineDefinition`'s
`toml`-crate-based parsing, and re-deriving a reliable comment-to-station
association from raw file text (a comment can precede, follow, or share a
line with its station, or describe something else entirely -- segment
history, sourcing notes) for a genuinely fuzzy text match was judged not
worth the false-positive risk for what the task calls out as a bonus, not
a requirement. If a future pass wants this, the CRS's canonical `name`
here is already available to compare against.

## Regenerating this snapshot

There is no automated regeneration script yet (see
`.github/workflows/validate-line-catalogue.yml`'s live-check job, which is
the intended eventual replacement for "someone re-runs a scraper by hand").
To refresh by hand: re-fetch each
`https://www.railwaycodes.org.uk/crs/crs<a-z>.shtm` page and
`https://www.railwaycodes.org.uk/operators/toccodes.shtm`, and re-apply the
extraction rules above (send a real `User-Agent` header -- the site 403s
the default `curl`/no-UA request). Keep both CSVs sorted by their first
column for a reviewable diff, matching `stanox-crs.csv`'s own convention.
