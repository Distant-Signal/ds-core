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

So this validator instead vendors its own snapshots, taken from sources
independent of this app's CIF pipeline. Since 2026-09-28 both are primary
Network Rail / National Rail data: `crs-tiploc.csv` from Network Rail's
**CORPUS** extract and `toc-codes.csv` from the Knowledgebase TOC List
(DQ13). CORPUS is not the CIF feed (and not the CIF-derived tables above,
whose licence hasn't been reviewed yet), so if this app's catalogue
authoring and its CIF ingestion shared one upstream quirk, the check would
still not rubber-stamp it. Before that, `crs-tiploc.csv` was scraped from
**railwaycodes.org.uk** (the "Railway Codes and other data" community
reference site); see "Before 2026-09-28" below. The `--live` tier still
scrapes that site's CRS pages (see "The live tier").

## Where the data comes from

### `crs-tiploc.csv`

**Source: Network Rail CORPUS** (Codes for Operations, Retail & Planning
-- a Unified Solution), the Rail Data Marketplace "NWR CORPUS" product,
licensed under the [Open Government Licence
v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
and credited on `/attribution`
(`frontend/components/OpenDataAttribution.tsx`, entry
`network-rail-corpus`). Generated 2026-09-28 from `CORPUSExtract.json`
(55,972 `TIPLOCDATA` rows; the file carries no extract date of its own --
its gzip header timestamp is 2026-09-07 08:16 UTC, and it was downloaded
2026-09-28; SHA-256 of the gzip
`0acc1c01790a1d8ec6d83aa2d330a38b06bbf11c7eec42c379e357ff2bf28de8`,
decompressed
`0cc7717f2c85ce6059e80ca764325ef00e75d094aa771a54e768a1988108f9da`).
The extract itself is not committed. With:

```text
cargo run -p line-catalogue-validator -- \
  --regenerate-crs-tiploc-from-corpus CORPUSExtract.json \
  --compare-with reference-data/crs-tiploc.csv --report report.txt
```

The rules are in "Regenerating this snapshot" below. Result: 4,116
distinct CRS codes, 4,162 rows (3,724 direct `(crs, tiploc)` pairs, 46
inferred station-part pairs, 392 CRS codes with no TIPLOC). `name` is now
CORPUS's upper-case `NLCDESC` (e.g. `CLAPHAM JUNCTION LONDON`). Every one of
the 2,446 distinct CRS codes used across the 243 `lines/*.toml` files on
2026-09-28 is in it, and so is every one of their 534 stated `(crs,
tiploc)` pairs: the validator reports **0 errors and 0 warnings** (the
railwaycodes file gave 0 errors and 6 warnings).

#### Compared with the railwaycodes.org.uk snapshot it replaced

3,704 pairs match. 66 pairs are new: 28 direct (mostly CRS codes CORPUS
now attaches to a TIPLOC railwaycodes lacked, e.g. `ZTU`/`TRNHMGN`,
`WEH`/`WHAMHL`) and 38 inferred platform TIPLOCs (Clapham Junction,
Victoria, Vauxhall, Waterloo, Salisbury bays...) -- among them
`CLJ`/`CLPHMJ1` and `BSK`/`BSNGSEB`, which `lines/*.toml` uses and
railwaycodes lacked (the six warnings before). Eight more inferred pairs
were already in the old file, including `WIM`/`WDON` and `LBG`/`LNDNBDC`,
which `lines/*.toml` also uses and which CORPUS describes only as `SOUTH
WEST` and `CENTRAL`.

927 pairs and 432 CRS codes are only in the old file. None is used by any
`lines/*.toml` file (0 errors, 0 warnings above). They are:

- **181 `X`/`Z`/`Q`-prefixed codes** -- freight and engineering
  pseudo-codes, London Underground/DLR/Overground interchange codes and
  bus/ferry codes -- that CORPUS no longer carries or never carried;
- **~50 London 2012 Olympic "For The Games" codes** (`EUX`, `KGZ`, `TGE`,
  `WEG`, ...) and a few similar event codes (`ALZ` Altrincham For Old
  Trafford);
- **provisional Crossrail codes** since replaced (`FAC`->`FDX`,
  `PAA`->`PDX`, `LIX`->`LSX`, `WCC`->`WHX`, `WOW`->`WWC`, `CWF`->`CWX`),
  and superseded aliases (`SRP` St Pancras domestic, `CJN` Clapham
  Junction Platform 2);
- **closed stations and lines** (Addiscombe, Angel Road, Bicester Town,
  North Woolwich, Folkestone Harbour, Heacham, Waterloo International...);
- **non-National-Rail systems**: Tyne & Wear Metro (Jarrow, Tynemouth,
  Whitley Bay, Gateshead...), Manchester Metrolink (Abraham Moss),
  Irish/continental ports and stations (Dublin, Larne, Rosslare,
  Eindhoven, Utrecht);
- **freight sidings, quarries, junctions and bus stops** that once had a
  code (Tonbridge Jubilee Sidings, Kaimes Quarry, Gascoigne Wood Jn,
  Talbot Green Bus Stop...).

Of the 927 pairs, 626 name a TIPLOC CORPUS does not have at all (209 of
them `CATZ...` placeholder TIPLOCs), 397 belong to CRS codes CORPUS does
not carry, and 117 TIPLOCs are in both files with a different CRS set --
almost all a TIPLOC railwaycodes listed under an old or alternative code as
well as its current one (`STPX`: `SRP`,`STP` -> `STP`; `FRNDNLT`:
`FAR`,`ZFD` -> `ZFD`), plus a few where CORPUS gives a different code
(`CATZ009`: `OLV` -> `LMN`, `MINFFR`: `MFD` -> `MFF`).

#### Before 2026-09-28: railwaycodes.org.uk

From 2026-09-21 to 2026-09-28 this file was scraped from
`https://www.railwaycodes.org.uk/crs/crs<a-z>.shtm` (26 pages, one per
initial letter of location name), each an HTML table with columns
`Location, CRS, NLC, TIPLOC, STANME, STANOX`. The `--live` tier still
reads those pages with the same rules. For every row that carries at
least one 3-letter CRS token:

- **Every footnote is deleted from the cell before any token is read out of
  it.** The site annotates individual codes (and location names) with a
  click-to-open note marked up as a self-contained three-deep span inside
  the same `<td>` as the real code:

  ```html
  <td>ABWD
  ABBEYWD<span class="popup" onclick="popup26()"><span class="popuptext"
    id="myPopup26"><span class="close">&#x2716;</span>Original code</span></span>
  </td>
  ```

  The note is free English prose. Merely stripping HTML tags leaves it
  behind, and uppercasing what remains turns ordinary words into things
  that pass the token filters below. This step is **not optional** -- see
  "A real bug this rule exists to prevent" at the end of this section.
- Every whitespace-separated token in the CRS cell matching `[A-Z]{3}` is
  taken as a CRS this location answers to (a handful of locations carry
  more than one, e.g. historical/superseded codes).
- Every whitespace-separated token in the TIPLOC cell matching `[A-Z0-9]{2,7}`
  is taken as a TIPLOC for that location (a handful of locations, mostly
  Underground/DLR interchanges, carry more than one).
- The final CSV had one row per `(crs, tiploc)` pair actually observed; a
  `crs` with no TIPLOC recorded anywhere got one row with an empty
  `tiploc` cell; `name` was the location name from the first source row
  that carried this `crs`.

That snapshot had 4,544 distinct CRS codes and 4,965 rows (generated
2026-09-21, corrected 2026-09-24 as below).

##### A real bug this rule exists to prevent

The first generation of this file (2026-09-21) stripped HTML tags but
**not** the footnote spans described above, so 340 of its 5,305 rows were
English prose from those notes, uppercased and mistaken for codes. Verified
by re-fetching all 26 pages on 2026-09-24 and re-applying the rules above
with footnotes stripped: the corrected extraction is a strict *subset* of
what was committed (4,965 rows, zero rows missing, 340 spurious), so the
error was purely additive -- no real pairing was ever lost. What the notes
produced:

- `"Original code"` / `"Earlier code"` / `"Later code"` and the other
  ~90 `Code ...` notes -> a TIPLOC token `CODE`, attached to **242**
  different CRS codes (Abbey Wood, Ashford (Kent), Blackfriars,
  Bournemouth, Clapham Junction, Vauxhall, ...).
- `"See <a>CRS explanation</a>"`, which every high-level/low-level station
  row carries -> CRS tokens `SEE` **and** `CRS`. Both are real codes
  (Southease and Carstairs), so each silently collected 27 TIPLOCs
  belonging to entirely unrelated stations (`GLGC`, `STPANCI`, `WLSD`, ...)
  and had its `name` overwritten with the note row's location.
- `"Code not certain; conflicting raw data"` (the Muck row) -> a CRS `RAW`
  that **has never been issued to anything**, plus a bogus TIPLOC `MUCK` on
  `NOT` (Nottingham) whose `name` became `Muck`. A fabricated CRS in this
  file is the worst of the three: `known_crs` is this validator's only
  *hard* failure, so a `lines/*.toml` typo of `RAW` would have passed CI.
- Notes on the *Location* cell (`"Believed to refer to track junction, not
  station"`, `"Listed as Scotrail Cardiff"`, the four Tonbridge Jubilee
  Siding notes) -> 10 `name` values carrying the note's `✖` close-button
  glyph and its text.

`crates/line-catalogue-validator/src/reference.rs`'s `strip_popups`
implements the footnote-stripping rule for the live tier, and two tests
there (`popup_footnotes_are_never_scraped_as_codes`,
`vendored_crs_tiploc_snapshot_carries_no_footnote_artifacts`) pin both the
parser and the committed snapshot against a recurrence.

### `toc-codes.csv`

**Source: the National Rail Knowledgebase Train Operating Company List**
(the RDM feed, RSPS5050 P-03-00 Rev A §3, that `crates/poller-tocs` polls
daily into production's `tocs` table). Regenerated 2026-09-27 (DQ13) from
production's `tocs` table, fetched from the feed at 2026-09-27 05:13 UTC,
with:

```sh
psql ... -c "\copy (SELECT atoc_code, name FROM tocs ORDER BY atoc_code) \
  TO STDOUT WITH (FORMAT csv, HEADER)" > reference-data/toc-codes.csv
```

which is byte-for-byte what `cargo run -p line-catalogue-validator --
--regenerate-toc-codes-from-rdm-xml <saved feed response.xml>` writes from
the feed's own XML (header `atoc_code,name`, one row per operator, sorted
by code, LF line endings) -- use whichever input is to hand.

Every operator in the feed is kept, with its Knowledgebase display name.
That is the list this app itself knows operators by, so "valid" here now
means "an operator code production recognises", not railwaycodes.org.uk's
"date range ends `to date`". Result: 40 codes. Compared with the previous
railwaycodes-derived file (33 codes), it adds `HS`, `HV`, `LN`, `LT`, `NR`,
`SX`, `WM`, `XP`, `XS`, `ZN` (Knowledgebase lists non-TOC operators such as
Network Rail, airports and Hovertravel, and splits `LN`/`WM` out of `LM`)
and drops `LF` (Grand Union), `NY` (North Yorkshire Moors Railway) and `WR`
(West Coast Railways), which the Knowledgebase list doesn't carry. None of
the 24 codes used across `lines/*.toml` on 2026-09-27 is affected; the
validator passes with zero errors.

Before 2026-09-27 this file was scraped from
`https://www.railwaycodes.org.uk/operators/toccodes.shtm` (rows whose date
range read `to date`).

## Decision (2026-09-28): conservative CORPUS inference

The user accepted the conservative inference rule for `crs-tiploc.csv`:
a CRS-less TIPLOC takes a station's CRS only if it shares the station's
STANOX and its CORPUS description either names the station followed only by
platform-ish words, or consists only of platform words and also shares the
station's NLC location (rules and measurements in "Regenerating this
snapshot" below). It was preferred over the NLC-group / STANOX-group
alternatives because those gave station codes to signals, junctions,
sidings and freight terminals (4,613 inferences for NLC-then-STANOX, 132
for "NLC and STANOX agree", still mostly junctions and signals), and a
wrong `(crs, tiploc)` pair silently vouches for a wrong catalogue entry,
whereas a missing one only produces a non-blocking warning.

Accepted costs:

- **Fewer inferences than expected**: 46, against roughly 150 genuine
  multi-TIPLOC stations estimated beforehand.
- **Known misses**: a platform TIPLOC with its own STANOX is never
  inferred, e.g. `WATR` "WATERLOO SUBURBAN" and `OXTEDBY` "OXTED BAY". If a
  line ever uses one, the validator gives a non-blocking tiploc-pairing
  warning.
- **Debatable inclusions**: `AVIGVIL` "AVIGNON VILLE" (under Avignon),
  `DINGMLC` "DINGWALL MIDDLE" (a level crossing whose description doesn't
  say so), `MINFFR` (the Ffestiniog Railway's Minffordd, under the Network
  Rail station).
- **Codes dropped with the railwaycodes.org.uk snapshot**: 927 pairs and
  432 CRS codes (pseudo, Olympic, superseded Crossrail and closed-station
  codes, see above). No line uses them today, but a future line using a
  pseudo or closed code now fails validation (an unknown CRS is a hard
  error).
- **Dependence on CORPUS wording**: the rule relies on how CORPUS words
  `NLCDESC` and on the word lists in
  `crates/line-catalogue-validator/src/regenerate.rs`. They may need
  maintenance when CORPUS changes; read the regeneration report's inferred
  pairs and "left out by name" list on every regeneration.
- **Point-in-time snapshot**: the file reflects the extract downloaded on
  2026-09-28 and goes stale until it is regenerated. That stays manual until
  the planned SFTP CORPUS ingest lands.

## The live tier (`--live`)

The weekly `--live` run (`.github/workflows/validate-line-catalogue.yml`,
schedule still disabled) checks the same three things as the fast tier
against fresher data:

| Check | Live source | Credential needed |
|---|---|---|
| operator codes | Knowledgebase TOC List feed (`RDM_API_KEY` + `RDM_TOCS_BASE_URL`), else the vendored `toc-codes.csv` | only for the live feed |
| CRS exists | railwaycodes.org.uk `crs<a-z>.shtm` (26 GETs, honest `User-Agent`) | none |
| CRS/TIPLOC pairing | same pages | none |

Since 2026-09-27 the live tier no longer scrapes railwaycodes.org.uk's
operator-codes page. Without RDM credentials it uses the vendored
Knowledgebase snapshot instead, which is the list production itself
recognises and so more authoritative than the community page was.

The CRS pages are the one remaining railwaycodes.org.uk fetch, because
nothing the project already has rights to covers a CRS/TIPLOC check
without a new credential:

- the Knowledgebase Stations feed (`poller-stations`) needs `RDM_API_KEY`
  and an account-specific base URL;
- Network Rail's CORPUS extract needs a registered Rail Data Marketplace
  account (the fast tier's snapshot is regenerated from one downloaded by
  hand, but CI has no credential to fetch a fresh one);
- this app's own `GET /public/stanox-crs` (CIF-derived, has `crs` and
  `tiploc`) would work as a source, but production's API has no public
  ingress today (it is reachable only on the tailnet), and its data comes
  from the CIF feed, whose licence review (LEG-22) is still open.

**This scrape is the only reason `/attribution` credits
railwaycodes.org.uk** (the `railwaycodes` entry in
`frontend/components/OpenDataAttribution.tsx`); no committed file comes
from the site any more. Remove the credit when this scrape is removed (and
the other way round). The validator's unit test
`railwaycodes_credit_exists_exactly_while_the_live_tier_scrapes_it`
(`crates/line-catalogue-validator/src/reference.rs`) fails while one exists
without the other.

## Known limitations (documented, not silently papered over)

- **The two tiers use different CRS/TIPLOC sources.** The fast tier's
  `crs-tiploc.csv` is Network Rail CORPUS (primary data); the `--live`
  tier still scrapes railwaycodes.org.uk, a well-regarded community
  reference (cross-referenced by the Open Rail Data wiki and the PyRCS
  Python package) but not an issuing authority. They disagree at the
  edges: railwaycodes keeps closed stations, Olympic-era and superseded
  codes that CORPUS has dropped (see "Compared with the railwaycodes.org.uk
  snapshot" above), so a code can pass `--live` and fail the fast tier.
  The fast tier is the stricter one and is what CI runs.
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
- **TIPLOC completeness is uneven for multi-TIPLOC stations.** CORPUS
  records the CRS only on a station's primary TIPLOC. The generator
  recovers platform TIPLOCs only when CORPUS describes them recognisably
  (see the rules in "Regenerating this snapshot"), and deliberately errs
  towards leaving a TIPLOC out rather than giving a station's CRS to a
  signal or junction at its throat. So a legitimate platform TIPLOC can
  still be missing (e.g. `WATR` "WATERLOO SUBURBAN" and `OXTEDBY` "OXTED
  BAY", which have their own STANOX). Because of this, and because
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

### From Network Rail / Knowledgebase data (preferred, DQ13)

`crates/line-catalogue-validator` has two regeneration modes (see its
`src/regenerate.rs`); each writes into `--reference-dir` (default
`reference-data`) and exits without validating:

- `--regenerate-toc-codes-from-rdm-xml <file>`: a saved response from the
  Knowledgebase Train Operating Company List feed (the one `poller-tocs`
  uses; needs an RDM subscription and API key). Production's `tocs` table
  is the same data -- see the `toc-codes.csv` section above.
- `--regenerate-crs-tiploc-from-corpus <CORPUSExtract.json>`: Network
  Rail's CORPUS extract, decompressed (the Rail Data Marketplace "NWR
  CORPUS" product, OGL v3; needs a registered account to download). Do not
  commit the extract. CORPUS fills `3ALPHA` (the CRS) only on a station's
  *primary* TIPLOC, so a station's other TIPLOCs (platform groups, bays)
  carry no CRS of their own. Every valid `TIPLOC` (2-7 uppercase
  letters/digits) therefore gets its CRS from the first of these rules
  that applies:

  1. **Direct**: one of its own rows has a `3ALPHA` that is a 3-letter
     CRS. Every such CRS is kept and no inference is attempted for that
     TIPLOC.
  2. **Station part**: otherwise, the candidate stations for each of its
     rows are the `3ALPHA` rows with the **same STANOX** (blank and
     all-zero STANOX never match), except ones whose own description marks
     them as a pseudo-station (`SIDINGS`, `YARD`, `DEPOT`, `CARRIAGE`,
     `LOOP`, ... -- e.g. `XCP` "BR CARRIAGE SIDINGS", which shares Clapham
     Junction's STANOX). A candidate counts if the row's description
     either
     - **names it** (rule "station part by name"): starts with the
       station's name -- its description, less a trailing `LONDON` and
       with `JN`/`JCN`/`SIG`/`SDG`... normalised -- optionally after a
       leading `LONDON`, and continues only with platform-ish words: no
       signal, junction, sidings, depot, loop, yard, crossover, level
       crossing, ground frame, freight, staff, bus-stop or similar word,
       and no signal number (a word of 3+ characters containing a digit).
       `CLAPHAM JN (WINDSOR)` and `VICTORIA PLAT 10 (TPS USE)` count;
       `CLAPHAM JN SIGNAL TVC147`, `CLAPHAM JUNCTION LOOP`, `WIMBLEDON
       SIGNAL VC827` and `STRATFORD CENTRAL JUNCTION` (all at their
       station's STANOX) do not; or
     - **qualifies it** (rule "station part by platform words"): consists
       only of platform words (`CENTRAL`, `EASTERN`, `SOUTH WEST`, `NO 4
       BAY PLATFORM`, `DOWN BAY`) *and* the row also shares the station's
       4-digit NLC location (the first four of the NLC's six digits; a
       numeric NLC is left-padded first). This is how London Bridge's
       `LNDNBDC` "CENTRAL" and Wimbledon's `WDON` "SOUTH WEST" get their
       CRS.

     Exactly one distinct CRS over all its rows: it gets that one.
     Several: left out and reported as ambiguous.
  3. Otherwise it is left out.

  Candidates come only from rows with their own `3ALPHA` (including rows
  with no usable TIPLOC), never from another inference, so the result does
  not depend on row order. The output format is unchanged: one row per
  `(crs, tiploc)` (the validator already accepts several TIPLOCs per CRS),
  an empty `tiploc` only for a CRS that ends up with no TIPLOC, `name` from
  the `NLCDESC` of the alphabetically-first *directly* paired TIPLOC (so an
  inferred platform row never renames a station), sorted by `crs,tiploc`,
  LF line endings. Which rule produced each pair is not in the CSV; it is
  in the report.

  **Why these rules** (measured on the 2026-09-28 extract; counts are
  CRS-less TIPLOCs given a CRS):

  | Rule | Inferred | Verdict |
  |---|---|---|
  | only CRS in the 4-digit NLC group, else only CRS at the STANOX (first version) | 4,613 (4,517 + 96) | rejected: overwhelmingly signals, junctions, sidings, depots and freight terminals sharing a station's NLC prefix (`BLTCHWJ` "BLETCHLEY WEST JN" -> `BLU`, `LEVE587` "LEVEN SIGNAL ETL587" -> `LEV`, `BRNSSDG` "BARONS COURT LAY BY SIDING" -> `ZBQ`) |
  | NLC and STANOX groups each have exactly one CRS and agree | 132 | rejected: still mostly junctions and signals at a station's STANOX (`STFDCJ`, `WIMB827`), and misses Clapham Junction entirely because a pseudo-CRS carriage siding shares its NLC group and STANOX |
  | NLC group + station name + non-station words excluded | 567 | rejected: NLC prefixes span freight terminals and quarries named after the town (`AVONCOL` "AVONMOUTH COAL SILO (COLAS)", `THEAHFH` "THEALE HANSON AGGS") |
  | same STANOX *and* NLC + station name + exclusions | 38 | good, but a strict subset of the next row: misses `STPADOM` "ST PANCRAS INTL (DOMESTIC)" and `FNTLSR` "FARRINGDON", whose NLC differs from their station's |
  | **same STANOX + station name + exclusions** (chosen, "by name") | 40 | every one a platform group, bay or alternative name of the station, e.g. Clapham Junction's `CLPHMJ1`/`C`/`M`/`W`, Victoria's `VICT9`-`VICT19`, Vauxhall and Waterloo main/Windsor sides |
  | **plus platform words only, same STANOX and NLC** (chosen, "by platform words") | +6 | `LNDNBDC`, `LNDNBDE`, `WDON`, `PERTH3P`, `PRSTN4B`, `SOTONB` |

  Rows two to four were measured with a prototype of the same word lists,
  so their counts are approximate. The STANOX fallback of the first
  version is gone: STANOX alone is not
  evidence (signals and junctions at a station's throat often share its
  STANOX), and it only contributes now together with a matching name or an
  NLC match. A handful of the 46 chosen inferences are debatable but
  harmless (`AVIGVIL` "AVIGNON VILLE" under Avignon, `DINGMLC` "DINGWALL
  MIDDLE" -- a level crossing whose description omits it, `MINFFR` the
  Ffestiniog Railway's Minffordd under the NR station); none is a signal,
  junction or siding.

  The report goes to stderr: per-rule pair counts, every inferred pair with
  its description and rule, ambiguous STANOX groups with their candidate
  CRS codes and TIPLOCs, and counts of TIPLOCs left out -- by name (at a
  station's STANOX but not named as part of it: 189 in the 2026-09-28
  extract, almost all signals, junctions, sidings and level crossings) and
  with no station at their STANOX. `--report <path>` writes the full report
  to a file instead (adding the "left out by name" list) and leaves a
  count-only summary on stderr. `--compare-with <crs-tiploc.csv>` adds
  agreement with an existing file: pairs matched, pairs only in the old
  file (still missing), pairs only in the new output, conflicts (a TIPLOC
  in both files with a different CRS set), CRS codes only on one side, and
  matched / only-new / conflict counts per rule; the `--report` file also
  lists every differing pair. The comparison file is read before anything
  is written, but to judge the rules without touching the committed file,
  point `--reference-dir` somewhere else:

  ```text
  cargo run -p line-catalogue-validator -- \
    --regenerate-crs-tiploc-from-corpus ~/CORPUSExtract.json \
    --reference-dir ~/crs-trial \
    --compare-with reference-data/crs-tiploc.csv \
    --report ~/crs-trial/report.txt
  ```

  Read the inferred pairs, the conflicts and any ambiguous groups before
  committing. Then run `cargo run -p line-catalogue-validator` (it must
  report 0 errors: CORPUS drops closed and superseded codes, so a newer
  extract can remove a CRS a line still uses) and `cargo test -p
  line-catalogue-validator`, and update the `crs-tiploc.csv` section above.

### `crs-tiploc.csv` from railwaycodes.org.uk (superseded 2026-09-28)

Kept for the live tier, which still applies these rules, and in case the
fast tier ever has to fall back to it. There is no automated regeneration
script. To refresh by hand: re-fetch each
`https://www.railwaycodes.org.uk/crs/crs<a-z>.shtm` page and re-apply the
extraction rules above (send an honest, identifying `User-Agent` header,
the same one `reference.rs`'s `USER_AGENT` sends -- the site 403s the
default no-UA request; never impersonate a browser). Keep the CSV sorted
by its first column for a reviewable diff, matching `stanox-crs.csv`'s own
convention.

**Delete the footnote spans from every cell before extracting any token**
-- the single rule the 2026-09-21 generation got wrong, and the one thing
a by-hand refresh is most likely to get wrong again. The check that catches
it: no real TIPLOC is shared by more than a handful of CRS codes, so if any
TIPLOC token in the output is claimed by dozens of them, a footnote has
been scraped as a code. `cargo test -p line-catalogue-validator` asserts
exactly that against the committed file
(`vendored_crs_tiploc_snapshot_carries_no_footnote_artifacts`), so a bad
regeneration fails CI rather than shipping.
