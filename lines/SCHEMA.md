# Line Definition Schema

Each file under `lines/` defines one National Rail "line" — the user-facing
unit a status will be reported against. Files are TOML, one line per file,
named `<id>.toml`.

## Fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `id` | string | yes | Stable, lowercase, hyphenated. Used in URLs. Never change once published. |
| `name` | string | yes | Display name. Change freely. |
| `mode` | string | yes | Always `national-rail` for now. Reserved for future use. |
| `category` | string | yes | One of `main-line`, `commuter`, `regional`, `operator`. Descriptive only: shown on the line's page and returned by the lines API; no status logic branches on it. (`custom` is reserved for user-created custom lines.) |
| `operators` | list[string] | yes | ATOC codes of TOCs whose services define this line. |
| `stations` | list[Station] | yes | Ordered list of CRS codes from one end to the other. |
| `sample_stations` | list[string] | no | CRS codes to poll for LDBWS sampling. Each must be one of this line's own `[[stations]]` (enforced by `line-catalogue-validator`). |
| `match_keywords` | list[string] | no | Free-text keywords for matching Knowledgebase incidents. |
| `excluded_keywords` | list[string] | no | Vetoes a Knowledgebase incident match. |
| `severity_overrides` | dict | no | Per-line threshold overrides. |
| `full_coverage_enabled` | bool | no | Opts the line into full-coverage (TRUST-vs-schedule) statistics. Defaults to `false`; the chart's `aggregator.fullCoverageEnabledDefault`/`api.fullCoverageEnabledDefault` (both `true` by default) enable it for every line regardless. |
| `destination_crs_filter` | list[string] | no | When inferring from LDBWS, only count services whose `destination_crs` is in this list. Use this to disambiguate at shared trunk stations. Entries are service destinations, so they may lie beyond this line's own stations, but each must be a real CRS (enforced by `line-catalogue-validator`). |
| `headcode_prefixes` | list[string] | no | Same idea, but matches against the service's headcode. |

## Station object

```toml
[[stations]]
crs = "EUS"        # required
tiploc = "EUSTON"  # optional, documentation/display only -- see note below
role = "terminus"  # optional: terminus | major | minor | junction
segment = "swr-trunk-waterloo"  # optional but strongly recommended
```

`tiploc` is **purely documentation/display metadata**. It is not required
for correctness and does not gate whether a station (or its whole line)
participates in schedule matching or `schedule_line_population` publishing
-- both of those now resolve real TIPLOCs from the CIF-derived `stanox_crs`
table at runtime, independent of this field (fixed 2026-09-09; previously a
station or even an entire line with no `tiploc` set here was silently
excluded from schedule matching, which is why this field used to be
described as more important than it is). Feel free to add it for a human
reader's benefit, but there is no need to add or backfill it just to make a
new station or line "work."

## Segments

A `segment` groups consecutive stations into a named section of track. The
same segment name appearing in **multiple line definitions** marks that
section as a *shared trunk*. The matcher uses this to decide whether an
incident at a station is exclusive to one line or affects every line that
shares the trunk.

### Shared-trunk rule of thumb

A junction station belongs to the **shared trunk** segment, not the exclusive
segment. The exclusive segment starts at the *next* station after the junction.

For example, on SWR:

```
WAT - CLJ - WIM - SUR - WOK | BSK - WIN - SOU | BCU | BMH - POO - WEY
[---------- (1) -----------] [----- (2) -----]  (3)  [---- (2) -----]

(1) swr-trunk-waterloo   (2) swr-swml-south   (3) swr-brockenhurst-junction
```

(Simplified: `lines/swr-south-west-main.toml` has a few more stations.)

WOK is on `swr-trunk-waterloo` (shared with Portsmouth Direct, Alton and
the other SWR routes out of Waterloo).
The South West Main Line's exclusive segment starts at BSK (Basingstoke).
BCU (Brockenhurst), where the Lymington branch leaves, is a junction too, so
it has its own narrow shared segment, `swr-brockenhurst-junction`, used by
both `swr-south-west-main.toml` and `swr-lymington-branch.toml`. The
stations either side of it stay on the exclusive `swr-swml-south`.

This way an incident at Woking propagates to every line using
`swr-trunk-waterloo` as a "shared trunk" event, an incident at Brockenhurst
reaches the South West Main and the Lymington branch, and an incident
anywhere else from Basingstoke south stays local to the South West Main.

## Severity tuning

Default thresholds live in `common::Defaults` (`crates/common/src/lib.rs`).
A line can override any of them:

```toml
[severity_overrides]
minor_delays_pct = 0.30       # default 0.25
reduced_service_pct = 0.40    # default 0.25 (cancellations)
delay_threshold_minutes = 10  # default 5
```

Tune these for lines whose normal operation differs from typical. A rural
line with one train per hour needs different thresholds from the WCML.

## Worked example

See `west-coast-main-line.toml` and `thameslink-core.toml` in this directory.

## Curation rules

- **Order stations geographically**, end to end, with branches noted in
  comments. The ordering is used to report an incident's affected route:
  the aggregator sorts the incident's matched stations into this order and
  reports the first and last as the route's `from`/`to` ends.
- **Keep `operators` accurate.** When a TOC franchise changes, update both
  the operator code and any historical line definitions. Old codes shouldn't
  silently match new operators.
- **Don't overload `match_keywords`.** Two or three high-precision phrases
  beats ten that produce false positives. Test each addition against recent
  incidents before merging.
- **One line per file.** Makes review and PR diffs sane.

## Generated pass-through stations (`generated/pass-through.toml`)

A line's `[[stations]]` are the stops worth showing, so they skip stations
its trains run through without the catalogue listing them: the Brighton
Main Line lists London Bridge and East Croydon, and its trains pass New
Cross Gate, Sydenham and Norwood Junction in between. An incident "between
New Cross Gate and Norwood Junction" would otherwise name no station of the
line. `generated/pass-through.toml` lists, per line and per pair of
consecutive `[[stations]]`, the stations its trains really run through
(calling or passing), from the CIF timetable:

```toml
source_dates = ["2026-10-07", "2026-10-10", "2026-10-11"]

[lines.southern-brighton-main-line]
LBG-ECR = ["NXG", "BCY", "HPA", "FOH", "SYD", "PNW", "ANZ", "NWD"]
```

It is used **only** by the incident matcher (`common::matcher`), so a
named place there counts as on the line for the station tier and the
two-place rule. It is never a stop: never in `stations`, never sampled,
never in a segment, never serialised to the API or shown on a page. It
lives in a subdirectory so the `*.toml` line glob never reads it as a line.

**It is generated, not edited.** `scripts/generate-pass-through.py` takes,
for each leg, the most common path of the line's own trains (the trains
whose route best fits the line) on a representative Wednesday, Saturday
and Sunday, and keeps only stations in the `stations` reference table. A
pair of consecutive stations none of the line's trains runs between
directly (a branch boundary in the station order, such as the Brighton Main
Line's Clapham Junction - London Bridge) is listed under `[breaks]`
(`southern-brighton-main-line = ["CLJ-LBG"]`), so the matcher never treats
that stretch as track when it measures how much of a section a line shares.

**Regenerate it** after each timetable change (the December and May
principal changes, once the new timetable is in `schedule_calling_points_full`,
which holds a rolling fortnight) and after changing a line's `[[stations]]`,
then commit the result:

```sh
# Against any database holding the schedule tables:
uv run scripts/generate-pass-through.py --database-url postgres://...
# Against production, read-only (each query runs in a READ ONLY transaction):
HTTPS_PROXY=socks5://127.0.0.1:1055 uv run scripts/generate-pass-through.py \
  --psql "kubectl --context mine-bringer-ts -n distant-signal exec -i \
  distant-signal-postgres-0 -- psql -U distant_signal -d distant_signal"
```

By default it uses the latest Wednesday, Saturday and Sunday the table
holds; `--date YYYY-MM-DD` (repeatable) picks others. CI
(`line-catalogue-validator`) fails if the file is missing or does not
parse, or names a line or CRS code the catalogue and
`reference-data/crs-tiploc.csv` do not know, and warns about legs left
stale by a catalogue edit (which the loader ignores until the next
regeneration). CI never queries a database.
