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
WAT - CLJ - WIM - SUR - WOK | BSK - WIN - SOU - BMH - POO - WEY
[------ swr-trunk-waterloo ------|---------- swr-swml-south ----------]
                              junction
```

WOK is on `swr-trunk-waterloo` (shared with Portsmouth Direct, Alton and
the other SWR routes out of Waterloo).
The South West Main Line's exclusive segment starts at BSK (Basingstoke).

This way an incident at Woking propagates to every line using
`swr-trunk-waterloo` as a "shared trunk" event; an incident at Basingstoke or further south stays
local to the South West Main.

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
