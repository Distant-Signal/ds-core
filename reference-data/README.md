# Reference data: sources and terms

The code in this repository, and the line catalogue in `lines/` (written for
Distant Signal), are licensed AGPL-3.0-or-later (`LICENSE`). Some reference
data files are derived from upstream data, which stays under its own terms.
Each directory of reference data has a README listing each file's source and
terms (repo split spec §6, "Data is separate from code"). The site credits
the same sources on `/attribution`
(`frontend/components/OpenDataAttribution.tsx`).

The operator treats the current use of the Rail Data Marketplace (RDM) feeds
as permitted under the RDM terms (decision of 2026-09-27).

This directory holds the core reference data: the line catalogue's
validation inputs, read by `line-catalogue-validator`. The rest lives next
to the crate that reads it (repo split spec §3.1, §9 phase 3 item 6):

| Directory | Files | Read by |
|---|---|---|
| `crates/common/reference-data/` | `tiploc-parent-stations.csv`, `supplementary-fixed-links.csv` | `common` (compiled in) |
| `crates/ds-store/reference-data/` | `delay-attribution-reasons.tsv` | `ds-store` (compiled in) |
| `crates/api/reference-data/` | `station-groups.csv`, `operator-groups.csv`, `operator-names.csv`, `fixed-link-operators.csv` | `api` (compiled in) |
| `crates/trust-consumer/reference-data/` | `stanox-crs.csv`, `stanox-crs.md` | `trust-consumer`, `trust-backlog-consumer` (copied into their images) |
| `crates/poller-transit-schedule/reference-data/` | `metrolink-rail-links.csv`, `net-rail-links.csv` | `poller-transit-schedule` (compiled in) |

| File | Source | Terms |
|---|---|---|
| `crs-tiploc.csv` | Network Rail CORPUS ("NWR CORPUS" on the RDM), extract of 2026-09-28. Provenance: `line-catalogue-validation.md`. | Open Government Licence v3.0. Contains information of Network Rail Infrastructure Limited. |
| `toc-codes.csv` | National Rail Knowledgebase Train Operating Company List (RDM), fetched 2026-09-27. Provenance: `line-catalogue-validation.md`. | RDM data sharing agreement (adapted Open Government Licence v3.0); data publisher Rail Delivery Group. |

`toc-codes.csv` stays here, not with the api: `line-catalogue-validator`
reads it, and the api only cross-checks its Delay Repay rules against it in
a test.

`line-catalogue-validation.md` is this repository's own documentation
(AGPL-3.0-or-later).
