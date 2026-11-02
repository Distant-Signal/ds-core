# Reference data: sources and terms

The code in this repository, and the line catalogue in `lines/` (written for
Distant Signal), are licensed AGPL-3.0-or-later (`LICENSE`). Some files here
are derived from upstream data, which stays under its own terms. This file
lists each file's source and terms (repo split spec §6, "Data is separate
from code"). The site credits the same sources on `/attribution`
(`frontend/components/OpenDataAttribution.tsx`).

The operator treats the current use of the Rail Data Marketplace (RDM) feeds
as permitted under the RDM terms (decision of 2026-09-27).

| File | Source | Terms |
|---|---|---|
| `crs-tiploc.csv` | Network Rail CORPUS ("NWR CORPUS" on the RDM), extract of 2026-09-28. Provenance: `line-catalogue-validation.md`. | Open Government Licence v3.0. Contains information of Network Rail Infrastructure Limited. |
| `toc-codes.csv` | National Rail Knowledgebase Train Operating Company List (RDM), fetched 2026-09-27. Provenance: `line-catalogue-validation.md`. | RDM data sharing agreement (adapted Open Government Licence v3.0); data publisher Rail Delivery Group. |
| `stanox-crs.csv` | The CIF timetable (Master Station Names and schedule `TI` records). Provenance: `stanox-crs.md`. | Rail Settlement Plan timetable data licence ("Source: RSP"). |
| `delay-attribution-reasons.tsv` | Network Rail, "Historic Delay Attribution Glossary" (2021-08-11). Provenance: the file's header and `docs/trust-reason-codes.md`. | Open Government Licence v3.0 (operator decision of 2026-09-27). Contains information of Network Rail Infrastructure Limited. |
| `tiploc-parent-stations.csv` | Written for Distant Signal; codes and distances checked against the CIF Master Station Names file. | AGPL-3.0-or-later; the codes are the CIF's ("Source: RSP"). |
| `supplementary-fixed-links.csv` | Written for Distant Signal from a gap analysis against the CIF fixed links (ALF). | AGPL-3.0-or-later; the codes are the CIF's ("Source: RSP"). |
| `fixed-link-operators.csv` | Seeded from production's CIF fixed links (2026-10-31), curated for Distant Signal. | AGPL-3.0-or-later; the codes are the CIF's ("Source: RSP"). |
| `operator-groups.csv`, `operator-names.csv` | Written for Distant Signal; operator codes as the timetable and the Knowledgebase use them. | AGPL-3.0-or-later. |
| `station-groups.csv` | Written for Distant Signal; the London Terminals list of the National Rail Routeing Guide. | AGPL-3.0-or-later (a list of station codes; no Routeing Guide text). |
| `transit/metrolink-rail-links.csv`, `transit/net-rail-links.csv` | Written for Distant Signal; stop ids from NaPTAN (Department for Transport), station codes from the CIF. | AGPL-3.0-or-later; the NaPTAN ids are under the Open Government Licence v3.0. |

`line-catalogue-validation.md` and `stanox-crs.md` are this repository's own
documentation (AGPL-3.0-or-later).
