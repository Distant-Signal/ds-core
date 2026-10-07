-- Which catalogue lines each incident matches, as decided by the ONE
-- matcher this codebase has (`common::matcher::lines_affected_by`, the same
-- function the aggregator runs to build live line status).
--
-- Why a new column rather than finally populating `affected_stations`:
-- RDM's Knowledgebase Incidents XML has no CRS field at all -- only a
-- free-text `Affects.RoutesAffected` -- so `affected_stations` has been
-- `'{}'` on every production row since the table was created, and the
-- archive's Line filter (`affected_stations && $2`) consequently returned
-- zero rows for every line including National Rail. See
-- docs/superpowers/specs/2026-09-16-tfl-incident-archive-design.md 1c.
-- Line-level attribution is the answer the matcher already produces from
-- prose keywords + the feed's structured operator list, and is what the
-- Line filter actually wants; deriving CRS codes from prose to then map
-- them back to lines would be a strictly lossier route to the same answer.
--
-- Deliberately NULLABLE with no default, unlike every other array column on
-- this table. NULL and '{}' are different facts and the difference is
-- operationally load-bearing:
--
--   NULL -> never computed. Every pre-existing row starts here, and stays
--           here until `backfill_incident_lines` runs. `SELECT count(*) FROM
--           incidents WHERE affected_lines IS NULL` is therefore a direct
--           answer to "is the backfill outstanding?", which a defaulted
--           '{}' would have made unanswerable -- indistinguishable from a
--           row that was processed and genuinely matched nothing.
--   '{}'  -> computed, matched no catalogue line. Real and common: the feed
--           carries incidents for operators and routes with no lines/*.toml
--           entry.
--
-- Either way the Line filter excludes the row (`NULL @> ARRAY[...]` is NULL,
-- which is not true), so existing rows behave exactly as they do today until
-- backfilled -- no regression, just a column that can say which case it is.
-- Readers that only want the list coalesce it; see `search_incidents`.
ALTER TABLE incidents
    ADD COLUMN affected_lines TEXT[];

-- Mirrors incidents_operators_gin: the Line filter is an array-overlap
-- predicate, which is exactly what GIN on a text[] serves.
CREATE INDEX incidents_affected_lines_gin ON incidents USING GIN (affected_lines);
