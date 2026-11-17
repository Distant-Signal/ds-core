SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- A "cleared" marker on journey breaks (DS-534, review J1 follow-up).
-- A dismissed break holds back a new one for the same cause on the same
-- leg while it is the journey's latest, so the traveller is not pushed the
-- same thing twice. That also held back a real repeat: the leg recovers
-- (is healthy again) and later goes wrong the same way. `cleared_at` is
-- stamped by the api, on a dismissed break only, when it next finds the
-- journey's plan working again; a break for the same cause and leg may
-- then open anew. NULL: never seen recovered since (the value of every
-- existing row, so today's behaviour is kept for them).
--
-- Expand-only. A nullable column with no default changes the catalog only
-- (no rewrite); its brief ACCESS EXCLUSIVE lock on `journey_breaks` is
-- bounded by the 5 s lock_timeout. Code before this change never reads or
-- writes it. No grant change: `journey_breaks` is granted to the api
-- (SIUD) and the notifier (S, plus U on its two stamp columns) by table,
-- and the new column is the api's to write.
-- -------------------------------------------------------------------------

ALTER TABLE journey_breaks
    ADD COLUMN cleared_at TIMESTAMPTZ;
