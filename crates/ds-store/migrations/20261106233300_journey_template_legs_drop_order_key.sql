-- contract: journey_template_legs' unique key on (template_id, leg_order) (no code reads it: nothing upserts on it, and 20261106233200's key on (template_id, COALESCE(variant_weekday, 0), leg_order) keeps each set of legs unique)
SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Drops the old unique key, so a weekday's variant legs can reuse the leg
-- positions of the template's own legs. Every row today has a NULL
-- variant_weekday, so the new key is exactly as strict for them. No code
-- writes ON CONFLICT on the old key; older apis delete a template's legs
-- and insert them again, which the new key accepts. The DROP takes a brief
-- ACCESS EXCLUSIVE lock on journey_template_legs, bounded by the 5 s
-- lock_timeout.
-- -------------------------------------------------------------------------

ALTER TABLE journey_template_legs
    DROP CONSTRAINT journey_template_legs_template_id_leg_order_key;
