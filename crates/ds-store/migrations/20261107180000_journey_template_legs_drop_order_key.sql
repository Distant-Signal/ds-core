-- contract: journey_template_legs' unique key on (template_id, leg_order) (code stopped using it in Distant-Signal/ds-workers@eac006cb550d297aa9a401ba451a40179fc76a98)
SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Regular journeys R3: drops the template legs' old unique key, so a
-- weekday's variant legs can reuse the leg positions of the template's own
-- legs. 20261107170200's key on (template_id, COALESCE(variant_weekday, 0),
-- leg_order) keeps each set of legs unique, and is exactly as strict as
-- the old one for the template's own legs (NULL variant_weekday).
--
-- No code relies on the old key: nothing writes ON CONFLICT on it, and the
-- apis delete a template's legs and insert them again, which the new key
-- accepts. The notifier that mints only one set of legs per day
-- (ds-workers eac006c, built against core v0.13.0) ships before any api
-- writes a variant leg; that order is the deploy's, not this migration's.
--
-- The DROP takes a brief ACCESS EXCLUSIVE lock on journey_template_legs,
-- bounded by the 5 s lock_timeout. The key's index goes with it.
-- -------------------------------------------------------------------------

ALTER TABLE journey_template_legs
    DROP CONSTRAINT journey_template_legs_template_id_leg_order_key;
