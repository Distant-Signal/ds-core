SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Tracking Iarnród Éireann trains (distant-signal's
-- docs/superpowers/specs/2026-11-05-irish-rail-train-tracking-design.md
-- §4.2). Expand-only: four nullable columns (no rewrite) and two checks
-- added NOT VALID, validated by
-- 20261106120100_ireland_train_tracking_validate.sql. Old code never
-- reads these columns.
-- -------------------------------------------------------------------------

-- Display names for a train with no GB station codes (Iarnród Éireann's
-- Origin/Destination). NULL for every GB train.
ALTER TABLE trains ADD COLUMN origin_name TEXT;
ALTER TABLE trains ADD COLUMN destination_name TEXT;
ALTER TABLE trains ADD CONSTRAINT trains_endpoint_names_check
    CHECK ((origin_name IS NULL OR char_length(origin_name) BETWEEN 1 AND 100)
       AND (destination_name IS NULL OR char_length(destination_name) BETWEEN 1 AND 100))
    NOT VALID;

-- A network-neutral station reference beside the CRS pins:
-- '<network>:<code>', e.g. 'ie:CNLLY'. NULL for every GB pin.
ALTER TABLE train_subscriptions ADD COLUMN pin_origin_ref TEXT;
ALTER TABLE train_subscriptions ADD COLUMN pin_destination_ref TEXT;
ALTER TABLE train_subscriptions ADD CONSTRAINT train_subscriptions_pin_ref_check
    CHECK ((pin_origin_ref IS NULL OR pin_origin_ref ~ '^[a-z]{2,8}:[A-Z0-9]{1,12}$')
       AND (pin_destination_ref IS NULL OR pin_destination_ref ~ '^[a-z]{2,8}:[A-Z0-9]{1,12}$'))
    NOT VALID;
