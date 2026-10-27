SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Timetable associations, pre-departure prediction phase 2 (DS-152,
-- docs/superpowers/specs/2026-10-21-pre-departure-prediction-design.md
-- section 3a and the phase plan). Additive: one new table.
--
-- The CIF `AA` records in force on each service date, after the STP
-- overlay (a short-term record overlays or cancels a permanent one, like
-- schedules): JJ (the associated train joins the main one), VV (it divides
-- from it) and NP (next working; a full delivery carries none). The
-- late-start prediction caps a platform-sequence (inferred) previous
-- working at medium confidence when either train is part of a join or
-- divide that day, where the inference is least safe.
--
-- Written by schedule-reference for each service date it publishes, right
-- after the date's services (one transaction replaces the date's rows,
-- best effort); pruned by the aggregator with the other schedule products.
-- -------------------------------------------------------------------------

CREATE TABLE schedule_associations (
    -- The main train's service date.
    service_date       DATE NOT NULL,
    main_uid           TEXT NOT NULL,
    assoc_uid          TEXT NOT NULL,
    -- The associated train's service date: the main train's, or the day
    -- after or before it (the record's date indicator).
    assoc_service_date DATE NOT NULL,
    category           TEXT NOT NULL CHECK (category IN ('JJ', 'VV', 'NP')),
    tiploc             TEXT NOT NULL,
    PRIMARY KEY (service_date, main_uid, assoc_uid, tiploc)
);

-- The prediction looks a train up as the associated one too.
CREATE INDEX schedule_associations_assoc ON schedule_associations (assoc_service_date, assoc_uid);

COMMENT ON TABLE schedule_associations IS
    'CIF AA associations (JJ join, VV divide, NP next working) in force per service date, STP overlay applied (DS-152).';
