SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECKs 20261023090300 added NOT VALID, and
-- `journey_revisions.break_id`'s foreign key from 20261023090100
-- (docs/superpowers/specs/2026-10-22-mid-journey-changes-design.md §10.1).
-- VALIDATE CONSTRAINT takes only SHARE UPDATE EXCLUSIVE, so the api keeps
-- writing while the rows are scanned. Every row was written under the
-- constraint already, so this cannot fail.
-- -------------------------------------------------------------------------

ALTER TABLE journey_revisions VALIDATE CONSTRAINT journey_revisions_reason_check;
ALTER TABLE journey_revisions VALIDATE CONSTRAINT journey_revisions_cause_check;
ALTER TABLE journey_revisions VALIDATE CONSTRAINT journey_revisions_fault_check;
ALTER TABLE journey_revisions VALIDATE CONSTRAINT journey_revisions_actor_check;
ALTER TABLE journey_revisions VALIDATE CONSTRAINT journey_revisions_revision_no_check;
ALTER TABLE journey_revision_legs VALIDATE CONSTRAINT journey_revision_legs_kind_check;
ALTER TABLE journey_revision_legs VALIDATE CONSTRAINT journey_revision_legs_fate_check;
ALTER TABLE journey_breaks VALIDATE CONSTRAINT journey_breaks_cause_check;
ALTER TABLE journey_breaks VALIDATE CONSTRAINT journey_breaks_fault_check;
ALTER TABLE journey_breaks VALIDATE CONSTRAINT journey_breaks_status_check;
ALTER TABLE journey_breaks VALIDATE CONSTRAINT journey_breaks_severity_check;
ALTER TABLE journey_breaks VALIDATE CONSTRAINT journey_breaks_opened_via_check;
ALTER TABLE journey_breaks VALIDATE CONSTRAINT journey_breaks_dismiss_reason_check;
ALTER TABLE journey_revisions VALIDATE CONSTRAINT journey_revisions_break_id_fkey;
