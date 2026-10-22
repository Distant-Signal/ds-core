SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- The text enums of the revision and break tables
-- (docs/superpowers/specs/2026-10-22-mid-journey-changes-design.md §4.1,
-- §10.1), added NOT VALID (a brief ACCESS EXCLUSIVE for the catalog only,
-- hence the timeout; new writes are checked at once) and validated by
-- 20261023090400 under SHARE UPDATE EXCLUSIVE, the two-file pattern of
-- 20261020150000/20261020150100. The tables are new, so nothing can fail.
-- -------------------------------------------------------------------------

ALTER TABLE journey_revisions
    ADD CONSTRAINT journey_revisions_reason_check
    CHECK (reason IN ('booked', 'replace', 'auto_shift', 'revert'))
    NOT VALID;

ALTER TABLE journey_revisions
    ADD CONSTRAINT journey_revisions_cause_check
    CHECK (cause IS NULL OR cause IN (
        'cancelled', 'part_cancelled', 'connection_missed', 'user_missed',
        'user_choice', 'skipped_stop'
    ))
    NOT VALID;

ALTER TABLE journey_revisions
    ADD CONSTRAINT journey_revisions_fault_check
    CHECK (fault IS NULL OR fault IN ('operator', 'user', 'none', 'unknown'))
    NOT VALID;

ALTER TABLE journey_revisions
    ADD CONSTRAINT journey_revisions_actor_check
    CHECK (actor IN ('owner', 'system'))
    NOT VALID;

ALTER TABLE journey_revisions
    ADD CONSTRAINT journey_revisions_revision_no_check
    CHECK (revision_no >= 1)
    NOT VALID;

ALTER TABLE journey_revision_legs
    ADD CONSTRAINT journey_revision_legs_kind_check
    CHECK (kind IN ('train', 'untracked'))
    NOT VALID;

ALTER TABLE journey_revision_legs
    ADD CONSTRAINT journey_revision_legs_fate_check
    CHECK (fate IN (
        'planned', 'travelled', 'missed', 'cancelled', 'part_cancelled', 'not_used'
    ))
    NOT VALID;

ALTER TABLE journey_breaks
    ADD CONSTRAINT journey_breaks_cause_check
    CHECK (cause IN (
        'cancelled', 'part_cancelled', 'connection_missed', 'user_missed', 'skipped_stop'
    ))
    NOT VALID;

ALTER TABLE journey_breaks
    ADD CONSTRAINT journey_breaks_fault_check
    CHECK (fault IN ('operator', 'user', 'unknown'))
    NOT VALID;

ALTER TABLE journey_breaks
    ADD CONSTRAINT journey_breaks_status_check
    CHECK (status IN ('open', 'accepted', 'dismissed', 'recovered', 'expired'))
    NOT VALID;

ALTER TABLE journey_breaks
    ADD CONSTRAINT journey_breaks_severity_check
    CHECK (severity BETWEEN 1 AND 4
        AND (notified_severity IS NULL OR notified_severity BETWEEN 1 AND 4))
    NOT VALID;

ALTER TABLE journey_breaks
    ADD CONSTRAINT journey_breaks_opened_via_check
    CHECK (opened_via IS NULL OR opened_via IN ('push', 'in_app'))
    NOT VALID;

ALTER TABLE journey_breaks
    ADD CONSTRAINT journey_breaks_dismiss_reason_check
    CHECK (dismiss_reason IS NULL OR dismiss_reason IN ('willMakeIt', 'notTravelling', 'other'))
    NOT VALID;
