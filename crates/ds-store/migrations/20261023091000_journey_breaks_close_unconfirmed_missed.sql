SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Close the "I missed this train" breaks nobody confirmed
-- (docs/superpowers/specs/2026-10-22-mid-journey-changes-design.md §8.2).
--
-- Until this release, tapping "I missed this train" opened a `user_missed`
-- break the moment the sheet loaded, before the traveller took another
-- train. Closing the sheet left it open, so the journey page and the home
-- card said "You missed the ..." for a train that may well have left on
-- time with the traveller on it. Opening the sheet now only looks; taking
-- another train is what records the miss. An open `user_missed` break is
-- one such look: dismiss it (`other`). Breaks the traveller acted on are
-- already `accepted` and untouched. Idempotent: a second run matches
-- nothing. Row locks on `journey_breaks` only.
-- -------------------------------------------------------------------------

UPDATE journey_breaks
SET status = 'dismissed',
    resolved_at = now(),
    dismiss_reason = 'other'
WHERE status = 'open'
  AND cause = 'user_missed';
