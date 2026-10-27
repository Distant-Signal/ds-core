SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- The device list on the Notifications page (DS-153): which browser each
-- push subscription is, and when it last got a push.
--
--   device_label    "Chrome on Android", worked out by the api from the
--                   User-Agent when the browser subscribes (the header
--                   itself is not stored); NULL for a subscription saved
--                   before this, or a browser it could not name
--   last_pushed_at  when the notifier last delivered a push to it; NULL
--                   until it does
--
-- The page shows the later of last_pushed_at and last_seen_at as "last
-- used". Expand-only: both columns are nullable with no default, so the
-- ADD COLUMNs change the catalog only; their brief ACCESS EXCLUSIVE on
-- `push_subscriptions` is bounded by the 5 s lock_timeout. Code before
-- this change never reads them.
-- -------------------------------------------------------------------------

ALTER TABLE push_subscriptions
    ADD COLUMN device_label TEXT,
    ADD COLUMN last_pushed_at TIMESTAMPTZ;
