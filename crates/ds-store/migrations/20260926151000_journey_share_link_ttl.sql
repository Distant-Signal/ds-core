-- ---------------------------------------------------------------------
-- 2026-09-26 review, L17: journey share links were minted with no expiry
-- (expires_at NULL, design doc 2026-09-23-unlisted-links-design.md §5's
-- original choice), so a token captured anywhere -- ingress access logs,
-- browser history, a Referer header -- granted read access forever.
-- routes::journeys now mints every link with a 30-day TTL
-- (JOURNEY_SHARE_LINK_TTL); this gives every ALREADY-active,
-- never-expiring journey link the same 30 days, counted from this
-- migration rather than from its original creation so no link someone is
-- actively using dies the instant this deploys. The owner can extend it
-- in place (POST /Journeys/{id}/share-link/extend) if it's still in use.
--
-- Only resource_type = 'journey': expires_at stays nullable, and whether a
-- resource type expires remains each caller's own choice. Plain UPDATE, no
-- index build -- no CONCURRENTLY/no-transaction concern.
-- ---------------------------------------------------------------------

UPDATE unlisted_links
SET expires_at = NOW() + INTERVAL '30 days'
WHERE resource_type = 'journey'
  AND revoked_at IS NULL
  AND expires_at IS NULL;
