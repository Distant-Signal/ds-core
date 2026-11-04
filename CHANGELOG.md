# Changelog

## v0.2.0 (2026-11-04)

- Join island-of-Ireland stations to live boards; keep line colours (#12)

**Migrations** (1): `20261104120000_island_of_ireland_live_codes.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it).

## v0.1.2 (2026-11-04)

- Point the contributing docs at guides that exist (#7)
- Run release-order.py from Distant-Signal/ci; name the daily scan (#8)
- Keep the last CORPUS JSON extract as a fallback under XML snapshots (#10)

## v0.1.1 (2026-11-04)

- Add users.discord_id and ON UPDATE CASCADE on every users(id) FK (#3)

**Migrations** (4): `20261104090000_users_discord_id.sql`, `20261104090100_users_discord_id_key.sql`, `20261104090200_users_fk_on_update_cascade.sql`, `20261104090300_users_fk_on_update_cascade_validate.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it).

## v0.1.0 (2026-11-03)

- Extracted from the monorepo (`Distant-Signal/distant-signal` at
  `9e0fff16`) with its history: the core crates, the migrations through
  `20261103180000_personal_api_tokens_push_notified_at`, `db-grants.yaml`, the roles setup, the Redis ACL and the
  line catalogue (repo split spec §9, phase 4). The schema, grants and ACL
  are exactly those the monorepo last deployed with core v0.1.0.
