# Changelog

## v0.6.0 (2026-11-06)

- Add the regular plan columns and the plan sweep's advisory lock (#25)
- Lock the checksums of the two regular plan migrations (#27)

**Migrations** (2): `20261106184000_regular_plan.sql`, `20261106184100_regular_plan_validate.sql`

## v0.5.0 (2026-11-06)

- Build with Rust 1.99.0, keep the MSRV at 1.88 (#21)
- Add regular journey targets and pairs to journey_templates (#22)
- Lock the checksums of the five newest migrations (#23)

**Migrations** (3): `20261106130000_regular_journeys.sql`, `20261106130100_regular_journeys_validate.sql`, `20261106130200_journey_templates_paired_template_id.sql`

## v0.4.1 (2026-11-05)

- Irish Rail: a live-board freshness source and five live-code overrides (#17)

**Grants**: `db-grants.yaml` changed (the roles Job applies it).

## v0.4.0 (2026-11-05)

- Lock the v0.2.0 and v0.3.0 migration checksums; fix a flaky test (#16)
- Track Iarnród Éireann trains from the board history (#18)

**Migrations** (2): `20261106120000_ireland_train_tracking.sql`, `20261106120100_ireland_train_tracking_validate.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it).

## v0.3.0 (2026-11-05)

- Keep Iarnród Éireann board history by train (#14)

**Migrations** (1): `20261105120000_island_of_ireland_train_history.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it).

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
