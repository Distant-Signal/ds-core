# Changelog

## v0.15.0 (2026-11-11)

- Release E of ingest phase 5: drop the distant_signal_app role (#60). The roles setup Job now retires it (`DROP OWNED BY`, then `DROP ROLE`), and refuses, failing the run and so the chart upgrade, while any session is connected as it or while it owns any object in any database. `observed/app_revokes` is gone from `db-grants.yaml` and `postgres-grants.sql`. `scripts/test-postgres-roles.py --check-app-retirement` proves the drop (CI's roles pass).

**Grants**: `db-grants.yaml` changed (the roles Job applies it).
**Roles**: the roles setup (`access/postgres-roles*`) changed.

**Migrations**: none.
**Consumers**: bump your ds-core pin to v0.15.0. ds-deploy's chart must be on the release E chart (its vendored roles setup and grants from v0.15.0) before Ranma-Config removes `postgresql.roles.app`; removing the value earlier leaves the old chart without its app role.

## v0.14.2 (2026-11-09)

- Add webhook_destinations for outbound webhooks (#57)
- Leave-by from the suggested plan for unpicked remind-me trains; notifier prunes the forward queue (#58)

**Migrations** (1): `20261109120000_webhook_destinations.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it).

## v0.14.1 (2026-11-08)

- Retry a failed incident chunk and text-changed publish (#55)

## v0.14.0 (2026-11-07)

- Drop the template legs' old unique key and let the notifier read day overrides (#52)
- Lock the checksum of the template legs' old key drop migration (#53)

**Migrations** (1): `20261107180000_journey_template_legs_drop_order_key.sql`
**Contract migrations**: `20261107180000_journey_template_legs_drop_order_key.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it).

## v0.13.0 (2026-11-07)

- Add day overrides for regular journeys (#49)
- Lock the checksums of the three day override migrations (#50)

**Migrations** (3): `20261107170000_day_overrides.sql`, `20261107170100_day_overrides_validate.sql`, `20261107170200_journey_template_legs_variant_key.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it).

## v0.12.0 (2026-11-07)

- Add recurrence patterns to regular journeys (#46)
- Lock the checksums of the two recurrence pattern migrations (#47)

**Migrations** (2): `20261107160000_recurrence_patterns.sql`, `20261107160100_recurrence_patterns_validate.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it).

## v0.11.0 (2026-11-07)

- Make an identical CORPUS redelivery a no-op (#42)
- Add time away and pause until for regular journeys (#41)
- Lock the checksums of the two CORPUS content hash migrations (#43)
- Lock the checksum of the time away migration (#44)

**Migrations** (3): `20261107140000_corpus_deliveries_content_sha256.sql`, `20261107140100_corpus_deliveries_content_sha256_validate.sql`, `20261107150000_time_away.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it).

## v0.10.0 (2026-11-06)

- Add ds_store::recurrence, the one rule for when a template is due (#39)

## v0.9.0 (2026-11-06)

- Add bank holidays, the division preference and the skip flag (#36)
- Lock the checksums of the two bank holidays migrations (#37)

**Migrations** (2): `20261106210000_bank_holidays.sql`, `20261106210100_bank_holidays_validate.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it).

## v0.8.1 (2026-11-06)

- Let the api read regular_journey_summaries for the account export (#34)

**Grants**: `db-grants.yaml` changed (the roles Job applies it).

## v0.8.0 (2026-11-06)

- Record each journey leg's final arrival on the leg (#29)
- Lock the checksums of the two leg final arrival migrations (#32)

**Migrations** (2): `20261106200000_journey_legs_final_arrival.sql`, `20261106200100_journey_legs_final_arrival_validate.sql`

## v0.7.0 (2026-11-06)

- Add leave-by buffers, preferences, push state and leave_by_sql (#26)
- Lock the checksums of the three leave-by migrations (#30)

**Migrations** (3): `20261106190000_leave_by.sql`, `20261106190100_leave_by_validate.sql`, `20261106190200_journeys_station_buffer.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it).

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
