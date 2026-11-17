# Changelog

## v0.15.1 (2026-11-17)

Review follow-ups (DS-534). A patch: no consumer constructs `ConsumerConfig` as a struct literal, so its new pub fields break nobody.

- Harden Darwin ordering guards, the ingest-stream transient delivery cap, metric label cardinality and decoder length bounds (#76): a Darwin upsert now escapes a stored future-stamped row and clamps the row `ts` to now plus 2 minutes (`FUTURE_SKEW`); `ConsumerConfig` gained pub fields `max_transient_deliveries` (default `Some(50)`) and `transient_stuck_deadline` (default 24h); `Handler::knows_schema` (default false) limits the metric schema labels until a schema is accepted; the decoder rejects a key or producer over 2048 bytes and a schema over 128 bytes as poison.

**Migrations**: none.
**Grants**: none (`db-grants.yaml` is unchanged).

**Consumers**: bump your ds-core pin to v0.15.1 (ds-ingest, ds-workers, ds-api, ds-pollers). ds-deploy's vendored grants and `migrate.image` do not move: there is no migration or grant change. Behaviour changes that arrive with the bump:
- Darwin writers (ds-ingest's ingest-writer, ds-api's schedule-ingest) see future-stamped rows replaced and row `ts` clamped to now plus 2 minutes.
- Stream consumers get the transient delivery cap (50 deliveries, 24h stuck deadline) by default; struct-literal construction of `ConsumerConfig` would break, and none exists in the consumer repositories.
- Follow-ups: ds-ingest can delete `heal_future_rows` and should override `knows_schema` in `WriterHandler`.

## v0.15.0 (2026-11-17)

Review fixes (DS-534). Minor because `ChangeOfIdentity` gained a pub field, a breaking Rust API change on 0.x.

- Fix ingest-stream, group restore, TRUST 0007 and GCP lifetime review findings (#69): the idle-consumer sweep decides on the smaller of `idle` and `inactive`; rejected rows are kept and their dead-letter write retried when it fails after commit; new `GroupCursor` and `restore_group_cursor` restore a consumer group to the last acked entry (`restore_group_position` is unchanged); `ChangeOfIdentity::revised_train_id` (`Option<String>`) and `identity_change()` carry a TRUST change of identity's new id (the field names come from the public Train Movements documentation; no captured fixture confirms them); the impersonated GCP token lifetime no longer subtracts request latency twice.
- Fix review findings in ds-store (TRUST state, Darwin, crosswalk) and fixed-link windows (#70): a duplicate TRUST movement no longer rewrites `train_current_state` (W1, refined by #73); `calls_for_trains` hides Darwin locations a newer schedule dropped (W2); `merge_services` keeps a schedule message with its own `ts` over a TS-only one (W3); the CORPUS JSON fallback no longer fills keys the XML left out when they are ambiguous (W4); `LinkWindow::new`, `fixed_links_from_where` and `fixed_link_between_where` read the day mask for the day past midnight (W5, signatures unchanged); the ignored Bank Holiday Running column is documented and pinned (W6).
- Fix the migrations, grants and run lock review findings (#71): `MIGRATOR` accepts a database migrated past the build (`ignore_missing`, known checksums still compared); contract migration headers must carry a `safe after core vX.Y.Z` floor (`check-migration-order.py --contract-headers`, run first by tag.yml; the four existing headers are exempted with their floors recorded); 12 `CREATE INDEX CONCURRENTLY` migrations index foreign keys that had no index on the child column; `RunLock::still_held()` and `ensure_held()` re-check the lock; `postgres-grants.sql` revokes group memberships `db-grants.yaml` no longer lists; the owner connection limit in `db-grants.yaml` now says 4.
- Fix matcher keyword boundaries, segment sharing and catalogue checks (#72): `match_keywords` and `excluded_keywords` match whole words (Unicode-aware, case-insensitive), so "LNER" no longer matches inside "vulnerable"; shared track and junctions at the listed stations move to shared segments (the lines matched do not change, the scope goes from `ExclusiveSegment` to `SharedSegment`); the validator checks line ids are unique and match their file names (`west-coast-main-line.toml` is now `lines/wcml.toml`) and gates segment sharing against `lines/segment-sharing-baseline.txt` (568 deferred cases that may only shrink); pass-through legs whose ends are no longer consecutive stop feeding `holds_place`.
- Let a newer or fuller duplicate movement update the current state (#73): a duplicate TRUST movement rewrites `train_current_state` when its event time is strictly newer, when it is `completed`, or when it has the same event time and carries more of `next_calling_point` and `eta_next`; a stored `completed` is never overwritten and an exact replay changes nothing.
- Lock the checksums of the 12 migrations from #71 (#74)

**Migrations** (12), all `CREATE INDEX CONCURRENTLY IF NOT EXISTS` in `-- no-transaction` files (index builds, no table rewrite): `20261116090000_train_notification_state_tracked_train_id_index.sql`, `20261116090100_journey_leg_notification_state_journey_leg_id_index.sql`, `20261116090200_groups_created_by_index.sql`, `20261116090300_group_trains_added_by_index.sql`, `20261116090400_group_invite_links_created_by_index.sql`, `20261116090500_custom_line_group_grants_granted_by_index.sql`, `20261116090600_group_journeys_added_by_index.sql`, `20261116090700_unlisted_links_created_by_index.sql`, `20261116090800_delegated_grants_user_id_index.sql`, `20261116090900_delegated_grant_confirmations_user_id_index.sql`, `20261116091000_personal_api_tokens_user_id_index.sql`, `20261116091100_delegated_deletions_grant_id_index.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it; the owner connection limit now says 4) and `postgres-grants.sql` step 2 now revokes group memberships the file no longer lists, for listed roles and for unlisted `distant_signal_*` roles; an unlisted role that owns nothing also loses its schema public grants. In the steady state there is nothing to revoke.

**Consumers**: bump your ds-core pin to v0.15.0; ds-deploy's vendored grants (`db-grants.yaml`, `postgres-grants.sql`) and `migrate.image` move to v0.15.0 too, so the 12 index migrations and the membership revokes apply. Behaviour changes that arrive with the bump:
- trust-consumer and trust-backlog-consumer: a change of identity now carries `revised_train_id` (use `identity_change()`); W1 and #73 change what the current-state write does for duplicate movements (ingest-writer too).
- movement-feed (ds-ingest) and enricher (ds-workers) may adopt `GroupCursor` and `restore_group_cursor` in place of `last_delivered_id`: call `delivered` per read id and `acked` after each XACK. Optional; `restore_group_position` still works.
- ds-api's planner gets the `LinkWindow` overnight weekday fix only once it bumps core (the call sites already pass the service date and running minute, so no code change is needed there); the api also reads the Darwin W2 filter, and darwin-relay the W3 merge change.
- `RunLock::ensure_held()` is available: ds-api's schedule-reference should call it between products or dates (a follow-up there).
- The aggregator (ds-workers) now sees whole-word matcher keywords, and "Also affects other lines on this route." appears where the catalogue's scope moved from `ExclusiveSegment` to `SharedSegment` (Thameslink core, Great Northern/Thameslink Cambridge, and the BCU, LLJ, SKI, LAN, CNF and DAR junctions; #72 lists them). The line id `wcml` is unchanged by the file rename.
- Everything else (ingest-stream idle sweep and dead-letter retry, GCP lifetime) is a fix with no consumer change.

## v0.14.5 (2026-11-13)

- Add train_service_labels and a delay context report (#67)
- Lock the checksum of the train_service_labels migration

**Migrations** (1): `20261113120000_train_service_labels.sql`
**Grants**: `db-grants.yaml` changed (the roles Job applies it).

**Consumers**: bump your ds-core pin to v0.14.5. Additive only: a new internal table `train_service_labels` and its grants; nothing is narrowed and no consumer is deployed yet. ds-deploy's vendored grants and `migrate.image` move to v0.14.5 when a consumer needs the table.

## v0.14.4 (2026-11-12)

- Replace internal hostnames and paths in docs (#65)
- Add a rust-toolchain.toml (#64)
- Narrow the api to SID on delegated_deletions (#63): the api role loses UPDATE on `delegated_deletions` and keeps SELECT, INSERT and DELETE. The api never updates a row, and the `users(id)` ON UPDATE CASCADE needs no grant. No migration.

**Grants**: `db-grants.yaml` changed (the roles Job applies it).

**Migrations**: none.
**Consumers**: bump your ds-core pin to v0.14.4. ds-deploy must bump its vendored grants (`db-grants.yaml`, `postgres-grants.sql`) to v0.14.4 for the narrowing to apply. Rollback: a patch release restoring SELECT, INSERT, UPDATE and DELETE (SIUD) for the api on `delegated_deletions`; a release is never moved or deleted.

## v0.14.3 (2026-11-11)

- Release E of ingest phase 5: drop the distant_signal_app role (#60). The roles setup Job now retires it (`DROP OWNED BY`, then `DROP ROLE`), and refuses, failing the run and so the chart upgrade, while any session is connected as it or while it owns any object in any database. `observed/app_revokes` is gone from `db-grants.yaml` and `postgres-grants.sql`. `scripts/test-postgres-roles.py --check-app-retirement` proves the drop (CI's roles pass).

**Grants**: `db-grants.yaml` changed (the roles Job applies it).
**Roles**: the roles setup (`access/postgres-roles*`) changed.

**Migrations**: none.
**Consumers**: bump your ds-core pin to v0.14.3. ds-deploy's chart must be on the release E chart (its vendored roles setup and grants from v0.14.3) before Ranma-Config removes `postgresql.roles.app`; removing the value earlier leaves the old chart without its app role.

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
