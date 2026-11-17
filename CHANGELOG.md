# Changelog

## v0.17.2 (2026-11-17)

Drops chrono-tz from ds-core (DS-534, RUSTSEC-2026-0555). A patch: an internal dependency swap behind an unchanged API, no migration, no grant or ACL change. chrono stays.

- Run `common::zone` on jiff and drop chrono-tz (#94): the public API is identical (chrono types in and out, the opaque `Zone`, `Zone::LONDON` and `Zone::DUBLIN`, every function and signature; `Zone` keeps `Copy`, `Eq`, `Hash`, `Debug` and `Display`, and `Debug` now prints `Zone("Europe/London")`). Behaviour is preserved: `Zone::parse` is still exact-case and accepts and refuses the same names (`GB`, `US/Eastern`, `Europe/Kiev`, `Europe/Kyiv`, `UTC` and `Etc/UTC` are accepted; `europe/london`, `utc`, `Z`, offsets, abbreviations, empty and padded input are refused; `Factory`, which only jiff's database has, is refused too), and the clock-change policy is the same, written out with jiff's ambiguity API instead of its default: the earliest reading of a repeated time, none for a skipped time, and for `to_utc_gap_shifted` the earliest reading of the time an hour later. The zone data is now jiff's bundled database (`jiff` 0.2.39 with `tzdb-bundle-always`, `jiff-tzdb` 0.1.9, tzdb 2026e; chrono-tz 0.10.4 had 2025b), with the host's zoneinfo switched off, so behaviour no longer depends on the machine or image. A later tzdb is a `cargo update -p jiff-tzdb` (or a Renovate bump) and a new core release. New `crates/common/tests/zone_python_zoneinfo.rs` checks London, Dublin, New York, Kolkata and Lord Howe against Python's `zoneinfo` for every minute around each 2025 and 2026 clock change and around changes in 1971, 2037, 2038, 2099 and 2100. One difference: chrono-tz stopped listing transitions after 2099, so summer time from 2100 on is now (correctly) applied; instants beyond year 9999 are read at the end of jiff's range.
- deny.toml: removed the RUSTSEC-2026-0555 (chrono-tz) ignore, as Cargo.lock no longer has chrono-tz. The chrono ignore (RUSTSEC-2026-0543) stays.

**Consumers**: no code changes are needed. Bump `core-version` to v0.17.2: chrono-tz is no longer in ds-core's tree, jiff (with jiff-tzdb) enters yours, so expect that Cargo.lock difference. chrono-tz leaves a consumer's own lock only once it has converted all of its direct chrono-tz uses to `common::zone`: ds-workers and ds-ingest are converted; ds-api (about 45 files) still has direct uses and must convert them first; ds-pollers has none. When `cargo tree -i chrono-tz` is empty in a repository, remove its own `chrono-tz` entries in Cargo.toml and its deny.toml ignore for RUSTSEC-2026-0555, and only then.

**Migrations**: none.

**Grants**: none. `db-grants.yaml`, `postgres-grants.sql` and the Redis ACL are unchanged.

## v0.17.1 (2026-11-17)

Groundwork for dropping chrono-tz (DS-534, RUSTSEC-2026-0555). A patch: one additive Rust module, no behaviour change, no migration, no grant or ACL change.

- Move every chrono-tz use in ds-core behind common::zone (#92): `common::zone` is now the only ds-core module that names chrono-tz, and its public API uses only chrono types and an opaque `Zone`. `rail_day`, `trust_timestamp`, `transit`, `incident_validity` (tests) and the ds-store reads, sweeps and tests call it instead of chrono-tz; their public signatures are unchanged. The DST and London pinning tests from #90 (tests only, already in v0.17.0) cover those functions and pass unchanged, and `zone` has its own tables for London, Dublin and a user-supplied zone around the 2025 and 2026 clock changes. The `chrono-tz` entry in ds-store's Cargo.toml is now unused and is left for the replacement step.

**Consumers**: `common::zone` is new and additive. Its API: the opaque `Zone` (consts `Zone::LONDON` and `Zone::DUBLIN`; `Zone::parse`, `name`, `local`, `offset_at`, `naive_local`, `date`, `time`, `resolve`, returning `LocalTime::{Single, Ambiguous { earliest, latest }, Gap}`, `to_utc_earliest` and `to_utc_gap_shifted`) and the free functions `london_local`, `london_date`, `london_time`, `london_naive`, `london_resolve`, `london_to_utc_earliest`, `london_to_utc_gap_shifted`, `dublin_local` and `dublin_to_utc_earliest`. `Zone::parse` is case-sensitive, exactly as chrono-tz's name parsing is today (case variants are rejected). Consumers bump `core-version` to v0.17.1, then convert their direct chrono-tz uses to `common::zone`, as a later mechanical PR per repository; the pinning tests from #90 and each consumer's own tests are the safety net. Nothing forces the bump: v0.17.0 keeps working.

**Migrations**: none.

**Grants**: none. `db-grants.yaml`, `postgres-grants.sql` and the Redis ACL are unchanged.

## v0.17.0 (2026-11-17)

Opaque public ids for journey legs, tracked trains and push devices, stage 1 (DS-534, round 2 after journeys). A minor, as v0.16.0 was for its id wave: nine additive migrations and one additive grant; nothing is removed or changed in place, no Rust API changes and no migration is a contract migration. The grant has an ordering requirement (below).

- Add opaque public_id to journey_legs, train_subscriptions and push_subscriptions (#89): the api serves these tables' global counters (`journey_legs.id`, `train_subscriptions.id`, `push_subscriptions.id`) in the shared journey view, push topics, webhook bodies, URLs and device lists. Each table gets `public_id`, the prefix (`l_`, `t_`, `d_`) plus the 32 hex digits of `gen_random_uuid()` (34 characters, not derived from `id`, so a path segment is a legacy id exactly when it is all digits and each kind has its own prefix), in the journeys pattern: the column is added nullable, its default is set after it exists (no table rewrite), existing rows are backfilled in one statement, and a `CHECK (public_id IS NOT NULL AND public_id ~ '^<prefix>_[0-9a-f]{32}$')` is added `NOT VALID` under a 5 s `lock_timeout`, a unique index is built concurrently, and the CHECK is validated. `SET NOT NULL` is left out, as for journeys (it counts as destructive DDL; the validated CHECK says the same). The current api, writer and notifier never name the column, so their inserts take the default and the migrations are safe to apply ahead of any consumer. The `train_subscriptions` `ADD COLUMN` takes a brief ACCESS EXCLUSIVE lock on a table the writer and consumers update constantly, bounded by the 5 s `lock_timeout` (the migrate Job retries). A DB test checks that an insert naming no `public_id` gets a well-formed one per table.
- Shorten the chrono and chrono-tz advisory ignores to 2027-02-17 (#88): deny.toml only; nothing a consumer compiles against.
- Fix the date of the chrono advisory ignores in the v0.16.1 entry below: they run until 2027-02-17, as `deny.toml` says (the entry said 2027-05-17, which is RUSTSEC-2023-0071's date). Documentation only.

**Migrations** (9), in order, all additive and expand-only; they must ship together:
1. `20261118100000_journey_legs_public_id.sql`
2. `20261118100100_journey_legs_public_id_index.sql` (`-- no-transaction`, `CREATE UNIQUE INDEX CONCURRENTLY`)
3. `20261118100200_journey_legs_public_id_validate.sql`
4. `20261118110000_train_subscriptions_public_id.sql`
5. `20261118110100_train_subscriptions_public_id_index.sql` (`-- no-transaction`, `CREATE UNIQUE INDEX CONCURRENTLY`)
6. `20261118110200_train_subscriptions_public_id_validate.sql`
7. `20261118120000_push_subscriptions_public_id.sql`
8. `20261118120100_push_subscriptions_public_id_index.sql` (`-- no-transaction`, `CREATE UNIQUE INDEX CONCURRENTLY`)
9. `20261118120200_push_subscriptions_public_id_validate.sql`

Each table has two transactional files and one `-- no-transaction` index file (three no-transaction files in all), each alone in its file. There are no `-- contract:` migrations. Production's latest applied migration is `20261117120300` once v0.16.0's migrations are applied; the migrate hook applies every pending migration in order.

**Grants**: `db-grants.yaml` and `postgres-grants.sql` changed by one additive grant (the roles Job applies it). The `notifier` role's column-level SELECT on `train_subscriptions` gains `public_id` (it was `id, user_id, trains_id, notifications_enabled, pin_origin_crs, pin_scheduled_departure, pin_destination_crs, pin_origin_ref`), so it can build push URLs and tags from the opaque id. Nothing else changes: the notifier already holds table-level SELECT, INSERT and UPDATE on `journey_legs` and SELECT and DELETE on `push_subscriptions`, the api holds SIUD on all three tables, and the writer, trust-backlog, trust-consumer and aggregator column lists on `train_subscriptions` and the writer's on `journey_legs` do not need the new column. A default needs no privilege on its column, so the notifier's existing column-level INSERT on `train_subscriptions` keeps working.

**Order** (this release's grant is read by the schema gate): `ds_store::schema::wait_for_schema` bakes each role's required privileges in at build time from the core the image links, so a notifier built on v0.17.0 requires SELECT on `train_subscriptions.public_id` and sits in the gate, exiting on its next restart, until the new grants are applied. ds-deploy must therefore move `core-version`, the vendored `db-grants.yaml` and `postgres-grants.sql` and `migrate.image` to v0.17.0 BEFORE (or in the same chart build as) any notifier image built on v0.17.0 is deployed, with a `grant-floors.toml` entry (`service = "notifier"`, `core = "v0.17.0"`, reason `train_subscriptions.public_id column grant`) checked by Distant-Signal/ci's `release-order.py check --grant-floors`. Images built on earlier cores are unaffected by an extra grant, and none of them names the new columns.

**Consumers** (the follow-ups, in the order of the design's PR plan, DS-534):
1. ds-deploy: `core-version`, vendored grants and `migrate.image` to v0.17.0, and the grant floor above. The migrate hook adds the nine migrations' columns, indexes and constraints.
2. ds-api: bump `core-version` to v0.17.0 and add the dual-accept `PublicRef` extractors and the additive `publicId` response fields (stage 1); it deploys after step 1.
3. distant-signal (frontend): move calls to public ids and bump the api pin once the api build exists.
4. ds-workers: bump `core-version` to v0.17.0; push URLs, tags and the hashed `Topic` header built from the public ids; its notifier image deploys only after step 1.
5. ds-deploy: pin the images from steps 2 to 4. ds-ingest and ds-pollers have no ordering requirement for this release.

Rollback: an image rollback is enough; the columns, indexes, constraints and the grant are additive and stay.

## v0.16.1 (2026-11-17)

Narrower notifier grants and stream-consumer Redis ACL (DS-534, security assessment L16). A patch: no migration, no Rust API change, nothing a consumer compiles against. It is a grants and ACL release, so the order consumers and ds-deploy take it in matters (below).

- Narrow the notifier's table grants and the stream consumers' Redis ACL (#83); also ignores the chrono and chrono-tz unmaintained advisories (RUSTSEC-2026-0543 and RUSTSEC-2026-0555, published today, "all versions affected") in deny.toml until 2027-02-17 so the blocking advisories job passes.

**Migrations**: none.

**Grants**: `db-grants.yaml` and `postgres-grants.sql` changed (the roles Job applies them; step 2 revokes the membership the file no longer lists). Only the `notifier` role (`distant_signal_notifier`) changes:
- It leaves the `read_shared` group; its groups are now `schema_gate` only. That removes SELECT on the 46 tables it held only through the group and no statement of the notifier (or of the ds-core functions it calls) touches: `trust_event_backlog`, `tracked_stop_boards`, `tracked_stop_arrival_boards`, `station_full_coverage_samples`, `full_coverage_line_stats`, `full_coverage_line_window_stats`, `island_of_ireland_lines`, `island_of_ireland_station_samples`, `island_of_ireland_trains`, `island_of_ireland_train_calls`, all 11 `transit_*` tables, `line_status`, `incidents`, `incident_history`, `incident_feed_state`, `ingest_freshness`, the four `line_status_*_stats` tables, `full_coverage_window_verdicts`, `corpus_crosswalk_build`, `corpus_stanox_crs`, `tocs`, `tfl_stops`, `tfl_stop_crs`, `tfl_line_routes`, `fixed_links`, `corpus_locations`, `corpus_deliveries`, `schedule_network_departures`, `schedule_line_population`, `line_train_summaries`, `tiploc_locations`, `schedule_reference_publishes` and `schedule_feed_ingests`.
- It gains table-level SELECT on `stations`, `island_of_ireland_stations` and `island_of_ireland_live_stations` (it read them through the group; its pushes name stations from them).
- Every other explicit notifier grant (table and column level, DML, sequences) is unchanged. No other role changes: the api, aggregator and writer stay in `read_shared`.
- `ds_store::schema::wait_for_schema` bakes each role's required privileges in at build time from the core the image links. A notifier built against v0.16.0 or earlier requires SELECT on every `read_shared` table, so once these grants are applied it would sit in `wait_for_schema` and exit when it next restarts (running pods keep working until then).

**Redis ACL**: `access/redis-users.acl.tpl` changed. `+xclaim` is removed from `trust-consumer`, `trust-backlog-consumer`, `full-coverage-consumer` and `ingest-writer`, and `+xgroup|createconsumer` from the three movement-events consumers. No service sends either (idle entries are taken over with XAUTOCLAIM; XREADGROUP creates its consumer); every other user is unchanged. It takes effect on the one Redis restart the chart's ACL ConfigMap change already causes. An ACL cannot separate the three movement consumers' groups on the shared `movement-events` stream (a comment in the template says so; a stream per group is a ds-ingest change).

**Consumers and ordering** (the grant floor is v0.16.1):
1. ds-workers bumps `core-version` to v0.16.1 and builds a new notifier image from it. The image already built from ds-workers main 065e800, `build.44047133373-065e800`, has the checked SQL (`check-notifier-grants.py`, ds-workers #43, passes against this release's `db-grants.yaml`) but is built against v0.16.0, so its baked-in gate still wants the `read_shared` tables: it is NOT enough. The notifier image that deploys ahead of these grants must carry a `core-version` label of v0.16.1 or later.
2. ds-deploy pins that notifier image (it may ship deploys that pin 065e800 or other v0.16.0 images, as long as `core-version`, the vendored grants and `migrate.image` stay at v0.16.0).
3. Only then does ds-deploy move `core-version`, the vendored `db-grants.yaml`, `postgres-grants.sql`, `redis-users.acl.tpl` and `migrate.image` to v0.16.1 (`scripts/sync-core.py`), with a `grant-floors.toml` entry (`service = "notifier"`, `core = "v0.16.1"`) checked by Distant-Signal/ci's `release-order.py check --grant-floors`, so a chart that pins an older notifier is refused. The other consumers' bumps (ds-api, ds-ingest, ds-pollers) have no ordering requirement.

Rollback: revert and cut a patch release; re-applying the previous `postgres-grants.sql` re-adds the membership (the roles Job is idempotent) and the ACL revert re-adds the commands on one restart. Nothing is dropped or migrated.

## v0.16.0 (2026-11-17)

Opaque ids, a break-cleared marker and the secret box's row-bound format, stage 1 (DS-534). A minor: eight additive migrations and a new public API in `ds-runtime`; nothing is removed or changed in place, and no migration is a contract migration.

- Add journeys.public_id, an opaque journey id (DS-534, stage 1) (#78): `public_id` is `j_` plus the 32 hex digits of `gen_random_uuid()` (34 characters, not derived from `id`), so a path segment is a legacy id exactly when it is all digits. The column is added nullable, its default is set after it exists (no table rewrite), existing rows are backfilled, and a `CHECK (public_id IS NOT NULL AND public_id ~ '^j_[0-9a-f]{32}$')` is added `NOT VALID` and then validated; `SET NOT NULL` is left out because the migration checks treat it as a contract migration, and it can follow later. The current api never names the column, so its inserts take the default and the migration is safe to apply ahead of the api.
- Add journey_breaks.cleared_at, a recovered-since-dismissal marker (#80): a nullable `timestamptz` with no default (a catalogue-only change), so existing rows stay NULL and behaviour is unchanged until the api reads it. The api stamps it on a dismissed break when it next finds the plan working again, and only a cleared dismissal stops holding a new break back for the same cause and leg.
- Add an opaque public_id to group_members (#79): a random id per (group, user) membership so the api can stop showing co-members each other's `users.id`. It is a stored random id (`replace(gen_random_uuid()::text, '-', '')`, no server secret to rotate), nullable with the default set before the backfill, a unique index built concurrently, then `CHECK (public_id IS NOT NULL) NOT VALID` and its validation. Existing api inserts do not name the column and keep working.
- Bind the secret box AAD to the row: v2 format, dual-read (DS-534 stage 1) (#81): additive API in `ds-runtime`: `Context::new(Column, row id)`, `KeyRing::open_in` (opens v1, ignoring the context, and v2), `KeyRing::seal_in`, `KeyRing::seal_as` and `KeyRing::with_write_format`. A v2 value's AAD is `distant-signal secret-box v2` plus the length-prefixed table, column and type-tagged row id; the version is the stored nonce's length (12 bytes is v1, 13 bytes, `0x02` plus the nonce, is v2). `DEFAULT_WRITE_FORMAT` is `Format::Legacy`, so nothing writes v2 yet: `seal` still writes v1 and `open` opens v1 only. New counter `distant_signal_secret_box_legacy_opens_total{purpose}` (`purpose` is `<table>.<column>`, registered at 0 for every known column by `KeyRing::parse`; `unscoped` for `open`). The plan for stages 1 to 4 is in docs/secret-box-aad.md.
- Date the cargo-deny ignores and bump the rust-ci pin (#85): every `[advisories] ignore` in deny.toml carries a reason and an expiry (RUSTSEC-2023-0071 expires 2027-05-17; the stale RUSTSEC-2026-0192 ignore is removed), and the `rust-ci.yml` pin moves to a Distant-Signal/ci commit with blocking cargo-deny advisories and an ignore-expiry check.
- Workflow and Renovate changes that were already on main at v0.15.1's tag and are not in its entry: `release.yml`'s `plan` job fails a tag whose commit is not reachable from `origin/main`, before anything is built (#77, first exercised by v0.15.1); renovate.json5 extends a pinned commit of Distant-Signal/ci's preset rather than its default branch (#82).

**Migrations** (8), in order, all additive and expand-only; they must ship together:
1. `20261117100000_journeys_public_id.sql`
2. `20261117100100_journeys_public_id_index.sql` (`-- no-transaction`, `CREATE UNIQUE INDEX CONCURRENTLY`)
3. `20261117100200_journeys_public_id_validate.sql`
4. `20261117110000_journey_breaks_cleared_at.sql`
5. `20261117120000_group_members_public_id.sql`
6. `20261117120100_group_members_public_id_index.sql` (`-- no-transaction`, `CREATE UNIQUE INDEX CONCURRENTLY`)
7. `20261117120200_group_members_public_id_check.sql`
8. `20261117120300_group_members_public_id_check_validate.sql`

There are no `-- contract:` migrations. Production's latest applied migration is `20261116091100` once v0.15.0's index migrations are applied; the migrate hook applies every pending migration in order.
**Grants**: none (`db-grants.yaml` is unchanged; `journeys`, `journey_breaks` and `group_members` are granted by table, so the new columns need no grant change).

**Consumers**: bump `core-version` to v0.16.0 in ds-api, ds-workers, ds-ingest and ds-pollers. ds-deploy's `core-version`, vendored files and `migrate.image` must move to v0.16.0 before any consumer image built against this release is deployed: the migrate hook adds `journeys.public_id`, `group_members.public_id` and `journey_breaks.cleared_at`, and an image that reads them fails against a database without them. These depend on this release: ds-api #69 (cleared marker), #73 (opaque member ids) and #75 (opaque journey ids), ds-workers #41 (push links by opaque id), and the frontend PRs #295 and #296. The secret box stage 1a is additive and writes stay v1; its follow-up is stage 1b in ds-api (INSERT then UPDATE for the webhook secret, using `seal_in`) and ds-workers (`open_in`), before the write format can move to v2 (a nonce-check migration and a later release). Watch `distant_signal_secret_box_legacy_opens_total` for the v1 opens. Workflow notes: release.yml's tag-on-main check and the blocking-advisories ci pin need nothing from consumers.

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
