# Secret box: binding sealed values to their row (DS-534)

`ds_runtime::secret_box` encrypts secrets kept in Postgres (AES-256-GCM, a
random nonce per value, a key id beside each value). Until this change every
value used one constant AAD, so anyone who can write the database could copy
a sealed value from one row or column into another and it would still open.
This document is the staged rollout that binds each value to its table,
column and row id, and the checklist for each stage.

The rollout has four stages. Stage 1 is this release. Stages 2 to 4 are
written out here so each can be filed as its own deploy.

## The format

| | v1 (legacy) | v2 |
|---|---|---|
| AAD | `distant-signal secret-box v1` | `distant-signal secret-box v2`, then for each of table, column and row id a 4-byte big-endian length and the bytes |
| stored `nonce` | the 12-byte nonce | `0x02` then the 12-byte nonce (13 bytes) |
| `ciphertext`, `key_id` | as before | as before |

The version is told by the stored nonce's length (12 or 13), never by trying
AADs or keys, so there is no trial-decryption ambiguity. A 13-byte nonce whose
first byte is not `0x02`, and any other length, do not open. The ciphertext
itself carries no version byte: AES-GCM output is indistinguishable from random,
so a prefix on it could not be told from data. The row id is tagged by type
(`RowId::int`, `text`, `bytes`), so the integer `1` and the text `"1"` differ.

API (all additive; `KeyRing::seal` and `open` are unchanged):

- `Context::new(Column, impl Into<RowId>)`, for example
  `Context::new(WEBHOOK_DESTINATION_SECRET, destination_id)`.
- `KeyRing::open_in(&Sealed, &Context)`: opens v1 (ignoring the context) and v2.
- `KeyRing::seal_in(plaintext, &Context)`: writes the ring's write format.
- `KeyRing::with_write_format(Format)` and `DEFAULT_WRITE_FORMAT`: the switch.
  **Stage 1 ships `Format::Legacy`**, so nothing writes v2 yet.
- `KeyRing::seal_as(plaintext, &Context, Format)`: an explicit format, for the
  backfill and tests.
- `KeyRing::seal` always writes v1; `KeyRing::open` opens v1 only and counts as
  `purpose="unscoped"`.

Metric: `distant_signal_secret_box_legacy_opens_total{purpose}` (the project
`distant_signal_` prefix on `secret_box_legacy_opens_total`), where `purpose`
is `<table>.<column>`, or `unscoped` for `open` without a context. It counts
successful opens of v1 values. `KeyRing::parse` registers every
`KNOWN_COLUMNS` purpose at 0. A new sealed column is added to `KNOWN_COLUMNS`.

## Inventory (read-only, as of ds-api, ds-workers and ds-ingest main on 2026-11-17)

Only one thing is sealed: the webhook destinations' secret. Gotify's app
token, ntfy's bearer token and the generic preset's HMAC secret are all this
one column set (`kind` picks the use). Web Push subscription keys
(`push_subscriptions.p256dh`, `auth`) are stored as plain `TEXT` and are not
sealed. ds-ingest does not use `secret_box`.

| Table | Sealed columns | Row id | Written by | Read by |
|---|---|---|---|---|
| `webhook_destinations` | `secret_ciphertext`, `secret_nonce`, `secret_key_id` (all three set or none) | `id` (`BIGINT`, identity) | ds-api `data/webhooks.rs` `create` (INSERT, with `data::seal` from `routes/webhooks.rs`); no UPDATE ever changes the secret | ds-workers notifier `queries.rs` `webhook_destinations_for_user` (`keys.open`) |

Both sides build their `KeyRing` from `WEBHOOK_SECRET_KEYS`. The account export
(`ds-api` `data/account.rs`) strips the three columns. The `nonce` column has
`CHECK (octet_length(secret_nonce) = 12)` (migration `20261109120000`), which
a v2 value violates: see stage 2.

Two facts shape the rest:

1. The row id is an identity column, assigned by the INSERT, so the api cannot
   seal for a new row before inserting it.
2. The secret is never updated after create, so every row is written once.

## Stage 1: dual-read code (this release)

- ds-core release (patch or minor, cut by the release PR flow): the format,
  `open_in`, `seal_in`, the metric. Writes stay v1.
- Stage 1b, per consumer, after the ds-core release: ds-api bumps `core-version`
  and calls `seal_in(secret, &Context::new(WEBHOOK_DESTINATION_SECRET, id))`
  where it can (it still writes v1 by default); ds-workers bumps and replaces
  `keys.open(&sealed)` with `keys.open_in(&sealed, &Context::new(WEBHOOK_DESTINATION_SECRET, row.id))`.
  Behaviour is unchanged while every value is v1. The api's create path
  becomes "INSERT without the secret, then UPDATE the three columns with the
  sealed value, in one transaction" so the row id is known (a no-op in
  effect for v1; required for v2). The api role already holds `SIUD`.
- Deploy 1b. Done when both ds-api and the notifier run 1b code **everywhere**
  (every replica, every Job and CronJob image that links `secret_box`).

Rollback: until stage 2, rolling back is free: nothing v2 exists.

## Stage 2: write v2

Preconditions: every consumer runs 1b (check the chart's pinned digests of
the api and the notifier, not tags), and the migration below is applied.

1. ds-core: a migration relaxing the nonce check, ahead of any v2 write:
   ```sql
   SET LOCAL lock_timeout = '5s';
   ALTER TABLE webhook_destinations
       DROP CONSTRAINT webhook_destinations_secret_nonce_check,
       ADD CONSTRAINT webhook_destinations_secret_nonce_check
           CHECK (secret_nonce IS NULL OR octet_length(secret_nonce) IN (12, 13)) NOT VALID;
   ```
   followed in a later migration by `VALIDATE CONSTRAINT` (shipped as
   `20261125100000_webhook_secret_nonce_v2.sql` and
   `20261125100100_webhook_secret_nonce_v2_validate.sql`, DS-615). The constraint name
   is the Postgres default for the inline column check; confirm it on a
   migrated database first. Existing rows (12 bytes) satisfy it, so `NOT VALID`
   only avoids the scan under lock.
2. ds-core release flipping `DEFAULT_WRITE_FORMAT` to `Format::V2` (the one-line
   change; a release is the only thing that changes the write format, so a
   consumer that has not bumped keeps writing v1). Cut it after the migration
   release is deployed (ds-migrate's hook runs before the pods roll, so one
   release carrying both is also safe, but separate releases make the ordering
   a fact rather than a sequence).
3. ds-api bumps to it and deploys. The notifier needs no change; it opens both.
4. Deploy order, strictly: migration, then every reader on 1b, then the writer
   on stage 2. Never the writer first.

Rollback after stage 2 starts is **not** free: an api or notifier on pre-1b
code cannot open a v2 value, and the notifier skips such destinations as
undecryptable (`notifier_webhook_send_total{outcome="undecryptable"}`). To roll
back, keep 1b code and flip the write format back (a ds-core patch); v2 rows
written meanwhile still open on 1b code.

## Stage 3: backfill legacy rows to v2

A one-shot, idempotent Job (re-run until it reports zero rows). Where it
runs: a ds-api subcommand (`ds-api backfill-secret-box`) run as a Kubernetes
Job from the api image with the api's database role and `WEBHOOK_SECRET_KEYS`.
The api role already has `SELECT`, `UPDATE` on `webhook_destinations`
(`SIUD`), so **no grants change is needed**. ds-migrate must not do it: it holds no
application keys and should not. The alternative, if a least-privilege
reviewer prefers it, is a dedicated narrow role with `SELECT` plus
`UPDATE (secret_ciphertext, secret_nonce, secret_key_id)` on the table, added
to `db-grants.yaml` and the chart's role list; the notifier's role should not
get it (it holds the keys and is a long-running reader).

Design:

- Select a batch of legacy rows, keyset-paged by `id` so it resumes:
  `SELECT id, secret_ciphertext, secret_nonce, secret_key_id FROM webhook_destinations WHERE secret_ciphertext IS NOT NULL AND octet_length(secret_nonce) = 12 AND id > $last ORDER BY id LIMIT 200`.
- For each row: `open_in` it (v1), `seal_as(.., Format::V2)` with the active key
  (this also rotates the key to the active one), then a compare-and-swap:
  `UPDATE webhook_destinations SET secret_ciphertext = $1, secret_nonce = $2, secret_key_id = $3 WHERE id = $4 AND secret_nonce = $5 AND secret_key_id = $6`
  with `$5`, `$6` the values read. Zero rows updated means a concurrent write won; skip
  it (the next pass sees it as v2 or legacy and handles it). Never an
  unconditional UPDATE: it could overwrite a newer value with an older one.
- A row that does not open (a retired key missing from the ring, or a
  tampered value) is logged with its id and left alone, and counted; the Job
  exits non-zero when any remain so the operator sees it. It never deletes or
  nulls a secret.
- One transaction per batch, a short sleep between batches, no long locks.
  Logs counts only, never plaintext, ciphertext or keys.
- Progress, as an ops runbook query (the version is the nonce length, so no
  byte of the ciphertext is inspected):
  ```sql
  SELECT count(*) FILTER (WHERE octet_length(secret_nonce) = 12) AS legacy,
         count(*) FILTER (WHERE octet_length(secret_nonce) = 13) AS v2
  FROM webhook_destinations WHERE secret_ciphertext IS NOT NULL;
  ```
  Do not use a first-byte test such as `substr(secret_nonce, 1, 1) = '\x02'`:
  a legacy nonce is 12 random bytes, so about 1 in 256 legacy rows would match.
  The length is the version.
- Done when the query reports `legacy = 0` and `distant_signal_secret_box_legacy_opens_total`
  has not increased for a full notifier retention period (at least a week,
  to cover rows opened rarely and a delayed deploy).

## Stage 4: drop the legacy fallback

Safe only when stage 3's query shows `legacy = 0` and the counter has been flat
for the agreed period, and no consumer or Job still calls `open` or `seal`
(both write or read v1; they are removed in this stage).

1. A ds-core release (breaking for `seal` and `open`, so a minor bump with
   the changelog saying so): `open_in` rejects a 12-byte nonce; `open` and `seal`
   are removed; `Format::Legacy`, `seal_as`'s legacy arm and `DEFAULT_WRITE_FORMAT`
   go; the migration `CHECK (octet_length(secret_nonce) = 13)` is added
   `NOT VALID` then validated (a `-- contract:` header, since old code stops
   being valid: safe after the release that stopped using v1).
2. Consumers bump. A consumer that bumps before the backfill is done would see
   its remaining legacy rows as undecryptable: the Stage 3 query is the gate.
3. Remove the `purpose="unscoped"` series with the counter once nothing emits it.

## Notes

- v2 does not bind the row's `user_id`; the row id is the primary key, which
  already identifies the row. Adding the owner would need a stable owner column
  and a rebind whenever it changes (`ON UPDATE CASCADE` on `users.id`), so it
  is left out on purpose.
- This protects against moving values between rows and columns. It does not
  protect against rolling a row back to an older valid value of the same row
  (replay) or against someone who holds the keys.
