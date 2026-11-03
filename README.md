# ds-core

The shared core of [Distant Signal](https://github.com/Distant-Signal/distant-signal),
a personal UK rail companion: the crates every service links, the database
schema and its migrations, the Postgres grants and roles, the Redis ACL and
the line catalogue. It is the one owner of each of them.

What lives here:

| Path | What |
|---|---|
| `crates/common` | The domain: line matcher, segments, severity, stations, TfL, TRUST timestamps, transit. |
| `crates/ds-runtime` | The domain-free runtime: logging, metrics, backoff, Postgres and Redis connections, service arguments, the User-Agent. |
| `crates/health-http` | The services' health and metrics endpoint. |
| `crates/trust-schema`, `crates/schedule-query` | The TRUST message schema and the timetable query types. |
| `crates/ingest-stream` | The versioned Redis stream envelope between the producers and the writer (golden-pinned). |
| `crates/ds-store` | The shared SQL, the schema gate, and `migrations/` (forward-only, checksummed in `tests/migration_checksums.lock`). `db-grants.yaml` classifies every database object and gives each service role its grants. |
| `crates/ds-migrate` | The migrator: the chart's migrate hook Job runs its image. |
| `crates/ds-lines`, `lines/` | The line catalogue (`lines/*.toml`, `lines/SCHEMA.md`), embedded in every binary that links `ds-lines`. |
| `crates/line-catalogue-validator`, `reference-data/` | Checks the catalogue's identifiers against the reference data. |
| `access/` | The Postgres roles setup and rendered grants, and the Redis ACL template. |

## Releases

Every release is a tag `vX.Y.Z` (see `CHANGELOG.md`). A release publishes:

- the crates, at the git tag: consumers depend on them by git URL and tag,
  all on the same tag;
- the image `ghcr.io/distant-signal/images/ds-migrate:vX.Y.Z`;
- release assets for the chart, each with a Sigstore bundle signed by this
  repository's `release.yml` at the tag
  (`.github/workflows/release.yml@refs/tags/vX.Y.Z`): `db-grants.yaml`,
  `postgres-grants.sql`, `postgres-roles.sql`, `postgres-roles-setup.sh`,
  `redis-users.acl.tpl`, `render-redis-acl.py` and the validator's
  `site-credits.json` (which sources the site must credit on
  `/attribution`), plus `SHA256SUMS`.

To release, open a pull request made by `uv run scripts/release.py prepare
--bump patch` (or `minor`: a breaking Rust API change or a contract
migration, while on 0.x). When it merges, `tag.yml` tags the merge commit
and starts `release.yml` on the tag, which publishes the image and the
assets. Every migration is at least a patch release, so the schema version
is the core version.

Releases are immutable: release.yml publishes a release only once every
asset is attached, after which neither the assets nor the tag can change
(the repository's immutable-releases setting and its `refs/tags/v*`
ruleset). A broken release is never redone; it is fixed forward with the
next patch release.

## Using it

```toml
# the consumer's workspace Cargo.toml
[workspace.dependencies]
common   = { git = "https://github.com/Distant-Signal/ds-core", tag = "v0.1.0" }
ds-store = { git = "https://github.com/Distant-Signal/ds-core", tag = "v0.1.0" }
# ... one line per core crate, all on the same tag
```

To work on a core change and a consumer together, patch the consumer onto a
local checkout without committing it:

```sh
cargo --config 'patch."https://github.com/Distant-Signal/ds-core".common.path="../ds-core/crates/common"' test
```

A service that needs a new grant opens a pull request here changing
`crates/ds-store/db-grants.yaml` (with its migration, if any), then bumps
its core version.

## Contributing

Issues live in the
[landing repository](https://github.com/Distant-Signal/distant-signal/issues)
(label `area:core`); see [CONTRIBUTING.md](CONTRIBUTING.md).

## Licence

AGPL-3.0-or-later. See [LICENSE](LICENSE). The reference data keeps its own
terms: see `reference-data/README.md` and the README beside each crate's
data.
