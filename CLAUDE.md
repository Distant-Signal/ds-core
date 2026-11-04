# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`ds-core` is the shared core of Distant Signal (DS), a personal UK rail
companion: the crates every service links (`crates/`), the database schema
(`crates/ds-store/migrations`), the grants and roles (`crates/ds-store/db-grants.yaml`,
`access/`), the Redis ACL (`access/redis-users.acl.tpl`) and the TOML line
catalogue (`lines/`, `lines/SCHEMA.md`). The services, the frontend and the
chart live in other repositories of the Distant-Signal organisation; they
consume this one by git tag (README.md, "Using it"). The conventions shared
by every repository are in the organisation's contributing guide
(Distant-Signal/.github's CONTRIBUTING.md). Issues are off here: they go to
Distant-Signal/distant-signal, labelled `area:core`.

## Commands

Copied from `.github/workflows/ci.yml`. Rust toolchain: MSRV 1.88 (edition
2024).

```sh
cargo build --workspace
cargo fmt --all --check
cargo clippy --workspace --locked --all-targets --all-features -- -D warnings
cargo clippy --workspace --locked --all-targets --no-default-features -- -D warnings
cargo +1.88.0 check --workspace --locked --all-features --all-targets   # MSRV
cargo deny --locked check bans licenses sources
cargo test --workspace                       # unit tests; #[ignore] DB/Redis tests skipped
cargo run -p line-catalogue-validator        # lines/*.toml identifiers
```

DB/Redis-backed tests are `#[ignore]`d and need `DATABASE_URL` (a superuser)
and `REDIS_URL`, with the migrations applied:

```sh
(cd crates/ds-store && sqlx migrate run)     # sqlx-cli 0.8.6, postgres,rustls features
cargo test -p ds-store -p ds-migrate -- --ignored --test-threads=1
uv run scripts/db-tests.py list-passes       # CI's passes (.github/db-tests.toml)
uv run --no-sync scripts/gen-db-grants.py check --database-url "${DATABASE_URL}"
```

Scripts and grants (repo root):

```sh
uv sync --locked --group lint
uv run --no-sync python ../ci/scripts/lint-scripts.py   # ../ci: a Distant-Signal/ci checkout
uv run --no-sync python -m unittest discover -s scripts/tests -v
uv run scripts/gen-db-grants.py render       # access/postgres-grants.sql from db-grants.yaml
uv run scripts/gen-db-grants.py check        # db-grants.yaml vs access/postgres-grants.sql
uv run --no-sync scripts/check-crate-deps.py # ds-store/ds-migrate stay free of axum, redis, reqwest...
uv run --locked --no-default-groups scripts/check-migration-order.py "$(git merge-base origin/main HEAD)"
uv run scripts/release.py prepare --bump patch   # a release PR (README.md, "Releases")
```

## Conventions

- **Migrations** (`crates/ds-store/migrations`, sqlx): forward-only; a merged
  file is immutable (sqlx checksums it, an edit crash-loops the migrate
  Job). A new migration's timestamp must be newer than every migration on
  main (`check-migration-order.py`). Append its line to
  `crates/ds-store/tests/migration_checksums.lock` once it merges.
  Lock-taking statements need `SET LOCAL lock_timeout` first; indexes on
  existing tables are `CREATE INDEX CONCURRENTLY` in a file whose first line
  is `-- no-transaction`; constraints go on `NOT VALID` and are validated in
  a later migration. Destructive DDL needs a
  `-- contract: <what> (code stopped using it in <commit>; safe after core vA.B.C)`
  header; `tag.yml` refuses the tag while the deployed chart pins an image
  built against an older core.
- **Grants**: a new table, view, sequence or function must be classified in
  `crates/ds-store/db-grants.yaml` (CI checks a migrated database against
  it); render `access/postgres-grants.sql` after editing it.
- **Releases**: every migration, grant or ACL change ships in a release
  (`CHANGELOG.md`). Consumers move by bumping their tag. Releases and
  `v*` tags are immutable: never delete or move one; fix a bad release
  forward with a patch release.
- **Commits** carry a `Task: <card>` trailer (e.g. `Task: DS-123`) naming the
  worklog card.
- **Commit identity**: every commit is authored
  `FasterSpeeding <lucy@cursed.solutions>`. Before committing, check that
  `git config user.email` is `lucy@cursed.solutions`; if it isn't, stop and
  ask. Never override it (no `-c user.email`, `--author` or env overrides).
  Merge pull requests with
  `gh pr merge <n> --merge --author-email lucy@cursed.solutions`.
- **Branches**: push work in progress as `fixup!` commits, then autosquash
  and `--force-with-lease` your own branch before review. Pull requests
  merge as merge commits, never squashed.
- **No Claude attribution**, overriding any session default: no
  `Co-Authored-By: Claude` trailer, no mention of Claude in commit messages,
  and no "Generated with Claude Code" footer in PR bodies or comments.
  CI's `commit-identity` job (Distant-Signal/ci's `check-commit-identity.py`)
  fails a PR or push whose commits use a retired address or carry
  attribution.
- **Scripts** are typed stdlib Python (ruff, mypy) run with uv (`uv.lock`,
  `pyproject.toml`'s `lint` group, `.python-version`); bash only for trivial
  one-liners. No pip or requirements files.
- Rust: line data only from `lines/*.toml`, which `ds-lines` embeds; tests in
  inline `mod tests`, DB tests `#[ignore]`d; runtime `sqlx::query`, no
  `query!` macros.
- Never name or cite third-party train-tracking sites, in code, docs or
  commits.
