#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI: its progress report is stdout
r"""Run a command against a fresh database with the Postgres role split.

  scripts/test-postgres-roles.py [--mode new|existing] [--keep] -- COMMAND...

Proves that no runtime path needs a superuser (docs/postgres-app-role.md).
Against the server in `DATABASE_URL` (a superuser, e.g. CI's `postgres`),
it creates a uniquely named database and five uniquely named roles with the
chart's own setup script (charts/distant-signal/files/postgres-roles.sql),
then runs COMMAND with

  DATABASE_URL=<the app role>  MIGRATION_DATABASE_URL=<the owner role>

exactly as the chart wires the api and the workers once
`postgresql.roles.enabled` is on. Afterwards it drops the database and the
roles (unless --keep).

--mode new (the default) is a brand-new cluster: the setup script runs on
the empty database first (the chart's initdb script), then the migrations
run as the owner role, then the setup script runs again (the chart's setup
Job). --mode existing is today's cluster being converted:
the migrations run as the superuser first, then the setup script moves
ownership to the owner role.

Needs psql (15+, for \getenv) and sqlx-cli on PATH. Example, as CI runs it:

  scripts/test-postgres-roles.py -- cargo test -p api -- --ignored \
      --test-threads=1 --skip corpus::db_tests ...
"""

import argparse
import os
import pathlib
import secrets
import subprocess
import sys
import urllib.parse
from collections.abc import Mapping, Sequence

ROOT = pathlib.Path(__file__).resolve().parent.parent
SETUP_SQL = ROOT / "charts" / "distant-signal" / "files" / "postgres-roles.sql"
MIGRATIONS = ROOT / "crates" / "api" / "migrations"
KINDS = ("owner", "app", "exporter", "dump", "backup")


def with_credentials(url: str, user: str, password: str, database: str) -> str:
    """Return `url` with its user, password and database replaced."""
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname or "localhost"
    if ":" in host:
        host = f"[{host}]"
    port = f":{parts.port}" if parts.port else ""
    quote = urllib.parse.quote
    netloc = f"{quote(user, safe='')}:{quote(password, safe='')}@{host}{port}"
    return urllib.parse.urlunsplit(
        (parts.scheme, netloc, f"/{database}", parts.query, "")
    )


def run(argv: Sequence[str], env: Mapping[str, str] | None = None) -> None:
    """Run argv, echoing it (never the environment, which holds passwords)."""
    print(f"== {' '.join(argv)}", flush=True)
    subprocess.run(argv, check=True, env=None if env is None else dict(env))  # noqa: S603  # argv from this script and its caller


def psql(url: str, *args: str, env: Mapping[str, str] | None = None) -> None:
    """Run psql against url with ON_ERROR_STOP."""
    run(["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-d", url, *args], env=env)


def main() -> int:
    """Set up, run the command, tear down; return the command's exit code."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=("new", "existing"), default="new")
    parser.add_argument(
        "--keep", action="store_true", help="keep the database and roles"
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command: list[str] = (
        args.command[1:] if args.command[:1] == ["--"] else args.command
    )
    if not command:
        parser.error("no command given after --")

    superuser_url = os.environ.get("DATABASE_URL", "")
    if not superuser_url:
        parser.error("DATABASE_URL (a superuser on the test server) is not set")
    superuser = urllib.parse.unquote(
        urllib.parse.urlsplit(superuser_url).username or ""
    )
    if not superuser:
        parser.error("DATABASE_URL must name its user")

    suffix = secrets.token_hex(4)
    database = f"ds_role_split_{suffix}"
    names = {kind: f"ds_rs_{kind}_{suffix}" for kind in KINDS}
    passwords = {kind: secrets.token_hex(16) for kind in KINDS}
    owner_url = with_credentials(
        superuser_url, names["owner"], passwords["owner"], database
    )
    app_url = with_credentials(superuser_url, names["app"], passwords["app"], database)
    admin_url = with_credentials(
        superuser_url,
        superuser,
        urllib.parse.unquote(urllib.parse.urlsplit(superuser_url).password or ""),
        database,
    )

    setup_env = dict(os.environ)
    for kind, password in passwords.items():
        setup_env[f"DS_PG_{kind.upper()}_PASSWORD"] = password
    setup_args = [
        f"--variable=old_owner={superuser}",
        *(f"--variable={kind}={name}" for kind, name in names.items()),
        "--variable=app_connection_limit=60",
        "--variable=owner_connection_limit=10",
        # The test database itself, so dropping it drops every grant.
        f"--variable=backup_database={database}",
        f"--file={SETUP_SQL}",
    ]
    migrate = ["sqlx", "migrate", "run", "--source", str(MIGRATIONS), "--database-url"]

    psql(superuser_url, "-c", f'CREATE DATABASE "{database}"')
    status = 1
    try:
        if args.mode == "new":
            psql(admin_url, *setup_args, env=setup_env)
            run([*migrate, owner_url])
            # The chart's setup Job runs again after the migrations (a
            # post-install/post-upgrade hook); it takes back the app role's
            # write access to _sqlx_migrations, which the owner's default
            # privileges granted when the migrator created it.
            psql(admin_url, *setup_args, env=setup_env)
        else:
            run([*migrate, admin_url])
            psql(admin_url, *setup_args, env=setup_env)
        # Test-only: the migration-fixture tests (legacy_backfill's,
        # full_coverage_line_stats_history_migration) build throwaway
        # schemas as the owner. Production's owner role needs no CREATE on
        # the database.
        psql(
            admin_url,
            "-c",
            f'GRANT CREATE ON DATABASE "{database}" TO "{names["owner"]}"',
        )
        test_env = dict(os.environ)
        test_env["DATABASE_URL"] = app_url
        test_env["MIGRATION_DATABASE_URL"] = owner_url
        print(
            f"== DATABASE_URL={names['app']}, MIGRATION_DATABASE_URL={names['owner']}",
            flush=True,
        )
        status = subprocess.run(command, check=False, env=test_env).returncode  # noqa: S603  # the caller's own command
        print(f"== command exited {status}")
    finally:
        if args.keep:
            print(f"kept database {database} and roles {', '.join(names.values())}")
        else:
            psql(
                superuser_url,
                "-c",
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)',
            )
            for name in names.values():
                psql(superuser_url, "-c", f'DROP ROLE IF EXISTS "{name}"')
    return status


if __name__ == "__main__":
    sys.exit(main())
