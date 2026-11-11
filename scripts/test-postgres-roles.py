#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI: its progress report is stdout
r"""Run a command against a fresh database with the Postgres role split.

  scripts/test-postgres-roles.py [--mode new|existing|per-service]
      [--narrow ROLE,...] [--observe ROLE,...] [--keep] -- COMMAND...

Proves that no runtime path needs a superuser (docs/postgres-app-role.md).
Against the server in `DATABASE_URL` (a superuser, e.g. CI's `postgres`),
it creates a uniquely named database and four uniquely named roles with the
chart's own setup script (access/postgres-roles.sql, copied into the chart),
then runs COMMAND with

  DATABASE_URL=<the owner role>  MIGRATION_DATABASE_URL=<the owner role>

(the owner is the only non-superuser role besides the per-service ones: the
shared DML-only app role was dropped in release E; a service's own role is
DATABASE_URL_<ROLE> in --mode per-service). Afterwards it drops the database
and the roles (unless --keep).

--mode new (the default) is a brand-new cluster: the setup script runs on
the empty database first (the chart's initdb script), then the migrations
run as the owner role, then the setup script runs again (the chart's setup
Job). --mode existing is today's cluster being converted:
the migrations run as the superuser first, then the setup script moves
ownership to the owner role.

--mode per-service is --mode new plus Stage 0b (postgresql.roles.perService):
after each setup-script run, postgres-grants.sql (generated from
db-grants.yaml by scripts/gen-db-grants.py) creates the group roles and the
per-service roles, also uniquely named. COMMAND then also gets
DATABASE_URL_<ROLE> (DATABASE_URL_API, DATABASE_URL_AGGREGATOR, ...) for
every role db-grants.yaml creates, so it can run each crate's DB suite as
its own service's role:

  uv run scripts/test-postgres-roles.py --mode per-service -- sh -c \
      'DATABASE_URL="$DATABASE_URL_API" cargo test -p api -- --ignored ...'

That mode reads db-grants.yaml, so it needs PyYAML (`uv run`).

Every role is narrow (holds exactly its own grants): release D narrowed the
last four and release E dropped the app role they used to inherit from.

--narrow api,aggregator,... (per-service mode only) sets
DS_TEST_NARROWED_ROLES to the list, so a test can check a refusal only where
it applies (ds_store::test_support::role_is_narrowed). The roles are narrow
either way; this only says which refusals the run should assert.

--observe ROLE,... (per-service mode only) is for a suite that still seeds its
fixtures through its own role's pool: after the setup runs it gives those
roles table-wide DML on every table in schema public (SELECT only on
_sqlx_migrations) and USAGE on every sequence, test-only, as the app role's
members had before release E. A run with --narrow and no --observe asserts
the grants as they are. The two lists cannot overlap.

--check-app-retirement (existing and new modes) proves release E's drop of the
shared app role (postgres-roles.sql step 4) on a cluster that still has it:
it recreates the role as the old script left it (grants on every table,
default privileges, a membership), then checks that the setup run refuses
while a session is connected as it and while it owns an object, and that
once neither holds, the next run drops it with every trace (grants, default
privileges, memberships) and a further run changes nothing.

--check-setup-job runs the setup scripts once more, before COMMAND, as the
chart's setup Job does (files/postgres-roles-setup.sh, one try each), while
another session holds ACCESS SHARE on every relation in schema public in an
open transaction, with lock_timeout=2s: a steady-state release must take
no ACCESS EXCLUSIVE lock (no DROP/CREATE POLICY, no ALTER ... OWNER), or it
would queue behind every long reader and block everything queued behind it
(2026-10-14, Helm v252). In per-service mode it first proves the check
bites: with one row policy dropped, that run must fail with a lock timeout
(55P03), and the next run, without the reader, puts the policy back.

Needs psql (15+, for \getenv) and sqlx-cli on PATH. COMMAND is anything;
CI runs one pass of scripts/db-tests.py from its prebuilt test archive:

  scripts/test-postgres-roles.py --mode existing -- \
      uv run --no-sync scripts/db-tests.py run existing --archive-file A

and locally a plain cargo command works as well:

  scripts/test-postgres-roles.py -- cargo test -p api -- --ignored \
      --test-threads=1 --skip corpus::db_tests ...
"""

import argparse
import dataclasses
import importlib.util
import os
import pathlib
import secrets
import subprocess
import sys
import time
import types
import urllib.parse
from collections.abc import Mapping, Sequence
from typing import cast

ROOT = pathlib.Path(__file__).resolve().parent.parent
SETUP_SQL = ROOT / "access" / "postgres-roles.sql"
SETUP_JOB = ROOT / "access" / "postgres-roles-setup.sh"
MIGRATIONS = ROOT / "crates" / "ds-store" / "migrations"
GRANTS_SQL = ROOT / "access" / "postgres-grants.sql"
GEN_DB_GRANTS = ROOT / "scripts" / "gen-db-grants.py"
# The application_name of the session AccessShareReader opens.
READER = "test-postgres-roles-reader"
# One row policy postgres-grants.sql made, as `name ON table`.
FIRST_POLICY = (
    "SELECT format('%I ON public.%I', policyname, tablename) FROM pg_policies "
    "WHERE schemaname = 'public' AND policyname LIKE 'ds\\_grants\\_%' "
    "ORDER BY 1 LIMIT 1"
)
KINDS = ("owner", "exporter", "dump", "backup")


def gen_db_grants() -> types.ModuleType:
    """Import scripts/gen-db-grants.py once (it needs PyYAML)."""
    loaded = sys.modules.get("gen_db_grants")
    if loaded is not None:
        return loaded
    spec = importlib.util.spec_from_file_location("gen_db_grants", GEN_DB_GRANTS)
    if spec is None or spec.loader is None:
        msg = f"cannot load {GEN_DB_GRANTS}"
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules["gen_db_grants"] = module
    spec.loader.exec_module(module)
    return module


def per_service_kinds() -> tuple[list[str], list[str]]:
    """Return the group and created-role keys of db-grants.yaml (needs PyYAML)."""
    model = gen_db_grants().load()
    return list(model.groups), [role.key for role in model.created()]


def check_role_keys(keys: Sequence[str], created: Sequence[str]) -> None:
    """Refuse a key that is not a created role of db-grants.yaml."""
    unknown = [k for k in keys if k not in created]
    if unknown:
        msg = f"{unknown} are not created roles of db-grants.yaml: {sorted(created)}"
        raise RuntimeError(msg)


def observed_dml_sql(role: str) -> str:
    """Return the test-only grants `--observe` gives `role` (a quoted name)."""
    return (
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public "
        f"TO {role};"
        f" REVOKE INSERT, UPDATE, DELETE ON public._sqlx_migrations FROM {role};"
        f" GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO {role}"
    )


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


class AccessShareReader:
    """A session holding ACCESS SHARE on every relation in schema public.

    Like a long report or pg_dump: it conflicts only with ACCESS EXCLUSIVE.
    The locks are held, in an open transaction, until close().
    """

    def __init__(self, url: str) -> None:
        """Connect to url and take the locks."""
        print("== a reader takes ACCESS SHARE on every relation in public", flush=True)
        self.proc = subprocess.Popen(  # noqa: S603  # fixed argv
            ["psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-d", url],  # noqa: S607  # psql from PATH, as everywhere here
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            env={**os.environ, "PGAPPNAME": READER},
        )
        assert self.proc.stdin is not None  # noqa: S101  # stdin=PIPE above
        assert self.proc.stdout is not None  # noqa: S101  # stdout=PIPE above
        self.proc.stdin.write(
            "BEGIN;\n"
            "DO $lock$ DECLARE r record; BEGIN\n"
            "  FOR r IN SELECT c.oid::regclass AS name FROM pg_class c\n"
            "    JOIN pg_namespace n ON n.oid = c.relnamespace\n"
            "    WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm')\n"
            "  LOOP EXECUTE format('LOCK TABLE %s IN ACCESS SHARE MODE', r.name);\n"
            "  END LOOP; END $lock$;\n"
            "SELECT 'held ' || count(*) FROM pg_locks\n"
            "  WHERE pid = pg_backend_pid() AND locktype = 'relation'\n"
            "    AND mode = 'AccessShareLock';\n"
        )
        self.proc.stdin.flush()
        line = self.proc.stdout.readline().strip()
        if not line.startswith("held "):
            self.close()
            msg = f"the reader did not take its locks: {line!r}"
            raise RuntimeError(msg)
        print(f"== the reader holds ACCESS SHARE on {line[5:]} relations", flush=True)

    def close(self) -> None:
        """Roll back, releasing the locks, and disconnect."""
        if self.proc.stdin is not None and not self.proc.stdin.closed:
            self.proc.stdin.write("ROLLBACK;\n")
            self.proc.stdin.close()
        self.proc.wait(timeout=60)


def setup_job_env(url: str, base: Mapping[str, str]) -> dict[str, str]:
    """Return base plus libpq's variables for url, as the setup Job sets them.

    Fail-fast settings: one try, and a lock wait over 2 s fails.
    """
    parts = urllib.parse.urlsplit(url)
    # A `host` query parameter (a socket directory) wins, as in libpq.
    host = urllib.parse.parse_qs(parts.query).get("host", [parts.hostname or ""])
    env = dict(base)
    env.update(
        {
            "PGHOST": host[0] or "localhost",
            "PGPORT": str(parts.port or 5432),
            "PGUSER": urllib.parse.unquote(parts.username or ""),
            "PGPASSWORD": urllib.parse.unquote(parts.password or ""),
            "PGDATABASE": parts.path.lstrip("/"),
            "PGAPPNAME": "test-postgres-roles-setup-job",
            "PGOPTIONS": "-c lock_timeout=2s -c statement_timeout=120s",
            "DS_SETUP_ATTEMPTS": "1",
        }
    )
    return env


def run_setup_job(args: Sequence[str], env: Mapping[str, str]) -> tuple[int, str]:
    """Run one script through the setup Job's entrypoint; return status, log."""
    argv = ["sh", str(SETUP_JOB), "run", *args]
    print(f"== {' '.join(argv)}", flush=True)
    result = subprocess.run(  # noqa: S603  # argv from this script
        argv, env=dict(env), capture_output=True, text=True, check=False
    )
    print(result.stdout, end="", flush=True)
    return result.returncode, result.stdout


def check_setup_job(
    admin_url: str,
    scripts: Sequence[Sequence[str]],
    env: Mapping[str, str],
) -> None:
    """Prove a steady-state setup run takes no ACCESS EXCLUSIVE lock.

    scripts: the psql arguments of each setup script, in the Job's order.
    """
    job_env = setup_job_env(admin_url, env)
    policy = subprocess.run(  # noqa: S603  # fixed argv
        [  # noqa: S607  # psql from PATH
            "psql",
            "-X",
            "-q",
            "-A",
            "-t",
            "-d",
            admin_url,
            "-c",
            FIRST_POLICY,
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    if policy:
        # The check bites: a changed policy means DROP/CREATE POLICY, ACCESS
        # EXCLUSIVE, which the reader holds off until lock_timeout.
        psql(admin_url, "-c", f"DROP POLICY {policy}")
        reader = AccessShareReader(admin_url)
        try:
            # Old enough for the setup script's list of the sessions in the
            # way (transactions over 5 s).
            time.sleep(6)
            outputs = [run_setup_job(args, job_env) for args in scripts]
        finally:
            reader.close()
        status, output = outputs[-1]
        if status != 75 or "55P03" not in output:  # noqa: PLR2004  # EX_TEMPFAIL
            msg = (
                f"recreating policy {policy} behind a reader must time out "
                f"(exit 75, 55P03), not exit {status}"
            )
            raise RuntimeError(msg)
        if READER not in output:
            msg = f"the setup script's list of blockers must name {READER}"
            raise RuntimeError(msg)
        print(f"== as expected, recreating {policy} timed out behind the reader")
        for args in scripts:
            status, _ = run_setup_job(args, job_env)
            if status != 0:
                msg = f"the setup run without the reader exited {status}"
                raise RuntimeError(msg)
    reader = AccessShareReader(admin_url)
    try:
        for args in scripts:
            status, _ = run_setup_job(args, job_env)
            if status != 0:
                msg = (
                    f"a steady-state setup run exited {status} behind a reader "
                    "holding ACCESS SHARE: it takes an ACCESS EXCLUSIVE lock"
                )
                raise RuntimeError(msg)
    finally:
        reader.close()
    print("== the steady-state setup run took no ACCESS EXCLUSIVE lock", flush=True)


def scalar(url: str, query: str, **variables: str) -> str:
    """Return the one value `query` selects, with psql variables (`:'name'`)."""
    argv = ["psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-d", url]
    for name, value in variables.items():
        argv += ["-v", f"{name}={value}"]
    return subprocess.run(  # noqa: S603  # argv from this script
        argv, input=query, capture_output=True, text=True, check=True
    ).stdout.strip()


def wait_for_sessions(admin_url: str, legacy: str, *, present: bool) -> None:
    """Wait (up to 10 s) until `legacy` has a session, or has none."""
    query = "SELECT count(*) FROM pg_stat_activity WHERE usename = :'legacy'"
    for _ in range(50):
        if (scalar(admin_url, query, legacy=legacy) != "0") == present:
            return
        time.sleep(0.2)


@dataclasses.dataclass(frozen=True)
class LegacyApp:
    """The retired app role a check recreates, and the roles it touches."""

    name: str
    owner: str
    member: str


def check_app_retirement(
    admin_url: str,
    setup_args: Sequence[str],
    env: Mapping[str, str],
    legacy: LegacyApp,
) -> None:
    """Prove postgres-roles.sql drops a legacy app role, and only when safe.

    `legacy.name` is the role name setup_args passes as `app`; `legacy.owner`
    is the schema owner and `legacy.member` another role to make a member of it.
    """
    job_env = setup_job_env(admin_url, env)
    database = urllib.parse.urlsplit(admin_url).path.lstrip("/")
    print("== recreate the app role as the old setup script left it", flush=True)
    scalar(
        admin_url,
        "CREATE ROLE :\"legacy\" LOGIN PASSWORD 'legacy' CONNECTION LIMIT 5;\n"
        'GRANT CONNECT ON DATABASE :"database" TO :"legacy";\n'
        'GRANT USAGE ON SCHEMA public TO :"legacy";\n'
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public"
        ' TO :"legacy";\n'
        "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public"
        ' TO :"legacy";\n'
        'ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA public'
        ' GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"legacy";\n'
        'GRANT :"legacy" TO :"member";\n',
        legacy=legacy.name,
        owner=legacy.owner,
        member=legacy.member,
        database=database,
    )
    # A live session blocks the drop.
    session = subprocess.Popen(  # noqa: S603  # fixed argv
        [  # noqa: S607  # psql from PATH
            "psql",
            "-X",
            "-q",
            "-d",
            with_credentials(admin_url, legacy.name, "legacy", database),
            "-c",
            "SELECT pg_sleep(600)",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_sessions(admin_url, legacy.name, present=True)
        status, output = run_setup_job(setup_args, job_env)
        if status == 0 or "session(s) are still connected" not in output:
            msg = (
                f"the setup run must refuse to drop {legacy.name} while it has "
                f"a session (exit {status})"
            )
            raise RuntimeError(msg)
    finally:
        session.terminate()
        session.wait(timeout=60)
        # The backend is still in pg_sleep: it only notices the client is gone
        # when it next writes.
        scalar(
            admin_url,
            "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity"
            " WHERE usename = :'legacy'",
            legacy=legacy.name,
        )
    wait_for_sessions(admin_url, legacy.name, present=False)
    # An owned object blocks it, whatever the database.
    scalar(
        admin_url,
        "CREATE TABLE public.retire_probe (i int);\n"
        'ALTER TABLE public.retire_probe OWNER TO :"legacy";\n',
        legacy=legacy.name,
    )
    status, output = run_setup_job(setup_args, job_env)
    if status == 0 or "it owns" not in output:
        msg = (
            f"the setup run must refuse to drop {legacy.name} while it owns "
            f"an object (exit {status})"
        )
        raise RuntimeError(msg)
    scalar(admin_url, "DROP TABLE public.retire_probe")
    status, _ = run_setup_job(setup_args, job_env)
    if status != 0:
        msg = f"the setup run that drops {legacy.name} exited {status}"
        raise RuntimeError(msg)
    leftovers = scalar(
        admin_url,
        "SELECT (SELECT count(*) FROM pg_roles WHERE rolname = :'legacy')"
        " || ',' || (SELECT count(*) FROM pg_default_acl"
        " WHERE defaclacl::text LIKE '%' || :'legacy' || '%')"
        " || ',' || (SELECT count(*) FROM pg_auth_members m"
        " JOIN pg_roles r ON r.oid = m.roleid WHERE r.rolname = :'legacy')",
        legacy=legacy.name,
    )
    if leftovers != "0,0,0":
        msg = (
            f"{legacy.name} left traces after the drop (role, default ACLs, "
            f"memberships): {leftovers}"
        )
        raise RuntimeError(msg)
    status, _ = run_setup_job(setup_args, job_env)
    if status != 0:
        msg = f"a setup run after the drop exited {status}"
        raise RuntimeError(msg)
    print(
        f"== as expected, {legacy.name} was refused twice, then dropped cleanly",
        flush=True,
    )


def main() -> int:  # noqa: C901, PLR0912, PLR0915  # one linear setup, run, teardown
    """Set up, run the command, tear down; return the command's exit code."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--mode", choices=("new", "existing", "per-service"), default="new"
    )
    parser.add_argument(
        "--keep", action="store_true", help="keep the database and roles"
    )
    parser.add_argument(
        "--narrow",
        default="",
        metavar="ROLE,...",
        help="per-service: the roles whose refusals the run asserts",
    )
    parser.add_argument(
        "--observe",
        default="",
        metavar="ROLE,...",
        help="per-service: give these roles table-wide DML (test-only)",
    )
    parser.add_argument(
        "--check-app-retirement",
        action="store_true",
        help="prove the setup script drops a legacy app role, and only when safe",
    )
    parser.add_argument(
        "--check-setup-job",
        action="store_true",
        help="prove a steady-state setup run takes no ACCESS EXCLUSIVE lock",
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

    narrow = [k.strip() for k in cast("str", args.narrow).split(",") if k.strip()]
    observe = [k.strip() for k in cast("str", args.observe).split(",") if k.strip()]
    if (narrow or observe) and args.mode != "per-service":
        parser.error("--narrow and --observe need --mode per-service")
    if set(narrow) & set(observe):
        parser.error("a role cannot be both --narrow and --observe")

    suffix = secrets.token_hex(4)
    database = f"ds_role_split_{suffix}"
    names = {kind: f"ds_rs_{kind}_{suffix}" for kind in KINDS}
    passwords = {kind: secrets.token_hex(16) for kind in KINDS}
    groups: list[str] = []
    services: list[str] = []
    if args.mode == "per-service":
        groups, services = per_service_kinds()
        check_role_keys([*narrow, *observe], services)
    service_names = {kind: f"ds_rs_{kind}_{suffix}" for kind in [*groups, *services]}
    service_passwords = {kind: secrets.token_hex(16) for kind in services}
    owner_url = with_credentials(
        superuser_url, names["owner"], passwords["owner"], database
    )
    admin_url = with_credentials(
        superuser_url,
        superuser,
        urllib.parse.unquote(urllib.parse.urlsplit(superuser_url).password or ""),
        database,
    )

    setup_env = dict(os.environ)
    for kind, password in [*passwords.items(), *service_passwords.items()]:
        setup_env[f"DS_PG_{kind.upper()}_PASSWORD"] = password
    setup_args = [
        f"--variable=old_owner={superuser}",
        *(f"--variable={kind}={name}" for kind, name in names.items()),
        # The retired shared role (never created here, unless a check does).
        f"--variable=app=ds_rs_app_{suffix}",
        "--variable=owner_connection_limit=10",
        # The test database itself, so dropping it drops every grant.
        f"--variable=backup_database={database}",
        f"--file={SETUP_SQL}",
    ]
    grants_args = [
        *(f"--variable={kind}={name}" for kind, name in service_names.items()),
        f"--file={GRANTS_SQL}",
    ]
    migrate = ["sqlx", "migrate", "run", "--source", str(MIGRATIONS), "--database-url"]

    def setup() -> None:
        psql(admin_url, *setup_args, env=setup_env)
        if args.mode == "per-service":
            psql(admin_url, *grants_args, env=setup_env)

    psql(superuser_url, "-c", f'CREATE DATABASE "{database}"')
    status = 1
    try:
        if args.mode in {"new", "per-service"}:
            setup()
            run([*migrate, owner_url])
            # The chart's setup Job runs again after the migrations (a
            # post-install hook, and pre-upgrade after the migrate Job); it
            # (per service) grants the groups and roles on the tables that
            # now exist.
            setup()
        else:
            run([*migrate, admin_url])
            psql(admin_url, *setup_args, env=setup_env)
        if args.check_setup_job:
            check_setup_job(
                admin_url,
                [setup_args, grants_args]
                if args.mode == "per-service"
                else [setup_args],
                setup_env,
            )
        if args.check_app_retirement:
            if args.mode == "per-service":
                parser.error("--check-app-retirement needs --mode new or existing")
            check_app_retirement(
                admin_url,
                setup_args,
                setup_env,
                LegacyApp(f"ds_rs_app_{suffix}", names["owner"], names["exporter"]),
            )
        if observe:
            psql(
                admin_url,
                "-c",
                "; ".join(observed_dml_sql(f'"{service_names[k]}"') for k in observe),
            )
            print(f"== table-wide DML for {', '.join(observe)} (--observe)")
        # Test-only: the migration-fixture tests (legacy_backfill's,
        # full_coverage_line_stats_history_migration) build throwaway
        # schemas as the owner. Production's owner role needs no CREATE on
        # the database.
        psql(
            admin_url,
            "-c",
            f'GRANT CREATE ON DATABASE "{database}" TO "{names["owner"]}"',
        )
        # Test-only, likewise: two DB tests build TEMP tables (ds-store's
        # schema gate test, as the owner or api role; ingest-writer's
        # stream_apply, as the owner or writer role). postgres-roles.sql
        # revokes TEMPORARY from PUBLIC (security review L5); no service
        # uses temporary tables at runtime.
        temp_roles = [names["owner"]] + [
            service_names[k] for k in ("api", "writer") if k in service_names
        ]
        psql(
            admin_url,
            "-c",
            f'GRANT TEMPORARY ON DATABASE "{database}" TO '
            + ", ".join(f'"{role}"' for role in temp_roles),
        )
        test_env = dict(os.environ)
        test_env["DATABASE_URL"] = owner_url
        test_env["MIGRATION_DATABASE_URL"] = owner_url
        for kind in services:
            test_env[f"DATABASE_URL_{kind.upper()}"] = with_credentials(
                superuser_url, service_names[kind], service_passwords[kind], database
            )
        # Tests that prove a narrowed role is refused a write check this
        # (ds_store::test_support::role_is_narrowed).
        test_env["DS_TEST_NARROWED_ROLES"] = ",".join(narrow)
        if services:
            print(f"== DATABASE_URL_<ROLE> for {', '.join(services)}", flush=True)
        owner_name = names["owner"]
        print(
            f"== DATABASE_URL={owner_name}, MIGRATION_DATABASE_URL={owner_name}",
            flush=True,
        )
        status = subprocess.run(command, check=False, env=test_env).returncode  # noqa: S603  # the caller's own command
        print(f"== command exited {status}")
    finally:
        if args.keep:
            kept = ", ".join([*names.values(), *service_names.values()])
            print(f"kept database {database} and roles {kept}")
        else:
            psql(
                superuser_url,
                "-c",
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)',
            )
            # The service roles before the groups they belong to.
            for name in [
                *(service_names[k] for k in services),
                *(service_names[k] for k in groups),
                *names.values(),
            ]:
                psql(superuser_url, "-c", f'DROP ROLE IF EXISTS "{name}"')
    return status


if __name__ == "__main__":
    sys.exit(main())
