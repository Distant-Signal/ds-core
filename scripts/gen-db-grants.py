#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI whose output (the failures) is stdout
r"""Render and check the Postgres grants in db-grants.yaml.

  uv run scripts/gen-db-grants.py render [--output FILE]
  uv run scripts/gen-db-grants.py check [--database-url URL]

`render` writes charts/distant-signal/files/postgres-grants.sql: an
idempotent psql script, run as a superuser after postgres-roles.sql (the
chart's role setup Job, when `postgresql.roles.perService.enabled`). It
creates the group roles (`read_shared`, `schema_gate`) and every role whose
status is not `planned`, with LOGIN, a CONNECTION LIMIT and a password from
DS_PG_<ROLE>_PASSWORD; makes each `observed` role a member of the app role
with no grants of its own (phase 0b); gives each `narrow` role exactly the
grants in the YAML; gives the groups their grants; and creates the
`row_policies` (a RESTRICTIVE policy `ds_grants_<role>` per table and
created role, dropping stale `ds_grants_*` ones). Tables that do not exist
yet (a new cluster's initdb run) are skipped.

`check` fails (exit 1, one line per problem) when:
  - the YAML is malformed: an unknown class, role, group or privilege, or
    a row policy on an unlisted table;
  - the connection limits of every role in the YAML (planned ones too) plus
    `other_roles` exceed max_connections minus superuser_reserved_connections;
  - postgres-grants.sql is not exactly what `render` writes;
  - with --database-url (a migrated database; CI's rust-test job): a table,
    view, sequence or function in schema public (extension members aside)
    is missing from the YAML, or the YAML lists one the database lacks.

Needs PyYAML (pyproject.toml's lint group) and, for --database-url, psql.
"""

from __future__ import annotations

import argparse
import dataclasses
import pathlib
import re
import subprocess
import sys
from typing import TYPE_CHECKING, cast

import yaml

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

ROOT = pathlib.Path(__file__).resolve().parent.parent
FILES = ROOT / "charts" / "distant-signal" / "files"
GRANTS_YAML = FILES / "db-grants.yaml"
GRANTS_SQL = FILES / "postgres-grants.sql"
SQL_TEMPLATE = ROOT / "scripts" / "db-grants.sql.tpl"

CLASSES = frozenset(
    {
        "personal",
        "shared-train",
        "ingest",
        "derived",
        "reference",
        "internal",
        "migrations",
    }
)
# Classes the read_shared group can SELECT.
READ_SHARED_CLASSES = frozenset({"shared-train", "ingest", "derived", "reference"})
STATUSES = frozenset({"planned", "observed", "narrow"})
PRIVILEGES = {"S": "SELECT", "I": "INSERT", "U": "UPDATE", "D": "DELETE"}
GROUPS = ("read_shared", "schema_gate")
# A role key is also a psql variable name and an env var suffix.
KEY_RE = re.compile(r"^[a-z][a-z0-9_]*$")
NAME_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
SIGNATURE_RE = re.compile(r"^([a-z_][a-z0-9_]*)\((.*)\)$")


class GrantsError(Exception):
    """The YAML does not describe a valid set of grants."""


@dataclasses.dataclass(frozen=True)
class Grant:
    """One role's privileges on one table."""

    role: str
    privileges: str  # a subset of "SIUD", in that order
    columns: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True)
class Table:
    """A table in schema public."""

    name: str
    cls: str
    grants: tuple[Grant, ...]


@dataclasses.dataclass(frozen=True)
class Role:
    """A login role of one service."""

    key: str
    name: str
    status: str
    groups: tuple[str, ...]
    connection_limit: int


@dataclasses.dataclass(frozen=True)
class Model:
    """The parsed db-grants.yaml."""

    max_connections: int
    superuser_reserved: int
    other_limits: Mapping[str, int]
    groups: Mapping[str, str]
    roles: Mapping[str, Role]
    tables: Mapping[str, Table]
    views: Mapping[str, Table]
    sequences: Mapping[str, str]  # sequence -> owning table
    functions: Mapping[str, tuple[str, ...]]  # signature -> roles
    # table -> role -> the SQL condition of its RESTRICTIVE policy
    row_policies: Mapping[str, Mapping[str, str]] = dataclasses.field(
        default_factory=dict
    )

    def budget(self) -> int:
        """Return the connections available to non-superusers."""
        return self.max_connections - self.superuser_reserved

    def limit_sum(self) -> int:
        """Every role's connection limit, planned ones included."""
        return sum(r.connection_limit for r in self.roles.values()) + sum(
            self.other_limits.values()
        )

    def created(self) -> list[Role]:
        """Return the roles postgres-grants.sql creates (not `planned`)."""
        return [r for r in self.roles.values() if r.status != "planned"]


def _mapping(value: object, where: str) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        msg = f"{where}: expected a mapping"
        raise GrantsError(msg)
    return {str(k): v for k, v in cast("dict[object, object]", value).items()}


def _int(value: object, where: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        msg = f"{where}: expected a whole number"
        raise GrantsError(msg)
    return value


def _str(value: object, where: str) -> str:
    if not isinstance(value, str) or not value:
        msg = f"{where}: expected a non-empty string"
        raise GrantsError(msg)
    return value


def _privileges(value: str, where: str) -> str:
    if (
        not value
        or any(c not in PRIVILEGES for c in value)
        or len(set(value)) != len(value)
    ):
        msg = f"{where}: privileges {value!r} must be letters from SIUD, each once"
        raise GrantsError(msg)
    return "".join(c for c in "SIUD" if c in value)


def _grants(value: object, where: str, roles: Mapping[str, Role]) -> tuple[Grant, ...]:
    grants: list[Grant] = []
    for role, spec in _mapping(value, where).items():
        if role not in roles:
            msg = f"{where}: unknown role {role!r}"
            raise GrantsError(msg)
        if isinstance(spec, str):
            grants.append(Grant(role, _privileges(spec, f"{where}.{role}")))
            continue
        body = _mapping(spec, f"{where}.{role}")
        privileges = _privileges(
            _str(body.get("privileges"), f"{where}.{role}.privileges"),
            f"{where}.{role}",
        )
        columns_raw = body.get("columns", [])
        if not isinstance(columns_raw, list) or not columns_raw:
            msg = f"{where}.{role}.columns: expected a non-empty list"
            raise GrantsError(msg)
        columns = tuple(
            _str(c, f"{where}.{role}.columns")
            for c in cast("list[object]", columns_raw)
        )
        grants.append(Grant(role, privileges, columns))
    return tuple(grants)


def _roles(raw: dict[str, object], groups: Mapping[str, str]) -> dict[str, Role]:
    roles: dict[str, Role] = {}
    for key, spec in _mapping(raw.get("roles"), "roles").items():
        where = f"roles.{key}"
        if not KEY_RE.match(key):
            msg = f"{where}: a role key must match {KEY_RE.pattern}"
            raise GrantsError(msg)
        body = _mapping(spec, where)
        status = _str(body.get("status"), f"{where}.status")
        if status not in STATUSES:
            msg = f"{where}.status: {status!r} is not one of {sorted(STATUSES)}"
            raise GrantsError(msg)
        groups_raw = body.get("groups", [])
        if not isinstance(groups_raw, list):
            msg = f"{where}.groups: expected a list"
            raise GrantsError(msg)
        member_of = tuple(_str(g, f"{where}.groups") for g in groups_raw)
        unknown = [g for g in member_of if g not in groups]
        if unknown:
            msg = f"{where}.groups: unknown group(s) {unknown}"
            raise GrantsError(msg)
        name = _str(body.get("name"), f"{where}.name")
        if not NAME_RE.match(name):
            msg = f"{where}.name: {name!r} is not a plain lower-case identifier"
            raise GrantsError(msg)
        roles[key] = Role(
            key,
            name,
            status,
            member_of,
            _int(body.get("connection_limit"), f"{where}.connection_limit"),
        )
    return roles


def _tables(raw: object, section: str, roles: Mapping[str, Role]) -> dict[str, Table]:
    tables: dict[str, Table] = {}
    for name, spec in _mapping(raw, section).items():
        where = f"{section}.{name}"
        body = _mapping(spec, where)
        cls = _str(body.get("class"), f"{where}.class")
        if cls not in CLASSES:
            msg = f"{where}.class: {cls!r} is not one of {sorted(CLASSES)}"
            raise GrantsError(msg)
        tables[name] = Table(
            name, cls, _grants(body.get("grants"), f"{where}.grants", roles)
        )
    return tables


def _row_policies(
    raw: object, tables: Mapping[str, Table], roles: Mapping[str, Role]
) -> dict[str, dict[str, str]]:
    """Parse `row_policies`: table -> role -> SQL condition."""
    policies: dict[str, dict[str, str]] = {}
    for table, spec in _mapping(raw, "row_policies").items():
        if table not in tables:
            msg = f"row_policies.{table}: not a listed table"
            raise GrantsError(msg)
        by_role: dict[str, str] = {}
        for role, condition in _mapping(spec, f"row_policies.{table}").items():
            if role not in roles:
                msg = f"row_policies.{table}: unknown role {role!r}"
                raise GrantsError(msg)
            by_role[role] = _str(condition, f"row_policies.{table}.{role}")
        policies[table] = by_role
    return policies


def parse(raw_obj: object) -> Model:
    """Validate and parse a loaded db-grants.yaml."""
    raw = _mapping(raw_obj, "db-grants.yaml")
    if raw.get("version") != 1:
        msg = "version: only version 1 is understood"
        raise GrantsError(msg)
    budget = _mapping(raw.get("budget"), "budget")
    groups = {
        key: _str(_mapping(spec, f"groups.{key}").get("name"), f"groups.{key}.name")
        for key, spec in _mapping(raw.get("groups"), "groups").items()
    }
    if sorted(groups) != sorted(GROUPS):
        msg = f"groups: exactly {list(GROUPS)} are expected"
        raise GrantsError(msg)
    roles = _roles(raw, groups)
    other_limits = {
        key: _int(
            _mapping(spec, f"other_roles.{key}").get("connection_limit"),
            f"other_roles.{key}.connection_limit",
        )
        for key, spec in _mapping(raw.get("other_roles"), "other_roles").items()
    }
    tables = _tables(raw.get("tables"), "tables", roles)
    views = _tables(raw.get("views"), "views", roles)
    sequences: dict[str, str] = {}
    for name, spec in _mapping(raw.get("sequences"), "sequences").items():
        table = _str(_mapping(spec, f"sequences.{name}").get("table"), name)
        if table not in tables:
            msg = f"sequences.{name}.table: {table!r} is not a listed table"
            raise GrantsError(msg)
        sequences[name] = table
    functions: dict[str, tuple[str, ...]] = {}
    for signature, spec in _mapping(raw.get("functions"), "functions").items():
        if not SIGNATURE_RE.match(signature):
            msg = f"functions: {signature!r} is not name(argtypes)"
            raise GrantsError(msg)
        grantees = _mapping(spec, f"functions.{signature}").get("grants", [])
        if not isinstance(grantees, list):
            msg = f"functions.{signature}.grants: expected a list of roles"
            raise GrantsError(msg)
        names = tuple(_str(g, f"functions.{signature}") for g in grantees)
        unknown = [g for g in names if g not in roles]
        if unknown:
            msg = f"functions.{signature}.grants: unknown role(s) {unknown}"
            raise GrantsError(msg)
        functions[signature] = names
    row_policies = _row_policies(raw.get("row_policies"), tables, roles)
    clash = set(tables) & set(views)
    if clash:
        msg = f"listed as both a table and a view: {sorted(clash)}"
        raise GrantsError(msg)
    return Model(
        max_connections=_int(budget.get("max_connections"), "budget.max_connections"),
        superuser_reserved=_int(
            budget.get("superuser_reserved_connections"),
            "budget.superuser_reserved_connections",
        ),
        other_limits=other_limits,
        groups=groups,
        roles=roles,
        tables=tables,
        views=views,
        sequences=sequences,
        functions=functions,
        row_policies=row_policies,
    )


def load(path: pathlib.Path = GRANTS_YAML) -> Model:
    """Parse db-grants.yaml at `path`."""
    return parse(yaml.safe_load(path.read_text(encoding="utf-8")))


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _values(rows: Iterable[Sequence[str]], columns: str) -> str:
    """Return a VALUES list, or a typed empty one."""
    body = ",\n        ".join(
        "(" + ", ".join(_sql_literal(v) for v in row) + ")" for row in rows
    )
    if not body:
        width = len(columns.split(","))
        nulls = ", ".join(["NULL::text"] * width)
        return f"(SELECT {nulls} WHERE false) AS v({columns})"
    return f"(VALUES\n        {body}) AS v({columns})"


def _psql_defaults(model: Model) -> list[str]:
    lines: list[str] = []

    def default(var: str, value: str) -> None:
        lines.extend([f"\\if :{{?{var}}}", "\\else", f"\\set {var} {value}", "\\endif"])

    default("app", "distant_signal_app")
    for key, name in model.groups.items():
        default(key, name)
    for role in model.created():
        default(role.key, role.name)
        default(f"{role.key}_connection_limit", str(role.connection_limit))
    for role in model.created():
        env = f"DS_PG_{role.key.upper()}_PASSWORD"
        lines.append(f"\\getenv {role.key}_password {env}")
        default(f"{role.key}_password", "''")
    return lines


def _settings(model: Model) -> str:
    def setting(var: str) -> str:
        return f"    set_config('ds_grants.{var}', :'{var}', true)"

    names = ["app", *model.groups]
    for role in model.created():
        names += [role.key, f"{role.key}_password", f"{role.key}_connection_limit"]
    return ",\n".join(setting(n) for n in names)


def _narrow_rows(
    model: Model,
) -> tuple[list[tuple[str, ...]], list[tuple[str, ...]], list[tuple[str, ...]]]:
    """Return the table, sequence and function grant rows of narrow roles."""
    narrow = {r.key for r in model.created() if r.status == "narrow"}
    tables: list[tuple[str, ...]] = []
    sequences: list[tuple[str, ...]] = []
    for table in [*model.tables.values(), *model.views.values()]:
        for grant in table.grants:
            if grant.role not in narrow:
                continue
            columns = ",".join(grant.columns)
            tables.extend(
                (table.name, grant.role, PRIVILEGES[letter], columns)
                for letter in grant.privileges
            )
            if "I" in grant.privileges:
                sequences.extend(
                    (seq, grant.role)
                    for seq, owner in model.sequences.items()
                    if owner == table.name
                )
    functions: list[tuple[str, ...]] = [
        (signature, role)
        for signature, roles in model.functions.items()
        for role in roles
        if role in narrow
    ]
    return tables, sequences, functions


def render(model: Model) -> str:
    """Return the postgres-grants.sql text for `model`."""
    created = model.created()
    role_rows = [(r.key, r.status) for r in created]
    group_rows = [(g,) for g in model.groups]
    table_rows, sequence_rows, function_rows = _narrow_rows(model)
    created_keys = {r.key for r in created}
    policy_rows = [
        (table, role, condition)
        for table, by_role in model.row_policies.items()
        for role, condition in by_role.items()
        if role in created_keys
    ]
    password_env = ", ".join(f"DS_PG_{r.key.upper()}_PASSWORD" for r in created)
    parts = {
        "PASSWORD_ENV": password_env or "none (every role is planned)",
        "PSQL_DEFAULTS": "\n".join(_psql_defaults(model)),
        "SETTINGS": _settings(model),
        "GROUP_ROWS": _values(group_rows, "kind"),
        "ROLE_ROWS": _values(role_rows, "kind, status"),
        "MEMBER_ROWS": _values(
            [(r.key, g) for r in created for g in r.groups], "kind, grp"
        ),
        "GRANTEE_ROWS": _values([*group_rows, *((k,) for k, _ in role_rows)], "kind"),
        "READ_SHARED_ROWS": _values(
            [(t.name,) for t in model.tables.values() if t.cls in READ_SHARED_CLASSES],
            "tbl",
        ),
        "TABLE_ROWS": _values(table_rows, "tbl, kind, priv, cols"),
        "SEQUENCE_ROWS": _values(sequence_rows, "seq, kind"),
        "FUNCTION_ROWS": _values(function_rows, "sig, kind"),
        "POLICY_ROWS": _values(policy_rows, "tbl, kind, cond"),
    }
    text = SQL_TEMPLATE.read_text(encoding="utf-8")
    for name, value in parts.items():
        text = text.replace(f"@@{name}@@", value)
    leftover = re.findall(r"@@[A-Z_]+@@", text)
    if leftover:
        msg = f"{SQL_TEMPLATE.name}: unknown placeholders {leftover}"
        raise GrantsError(msg)
    return text


def problems_in_model(model: Model) -> list[str]:
    """Budget and consistency problems of a parsed model."""
    problems: list[str] = []
    total = model.limit_sum()
    if total > model.budget():
        problems.append(
            f"connection limits sum to {total}, more than max_connections "
            f"({model.max_connections}) minus superuser_reserved_connections "
            f"({model.superuser_reserved}) = {model.budget()}"
        )
    names = [r.name for r in model.roles.values()] + list(model.groups.values())
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        problems.append(f"role names used twice: {duplicates}")
    return problems


def database_objects(database_url: str) -> dict[str, set[str]]:
    """Tables, views, sequences and functions in schema public, via psql."""
    query = """
SELECT CASE WHEN c.relkind IN ('r', 'p', 'f') THEN 'table'
            WHEN c.relkind IN ('v', 'm') THEN 'view'
            ELSE 'sequence' END, c.relname
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'f', 'v', 'm', 'S')
  AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass
                  AND d.objid = c.oid AND d.deptype = 'e')
UNION ALL
SELECT 'function', p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')'
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'public'
  AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass
                  AND d.objid = p.oid AND d.deptype = 'e')
"""
    argv = ["psql", "-X", "-A", "-t", "-F", "\t", "-v", "ON_ERROR_STOP=1"]
    argv += ["-d", database_url, "-c", query]
    # psql from PATH, as scripts/test-postgres-roles.py runs it.
    result = subprocess.run(  # noqa: S603  # argv built here; the URL is the caller's
        argv, check=True, capture_output=True, text=True
    )
    objects: dict[str, set[str]] = {
        "table": set(),
        "view": set(),
        "sequence": set(),
        "function": set(),
    }
    for line in result.stdout.splitlines():
        if line.strip():
            kind, name = line.split("\t", 1)
            objects[kind].add(name)
    return objects


def problems_against_database(
    model: Model, objects: Mapping[str, set[str]]
) -> list[str]:
    """Differences between the YAML and the migrated schema."""
    # Signatures from pg_get_function_identity_arguments name the
    # arguments ("analyze_publish_keys(target text)"); the YAML lists types.
    listed: dict[str, set[str]] = {
        "table": set(model.tables),
        "view": set(model.views),
        "sequence": set(model.sequences),
        "function": {s.split("(", 1)[0] for s in model.functions},
    }
    actual = dict(objects)
    actual["function"] = {s.split("(", 1)[0] for s in objects.get("function", set())}
    problems: list[str] = []
    for kind, names in listed.items():
        have = actual.get(kind, set())
        problems.extend(
            f"{kind} {name} is in schema public but not classified in db-grants.yaml"
            for name in sorted(have - names)
        )
        problems.extend(
            f"{kind} {name} is in db-grants.yaml but not in the migrated schema"
            for name in sorted(names - have)
        )
    return problems


def main(argv: Sequence[str] | None = None) -> int:
    """Run `render` or `check`."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    render_p = sub.add_parser("render", help="write postgres-grants.sql")
    render_p.add_argument("--output", type=pathlib.Path, default=GRANTS_SQL)
    check_p = sub.add_parser("check", help="check the YAML, the SQL and a database")
    check_p.add_argument("--database-url", help="a migrated database to compare with")
    check_p.add_argument("--yaml", type=pathlib.Path, default=GRANTS_YAML)
    check_p.add_argument("--sql", type=pathlib.Path, default=GRANTS_SQL)
    args = parser.parse_args(argv)

    if args.command == "render":
        output = cast("pathlib.Path", args.output)
        output.write_text(render(load()), encoding="utf-8")
        print(f"wrote {output}")
        return 0

    try:
        model = load(cast("pathlib.Path", args.yaml))
    except GrantsError as err:
        print(f"db-grants.yaml: {err}")
        return 1
    problems = problems_in_model(model)
    sql_path = cast("pathlib.Path", args.sql)
    current = sql_path.read_text(encoding="utf-8") if sql_path.exists() else ""
    if current != render(model):
        problems.append(
            f"{sql_path.name} is stale: run `uv run scripts/gen-db-grants.py render`"
        )
    if args.database_url:
        problems += problems_against_database(
            model, database_objects(cast("str", args.database_url))
        )
    for problem in problems:
        print(problem)
    if not problems:
        created = ", ".join(r.name for r in model.created()) or "none"
        print(
            f"ok: {len(model.tables)} tables, {len(model.views)} views, "
            f"{len(model.sequences)} sequences, {len(model.functions)} functions; "
            f"connection limits {model.limit_sum()}/{model.budget()}; "
            f"created roles: {created}"
        )
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
