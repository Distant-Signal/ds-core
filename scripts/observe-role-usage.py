#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI: the report goes to stdout or --output
r"""Report which tables each Postgres role really uses, against db-grants.yaml.

  uv run scripts/observe-role-usage.py --database-url URL [--output REPORT.md]
  uv run scripts/observe-role-usage.py --from-file STATEMENTS [--output ...]

Phase 0b of the ingest architecture (spec §6.5): every service connects as
its own role (still a member of the app role), and after about 7 days
pg_stat_statements holds each role's statements. This script reads them,
READ-ONLY, extracts the tables each statement touches and with which verb,
and writes a Markdown report per role with a diff against the role's
grants in charts/distant-signal/files/db-grants.yaml:

  - "used, not granted": would fail with `permission denied` once the role
    is narrowed. Fix db-grants.yaml (or the code) before narrowing;
  - "granted, not seen": may be unneeded, or just not exercised in the
    window (a monthly CORPUS load, a rare error path). Check before
    removing.

Verbs: S (read: FROM, JOIN, USING, a subquery, and the WHERE/RETURNING of
an UPDATE or DELETE), I (INSERT), U (UPDATE; also INSERT ... ON CONFLICT DO
UPDATE and SELECT ... FOR UPDATE/SHARE, which need it), D (DELETE).
pg_stat_statements text is normalised ($1 for constants) and may be
truncated by track_activity_query_size; only names listed in db-grants.yaml
count, so catalog queries and functions are ignored. It is a heuristic,
not a SQL parser: the per-role DB suites in CI stay the real proof.

--database-url connects with psql in a read-only transaction. Production is
reached through kubectl instead, so --from-file reads the same rows saved
from psql (NUL-separated: role, calls, query):

  kubectl -n distant-signal exec -i distant-signal-postgres-0 -- \
    psql -U distant_signal -d distant_signal -X -q -A -t -z -0 \
      -c "SET default_transaction_read_only = on" -c "$(
        uv run scripts/observe-role-usage.py --print-query)" > statements.bin

Needs PyYAML (pyproject.toml's lint group).
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import pathlib
import re
import subprocess
import sys
from typing import TYPE_CHECKING, cast

import yaml

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

ROOT = pathlib.Path(__file__).resolve().parent.parent
GRANTS_YAML = ROOT / "charts" / "distant-signal" / "files" / "db-grants.yaml"
READ_SHARED_CLASSES = frozenset({"shared-train", "ingest", "derived", "reference"})
VERBS = "SIUD"

QUERY = """SELECT r.rolname, s.calls, s.query
FROM pg_stat_statements s
JOIN pg_roles r ON r.oid = s.userid
WHERE s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND r.rolname LIKE 'distant\\_signal\\_%'
ORDER BY r.rolname, s.calls DESC"""

_STRING = re.compile(r"'(?:[^']|'')*'")
_LINE_COMMENT = re.compile(r"--[^\n]*")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_IDENT = r"(?:\"?(?:public\"?\.\"?)?)([a-z_][a-z0-9_$]*)\"?"
_CTE = re.compile(
    r"(?:\bwith\b(?:\s+recursive)?|,)\s+([a-z_][a-z0-9_]*)\s*(?:\([^)]*\)\s*)?"
    r"as\s+(?:not\s+)?(?:materialized\s+)?\("
)
_INSERT = re.compile(r"\binsert\s+into\s+" + _IDENT)
_UPDATE = re.compile(
    r"(?<!\bdo\s)(?<!\bfor\s)(?<!\bkey\s)\bupdate\s+(?:only\s+)?" + _IDENT
)
_DELETE = re.compile(r"\bdelete\s+from\s+(?:only\s+)?" + _IDENT)
_READ = re.compile(r"\b(?:from|join|using)\s+(?:only\s+|lateral\s+)?" + _IDENT)
_FROM_LIST_MORE = re.compile(r"\s*(?:as\s+)?(?:[a-z_][a-z0-9_]*\s*)?,\s*" + _IDENT)
_LOCK = re.compile(r"\bfor\s+(?:no\s+key\s+)?(?:update|share)\b")
_CONFLICT_UPDATE = re.compile(r"\bon\s+conflict\b[^;]*?\bdo\s+update\b")


def normalise(sql: str) -> str:
    """Lower-case `sql` without comments and string literals."""
    sql = _BLOCK_COMMENT.sub(" ", sql)
    sql = _LINE_COMMENT.sub(" ", sql)
    sql = _STRING.sub("''", sql)
    return re.sub(r"\s+", " ", sql.lower()).strip()


def _writes(text: str, add: Callable[[str, str], None]) -> None:
    """Record INSERT, UPDATE and DELETE targets."""
    filtered = " where " in f" {text} " or " returning " in f" {text} "
    for match in _INSERT.finditer(text):
        add(match.group(1), "I")
        if _CONFLICT_UPDATE.search(text, match.end()):
            add(match.group(1), "U")
            add(match.group(1), "S")
    for pattern, verb in ((_UPDATE, "U"), (_DELETE, "D")):
        for match in pattern.finditer(text):
            add(match.group(1), verb)
            if filtered:
                add(match.group(1), "S")


def _reads(text: str, add: Callable[[str, str], None]) -> None:
    """Record FROM, JOIN and USING sources (and FROM a, b lists)."""
    for match in _READ.finditer(text):
        # `DELETE FROM t` is a delete, not a read of t.
        if not text[: match.start()].rstrip().endswith("delete"):
            add(match.group(1), "S")
        position = match.end()
        while more := _FROM_LIST_MORE.match(text, position):
            add(more.group(1), "S")
            position = more.end()


def verbs_by_table(sql: str, known: Iterable[str]) -> dict[str, set[str]]:
    """Return {table: verbs} for one statement, limited to `known` tables."""
    text = normalise(sql)
    known_set = set(known)
    ctes = set(_CTE.findall(text)) - known_set
    used: dict[str, set[str]] = {}

    def add(table: str, verb: str) -> None:
        if table in known_set and table not in ctes:
            used.setdefault(table, set()).add(verb)

    _writes(text, add)
    _reads(text, add)
    if _LOCK.search(text):
        # SELECT ... FOR UPDATE/SHARE needs UPDATE on the locked tables.
        for table, verbs in list(used.items()):
            if "S" in verbs:
                add(table, "U")
    return used


@dataclasses.dataclass(frozen=True)
class Statement:
    """One pg_stat_statements row."""

    role: str
    calls: int
    query: str


def parse_rows(data: bytes) -> list[Statement]:
    """Rows from `psql -A -t -z -0` (NUL between fields and between rows)."""
    fields = data.decode("utf-8", errors="replace").split("\0")
    if fields and fields[-1] in {"", "\n"}:
        fields.pop()
    rows: list[Statement] = []
    for i in range(0, len(fields) - 2, 3):
        role, calls, query = fields[i : i + 3]
        # A stray command tag ("SET" without psql -q) precedes the first row.
        role_name = role.strip().splitlines()[-1] if role.strip() else ""
        rows.append(Statement(role_name, int(calls.strip() or 0), query))
    return rows


def fetch(database_url: str) -> list[Statement]:
    """Read pg_stat_statements read-only over psql."""
    argv = ["psql", "-X", "-A", "-t", "-z", "-0", "-v", "ON_ERROR_STOP=1"]
    argv += ["-d", database_url, "-c", QUERY]
    env = dict(os.environ)
    env["PGOPTIONS"] = (
        env.get("PGOPTIONS", "") + " -c default_transaction_read_only=on"
    ).strip()
    # psql from PATH, as scripts/test-postgres-roles.py runs it.
    result = subprocess.run(  # noqa: S603  # argv built here; the URL is the caller's
        argv, check=True, capture_output=True, env=env
    )
    return parse_rows(result.stdout)


@dataclasses.dataclass
class RoleUsage:
    """What one role was seen doing."""

    calls: int = 0
    statements: int = 0
    tables: dict[str, set[str]] = dataclasses.field(default_factory=dict)


def observe(rows: Iterable[Statement], known: Iterable[str]) -> dict[str, RoleUsage]:
    """Aggregate statements into per-role table verbs."""
    known_list = list(known)
    usage: dict[str, RoleUsage] = {}
    for row in rows:
        role = usage.setdefault(row.role, RoleUsage())
        role.calls += row.calls
        role.statements += 1
        for table, verbs in verbs_by_table(row.query, known_list).items():
            role.tables.setdefault(table, set()).update(verbs)
    return usage


def granted(raw: Mapping[str, object]) -> dict[str, dict[str, set[str]]]:
    """Return {role name: {table: verbs}} from db-grants.yaml, read_shared included."""
    roles = cast("dict[str, dict[str, object]]", raw.get("roles") or {})
    tables = cast("dict[str, dict[str, object]]", raw.get("tables") or {})
    result: dict[str, dict[str, set[str]]] = {}
    for key, role in roles.items():
        name = str(role["name"])
        groups = cast("list[str]", role.get("groups") or [])
        mine: dict[str, set[str]] = {}
        for table, spec in tables.items():
            grants = cast("dict[str, object]", spec.get("grants") or {})
            if "read_shared" in groups and spec.get("class") in READ_SHARED_CLASSES:
                mine.setdefault(table, set()).add("S")
            if key in grants:
                value = grants[key]
                privileges = (
                    value
                    if isinstance(value, str)
                    else str(cast("dict[str, object]", value)["privileges"])
                )
                mine.setdefault(table, set()).update(privileges)
        if "schema_gate" in groups:
            mine.setdefault("_sqlx_migrations", set()).add("S")
        result[name] = mine
    return result


def _fmt(verbs: Iterable[str]) -> str:
    return "".join(v for v in VERBS if v in set(verbs))


def report(
    usage: Mapping[str, RoleUsage], grants: Mapping[str, Mapping[str, set[str]]]
) -> str:
    """Return the Markdown report."""
    lines = [
        "# Role usage report",
        "",
        "From pg_stat_statements (scripts/observe-role-usage.py), compared with",
        "charts/distant-signal/files/db-grants.yaml. S/I/U/D as in that file.",
        "",
    ]
    for role in sorted(set(usage) | set(grants)):
        seen = usage.get(role, RoleUsage())
        mine = grants.get(role, {})
        lines += [
            f"## {role}",
            "",
            f"{seen.statements} statements, {seen.calls} calls.",
            "",
        ]
        if role not in grants:
            lines += ["Not in db-grants.yaml.", ""]
        missing = {
            t: v - mine.get(t, set())
            for t, v in seen.tables.items()
            if v - mine.get(t, set())
        }
        unused = {
            t: v - seen.tables.get(t, set())
            for t, v in mine.items()
            if v - seen.tables.get(t, set())
        }
        lines += ["| Table | Seen | Granted | Used, not granted | Granted, not seen |"]
        lines += ["|---|---|---|---|---|"]
        for table in sorted(set(seen.tables) | set(mine)):
            lines.append(
                f"| `{table}` | {_fmt(seen.tables.get(table, ()))} | "
                f"{_fmt(mine.get(table, ()))} | {_fmt(missing.get(table, ()))} | "
                f"{_fmt(unused.get(table, ()))} |"
            )
        lines += [
            "",
            (
                f"Used, not granted: {len(missing)} tables. "
                f"Granted, not seen: {len(unused)} tables."
            ),
            "",
        ]
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Fetch or read the statements, then write the report."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--database-url", help="read pg_stat_statements over psql")
    source.add_argument("--from-file", type=pathlib.Path, help="saved psql -z -0 rows")
    source.add_argument(
        "--print-query", action="store_true", help="print the SQL and exit"
    )
    parser.add_argument("--output", type=pathlib.Path, help="write the report here")
    parser.add_argument("--yaml", type=pathlib.Path, default=GRANTS_YAML)
    args = parser.parse_args(argv)
    if args.print_query:
        print(QUERY)
        return 0
    raw = cast(
        "dict[str, object]",
        yaml.safe_load(cast("pathlib.Path", args.yaml).read_text(encoding="utf-8")),
    )
    tables = list(cast("dict[str, object]", raw.get("tables") or {}))
    tables += list(cast("dict[str, object]", raw.get("views") or {}))
    if args.database_url:
        rows = fetch(cast("str", args.database_url))
    else:
        rows = parse_rows(cast("pathlib.Path", args.from_file).read_bytes())
    text = report(observe(rows, tables), granted(raw))
    if args.output:
        cast("pathlib.Path", args.output).write_text(text, encoding="utf-8")
        print(f"wrote {args.output}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
