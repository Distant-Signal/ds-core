#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI whose output (the findings) is stdout
"""Fail if migrations changed unsafely since BASE (DB review 2026-09-27).

  uv run scripts/check-migration-order.py BASE

Fails (exit 1) if, since BASE (A4/A5, DB2-35, INF-16):
  - a migration was added whose version is not greater than every
    migration BASE already had; or
  - a migration BASE already had was modified, deleted or renamed; or
  - a checked migration holds destructive DDL without a contract header
    (below).

sqlx applies any local migration missing from _sqlx_migrations whatever
its version, so a branch whose migration timestamp predates one already
merged (and deployed) runs it late, out of order, with no error. Give it a
later timestamp instead.

A merged migration is immutable: sqlx compares each applied file's SHA-384
with _sqlx_migrations.checksum at startup, so an edited file crash-loops
the api on every database that already ran it, and a renamed or deleted
one leaves an applied row with no file. This diff-against-BASE check needs
no upkeep; crates/api/tests/migration_checksums.rs (the checksum lock)
covers the same ground for migrations recorded in its .lock file, including
changes made outside a PR.

Destructive DDL needs a contract header (ingest spec section 9.6, "expand
before code, contract after code"; plan task 1B.5):
  -- contract: <what> (code stopped using it in <commit>)
in the file's leading comment block (after `-- no-transaction`, if any).
Such a migration must ship at least one release after every binary that
used the object. Destructive means:
  - DROP TABLE / VIEW / MATERIALIZED VIEW / FUNCTION / PROCEDURE / TYPE /
    SEQUENCE / SCHEMA, and ALTER TABLE ... DROP [COLUMN];
  - any RENAME (ALTER TABLE/VIEW/INDEX/TYPE/... RENAME [COLUMN|TO|VALUE]);
  - ALTER TABLE ... ALTER [COLUMN] c [SET DATA] TYPE ...;
  - ALTER TABLE ... ALTER [COLUMN] c SET NOT NULL.
Column changes on a table created, or a column added, earlier in the same
file are exempt (no deployed code can use them yet), as is a DROP TABLE of
a table the same file created (a scratch table). A DO block's body is
scanned like top-level SQL; other dollar-quoted bodies (function
definitions) are not, since they do not run at migration time. Comments
and string literals are ignored, so dynamic DDL (EXECUTE '...') is not
seen: review it by hand.

The contract check covers the migrations added since BASE plus every
migration newer than CONTRACT_CHECK_CUTOFF, the newest migration when the
check was introduced (2026-10-07). Older ones are grandfathered: they are
applied in production, and adding a header would change their sqlx
checksums, which crates/api/tests/migration_checksums.rs locks.

BASE is a commit, e.g. `$(git merge-base origin/main HEAD)` for a branch, or
the pre-push commit for a push. Run it before merging a worktree branch
locally:
  uv run scripts/check-migration-order.py "$(git merge-base main HEAD)"

Paths are relative to the current directory, which must be the repo root
(as in CI). Findings are GitHub Actions `::error file=...::` lines on
stdout. A git failure (e.g. an unknown BASE) exits with git's status.
Stdlib only.
"""

import argparse
import re
import subprocess
import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field

MIGRATIONS = "crates/api/migrations"
# The version is the leading digits of the file name: <dir>/<version>_<name>.sql.
VERSION = re.compile(r".*/([0-9]+)_[^/]*\.sql")
# Migrations at or below this version predate the contract check and are
# only scanned when added since BASE; see the module docstring.
CONTRACT_CHECK_CUTOFF = 20261007210000
CONTRACT_HEADER = re.compile(r"--\s*contract:\s*\S.*")
# DROP <keyword> → the finding's name for it.
DROPPABLE = {
    "TABLE": "DROP TABLE",
    "VIEW": "DROP VIEW",
    "MATERIALIZED": "DROP MATERIALIZED VIEW",
    "FUNCTION": "DROP FUNCTION",
    "PROCEDURE": "DROP PROCEDURE",
    "TYPE": "DROP TYPE",
    "SEQUENCE": "DROP SEQUENCE",
    "SCHEMA": "DROP SCHEMA",
}
# CREATE [modifier ...] TABLE.
TABLE_MODIFIERS = {"TEMP", "TEMPORARY", "UNLOGGED", "GLOBAL", "LOCAL"}
# ALTER TABLE ... ADD <one of these> adds a constraint, not a column.
CONSTRAINT_STARTS = {"CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN", "EXCLUDE"}
# ALTER TABLE ... ADD/DROP [COLUMN] <guard> name.
COLUMN_GUARDS = {"ADD": ("IF", "NOT", "EXISTS"), "DROP": ("IF", "EXISTS")}
# In a DO block, a DDL statement may follow these PL/pgSQL keywords.
DDL_VERBS = {"CREATE", "DROP", "ALTER"}
PLPGSQL_LEADS = {"BEGIN", "THEN", "ELSE", "LOOP"}
DOLLAR_TAG = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")
WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")


class GitError(Exception):
    """A git command failed; its stderr has already been shown."""

    def __init__(self, returncode: int) -> None:
        """Record git's exit status."""
        super().__init__(f"git exited {returncode}")
        self.returncode = returncode


def git_output(*args: str) -> str:
    """Run git and return its stdout; git's stderr passes through."""
    result = subprocess.run(  # noqa: S603  # fixed git subcommands
        ["git", *args],  # noqa: S607  # git from PATH
        check=False,
        stdout=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise GitError(result.returncode)
    return result.stdout


def git(*args: str) -> list[str]:
    """Run git and return its non-empty stdout lines."""
    return [line for line in git_output(*args).split("\n") if line]


@dataclass(frozen=True)
class Token:
    """A word (upper-cased; lower-cased if quoted) or one punctuation char."""

    text: str
    line: int
    # A double-quoted identifier: never a keyword.
    quoted: bool = False
    # The body of a dollar-quoted string ($$...$$ or $tag$...$tag$).
    body: str | None = None


def _skip_block_comment(sql: str, i: int) -> int:
    """Return the index after the (nestable) block comment starting at i."""
    depth = 0
    while i < len(sql):
        if sql.startswith("/*", i):
            depth += 1
            i += 2
        elif sql.startswith("*/", i):
            depth -= 1
            i += 2
            if depth == 0:
                return i
        else:
            i += 1
    return i


def _skip_string(sql: str, i: int) -> int:
    """Return the index after the single-quoted literal starting at i."""
    i += 1
    while i < len(sql):
        if sql[i] == "\\" or sql.startswith("''", i):
            # A backslash escape (E'...') or a doubled quote.
            i += 2
        elif sql[i] == "'":
            return i + 1
        else:
            i += 1
    return i


def _skip_noise(sql: str, i: int) -> int:
    """Return the index after whitespace, a comment or a literal at i (or i)."""
    if sql[i].isspace():
        return i + 1
    if sql.startswith("--", i):
        end = sql.find("\n", i)
        return len(sql) if end < 0 else end
    if sql.startswith("/*", i):
        return _skip_block_comment(sql, i)
    if sql[i] == "'":
        return _skip_string(sql, i)
    return i


def _next_token(sql: str, i: int, line: int) -> tuple[Token | None, int]:
    """Read one token (None for whitespace, a comment or a literal) at i."""
    if (end := _skip_noise(sql, i)) != i:
        return None, end
    ch = sql[i]
    if ch == '"':
        end = sql.find('"', i + 1)
        end = len(sql) if end < 0 else end
        return Token(sql[i + 1 : end].lower(), line, quoted=True), end + 1
    if (tag := DOLLAR_TAG.match(sql, i)) is not None:
        delimiter = tag.group(0)
        end = sql.find(delimiter, tag.end())
        end = len(sql) if end < 0 else end
        return Token("$$", line, body=sql[tag.end() : end]), end + len(delimiter)
    if (word := WORD.match(sql, i)) is not None:
        return Token(word.group(0).upper(), line), word.end()
    return Token(ch, line), i + 1


def tokenize(sql: str) -> list[Token]:
    """Split SQL into words and punctuation, dropping comments and literals."""
    tokens: list[Token] = []
    i = 0
    line = 1
    while i < len(sql):
        token, end = _next_token(sql, i, line)
        if token is not None:
            tokens.append(token)
        line += sql.count("\n", i, end)
        i = end
    return tokens


def statements(tokens: list[Token]) -> Iterator[list[Token]]:
    """Yield the token lists between top-level semicolons."""
    current: list[Token] = []
    for token in tokens:
        if token.text == ";" and not token.quoted:
            if current:
                yield current
            current = []
        else:
            current.append(token)
    if current:
        yield current


def _kw(tokens: list[Token], i: int) -> str:
    """Return the keyword at i ('' for a quoted identifier or past the end)."""
    if i < len(tokens) and not tokens[i].quoted:
        return tokens[i].text
    return ""


def _ident(tokens: list[Token], i: int) -> str:
    """Return the identifier at i, lower-cased ('' past the end)."""
    return tokens[i].text.lower() if i < len(tokens) else ""


def _name(tokens: list[Token], i: int) -> tuple[str, int]:
    """Read a maybe schema-qualified name at i: (its last part, next i)."""
    name = ""
    while i < len(tokens):
        name = _ident(tokens, i)
        i += 1
        if _kw(tokens, i) != ".":
            break
        i += 1
    return name, i


def _skip(tokens: list[Token], i: int, *words: str) -> int:
    """Skip the optional keyword sequence at i, if all of it is there."""
    if all(_kw(tokens, i + n) == word for n, word in enumerate(words)):
        return i + len(words)
    return i


def _actions(tokens: list[Token]) -> Iterator[list[Token]]:
    """Split an ALTER TABLE's action list on top-level commas."""
    depth = 0
    current: list[Token] = []
    for token in tokens:
        if not token.quoted and token.text == "(":
            depth += 1
        elif not token.quoted and token.text == ")":
            depth -= 1
        elif not token.quoted and token.text == "," and depth == 0:
            yield current
            current = []
            continue
        current.append(token)
    if current:
        yield current


@dataclass
class _Scope:
    """What the migration has created so far: changing these is not a contract."""

    tables: set[str] = field(default_factory=set)
    columns: set[tuple[str, str]] = field(default_factory=set)


def _table_action(table: str, action: list[Token], scope: _Scope) -> str | None:
    """Return the finding for one ALTER TABLE action, recording added columns."""
    verb = _kw(action, 0)
    j = _skip(action, 1, "COLUMN")
    j = _skip(action, j, *COLUMN_GUARDS.get(verb, ()))
    column = _ident(action, j)
    finding = None
    if verb == "ADD" and _kw(action, j) not in CONSTRAINT_STARTS:
        scope.columns.add((table, column))
    elif verb == "DROP" and _kw(action, 1) != "CONSTRAINT":
        finding = f"ALTER TABLE {table} DROP COLUMN {column}"
    elif verb == "ALTER":
        rest = [_kw(action, n) for n in range(j + 1, j + 4)]
        what = f"ALTER TABLE {table} ALTER COLUMN {column}"
        if rest[0] == "TYPE" or rest == ["SET", "DATA", "TYPE"]:
            finding = f"{what} TYPE"
        elif rest == ["SET", "NOT", "NULL"]:
            finding = f"{what} SET NOT NULL"
    if table in scope.tables or (table, column) in scope.columns:
        return None
    return finding


def _alter(statement: list[Token], scope: _Scope) -> Iterator[tuple[int, str]]:
    """Yield (line, finding) for an ALTER statement."""
    kind = _kw(statement, 1)
    i = 2
    if kind == "MATERIALIZED":
        kind, i = "MATERIALIZED VIEW", 3
    i = _skip(statement, i, "IF", "EXISTS")
    i = _skip(statement, i, "ONLY")
    name, i = _name(statement, i)
    if any(_kw(statement, n) == "RENAME" for n in range(len(statement))):
        yield statement[0].line, f"ALTER {kind} {name} ... RENAME"
    elif kind == "TABLE":
        for action in _actions(statement[i:]):
            if (what := _table_action(name, action, scope)) is not None:
                yield action[0].line, what


def _create(statement: list[Token], scope: _Scope) -> None:
    """Record the table a CREATE TABLE statement creates."""
    i = _skip(statement, 1, "OR", "REPLACE")
    while _kw(statement, i) in TABLE_MODIFIERS:
        i += 1
    if _kw(statement, i) == "TABLE":
        i = _skip(statement, i + 1, "IF", "NOT", "EXISTS")
        scope.tables.add(_name(statement, i)[0])


def _drop(statement: list[Token], scope: _Scope) -> Iterator[tuple[int, str]]:
    """Yield the finding for a DROP of a contract object kind."""
    kind = DROPPABLE.get(_kw(statement, 1))
    if kind is None:
        return
    i = 3 if kind == "DROP MATERIALIZED VIEW" else 2
    i = _skip(statement, i, "IF", "EXISTS")
    name = _name(statement, i)[0]
    if not (kind == "DROP TABLE" and name in scope.tables):
        yield statement[0].line, f"{kind} {name}"


def _do_block(statement: list[Token], scope: _Scope) -> Iterator[tuple[int, str]]:
    """Yield the findings in a DO block's body, on the file's line numbers."""
    for token in statement:
        if token.body is not None:
            for inner, what in destructive_statements(token.body, scope, plpgsql=True):
                yield token.line + inner - 1, what


def _statement_findings(
    statement: list[Token], scope: _Scope
) -> Iterator[tuple[int, str]]:
    """Yield (line, finding) for one statement, recording what it creates."""
    match _kw(statement, 0):
        case "DO":
            yield from _do_block(statement, scope)
        case "CREATE":
            _create(statement, scope)
        case "DROP":
            yield from _drop(statement, scope)
        case "ALTER":
            yield from _alter(statement, scope)
        case _:
            pass


def _plpgsql_statement(statement: list[Token]) -> list[Token]:
    """Drop a PL/pgSQL statement's control-flow prefix (BEGIN, IF ... THEN)."""
    for i in range(len(statement)):
        if _kw(statement, i) in DDL_VERBS and (
            i == 0 or _kw(statement, i - 1) in PLPGSQL_LEADS
        ):
            return statement[i:]
    return statement


def destructive_statements(
    sql: str, scope: _Scope | None = None, *, plpgsql: bool = False
) -> list[tuple[int, str]]:
    """Return (line, description) for each destructive statement in SQL.

    With plpgsql (a DO block's body), each statement's control-flow prefix
    is skipped first. DDL run through EXECUTE '...' is not seen.
    """
    scope = _Scope() if scope is None else scope
    return [
        finding
        for statement in statements(tokenize(sql))
        for finding in _statement_findings(
            _plpgsql_statement(statement) if plpgsql else statement, scope
        )
    ]


def has_contract_header(sql: str) -> bool:
    """Whether SQL's leading comment block holds a `-- contract: ...` line."""
    for raw in sql.splitlines():
        line = raw.strip()
        if not line:
            continue
        if not line.startswith("--"):
            return False
        if CONTRACT_HEADER.fullmatch(line):
            return True
    return False


def contract_findings(path: str, sql: str) -> list[str]:
    """Return ::error lines for destructive DDL in SQL without the header."""
    if has_contract_header(sql):
        return []
    return [
        f"::error file={path},line={line}::{path}:{line}: {what} is destructive "
        "DDL. Add a `-- contract: <what> (code stopped using it in <commit>)` "
        "header to the file's leading comments, and ship it at least one "
        "release after every binary that used the object (ingest spec "
        "section 9.6)."
        for line, what in destructive_statements(sql)
    ]


def version_of(path: str) -> str | None:
    """Return a migration path's version digits, or None if it has none."""
    match = VERSION.fullmatch(path)
    return match.group(1) if match else None


def check_contracts(added: list[str]) -> int:
    """Print the contract findings for HEAD's checked migrations; 1 if any."""
    newer = [
        path
        for path in git("ls-tree", "-r", "--name-only", "HEAD", "--", f"{MIGRATIONS}/")
        if (version := version_of(path)) is not None
        and int(version) > CONTRACT_CHECK_CUTOFF
    ]
    checked = sorted(set(newer) | {path for path in added if path.endswith(".sql")})
    status = 0
    for path in checked:
        findings = contract_findings(path, git_output("show", f"HEAD:{path}"))
        for finding in findings:
            print(finding)
        status |= 1 if findings else 0
    if not status:
        print(
            f"no destructive DDL without a contract header in {len(checked)} "
            f"checked migration(s) (added since BASE, or newer than "
            f"{CONTRACT_CHECK_CUTOFF})"
        )
    return status


def check(base: str) -> int:
    """Print the findings for BASE..HEAD; 1 if any, else 0."""
    base_versions = [
        version
        for path in git("ls-tree", "-r", "--name-only", base, "--", f"{MIGRATIONS}/")
        if (version := version_of(path)) is not None
    ]
    if not base_versions:
        print(f"no migrations at {base}; nothing to compare")
        return 0
    max_base = max(base_versions, key=int)

    status = 0

    # --no-renames reports a rename as a deletion plus an addition, so the old
    # name fails here and the new name is checked as an added file below.
    changed = [
        (kind, path)
        for line in git(
            "diff",
            "--no-renames",
            "--name-status",
            "--diff-filter=MDT",
            base,
            "HEAD",
            "--",
            f"{MIGRATIONS}/",
        )
        for kind, _, path in [line.partition("\t")]
        if path.endswith(".sql")
    ]
    for kind, path in changed:
        what = "deleted (or renamed)" if kind == "D" else "modified"
        print(
            f"::error file={path}::{path} already exists on the base and was "
            f"{what}. Merged migrations are immutable: sqlx checks every applied "
            "file's checksum at startup. Add a new migration instead."
        )
        status = 1

    added = [
        path
        for path in git(
            "diff",
            "--no-renames",
            "--name-only",
            "--diff-filter=A",
            base,
            "HEAD",
            "--",
            f"{MIGRATIONS}/",
        )
        if path.endswith(".sql")
    ]
    for path in added:
        version = version_of(path)
        if version is None:
            print(f"::error file={path}::cannot read a numeric version from {path}")
            status = 1
        elif int(version) <= int(max_base):
            print(
                f"::error file={path}::{path} (version {version}) is not newer than "
                f"the newest migration on the base ({max_base}); sqlx would apply "
                "it out of order. Give it a later timestamp."
            )
            status = 1
        else:
            print(f"ok: {path} ({version} > {max_base})")

    if not added:
        print(f"no migrations added since {base} (newest there: {max_base})")
    if not changed:
        print(f"no existing migrations modified, deleted or renamed since {base}")
    return status | check_contracts(added)


def main(argv: Sequence[str] | None = None) -> int:
    """Parse BASE and run the check; returns the exit status."""
    parser = argparse.ArgumentParser(
        description="Check that migrations since BASE are new and newer than "
        "BASE's, and that destructive DDL carries a contract header."
    )
    parser.add_argument("base", metavar="BASE", help="the commit to compare HEAD with")
    args = parser.parse_args(argv)
    try:
        return check(args.base)
    except GitError as error:
        return error.returncode


if __name__ == "__main__":
    sys.exit(main())
