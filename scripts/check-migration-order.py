#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI whose output (the findings) is stdout
"""Fail if migrations changed unsafely since BASE (DB review 2026-09-27).

  uv run scripts/check-migration-order.py BASE

Fails (exit 1) if, since BASE (A4/A5, DB2-35, INF-16):
  - a migration was added whose version is not greater than every
    migration BASE already had; or
  - a migration BASE already had was modified, deleted or renamed.

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
from collections.abc import Sequence

MIGRATIONS = "crates/api/migrations"
# The version is the leading digits of the file name: <dir>/<version>_<name>.sql.
VERSION = re.compile(r".*/([0-9]+)_[^/]*\.sql")


class GitError(Exception):
    """A git command failed; its stderr has already been shown."""

    def __init__(self, returncode: int) -> None:
        """Record git's exit status."""
        super().__init__(f"git exited {returncode}")
        self.returncode = returncode


def git(*args: str) -> list[str]:
    """Run git and return its stdout lines; git's stderr passes through."""
    result = subprocess.run(  # noqa: S603  # fixed git subcommands
        ["git", *args],  # noqa: S607  # git from PATH
        check=False,
        stdout=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise GitError(result.returncode)
    return [line for line in result.stdout.split("\n") if line]


def version_of(path: str) -> str | None:
    """Return a migration path's version digits, or None if it has none."""
    match = VERSION.fullmatch(path)
    return match.group(1) if match else None


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
    return status


def main(argv: Sequence[str] | None = None) -> int:
    """Parse BASE and run the check; returns the exit status."""
    parser = argparse.ArgumentParser(
        description="Check that migrations since BASE are new and newer than BASE's."
    )
    parser.add_argument("base", metavar="BASE", help="the commit to compare HEAD with")
    args = parser.parse_args(argv)
    try:
        return check(args.base)
    except GitError as error:
        return error.returncode


if __name__ == "__main__":
    sys.exit(main())
