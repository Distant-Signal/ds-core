#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI whose output (the version, the notes) is stdout
"""Prepare and describe ds-core releases (repo split spec §5.1, §5.2).

  uv run scripts/release.py prepare --bump patch|minor [--date YYYY-MM-DD]
  uv run scripts/release.py check
  uv run scripts/release.py version
  uv run scripts/release.py notes
  uv run scripts/release.py assets --out DIR

A release is a pull request made by `prepare`: it bumps
`[workspace.package] version` in Cargo.toml (every crate inherits it),
refreshes Cargo.lock, and prepends a CHANGELOG.md section listing the
merged pull requests since the last `v*` tag, flagging new migrations
(contract migrations by name), grants changes and Redis ACL changes. When
it merges, .github/workflows/tag.yml tags the merge commit and starts
release.yml on the tag, which publishes the ds-migrate image and the
assets.

Versioning, while on 0.x: a breaking Rust API change or a contract
migration is a minor bump, anything else a patch. Every migration is at
least a patch release, so the schema version is the core version.

`check` (CI) fails when a crate doesn't inherit the workspace version or
CHANGELOG.md's newest section isn't the workspace version. `version`
prints `vX.Y.Z`; `notes` prints its CHANGELOG section; `assets` copies the
files the release attaches (each is signed by release.yml) into DIR.

Stdlib only.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import shutil
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

ROOT = Path(__file__).resolve().parent.parent
CHANGELOG = "CHANGELOG.md"
MIGRATIONS = "crates/ds-store/migrations"
GRANTS = "crates/ds-store/db-grants.yaml"
ACL = "access/redis-users.acl.tpl"
ROLES = ("access/postgres-roles.sql", "access/postgres-roles-setup.sh")
# The release assets, in the order the release lists them. The landing
# repository's sync-core.py vendors each under its own name: the first five
# into charts/distant-signal/files/, the renderer into scripts/ (the chart's
# phase 0 check renders the ACL with it), and the validator's site credits
# into frontend/lib/ (the web app's tests check /attribution against them).
ASSETS = (
    GRANTS,
    "access/postgres-grants.sql",
    *ROLES,
    ACL,
    "scripts/render-redis-acl.py",
    "crates/line-catalogue-validator/site-credits.json",
)
CHANGELOG_HEADER = "# Changelog\n"
SECTION = re.compile(r"^## (v\d+\.\d+\.\d+)\b.*$", re.MULTILINE)
VERSION = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
MERGE = re.compile(r"^Merge pull request #(\d+) from \S+$")
CONTRACT = re.compile(r"^--\s*contract:", re.MULTILINE)


def bump(version: str, part: str) -> str:
    """Return `version` (X.Y.Z) bumped by `part` ("patch" or "minor")."""
    match = VERSION.match(version)
    if match is None:
        msg = f"not a version: {version!r}"
        raise ValueError(msg)
    major, minor, patch = (int(g) for g in match.groups())
    if part == "minor":
        return f"{major}.{minor + 1}.0"
    if part == "patch":
        return f"{major}.{minor}.{patch + 1}"
    msg = f"unknown bump: {part!r}"
    raise ValueError(msg)


def workspace_version(cargo_toml: str) -> str:
    """Return `[workspace.package] version` from the root Cargo.toml text."""
    data = tomllib.loads(cargo_toml)
    version = data.get("workspace", {}).get("package", {}).get("version")
    if not isinstance(version, str):
        msg = "Cargo.toml has no [workspace.package] version"
        raise TypeError(msg)
    return version


def set_workspace_version(cargo_toml: str, version: str) -> str:
    """Return the root Cargo.toml text with its workspace version replaced."""
    block = re.search(
        r"^\[workspace\.package\]\n(?:(?!^\[).*\n?)*", cargo_toml, re.MULTILINE
    )
    if block is None:
        msg = "Cargo.toml has no [workspace.package] table"
        raise ValueError(msg)
    text, count = re.subn(
        r'^version = "[^"]*"$',
        f'version = "{version}"',
        block.group(0),
        count=1,
        flags=re.MULTILINE,
    )
    if count != 1:
        msg = "[workspace.package] has no version line"
        raise ValueError(msg)
    return cargo_toml[: block.start()] + text + cargo_toml[block.end() :]


def sections(changelog: str) -> list[tuple[str, str]]:
    """Return CHANGELOG.md's (version, body) sections, newest first."""
    found = list(SECTION.finditer(changelog))
    out: list[tuple[str, str]] = []
    for i, match in enumerate(found):
        end = found[i + 1].start() if i + 1 < len(found) else len(changelog)
        out.append((match.group(1), changelog[match.end() : end].strip("\n")))
    return out


@dataclass
class Changes:
    """What a release contains, from git."""

    titles: list[str] = field(default_factory=list)
    migrations: list[str] = field(default_factory=list)
    contracts: list[str] = field(default_factory=list)
    grants: bool = False
    roles: bool = False
    acl: bool = False


def render_section(version: str, date: dt.date, changes: Changes) -> str:
    """Return the CHANGELOG.md section for a release."""
    lines = [f"## {version} ({date.isoformat()})", ""]
    lines += [f"- {title}" for title in changes.titles] or ["- No pull requests."]
    flags: list[str] = []
    if changes.migrations:
        flags.append(
            f"**Migrations** ({len(changes.migrations)}): "
            + ", ".join(f"`{m}`" for m in changes.migrations)
        )
    if changes.contracts:
        flags.append(
            "**Contract migrations**: " + ", ".join(f"`{m}`" for m in changes.contracts)
        )
    if changes.grants:
        flags.append("**Grants**: `db-grants.yaml` changed (the roles Job applies it).")
    if changes.roles:
        flags.append("**Roles**: the roles setup (`access/postgres-roles*`) changed.")
    if changes.acl:
        flags.append("**Redis ACL**: `access/redis-users.acl.tpl` changed.")
    if flags:
        lines += ["", *flags]
    return "\n".join(lines) + "\n"


def prepend(changelog: str, section: str) -> str:
    """Return CHANGELOG.md with `section` as its newest entry."""
    body = changelog.removeprefix(CHANGELOG_HEADER).lstrip("\n")
    return f"{CHANGELOG_HEADER}\n{section}" + (f"\n{body}" if body else "")


def git(root: Path, *args: str) -> str:
    """Return a git command's stdout, run in `root`."""
    return subprocess.run(  # noqa: S603  # git with this script's own arguments
        ["git", *args],  # noqa: S607  # git from PATH
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def last_tag(root: Path) -> str | None:
    """Return the newest `v*` tag reachable from HEAD, if any."""
    result = subprocess.run(
        ["git", "describe", "--tags", "--abbrev=0", "--match", "v[0-9]*"],  # noqa: S607  # git from PATH
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() or None


def pr_titles(log: str) -> list[str]:
    """Return the titles in `git log --first-parent --format=%s%x00%b%x1e`.

    A merge commit's title is its body's first line plus `(#N)`; any other
    first-parent commit (a squash merge, a direct push) is its subject.
    """
    titles: list[str] = []
    for raw in log.split("\x1e"):
        record = raw.strip("\n")
        if not record:
            continue
        subject, _, body = record.partition("\x00")
        merge = MERGE.match(subject)
        if merge:
            title = next((x.strip() for x in body.splitlines() if x.strip()), subject)
            titles.append(f"{title} (#{merge.group(1)})")
        else:
            titles.append(subject)
    return titles


def collect(root: Path, since: str | None) -> Changes:
    """Return what changed since the tag `since` (everything when None)."""
    span = [f"{since}..HEAD"] if since else ["HEAD"]
    log = git(
        root, "log", "--first-parent", "--reverse", "--format=%s%x00%b%x1e", *span
    )
    changes = Changes(titles=pr_titles(log))
    if since:
        status = git(root, "diff", "--name-status", "--no-renames", since, "HEAD")
        changed = {
            line.split("\t", 1)[1]: line.split("\t", 1)[0]
            for line in status.splitlines()
            if "\t" in line
        }
    else:
        changed = dict.fromkeys(git(root, "ls-files").splitlines(), "A")
    for path, kind in sorted(changed.items()):
        if path.startswith(f"{MIGRATIONS}/") and path.endswith(".sql") and kind == "A":
            name = Path(path).name
            changes.migrations.append(name)
            if CONTRACT.search((root / path).read_text(encoding="utf-8")):
                changes.contracts.append(name)
    changes.grants = GRANTS in changed
    changes.roles = any(r in changed for r in ROLES)
    changes.acl = ACL in changed
    return changes


def check(root: Path) -> list[str]:
    """Return the problems `check` reports."""
    problems: list[str] = []
    version = workspace_version((root / "Cargo.toml").read_text(encoding="utf-8"))
    for manifest in sorted(root.glob("crates/*/Cargo.toml")):
        package = tomllib.loads(manifest.read_text(encoding="utf-8")).get("package", {})
        if package.get("version") != {"workspace": True}:
            problems.append(
                f"{manifest.relative_to(root)}: use `version.workspace = true`"
            )
    changelog = root / CHANGELOG
    found = (
        sections(changelog.read_text(encoding="utf-8")) if changelog.exists() else []
    )
    if not found or found[0][0] != f"v{version}":
        newest = found[0][0] if found else "none"
        problems.append(
            f"{CHANGELOG}'s newest section is {newest}, not v{version}:"
            " run scripts/release.py prepare"
        )
    return problems


def main(argv: Sequence[str] | None = None) -> int:
    """Run a subcommand."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    sub = parser.add_subparsers(dest="command", required=True)
    prepare_p = sub.add_parser("prepare", help="bump the version and the changelog")
    prepare_p.add_argument("--bump", choices=("patch", "minor"), required=True)
    prepare_p.add_argument("--date", type=dt.date.fromisoformat)
    prepare_p.add_argument(
        "--no-lock", action="store_true", help="leave Cargo.lock alone (tests)"
    )
    sub.add_parser("check", help="crates inherit the version; the changelog has it")
    sub.add_parser("version", help="print vX.Y.Z")
    sub.add_parser("notes", help="print this version's changelog section")
    assets_p = sub.add_parser("assets", help="copy the release assets")
    assets_p.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    root: Path = args.root
    cargo = root / "Cargo.toml"
    version = f"v{workspace_version(cargo.read_text(encoding='utf-8'))}"

    if args.command == "version":
        print(version)
        return 0
    if args.command == "check":
        problems = check(root)
        for problem in problems:
            print(f"::error::{problem}")
        return 1 if problems else 0
    if args.command == "notes":
        for found, body in sections((root / CHANGELOG).read_text(encoding="utf-8")):
            if found == version:
                print(body)
                return 0
        print(f"::error::{CHANGELOG} has no {version} section", file=sys.stderr)
        return 1
    if args.command == "assets":
        args.out.mkdir(parents=True, exist_ok=True)
        for asset in ASSETS:
            shutil.copyfile(root / asset, args.out / Path(asset).name)
        return 0

    new = bump(version.removeprefix("v"), args.bump)
    date = args.date or dt.datetime.now(dt.UTC).date()
    section = render_section(f"v{new}", date, collect(root, last_tag(root)))
    cargo.write_text(
        set_workspace_version(cargo.read_text(encoding="utf-8"), new), encoding="utf-8"
    )
    changelog = root / CHANGELOG
    old = changelog.read_text(encoding="utf-8") if changelog.exists() else ""
    changelog.write_text(prepend(old, section), encoding="utf-8")
    if not args.no_lock:
        subprocess.run(
            ["cargo", "update", "--workspace", "--offline"],  # noqa: S607  # cargo from PATH
            cwd=root,
            check=True,
        )
    print(
        f"{version} -> v{new}: review CHANGELOG.md, then open the release pull request"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
