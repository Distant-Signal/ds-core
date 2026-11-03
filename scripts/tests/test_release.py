"""Tests for scripts/release.py.

uv run python -m unittest discover -s scripts/tests
"""

import datetime as dt
import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType

SCRIPT = Path(__file__).resolve().parent.parent / "release.py"


def load_script() -> ModuleType:
    """Import the script as a module."""
    spec = importlib.util.spec_from_file_location("release", SCRIPT)
    if spec is None or spec.loader is None:
        raise ImportError(SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["release"] = module
    spec.loader.exec_module(module)
    return module


release = load_script()

CARGO = """[workspace]
members = ["crates/a"]

[workspace.package]
edition = "2024"
version = "0.1.0"

[workspace.lints.rust]
unsafe_code = "deny"
"""


def git(root: Path, *args: str) -> None:
    """Run git in `root`, quietly, as a fixed test identity."""
    subprocess.run(  # noqa: S603  # a fixed git command
        [  # noqa: S607  # git from PATH
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "commit.gpgsign=false",
            *args,
        ],
        cwd=root,
        check=True,
        capture_output=True,
    )


class PureTests(unittest.TestCase):
    """The text transformations."""

    def test_bump(self) -> None:
        """Patch and minor bumps; minor resets the patch."""
        self.assertEqual(release.bump("0.1.0", "patch"), "0.1.1")
        self.assertEqual(release.bump("0.1.9", "minor"), "0.2.0")
        with self.assertRaises(ValueError):
            release.bump("0.1", "patch")

    def test_set_workspace_version_touches_only_its_table(self) -> None:
        """Only [workspace.package]'s version line changes."""
        text = CARGO.replace(
            "[workspace.lints.rust]\n", '[workspace.lints.rust]\nversion = "x"\n'
        )
        out = release.set_workspace_version(text, "0.2.0")
        self.assertEqual(release.workspace_version(out), "0.2.0")
        self.assertIn('version = "x"', out)

    def test_sections_and_prepend(self) -> None:
        """A prepended section becomes the newest; bodies are kept."""
        old = "# Changelog\n\n## v0.1.0 (2026-11-03)\n\n- First.\n"
        section = release.render_section(
            "v0.1.1", dt.date(2026, 11, 10), release.Changes(titles=["Next (#2)"])
        )
        new = release.prepend(old, section)
        found = release.sections(new)
        self.assertEqual([v for v, _ in found], ["v0.1.1", "v0.1.0"])
        self.assertEqual(found[1][1], "- First.")
        self.assertTrue(new.startswith("# Changelog\n\n## v0.1.1 (2026-11-10)\n"))

    def test_render_flags(self) -> None:
        """Migrations, contracts, grants and the ACL are called out."""
        text = release.render_section(
            "v0.2.0",
            dt.date(2026, 11, 10),
            release.Changes(
                titles=["Drop x (#3)"],
                migrations=["1_a.sql", "2_b.sql"],
                contracts=["2_b.sql"],
                grants=True,
                acl=True,
            ),
        )
        self.assertIn("**Migrations** (2): `1_a.sql`, `2_b.sql`", text)
        self.assertIn("**Contract migrations**: `2_b.sql`", text)
        self.assertIn("**Grants**", text)
        self.assertIn("**Redis ACL**", text)
        self.assertNotIn("**Roles**", text)

    def test_pr_titles(self) -> None:
        """A merge commit's title is its body's first line with the PR number."""
        log = (
            "Merge pull request #12 from Distant-Signal/x\x00Add a thing\n\nmore\x1e\n"
            "A direct commit\x00\x1e\n"
        )
        self.assertEqual(
            release.pr_titles(log), ["Add a thing (#12)", "A direct commit"]
        )


class RepositoryTests(unittest.TestCase):
    """Prepare and check against a scratch repository."""

    def make_repo(self, root: Path) -> None:
        """Make a tagged v0.1.0 repository with one crate."""
        (root / "crates/a").mkdir(parents=True)
        (root / "crates/a/Cargo.toml").write_text(
            '[package]\nname = "a"\nversion.workspace = true\n'
        )
        (root / "Cargo.toml").write_text(CARGO)
        (root / "CHANGELOG.md").write_text(
            "# Changelog\n\n## v0.1.0 (2026-11-03)\n\n- First.\n"
        )
        (root / "crates/ds-store/migrations").mkdir(parents=True)
        git(root, "init", "-q", "-b", "main")
        git(root, "add", ".")
        git(root, "commit", "-q", "-m", "First")
        git(root, "tag", "v0.1.0")

    def test_check_passes_then_flags_a_stale_changelog(self) -> None:
        """Check passes on a consistent tree, fails once the version moves alone."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_repo(root)
            self.assertEqual(release.check(root), [])
            (root / "Cargo.toml").write_text(CARGO.replace('"0.1.0"', '"0.1.1"'))
            self.assertEqual(len(release.check(root)), 1)

    def test_check_wants_inherited_crate_versions(self) -> None:
        """A crate with its own version is a problem."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_repo(root)
            (root / "crates/a/Cargo.toml").write_text(
                '[package]\nname = "a"\nversion = "0.1.0"\n'
            )
            self.assertIn("crates/a/Cargo.toml", release.check(root)[0])

    def test_prepare(self) -> None:
        """Prepare bumps the version and records the migration since the tag."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_repo(root)
            migration = root / "crates/ds-store/migrations/20261110000000_drop.sql"
            migration.write_text(
                "-- contract: x (code stopped using it in abc)\nDROP TABLE x;\n"
            )
            git(root, "add", ".")
            git(root, "commit", "-q", "-m", "Drop x")
            rc = release.main(
                [
                    "--root",
                    str(root),
                    "prepare",
                    "--bump",
                    "minor",
                    "--date",
                    "2026-11-10",
                    "--no-lock",
                ]
            )
            self.assertEqual(rc, 0)
            self.assertEqual(
                release.workspace_version((root / "Cargo.toml").read_text()), "0.2.0"
            )
            (version, body), _ = release.sections((root / "CHANGELOG.md").read_text())
            self.assertEqual(version, "v0.2.0")
            self.assertIn("- Drop x", body)
            self.assertIn("**Contract migrations**: `20261110000000_drop.sql`", body)
            self.assertEqual(release.check(root), [])


if __name__ == "__main__":
    unittest.main()
