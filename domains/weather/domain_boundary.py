"""Enforce domain-local placement for changed pipeline artifacts."""
from __future__ import annotations

import argparse
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
import subprocess


_CONTROL_PLANE_FILES = frozenset({".gitignore", "AGENTS.md", "CLAUDE.md"})
_CONTROL_PLANE_PREFIXES = (".agents/", ".claude/", ".github/", ".omx/", "docs/agent/")


def _relative_path(path: str) -> PurePosixPath:
    normalized = path.replace("\\", "/")
    candidate = PurePosixPath(normalized)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError(f"path must be repository-relative: {path}")
    return candidate


def _is_control_plane_path(relative_path: PurePosixPath) -> bool:
    path = str(relative_path)
    return path in _CONTROL_PLANE_FILES or path.startswith(_CONTROL_PLANE_PREFIXES)


def validate_paths(root: Path, paths: Iterable[str]) -> list[str]:
    """Return violations when changed product artifacts leave a domain folder."""
    violations: list[str] = []
    for raw_path in paths:
        relative_path = _relative_path(raw_path)
        parts = relative_path.parts
        if _is_control_plane_path(relative_path):
            continue
        if len(parts) >= 3 and parts[0] == "domains" and parts[1]:
            continue
        violations.append(f"{relative_path}: product artifacts must live under domains/<domain>/")
    return violations


def _git_changed_paths(root: Path, arguments: list[str]) -> list[str]:
    completed = subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in completed.stdout.splitlines() if line]


def changed_paths(root: Path, base_ref: str) -> list[str]:
    """List added/modified paths from the branch, index, working tree, and untracked files."""
    commands = (
        ["diff", "--name-only", "--diff-filter=AMR", base_ref],
        ["ls-files", "--others", "--exclude-standard"],
    )
    result: list[str] = []
    for command in commands:
        for path in _git_changed_paths(root, command):
            if path not in result:
                result.append(path)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", default="origin/dev")
    parser.add_argument("--path", action="append", dest="paths")
    arguments = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    paths = arguments.paths or changed_paths(root, arguments.base_ref)
    violations = validate_paths(root, paths)
    if violations:
        print("FAIL: domain artifact boundary violations")
        for violation in violations:
            print(f"- {violation}")
        return 1
    print(f"PASS: {len(paths)} changed product artifact path(s) satisfy the domain boundary.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
