# shared-domains: all
"""Enforce placement and ownership rules for domain pipeline artifacts."""
from __future__ import annotations

import argparse
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
import re
import subprocess


_OWNER_PATTERN = re.compile(r"shared-domains:\s*([^\s>]+)", re.IGNORECASE)
_CONTROL_PLANE_FILES = frozenset({".gitignore", "AGENTS.md", "CLAUDE.md"})
_CONTROL_PLANE_PREFIXES = (".agents/", ".claude/", ".github/", ".omx/", "docs/agent/")


def _relative_path(path: str) -> PurePosixPath:
    normalized = path.replace("\\", "/")
    candidate = PurePosixPath(normalized)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError(f"path must be repository-relative: {path}")
    return candidate


def _shared_owner_violation(root: Path, relative_path: PurePosixPath) -> str | None:
    target = root / relative_path
    try:
        content = target.read_text(encoding="utf-8")[:4096]
    except FileNotFoundError:
        return f"{relative_path}: shared artifact is missing from the working tree"

    match = _OWNER_PATTERN.search(content)
    if match is None:
        return (
            f"{relative_path}: shared artifact must declare shared-domains: all "
            "or at least two domains"
        )

    owners = [owner.strip().lower() for owner in match.group(1).split(",") if owner.strip()]
    if owners == ["all"]:
        return None
    if len(set(owners)) < 2:
        return f"{relative_path}: shared-domains must be 'all' or name at least two domains"
    return None


def _is_control_plane_path(relative_path: PurePosixPath) -> bool:
    path = str(relative_path)
    return path in _CONTROL_PLANE_FILES or path.startswith(_CONTROL_PLANE_PREFIXES)


def validate_paths(root: Path, paths: Iterable[str]) -> list[str]:
    """Return placement/ownership violations for changed product artifacts."""
    violations: list[str] = []
    for raw_path in paths:
        relative_path = _relative_path(raw_path)
        parts = relative_path.parts
        if _is_control_plane_path(relative_path):
            continue
        if len(parts) >= 3 and parts[0] == "domains" and parts[1]:
            continue
        if parts and parts[0] == "common":
            violation = _shared_owner_violation(root, relative_path)
            if violation is not None:
                violations.append(violation)
            continue
        if len(parts) >= 4 and parts[:2] == ("docs", "cross-domain") and parts[2]:
            violation = _shared_owner_violation(root, relative_path)
            if violation is not None:
                violations.append(violation)
            continue
        violations.append(
            f"{relative_path}: product artifacts must live under domains/<domain>/, "
            "common/, or docs/cross-domain/<scope>/"
        )
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
        ["diff", "--name-only", "--diff-filter=AMR", f"{base_ref}...HEAD"],
        ["diff", "--name-only", "--diff-filter=AMR"],
        ["diff", "--cached", "--name-only", "--diff-filter=AMR"],
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
    root = Path(__file__).resolve().parents[1]
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
