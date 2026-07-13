import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
ALLOWED_PREFIX = "domains/weather/"


def _git_paths(*args: str) -> set[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return {
        path.strip().replace("\\", "/")
        for path in result.stdout.splitlines()
        if path.strip()
    }


def _changed_paths() -> list[str]:
    paths = set()
    base_ref = os.getenv("WEATHER_DOMAIN_BASE_REF", "origin/dev")

    if subprocess.run(
        ["git", "rev-parse", "--verify", base_ref],
        cwd=ROOT,
        capture_output=True,
        text=True,
    ).returncode == 0:
        paths.update(_git_paths("diff", "--name-only", f"{base_ref}...HEAD"))

    paths.update(_git_paths("diff", "--name-only"))
    paths.update(_git_paths("diff", "--cached", "--name-only"))
    paths.update(_git_paths("ls-files", "--others", "--exclude-standard"))
    return sorted(paths)


def test_weather_branch_changes_stay_inside_weather_domain():
    violations = [
        path for path in _changed_paths() if not path.startswith(ALLOWED_PREFIX)
    ]

    assert violations == [], (
        "Weather 작업은 domains/weather/** 안에서만 변경할 수 있습니다. "
        f"범위 밖 변경: {violations}"
    )
