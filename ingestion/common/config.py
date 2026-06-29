"""Domain-agnostic runtime config: R2 credentials, run context, env loading.

Source API keys are domain-specific and live in each domain's own config; this
module only knows about the shared R2 target and the partition layout. Values
come from the process environment (Airflow injects ``sample/.env`` via
``env_file``); a ``.env`` path can also be passed for local runs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))


def load_env_file(path: str | None) -> dict[str, str]:
    """Parse a dotenv-style file into a dict. Missing/empty path -> empty dict."""
    values: dict[str, str] = {}
    if not path or not os.path.exists(path):
        return values
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            values[key] = value
    return values


def pick(name: str, env: dict[str, str]) -> str:
    """Process env wins over the .env fallback."""
    return os.environ.get(name) or env.get(name, "")


@dataclass(frozen=True)
class R2Settings:
    """Cloudflare R2 (S3-compatible) target for one environment."""

    target: str  # "dev" | "prod"
    endpoint: str
    access_key_id: str
    secret_access_key: str
    bucket: str


def build_r2_settings(target: str = "dev", env_file: str | None = None) -> R2Settings:
    """Resolve R2 settings for ``target``.

    dev -> ``R2_DEV_*`` (bucket ``seoul-dev``); prod -> ``R2_*`` (bucket ``seoul``).
    """
    env = load_env_file(env_file)
    prefix = "R2_DEV_" if target == "dev" else "R2_"
    return R2Settings(
        target=target,
        endpoint=pick(prefix + "ENDPOINT", env),
        access_key_id=pick(prefix + "ACCESS_KEY_ID", env),
        secret_access_key=pick(prefix + "SECRET_ACCESS_KEY", env),
        bucket=pick(prefix + "BUCKET_NAME", env),
    )


def missing_r2(settings: R2Settings) -> list[str]:
    """Names of required-but-empty R2 fields (for preflight error messages)."""
    prefix = "R2_DEV_" if settings.target == "dev" else "R2_"
    pairs = (
        ("ENDPOINT", settings.endpoint),
        ("ACCESS_KEY_ID", settings.access_key_id),
        ("SECRET_ACCESS_KEY", settings.secret_access_key),
        ("BUCKET_NAME", settings.bucket),
    )
    return [prefix + suffix for suffix, value in pairs if not value]


@dataclass(frozen=True)
class RunContext:
    """Identifies a single ingestion run; pins the partition timestamps."""

    load_date: str  # YYYY-MM-DD in KST -- the partition key
    ingest_ts: str  # YYYYMMDDTHHMMSSZ in UTC -- groups one run's objects
    run_id: str  # free-form (Airflow run id, or "manual" for CLI)

    @staticmethod
    def create(run_id: str = "manual") -> "RunContext":
        now_utc = datetime.now(timezone.utc)
        return RunContext(
            load_date=now_utc.astimezone(KST).strftime("%Y-%m-%d"),
            ingest_ts=now_utc.strftime("%Y%m%dT%H%M%SZ"),
            run_id=run_id,
        )


def landing_prefix(root: str, source: str, dataset: str, ctx: RunContext) -> str:
    """Object-key prefix for one dataset's run (no trailing slash).

    ``<root>/<source>/<dataset>/load_date=<KST>/ingest_ts=<UTC>``
    """
    return (
        f"{root}/{source}/{dataset}"
        f"/load_date={ctx.load_date}/ingest_ts={ctx.ingest_ts}"
    )
