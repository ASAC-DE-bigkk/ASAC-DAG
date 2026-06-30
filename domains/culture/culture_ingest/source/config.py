"""Culture-domain config: landing root + source API keys.

The R2 target (shared) comes from ``culture_ingest.common.config``; only the
culture-specific source keys and the bronze landing root live here.
"""

from __future__ import annotations

from dataclasses import dataclass

from culture_ingest.common.config import load_env_file, pick

# Raw-as-ingested objects land under the culture domain's bronze layer prefix.
LANDING_ROOT = "bronze/culture"

KOPIS_KEY_ENV = "KOPIS_SERVICE_KEY"
SEOUL_KEY_ENV = "SEOUL_OPENAPI_KEY"


@dataclass(frozen=True)
class SourceKeys:
    kopis: str
    seoul: str


def source_keys(env_file: str | None = None) -> SourceKeys:
    env = load_env_file(env_file)
    return SourceKeys(kopis=pick(KOPIS_KEY_ENV, env), seoul=pick(SEOUL_KEY_ENV, env))


def missing_keys(keys: SourceKeys) -> list[str]:
    missing: list[str] = []
    if not keys.kopis:
        missing.append(KOPIS_KEY_ENV)
    if not keys.seoul:
        missing.append(SEOUL_KEY_ENV)
    return missing
