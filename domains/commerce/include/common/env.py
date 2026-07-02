"""commerce 전용 .env 로더 — 번들 자립용(루트 .env/compose 를 건드리지 않는다).

DAG 임포트 시 1회 `load_commerce_env()` 를 호출해 이 번들의 `.env.commerce` 를
`os.environ` 에 채운다. `setdefault` 의미라 **이미 설정된 프로세스 env(루트 .env /
compose environment:)가 우선**이고, 비어 있는 commerce 전용 값만 이 파일이 채운다.

`.env.commerce` 값에는 `${VAR}` / `${VAR:-default}` 참조를 쓸 수 있고, 로더가 **현재
프로세스 env(= 루트 .env/compose 가 주입한 값)** 로 치환한다. 그래서 루트 `.env` 와
겹치는 값(R2 자격증명/엔드포인트/버킷)은 중복 저장하지 않고 루트 키에서 불러온다.
이름이 다른 경우(예: 루트 `R2_DEV_BUCKET_NAME` → commerce `R2_BUCKET`)도 참조로 잇는다.

- 외부 의존성 없음(python-dotenv 불필요) — `KEY=VALUE` 단순 파서 + `${VAR}` 치환.
- 경로 override: 환경변수 `COMMERCE_ENV_FILE`.
- 시크릿(SEOUL_API_KEY_COMM/R2 토큰) 값은 로그에 남기지 않는다 — 개수만 기록(CLAUDE.md §2.5).
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Mapping

log = logging.getLogger(__name__)

# include/common/env.py → parents[2] == dags/domains/commerce/
_DEFAULT_ENV_FILE = Path(__file__).resolve().parents[2] / ".env.commerce"

# ${NAME} · ${NAME:-default}(미설정/빈값 시 default) · ${NAME-default}(미설정 시 default)
_VAR_RE = re.compile(r"\$\{([A-Za-z_]\w*)(?:(:?-)([^}]*))?\}")

_loaded = False


def env_file_path() -> Path:
    return Path(os.getenv("COMMERCE_ENV_FILE", str(_DEFAULT_ENV_FILE)))


def _interpolate(value: str, lookup: Mapping[str, str]) -> str:
    """`${VAR}` / `${VAR:-default}` 를 lookup(프로세스 env)으로 치환."""
    def repl(m: re.Match) -> str:
        name, op, default = m.group(1), m.group(2), m.group(3)
        val = lookup.get(name)
        if op is None:                       # ${NAME}
            return val if val is not None else ""
        use_default = val is None or (op == ":-" and val == "")
        return (default or "") if use_default else val
    return _VAR_RE.sub(repl, value)


def _parse(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, sep, val = line.partition("=")
        if not sep:
            continue
        key = key.strip()
        if not key:
            continue
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        out[key] = val
    return out


def load_commerce_env(*, override: bool = False) -> Path | None:
    """commerce `.env.commerce` 를 `os.environ` 에 적재(프로세스당 1회).

    Args:
        override: True 면 기존 프로세스 env 값도 덮어쓴다(기본 False = setdefault).

    Returns:
        적재한 파일 경로. 파일이 없거나 읽기에 실패하면 None(조용히 통과 — 배포 환경이
        env 를 직접 주입하는 경우를 막지 않기 위함).
    """
    global _loaded
    if _loaded:
        return None
    _loaded = True

    path = env_file_path()
    if not path.is_file():
        log.info("commerce env 파일 없음(스킵): %s", path)
        return None
    try:
        pairs = _parse(path.read_text(encoding="utf-8"))
    except OSError as exc:
        log.warning("commerce env 파일 읽기 실패(%s): %s", path, exc)
        return None

    applied = 0
    for key, raw in pairs.items():
        # 치환은 live os.environ 기준 → 루트 .env 값 + 앞서 적용된 commerce 키를 본다.
        val = _interpolate(raw, os.environ)
        if override or key not in os.environ:
            os.environ[key] = val
            applied += 1
    log.info("commerce env 적재: %s (%d/%d 적용, override=%s)",
             path, applied, len(pairs), override)
    return path
