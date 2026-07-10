"""공통 redaction 모듈(#77)로의 안전한 진입점 (#144).

`common.security.redaction` 은 dags 루트 기준 패키지라, Airflow 런타임(DAG 이 루트 삽입)
밖의 실행 문맥(단독 스크립트·host pytest)에서는 import 가 깨질 수 있다. 여기서 루트를
계산해 보장한 뒤 재노출한다 — culture 코드는 이 모듈만 import 하면 된다.
"""
from __future__ import annotations

import os
import sys

# common/ -> culture_ingest -> culture -> domains -> dags 루트 (4단계 상위)
_DAGS_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..")
)
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.security.redaction import (  # noqa: E402  (경로 보장 후 import)
    PLACEHOLDER,
    redact,
    refresh_env_secrets,
    register_secret,
)

__all__ = [
    "PLACEHOLDER",
    "redact",
    "refresh_env_secrets",
    "register_secret",
]
