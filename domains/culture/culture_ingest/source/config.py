"""culture 도메인 설정: 적재 루트 + 소스 API 키.

공용 R2 적재 대상은 ``culture_ingest.common.config``에서 가져온다. 여기에는
culture 전용 소스 키와 bronze 적재 루트만 둔다.
"""

from __future__ import annotations

from dataclasses import dataclass

from culture_ingest.common.config import load_env_file, pick

# 받아온 원본 객체는 culture 도메인의 bronze 레이어 prefix 아래에 적재된다.
LANDING_ROOT = "bronze/culture"

KOPIS_KEY_ENV = "KOPIS_SERVICE_KEY"   # KOPIS 인증키 환경변수 이름
SEOUL_KEY_ENV = "SEOUL_OPENAPI_KEY"   # 서울 열린데이터 인증키 환경변수 이름


@dataclass(frozen=True)
class SourceKeys:
    """두 소스의 인증키 묶음."""

    kopis: str
    seoul: str


def source_keys(env_file: str | None = None) -> SourceKeys:
    """환경변수(+선택적 .env)에서 두 소스 인증키를 읽어 온다."""
    env = load_env_file(env_file)
    return SourceKeys(kopis=pick(KOPIS_KEY_ENV, env), seoul=pick(SEOUL_KEY_ENV, env))


def missing_keys(keys: SourceKeys) -> list[str]:
    """비어 있는 인증키 환경변수 이름 목록 (사전 점검용)."""
    missing: list[str] = []
    if not keys.kopis:
        missing.append(KOPIS_KEY_ENV)
    if not keys.seoul:
        missing.append(SEOUL_KEY_ENV)
    return missing
