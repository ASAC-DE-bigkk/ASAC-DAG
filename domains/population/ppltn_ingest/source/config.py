"""population 도메인 설정: bronze 적재 루트 + 소스 식별자 + 인증키.

공용 R2/Trino 대상은 ``ppltn_ingest.common``에서 온다. 여기에는 population 소스
(서울 citydata_ppltn) 전용 값만 둔다.
"""

from __future__ import annotations

import os

from ..common.config import load_env_file, pick

# raw 원본 객체가 적재되는 raw prefix (이슈 #16 → #75: raw/<domain>).
LANDING_ROOT = "raw/population"

# 추적 메타데이터 source_id -- raw path와 bronze row 양쪽에 쓰인다.
SOURCE_ID = "seoul_ppltn"
SOURCE_DOMAIN = "population"

# 서울 열린데이터광장 실시간 도시데이터 API.
SEOUL_API_KEY_ENV = "SEOUL_API_KEY_PPLT"
SEOUL_OPEN_API_BASE_URL = os.environ.get(
    "SEOUL_OPEN_API_BASE_URL",
    "http://openapi.seoul.go.kr:8088",
)
# citydata_ppltn 응답의 행 컨테이너 키(점 포함) 및 API 정상 코드.
PPLTN_CONTAINER_KEY = "SeoulRtd.citydata_ppltn"
SEOUL_OK_CODES = ("INFO-000",)


def source_api_key(env_file: str | None = None) -> str:
    """환경변수(+선택적 .env)에서 서울 API 인증키를 읽어 온다."""
    return pick(SEOUL_API_KEY_ENV, load_env_file(env_file))


def missing_key(api_key: str) -> list[str]:
    """비어 있는 인증키 환경변수 이름 목록 (사전 점검용)."""
    return [] if api_key else [SEOUL_API_KEY_ENV]
