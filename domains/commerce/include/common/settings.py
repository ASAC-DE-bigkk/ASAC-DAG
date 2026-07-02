"""런타임 설정(환경변수). DB 없음 — 스토리지(R2/local) + 서울 OpenAPI 만.

가벼운 dataclass 라 DAG 파싱 중 임포트해도 부담 없다.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    schema_version: str
    # ── storage ──
    storage_backend: str           # local | r2
    storage_prefix: str            # bucket 아래 공통 접두(예: dev/<id>). 비우면 없음
    local_data_root: str
    r2_endpoint: str
    r2_bucket: str
    r2_access_key_id: str
    r2_secret_access_key: str
    r2_region: str
    # ── 서울 OpenAPI ──
    seoul_openapi_key: str          # 인증키 — bronze/로그/경로에 절대 저장 금지
    seoul_openapi_base_url: str
    seoul_page_size: int            # 1회 조회 건수(서울 상한 1000)
    seoul_max_pages: int | None     # None = 무제한(끝까지 순회). 일반 API 는 호출 횟수 제한 없음
    seoul_request_delay_seconds: float


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_limit(name: str) -> int | None:
    """횟수 제한 파싱. **값이 없으면(미설정/빈값) 제한 없음(None)**.

    일반 API 는 호출 횟수 제한이 없으므로 기본은 무제한. 양수만 부분 수집(개발용) 캡으로
    동작하고, 0·음수·비숫자도 무제한(None)으로 취급한다.
    """
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def get_settings() -> Settings:
    return Settings(
        schema_version=os.getenv("SCHEMA_VERSION", "v1"),
        storage_backend=os.getenv("STORAGE_BACKEND", "local").lower(),
        storage_prefix=os.getenv("COMMERCE_STORAGE_PREFIX", "").strip().strip("/"),
        local_data_root=os.getenv("LOCAL_DATA_ROOT", "/opt/airflow/data"),
        r2_endpoint=os.getenv("R2_ENDPOINT", ""),
        r2_bucket=os.getenv("R2_BUCKET", ""),
        r2_access_key_id=os.getenv("R2_ACCESS_KEY_ID", ""),
        r2_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY", ""),
        r2_region=os.getenv("R2_REGION", "auto"),
        seoul_openapi_key=os.getenv("SEOUL_API_KEY_COMM", ""),
        seoul_openapi_base_url=os.getenv(
            "SEOUL_OPENAPI_BASE_URL", "http://openapi.seoul.go.kr:8088"
        ),
        seoul_page_size=min(_env_int("SEOUL_PAGE_SIZE", 1000), 1000),
        seoul_max_pages=_env_limit("SEOUL_MAX_PAGES"),  # 값 없으면 무제한(None)
        seoul_request_delay_seconds=_env_float("SEOUL_REQUEST_DELAY_SECONDS", 0.2),
    )
