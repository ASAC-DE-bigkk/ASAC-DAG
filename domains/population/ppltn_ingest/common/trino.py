"""Trino/Iceberg 접속 helper (이슈 #16의 얇은 ``common/trino``).

Trino 연결 생성, dev/prod 카탈로그 선택, SQL 식별자 검증, 스키마 생성만 담당한다.
bronze 테이블 DDL/INSERT 같은 도메인 의미가 있는 SQL은 ``common/bronze``에 둔다.

메인 Airflow 이미지에 ``trino`` DBAPI 클라이언트가 설치돼 있어(Dockerfile.airflow)
``trino.dbapi``를 그대로 쓴다. dev/prod는 카탈로그로 가른다:
dev -> ``iceberg_dev``(seoul-dev warehouse), prod -> ``iceberg``(seoul warehouse).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .config import normalize_target

# 안전한 SQL 식별자(카탈로그/스키마/테이블)만 허용 -- 인젝션 방지.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sql_identifier(value: str) -> str:
    """SQL 식별자 검증. 허용 패턴 외 값은 즉시 실패."""
    if not _IDENT_RE.match(value):
        raise ValueError(f"unsafe SQL identifier: {value}")
    return value


def sql_string(value: object) -> str:
    """문자열 리터럴(작은따옴표 이스케이프). None/빈 문자열 -> NULL."""
    if value is None:
        return "NULL"
    text = str(value).strip()
    if text == "":
        return "NULL"
    return "'" + text.replace("'", "''") + "'"


def sql_int(value: object) -> str:
    """정수 리터럴. None/빈 값 -> NULL."""
    if value is None or value == "":
        return "NULL"
    return str(int(value))


@dataclass(frozen=True)
class TrinoSettings:
    """Trino 접속 + 대상 카탈로그/스키마."""

    host: str
    port: int
    user: str
    http_scheme: str
    catalog: str  # dev -> iceberg_dev, prod -> iceberg
    schema: str   # 도메인 스키마 (population -> seoul_ppltn)


def build_trino_settings(target: str = "dev", env: dict | None = None) -> TrinoSettings:
    """``target``에 맞는 Trino/Iceberg 접속 설정. 값은 환경변수에서."""
    import os

    target = normalize_target(target)
    env = env if env is not None else os.environ
    dev = target == "dev"
    catalog = (
        env.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
        if dev
        else env.get("TRINO_ICEBERG_CATALOG", "iceberg")
    )
    return TrinoSettings(
        host=env.get("TRINO_HOST", "trino"),
        port=int(env.get("TRINO_PORT", "8080")),
        user=env.get("TRINO_USER", "airflow"),
        http_scheme=env.get("TRINO_HTTP_SCHEME", "http"),
        catalog=sql_identifier(catalog),
        schema=sql_identifier(env.get("SEOUL_PPLTN_SCHEMA", "seoul_ppltn")),
    )


def connect(settings: TrinoSettings):
    """Trino DBAPI 연결을 만든다."""
    import trino.dbapi

    return trino.dbapi.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        catalog=settings.catalog,
        http_scheme=settings.http_scheme,
    )


def ensure_schema(cursor, catalog: str, schema: str) -> None:
    """스키마를 멱등 생성한다(이미 있으면 무시)."""
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {sql_identifier(catalog)}.{sql_identifier(schema)}")
    except Exception as exc:  # noqa: BLE001 -- 동시 생성 레이스만 무시
        if "already exists" not in str(exc).lower():
            raise
