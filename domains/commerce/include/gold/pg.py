"""gold 서빙 DB(PostgreSQL) 접속 — compose 의 serving-postgres 컨테이너.

env(.env.commerce — 번들 규약): COMMERCE_GOLD_PG_HOST/PORT/DB/USER/PASSWORD.
기본값은 compose dev 기본(serving/serving)과 정합 — prod 는 env 로 교체.
비밀번호는 security.register_secret 로 로그 마스킹에 등록(가능한 경우).
"""
from __future__ import annotations

import os


def connect():
    """psycopg2 연결(autocommit off — 호출측이 트랜잭션 경계 관리)."""
    import psycopg2                          # 지연 임포트 — DAG 파싱 경량 유지

    password = os.getenv("COMMERCE_GOLD_PG_PASSWORD", "serving")
    try:                                     # 마스킹 등록(모듈 없으면 무시)
        from security import register_secret
        register_secret(password)
    except Exception:
        pass
    return psycopg2.connect(
        host=os.getenv("COMMERCE_GOLD_PG_HOST", "serving-postgres"),
        port=int(os.getenv("COMMERCE_GOLD_PG_PORT", "5432")),
        dbname=os.getenv("COMMERCE_GOLD_PG_DB", "serving"),
        user=os.getenv("COMMERCE_GOLD_PG_USER", "serving"),
        password=password,
        connect_timeout=int(os.getenv("COMMERCE_GOLD_PG_TIMEOUT", "10")),
    )


def execute_values(cur, sql: str, rows: list[tuple], page_size: int = 2000) -> None:
    """psycopg2.extras.execute_values 래퍼(배치 insert). 대용량 초기 적재 라운드트립 감소."""
    from psycopg2.extras import execute_values as _ev

    _ev(cur, sql, rows, page_size=page_size)
