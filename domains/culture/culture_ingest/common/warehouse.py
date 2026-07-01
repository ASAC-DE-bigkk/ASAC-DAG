"""bronze Iceberg 적재 — Trino HTTP API 경유.

R2의 culture raw 객체를 파싱한 레코드를 ``<catalog>.<schema>.bronze_<dataset>``
Iceberg 테이블로 ``INSERT`` 한다. 메인 Airflow 파이썬에 ``trino`` 클라이언트가
없으므로(이미지가 dbt-trino를 별도 venv에 둠) 표준 Trino **HTTP API**(`requests`)를
직접 쓴다 — 형제 도메인의 ``trino.dbapi`` 사용과 기능적으로 동일하다.

경계: 멘티는 자기 도메인 스키마에만 쓴다 → 기본 스키마 ``culture``
(`iceberg.culture.bronze_*`). dev/prod는 카탈로그로 가른다(계획안 Slide 10).
적재는 ``ingest_ts`` 파티션 기준 delete-then-insert로 **멱등**하게 만든다.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone

import requests

# 안전한 SQL 식별자(카탈로그/스키마/테이블)만 허용 — 인젝션 방지.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ident(value: str) -> str:
    if not _IDENT_RE.match(value):
        raise ValueError(f"unsafe SQL identifier: {value}")
    return value


def _lit(value) -> str:
    """문자열 리터럴(작은따옴표 이스케이프). None -> NULL."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class WarehouseSettings:
    host: str
    port: int
    user: str
    http_scheme: str
    catalog: str  # dev -> iceberg_dev, prod -> iceberg
    schema: str   # culture (도메인 스키마)


def build_warehouse_settings(target: str = "prod", env: dict | None = None) -> WarehouseSettings:
    """target에 맞는 Trino/Iceberg 접속 설정. 값은 환경변수에서."""
    env = env if env is not None else os.environ
    dev = target == "dev"
    catalog = (
        env.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
        if dev
        else env.get("TRINO_ICEBERG_CATALOG", "iceberg")
    )
    return WarehouseSettings(
        host=env.get("TRINO_HOST", "trino"),
        port=int(env.get("TRINO_PORT", "8080")),
        user=env.get("TRINO_USER", "airflow"),
        http_scheme=env.get("TRINO_HTTP_SCHEME", "http"),
        catalog=catalog,
        schema=env.get("CULTURE_BRONZE_SCHEMA", "culture"),
    )


class TrinoClient:
    """최소 Trino HTTP 클라이언트: POST /v1/statement 후 nextUri를 끝까지 따라간다."""

    def __init__(self, settings: WarehouseSettings, timeout: int = 60):
        self.timeout = timeout
        self.base = f"{settings.http_scheme}://{settings.host}:{settings.port}/v1/statement"
        self.headers = {"X-Trino-User": settings.user}

    def execute(self, sql: str) -> list[list]:
        """SQL 1건 실행, 결과 행(있으면)을 반환. 에러면 RuntimeError."""
        resp = requests.post(self.base, data=sql.encode("utf-8"), headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        payload = resp.json()
        rows: list[list] = []
        while True:
            if payload.get("data"):
                rows.extend(payload["data"])
            err = payload.get("error")
            if err:
                raise RuntimeError(f"Trino error: {err.get('message')} [{err.get('errorName')}]")
            nxt = payload.get("nextUri")
            if not nxt:
                return rows
            follow = requests.get(nxt, headers=self.headers, timeout=self.timeout)
            follow.raise_for_status()
            payload = follow.json()


# bronze 테이블 컬럼 (모든 데이터셋 공통: 레코드 1건 = 1행, 원본은 record_json).
_COLUMNS = (
    "dataset",
    "source",
    "endpoint",
    "record_seq",
    "record_json",
    "raw_object_key",
    "page_no",
    "load_date",
    "ingest_ts",
    "run_id",
    "collected_at",
)


class BronzeWarehouse:
    """culture bronze Iceberg 테이블 생성/적재."""

    def __init__(self, settings: WarehouseSettings):
        self.s = settings
        self.client = TrinoClient(settings)

    def qualified(self, dataset: str) -> str:
        return f"{_ident(self.s.catalog)}.{_ident(self.s.schema)}.{_ident('bronze_' + dataset)}"

    def ensure_table(self, dataset: str) -> str:
        """스키마 + bronze 테이블을 멱등 생성하고 정규화된 이름을 반환."""
        self.client.execute(
            f"CREATE SCHEMA IF NOT EXISTS {_ident(self.s.catalog)}.{_ident(self.s.schema)}"
        )
        table = self.qualified(dataset)
        self.client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                dataset varchar,
                source varchar,
                endpoint varchar,
                record_seq integer,
                record_json varchar,
                raw_object_key varchar,
                page_no varchar,
                load_date varchar,
                ingest_ts varchar,
                run_id varchar,
                collected_at timestamp(6)
            )
            WITH (format = 'PARQUET', partitioning = ARRAY['load_date'])
            """
        )
        return table

    def load(self, ds, ctx, records: list, batch_size: int = 500) -> int:
        """레코드를 bronze 테이블에 적재(멱등). ``records`` = (raw_object_key, page_no, dict) 목록.

        같은 ``ingest_ts`` 파티션을 먼저 지우고 다시 넣어, 재실행이 중복을 만들지 않게 한다.
        반환: 적재 행 수.
        """
        if not records:
            return 0
        table = self.ensure_table(ds.name)
        # 멱등: 이번 실행분(ingest_ts)을 제거 후 삽입.
        self.client.execute(f"DELETE FROM {table} WHERE ingest_ts = {_lit(ctx.ingest_ts)}")

        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        collected = "TIMESTAMP " + _lit(now_utc)
        col_list = "(" + ", ".join(_COLUMNS) + ")"
        prefix = f"INSERT INTO {table} {col_list} VALUES "

        # 배치를 행 수(batch_size)뿐 아니라 **SQL UTF-8 바이트 길이**로도 끊는다.
        # record_json이 큰 데이터셋(전시·세종 등)은 500행이면 INSERT가 Trino 최대 쿼리
        # 길이를 넘긴다. 한글은 UTF-8에서 1자=최대 3바이트라 문자 수로 세면 과소 측정되고
        # (실제 전송은 sql.encode("utf-8")), 문자 800k라도 바이트로는 훨씬 커질 수 있으므로
        # 실제 바이트로 세어 안전하게 분할한다(#21).
        max_sql_bytes = 800_000

        inserted = 0
        buffer: list[str] = []
        buffer_len = len(prefix.encode("utf-8"))

        def _flush() -> None:
            nonlocal inserted, buffer, buffer_len
            if buffer:
                self.client.execute(prefix + ", ".join(buffer))
                inserted += len(buffer)
                buffer = []
                buffer_len = len(prefix.encode("utf-8"))

        for seq, (raw_object_key, page_no, record) in enumerate(records):
            record_json = json.dumps(record, ensure_ascii=False)
            value = (
                "("
                + ", ".join(
                    [
                        _lit(ds.name),
                        _lit(ds.source),
                        _lit(ds.endpoint),
                        str(seq),
                        _lit(record_json),
                        _lit(raw_object_key),
                        _lit(page_no),
                        _lit(ctx.load_date),
                        _lit(ctx.ingest_ts),
                        _lit(ctx.run_id),
                        collected,
                    ]
                )
                + ")"
            )
            # 행 수 또는 SQL 바이트 상한에 닿으면 먼저 비운다(현재 value는 다음 배치로).
            vbytes = len(value.encode("utf-8"))
            if buffer and (len(buffer) >= batch_size or buffer_len + vbytes + 2 > max_sql_bytes):
                _flush()
            buffer.append(value)
            buffer_len += vbytes + 2

        _flush()
        return inserted

    def count(self, dataset: str, ingest_ts: str | None = None) -> int:
        """검증용: 테이블(또는 특정 ingest_ts 파티션) 행 수."""
        sql = f"SELECT count(*) FROM {self.qualified(dataset)}"
        if ingest_ts:
            sql += f" WHERE ingest_ts = {_lit(ingest_ts)}"
        rows = self.client.execute(sql)
        return int(rows[0][0]) if rows else 0
