"""citydata bronze Iceberg 적재 -- (장소 × 블록) 행 + 원본 블록 payload (#192).

``common/bronze``(인구 전용)와 같은 schema-on-read 원칙이되, citydata 는 응답이
~20개 블록 복합이라 **블록 1개 = 행 1개**로 분해해 담는다. 250컬럼 와이드 테이블
대신 (area, block_name, payload) 구조 -- silver 가 블록별로 골라 파싱한다.

적재는 ``ingest_ts`` 기준 delete-then-insert 로 멱등(재시도 안전). payload 가 커서
(도로 블록 제외 후에도 수 KB) INSERT 배치 크기를 인구 bronze 보다 작게 잡는다.
"""

from __future__ import annotations

import dataclasses
import os
from dataclasses import dataclass
from datetime import datetime, timezone

from .trino import build_trino_settings, connect, ensure_schema, sql_identifier, sql_int, sql_string

CITYDATA_BRONZE_TABLE = "bronze_seoul_citydata"

# citydata 는 **seoul_citydata 스키마로 분리**(#69) — population(seoul_ppltn)과 격리한다.
# (상권·따릉이·대기질 등은 인구와 별개 신호라 스키마도 분리한다.)
CITYDATA_SCHEMA_ENV = "SEOUL_CITYDATA_SCHEMA"
DEFAULT_CITYDATA_SCHEMA = "seoul_citydata"

_COLUMNS = (
    "request_id",
    "source_id",
    "requested_area_nm",
    "area_nm",
    "area_cd",
    "block_name",
    "payload",
    "payload_hash",
    "raw_object_key",
    "http_status",
    "collected_at",
    "load_date",
    "ingest_ts",
    "dag_run_id",
)


@dataclass
class CitydataBronzeRow:
    """bronze 한 행 = 장소 1곳의 블록 1개 원본 + 추적 메타데이터."""

    request_id: str
    source_id: str
    requested_area_nm: str
    area_nm: str | None
    area_cd: str | None
    block_name: str
    payload: str          # ★ 블록 원본 JSON (내부 파싱 안 함)
    payload_hash: str     # 전체 응답 bytes 의 SHA-256 (블록이 아니라 응답 단위)
    raw_object_key: str
    http_status: int | None


class CitydataBronze:
    """citydata bronze Iceberg 테이블 생성/적재 (Trino DBAPI 경유)."""

    def __init__(self, settings=None, *, target: str = "dev", schema: str | None = None):
        # citydata bronze 는 seoul_citydata 스키마에 적재(population 격리).
        # 명시 schema > SEOUL_CITYDATA_SCHEMA env > 기본 seoul_citydata.
        base = settings or build_trino_settings(target)
        citydata_schema = schema or os.environ.get(CITYDATA_SCHEMA_ENV, DEFAULT_CITYDATA_SCHEMA)
        self.s = dataclasses.replace(base, schema=sql_identifier(citydata_schema))
        self.conn = connect(self.s)
        self.cur = self.conn.cursor()

    @property
    def qualified(self) -> str:
        return f"{sql_identifier(self.s.catalog)}.{sql_identifier(self.s.schema)}.{sql_identifier(CITYDATA_BRONZE_TABLE)}"

    def ensure_table(self) -> str:
        ensure_schema(self.cur, self.s.catalog, self.s.schema)
        self.cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.qualified} (
                request_id varchar,
                source_id varchar,
                requested_area_nm varchar,
                area_nm varchar,
                area_cd varchar,
                block_name varchar,
                payload varchar,
                payload_hash varchar,
                raw_object_key varchar,
                http_status integer,
                collected_at timestamp(6),
                load_date varchar,
                ingest_ts varchar,
                dag_run_id varchar
            )
            WITH (format = 'PARQUET', partitioning = ARRAY['load_date'])
            """
        )
        return self.qualified

    def load(self, rows: list[CitydataBronzeRow], *, load_date: str, ingest_ts: str,
             dag_run_id: str, batch_size: int = 20) -> int:
        """블록 행들을 멱등 적재. 같은 ``ingest_ts`` 를 지우고 다시 넣는다."""
        if not rows:
            return 0
        table = self.ensure_table()
        self.cur.execute(f"DELETE FROM {table} WHERE ingest_ts = {sql_string(ingest_ts)}")
        self.cur.fetchall()

        collected = "TIMESTAMP " + sql_string(
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        )
        prefix = f"INSERT INTO {table} ({', '.join(_COLUMNS)}) VALUES "

        inserted = 0
        buffer: list[str] = []

        def _flush() -> None:
            nonlocal inserted, buffer
            if buffer:
                self.cur.execute(prefix + ", ".join(buffer))
                self.cur.fetchall()
                inserted += len(buffer)
                buffer = []

        for r in rows:
            buffer.append(
                "(" + ", ".join([
                    sql_string(r.request_id),
                    sql_string(r.source_id),
                    sql_string(r.requested_area_nm),
                    sql_string(r.area_nm),
                    sql_string(r.area_cd),
                    sql_string(r.block_name),
                    sql_string(r.payload),
                    sql_string(r.payload_hash),
                    sql_string(r.raw_object_key),
                    sql_int(r.http_status),
                    collected,
                    sql_string(load_date),
                    sql_string(ingest_ts),
                    sql_string(dag_run_id),
                ]) + ")"
            )
            if len(buffer) >= batch_size:
                _flush()
        _flush()
        return inserted
