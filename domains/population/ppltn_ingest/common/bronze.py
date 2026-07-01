"""bronze Iceberg 적재 -- 메타데이터 + 원본 payload(단일 컬럼).

**schema-on-read** 방식이다. DAG(파이썬)는 응답을 필드로 분해하지 않고, 원본 레코드
JSON을 ``payload`` 컬럼에 통째로 넣고 이슈 #16의 추적 메타데이터만 남긴다. 개별 필드
분해(area_nm, congest_lvl 등)는 후속 **silver(dbt)** 가 ``json_extract`` 로 한다.

이 설계의 이점:
* API가 필드를 추가/변경해도 bronze 스키마가 안 깨진다(이전 COLUMN_NOT_FOUND 원인 제거).
* 파싱 규칙을 SQL(dbt)로 버전 관리한다.
* 원본이 payload로 보존돼 재처리(replay)가 가능하다.

적재는 ``ingest_ts`` 파티션 기준 delete-then-insert로 **멱등**하게 만든다(재시도 안전).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .trino import TrinoSettings, connect, ensure_schema, sql_identifier, sql_int, sql_string

BRONZE_TABLE = "bronze_seoul_ppltn"

# bronze 테이블 컬럼 순서 (이슈 #16 메타데이터 최소기준 + payload).
_COLUMNS = (
    "request_id",
    "source_id",
    "requested_area_nm",
    "result_code",
    "result_msg",
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
class BronzeRow:
    """bronze 한 행 = 장소 1건의 원본 레코드 + 추적 메타데이터."""

    request_id: str
    source_id: str
    requested_area_nm: str
    result_code: str | None
    result_msg: str | None
    payload: str  # ★ 원본 레코드 JSON (분해 안 함)
    payload_hash: str
    raw_object_key: str
    http_status: int | None


class Bronze:
    """population bronze Iceberg 테이블 생성/적재 (Trino DBAPI 경유)."""

    def __init__(self, settings: TrinoSettings):
        self.s = settings
        self.conn = connect(settings)
        self.cur = self.conn.cursor()

    @property
    def qualified(self) -> str:
        return f"{sql_identifier(self.s.catalog)}.{sql_identifier(self.s.schema)}.{sql_identifier(BRONZE_TABLE)}"

    def ensure_table(self) -> str:
        """스키마 + bronze 테이블을 멱등 생성하고 정규화된 이름을 반환."""
        ensure_schema(self.cur, self.s.catalog, self.s.schema)
        self.cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.qualified} (
                request_id varchar,
                source_id varchar,
                requested_area_nm varchar,
                result_code varchar,
                result_msg varchar,
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

    def load(self, rows: list[BronzeRow], *, load_date: str, ingest_ts: str, dag_run_id: str, batch_size: int = 50) -> int:
        """행들을 bronze에 멱등 적재한다. 같은 ``ingest_ts`` 파티션을 지우고 다시 넣는다.

        반환: 적재 행 수.
        """
        if not rows:
            return 0
        table = self.ensure_table()
        # 멱등: 이번 실행분(ingest_ts) 제거 후 삽입.
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
            value = (
                "("
                + ", ".join(
                    [
                        sql_string(r.request_id),
                        sql_string(r.source_id),
                        sql_string(r.requested_area_nm),
                        sql_string(r.result_code),
                        sql_string(r.result_msg),
                        sql_string(r.payload),
                        sql_string(r.payload_hash),
                        sql_string(r.raw_object_key),
                        sql_int(r.http_status),
                        collected,
                        sql_string(load_date),
                        sql_string(ingest_ts),
                        sql_string(dag_run_id),
                    ]
                )
                + ")"
            )
            buffer.append(value)
            if len(buffer) >= batch_size:
                _flush()
        _flush()
        return inserted

    def count(self, ingest_ts: str | None = None) -> int:
        """검증용: 테이블(또는 특정 ingest_ts 파티션) 행 수."""
        sql = f"SELECT count(*) FROM {self.qualified}"
        if ingest_ts:
            sql += f" WHERE ingest_ts = {sql_string(ingest_ts)}"
        self.cur.execute(sql)
        row = self.cur.fetchone()
        return int(row[0]) if row else 0
