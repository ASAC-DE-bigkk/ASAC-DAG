"""culture SLO 로더 IO — 트리노/R2/Airflow 실행부.

순수 로직(스캔·행빌드)은 loader.py(호스트 pytest). 여기는 부수효과(트리노 write,
Airflow meta 읽기)만 조립하므로 컨테이너 라이브(게이트 후)에서 검증한다.

- run_report: warehouse 디스패치(trino) 재사용. 리포트 1건 = 자기 ingest_ts 로
  load() → delete+insert 가 리포트 단위 멱등(재실행이 중복/누락 안 만듦).
- dag_runs: 타입드 컬럼(11열 record_json 형태 아님)이라 별도 CREATE + 14일 윈도우
  delete+insert. 첫 실행(빈 표)은 전체 이력 적재(설계 §3).
"""
from __future__ import annotations

import datetime as _dt
from types import SimpleNamespace

from culture_ingest.common.config import RunContext
from culture_ingest.common.warehouse import _ident, _lit
from culture_ingest.slo.loader import (
    CULTURE_SLO_DAG_IDS,
    dag_run_row,
    report_records,
    scan_new_reports,
)
from culture_ingest.source.ingest import build_r2_sink, build_warehouse

_KST = _dt.timezone(_dt.timedelta(hours=9))

# warehouse.load 는 ds.name/.source/.endpoint 만 읽는다. 표는 bronze_<name>.
_RUN_REPORT_DS = SimpleNamespace(name="culture_run_report", source="culture", endpoint="_reports")
_DAG_RUNS_DATASET = "culture_dag_runs"  # -> bronze_culture_dag_runs


def load_run_reports(*, target: str = "dev") -> int:
    """R2 _reports 스캔 → bronze_culture_run_report 적재. 반환: 신규 적재 행수."""
    sink = build_r2_sink(target)
    wh = build_warehouse(target, engine="trino")
    table = wh.ensure_table(_RUN_REPORT_DS.name)  # bronze_culture_run_report (11열 record_json)
    loaded = {row[0] for row in wh.client.execute(f"SELECT DISTINCT ingest_ts FROM {table}")}
    inserted = 0
    for key, report in scan_new_reports(sink, loaded):
        ctx = RunContext(
            load_date=report.get("load_date"),
            ingest_ts=report.get("ingest_ts"),
            run_id=report.get("run_id", "unknown"),
        )
        inserted += wh.load(_RUN_REPORT_DS, ctx, report_records([(key, report)]))
    return inserted


def _ensure_dag_runs_table(wh) -> str:
    """타입드 bronze_culture_dag_runs 를 멱등 생성하고 정규화 이름 반환."""
    wh.client.execute(
        f"CREATE SCHEMA IF NOT EXISTS {_ident(wh.s.catalog)}.{_ident(wh.s.schema)}"
    )
    table = wh.qualified(_DAG_RUNS_DATASET)  # cat.schema.bronze_culture_dag_runs (_ident 적용)
    wh.client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            domain varchar,
            dag_id varchar,
            run_id varchar,
            state varchar,
            run_type varchar,
            start_at varchar,
            end_at varchar,
            duration_sec double,
            load_date varchar
        )
        WITH (format = 'PARQUET', partitioning = ARRAY['load_date'])
        """
    )
    return table


def _num(value) -> str:
    return "NULL" if value is None else repr(float(value))


def load_dag_runs(
    *,
    target: str = "dev",
    dag_ids: tuple[str, ...] = CULTURE_SLO_DAG_IDS,
    window_days: int = 14,
    domain: str = "culture",
) -> int:
    """Airflow 메타DB dag_run 스캔 → bronze_culture_dag_runs 적재(14일 윈도우 멱등).

    첫 실행(빈 표)은 전체 이력. 이후는 최근 window_days 만 delete+insert(지각 상태변경 흡수).
    """
    import os

    from sqlalchemy import bindparam, create_engine, text

    wh = build_warehouse(target, engine="trino")
    table = _ensure_dag_runs_table(wh)
    existing = int(wh.client.execute(f"SELECT count(*) FROM {table}")[0][0])
    first_run = existing == 0

    cutoff_utc = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=window_days)
    cutoff_kst_date = (_dt.datetime.now(_KST) - _dt.timedelta(days=window_days)).date().isoformat()

    # Airflow 3.0 은 태스크에서 ORM(create_session) 접근 금지(#303) → 메타DB read-only 직접 조회.
    sql = (
        "SELECT dag_id, run_id, state, run_type, start_date, end_date "
        "FROM dag_run WHERE dag_id IN :ids AND start_date IS NOT NULL"
    )
    params = {"ids": list(dag_ids)}
    if not first_run:
        sql += " AND start_date >= :cutoff"
        params["cutoff"] = cutoff_utc
    stmt = text(sql).bindparams(bindparam("ids", expanding=True))
    engine = create_engine(os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"])
    try:
        with engine.connect() as conn:
            rows = [
                dag_run_row(SimpleNamespace(**dict(r._mapping)), domain=domain)
                for r in conn.execute(stmt, params)
            ]
    finally:
        engine.dispose()

    # 멱등: 첫 실행이면 표가 비어 delete 무의미 → 전량 append. 이후는 14일 윈도우 교체.
    if not first_run:
        wh.client.execute(f"DELETE FROM {table} WHERE load_date >= {_lit(cutoff_kst_date)}")
    if not rows:
        return 0

    cols = "(domain, dag_id, run_id, state, run_type, start_at, end_at, duration_sec, load_date)"
    values = [
        "("
        + ", ".join(
            [
                _lit(r["domain"]),
                _lit(r["dag_id"]),
                _lit(r["run_id"]),
                _lit(r["state"]),
                _lit(r["run_type"]),
                _lit(r["start_at"]),
                _lit(r["end_at"]),
                _num(r["duration_sec"]),
                _lit(r["load_date"]),
            ]
        )
        + ")"
        for r in rows
    ]
    inserted = 0
    batch = 200
    for i in range(0, len(values), batch):
        chunk = values[i : i + batch]
        wh.client.execute(f"INSERT INTO {table} {cols} VALUES " + ", ".join(chunk))
        inserted += len(chunk)
    return inserted
