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

# 메타DB Connection id — 도메인 중립(메타DB 는 팀 공용, §6.2 _shared 승격 대비).
# 등록은 CLI 1회(런북 참조). 미등록이면 load_dag_runs 가 스킵한다(안전망).
METADB_CONN_ID = "airflow_metadb"


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
            retried_tasks bigint,
            max_try bigint,
            load_date varchar
        )
        WITH (format = 'PARQUET', partitioning = ARRAY['load_date'])
        """
    )
    # 이미 만들어진 표에는 CREATE IF NOT EXISTS 가 컬럼을 안 붙인다 — ASAC-DAG#521 에서
    # `_catalog` 가 정확히 이걸로 갈라졌다(선언 15컬럼 vs 라이브 8컬럼). 신규 컬럼은
    # 발행 시점에 명시적으로 맞춘다. ADD COLUMN IF NOT EXISTS 라 재실행에 안전하고,
    # 기존 행은 NULL = "그 시절엔 안 재던 값"으로 남는다(0 으로 채우지 않는다).
    for column, sql_type in (("retried_tasks", "bigint"), ("max_try", "bigint")):
        wh.client.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {_ident(column)} {sql_type}"
        )
    return table


def _num(value) -> str:
    return "NULL" if value is None else repr(float(value))


def _int(value) -> str:
    """bigint 리터럴. NULL 은 그대로 둔다 — 0 으로 접으면 '관측 없음'이 '재시도 없음'이 된다."""
    return "NULL" if value is None else str(int(value))


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
    from sqlalchemy import bindparam, text

    wh = build_warehouse(target, engine="trino")
    table = _ensure_dag_runs_table(wh)
    existing = int(wh.client.execute(f"SELECT count(*) FROM {table}")[0][0])
    first_run = existing == 0

    cutoff_utc = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=window_days)
    cutoff_kst_date = (_dt.datetime.now(_KST) - _dt.timedelta(days=window_days)).date().isoformat()

    # Airflow 3.0 은 태스크 워커에서 메타DB 직결을 격리한다(ORM·SQL_ALCHEMY_CONN env 둘 다, #303).
    # v2(#411): PostgresHook + 정의된 Connection(METADB_CONN_ID) — 태스크 허용 경로.
    # Connection 미등록(스택 재구축 직후 등)이면 스킵 — 핵심 SLO(run_report 기반)는 dag_run 과
    # 무관하므로 마트는 정상 동작하고, 표는 빈 채로 보장된다(silver 가 읽음). 등록 절차 = 런북.
    try:
        # 재시도 집계(#201) — task_instance 를 run 단위로 접어 붙인다. dag_run 만 보면
        # 재시도로 살아난 런이 깨끗한 런과 똑같이 success 라, 재발이 지표에 안 남는다.
        # LEFT JOIN 이라 태스크가 아직 없는 run(막 시작)은 NULL 로 남는다 — 0 이 아니다.
        sql = (
            "SELECT d.dag_id, d.run_id, d.state, d.run_type, d.start_date, d.end_date, "
            "       t.retried_tasks, t.max_try "
            "FROM dag_run d "
            "LEFT JOIN ("
            "    SELECT dag_id, run_id, "
            "           count(*) FILTER (WHERE try_number > 1) AS retried_tasks, "
            "           max(try_number) AS max_try "
            "    FROM task_instance GROUP BY dag_id, run_id"
            ") t ON t.dag_id = d.dag_id AND t.run_id = d.run_id "
            "WHERE d.dag_id IN :ids AND d.start_date IS NOT NULL"
        )
        params = {"ids": list(dag_ids)}
        if not first_run:
            sql += " AND d.start_date >= :cutoff"
            params["cutoff"] = cutoff_utc
        stmt = text(sql).bindparams(bindparam("ids", expanding=True))

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=METADB_CONN_ID)
        try:
            engine = hook.get_sqlalchemy_engine()
        except AttributeError:  # provider 구버전 폴백 — 동일 효과
            from sqlalchemy import create_engine

            engine = create_engine(hook.get_uri())
        try:
            with engine.connect() as conn:
                rows = [
                    dag_run_row(SimpleNamespace(**dict(r._mapping)), domain=domain)
                    for r in conn.execute(stmt, params)
                ]
        finally:
            engine.dispose()
    except Exception as exc:  # noqa: BLE001 — Connection 미등록/메타DB 접근 불가 방어
        print(
            f"slo: dag_run enrichment 스킵({type(exc).__name__}) — "
            f"Connection '{METADB_CONN_ID}' 등록 여부 확인(런북 참조)."
        )
        return 0

    # 멱등: 첫 실행이면 표가 비어 delete 무의미 → 전량 append. 이후는 14일 윈도우 교체.
    if not first_run:
        wh.client.execute(f"DELETE FROM {table} WHERE load_date >= {_lit(cutoff_kst_date)}")
    if not rows:
        return 0

    cols = ("(domain, dag_id, run_id, state, run_type, start_at, end_at, duration_sec, "
            "retried_tasks, max_try, load_date)")
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
                _int(r["retried_tasks"]),
                _int(r["max_try"]),
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
