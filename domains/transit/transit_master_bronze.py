"""transit 마스터 저빈도 수집 DAG (#162) — 파일명=dag_id.

서울 열린데이터광장 마스터 2종을 **@weekly** 로 R2 에 랜딩(raw)한 뒤 Trino 로
Iceberg 브론즈에 **load_date 단위 멱등** 적재한다:

1. subwayStationMaster → bronze_subway_station_master (~784행, WGS84 LAT/LOT)
2. GetParkInfo         → bronze_park_info_master     (~2,204행, 주소·좌표)

⚠️ GetParkInfo(마스터) ≠ GetParkingInfo(실시간, transit_parking_bronze). 별개 서비스.

각 원천은 land→load→verify 3태스크 체인이며 두 체인은 한 DAG 안에서 병렬로 돈다.
순수 로직은 seoul_transit.masters(테스트 대상), 이 파일은 오케스트레이션만.
Trino 헬퍼·멱등 skip·verify 는 common_admin_dong_bronze.py 표준을 이식했다.
공통 자산 재사용: SeoulOpenApiClient(#78) · common.storage(#109) · 에러 콜백(#77).
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime, timezone

# 동봉 패키지(seoul_transit)는 이 DAG 파일과 같은 폴더에 있다. Airflow 3.x 는 dags 하위
# 디렉터리를 sys.path 에 자동 추가하지 않으므로 자기 폴더 + dags 루트를 직접 올린다.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from common.errors.airflow import problem_failure_callback
from common.runmetrics import track

from seoul_transit import masters
from seoul_transit.masters import SPECS, MasterSpec

LOGGER = logging.getLogger(__name__)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Trino VALUES 배치 크기 — QUERY_TEXT_TOO_LARGE 회피(admin_dong/traffic 교훈). 500 보수적.
_INSERT_BATCH = 500

# 계보 컬럼(원천 필드 뒤에 공통으로 붙는다) — 이름→Iceberg 타입. collected_at 만
# timestamp(6), 나머지 varchar. 컬럼 이름 리스트와 DDL 타입을 여기 한 곳에서 파생한다.
_LINEAGE_COLUMNS: dict[str, str] = {
    "raw_object_key": "varchar",
    "source_system": "varchar",
    "collected_at": "timestamp(6)",
    "load_date": "varchar",
    "dag_run_id": "varchar",
}


# ── env / SQL 헬퍼 (admin_dong/traffic runtime 관례 이식) ─────────────────────────
def is_dev_target() -> bool:
    return os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"


def trino_catalog() -> str:
    # canonical 키 하나 — 값이 배포 환경을 따라간다(미설정 시 기본만 타깃별).
    return os.environ.get("TRINO_ICEBERG_CATALOG") or (
        "iceberg_dev" if is_dev_target() else "iceberg")


def transit_schema() -> str:
    # 도메인 공용 스키마(#367): prod/dev 동일 transit, 환경 분리는 카탈로그(trino_catalog)가 담당.
    return os.environ.get("TRANSIT_SCHEMA", "transit")


def sql_identifier(value: str) -> str:
    if not _IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def sql_string(value: object) -> str:
    if value is None or value == "":
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_timestamp(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return "TIMESTAMP " + sql_string(utc_value.strftime("%Y-%m-%d %H:%M:%S.%f"))


def current_dag_run_id() -> str:
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual")


def _trino_cursor():
    import trino.dbapi

    catalog = sql_identifier(trino_catalog())
    schema = sql_identifier(transit_schema())
    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog,
        schema=schema,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return connection.cursor(), catalog, schema


def _qualified_table(catalog: str, schema: str, spec: MasterSpec) -> str:
    return f"{catalog}.{schema}.{sql_identifier(spec.table)}"


def _all_columns(spec: MasterSpec) -> list[str]:
    return masters.column_names(spec) + list(_LINEAGE_COLUMNS)


def _ensure_table(cursor, catalog: str, schema: str, spec: MasterSpec) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = _qualified_table(catalog, schema, spec)
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
    except Exception as exc:  # noqa: BLE001
        if "already exists" not in str(exc).lower():
            raise
    # 원천 필드는 전부 varchar(코드류 선행 0 보존), 계보 컬럼 타입은 _LINEAGE_COLUMNS 파생.
    column_defs = [f"{sql_identifier(c)} varchar" for c in masters.column_names(spec)]
    column_defs += [f"{name} {sqltype}" for name, sqltype in _LINEAGE_COLUMNS.items()]
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {qualified_table} (\n  "
        + ",\n  ".join(column_defs)
        + "\n) WITH (format = 'PARQUET')"
    )
    return qualified_table


# ── task 1: R2 랜딩 ──────────────────────────────────────────────────────────────
def land_master(spec_key: str, **context) -> dict:
    spec = SPECS[spec_key]
    run_id = context.get("run_id") or current_dag_run_id()

    key = masters.load_key()
    client = masters.build_client(masters.build_core(), key)

    pages = masters.iter_master(client, spec)
    result = masters.land_master(pages, spec, run_id=str(run_id))

    LOGGER.info(
        "R2 랜딩 완료 [%s] — pages=%d rows=%d bytes=%d load_date=%s",
        spec.dataset, len(result["object_keys"]), result["rows"],
        result["bytes"], result["load_date"],
    )
    return {
        "dataset": spec.dataset,
        "rows": result["rows"],
        "load_date": result["load_date"],
        "object_keys": result["object_keys"],
    }


# ── task 2: Trino Iceberg 브론즈 적재 (load_date 단위 멱등) ───────────────────────
def load_master(spec_key: str, **context) -> dict:
    spec = SPECS[spec_key]
    ti = context["ti"]
    landed = ti.xcom_pull(task_ids=f"land_{spec.dataset}")
    object_keys = landed["object_keys"]
    load_date = landed["load_date"]
    landed_rows = landed["rows"]
    dag_run_id = str(context.get("run_id") or current_dag_run_id())

    cursor, catalog, schema = _trino_cursor()
    qualified_table = _ensure_table(cursor, catalog, schema, spec)

    # 멱등/자가치유: 이 load_date 의 기존 행수를 이번 랜딩 행수와 비교한다.
    #   existing == landed_rows : 진짜 멱등 재실행 → skip.
    #   existing >  0 && != landed_rows : partial(배치 중간 실패) 또는 당일 원천 행수 변동
    #       → DELETE 후 재적재(self-heal, Iceberg DELETE 지원). verify 영구 실패 방지.
    #   existing == 0 : 최초 적재 → 그대로 진행.
    cursor.execute(
        f"SELECT count(*) FROM {qualified_table} WHERE load_date = {sql_string(load_date)}"
    )
    existing = cursor.fetchone()[0]
    action = masters.load_action(existing, landed_rows)
    if action == "skip":
        LOGGER.info(
            "멱등 skip [%s] — load_date=%s 는 이미 %d행 적재됨(랜딩 행수와 일치, 재적재 안 함)",
            spec.dataset, load_date, existing,
        )
        return {"dataset": spec.dataset, "inserted": 0, "skipped": True, "existing": existing}
    if action == "reload":
        LOGGER.info(
            "self-heal 재적재 [%s] — load_date=%s 기존 %d행 != 랜딩 %d행 "
            "(partial/원천변동 의심) → DELETE 후 재적재",
            spec.dataset, load_date, existing, landed_rows,
        )
        cursor.execute(
            f"DELETE FROM {qualified_table} WHERE load_date = {sql_string(load_date)}"
        )

    store = masters.build_r2_storage()
    collected_at = datetime.now(timezone.utc)
    ts_literal = sql_timestamp(collected_at)
    columns = _all_columns(spec)
    columns_sql = ", ".join(sql_identifier(c) for c in columns)

    pending: list[str] = []
    inserted = 0

    def flush() -> None:
        nonlocal inserted, pending
        if not pending:
            return
        cursor.execute(
            f"INSERT INTO {qualified_table} ({columns_sql}) VALUES {', '.join(pending)}"
        )
        inserted += len(pending)
        pending = []

    for object_key in object_keys:
        document = json.loads(store.read_bytes(object_key).decode("utf-8"))
        for row in masters.rows_from_document(document, spec):
            values = [sql_string(row.get(f)) for f in spec.fields]
            values += [
                sql_string(object_key),
                sql_string(spec.source_system),
                ts_literal,
                sql_string(load_date),
                sql_string(dag_run_id),
            ]
            pending.append("(" + ", ".join(values) + ")")
            if len(pending) >= _INSERT_BATCH:
                flush()
    flush()

    LOGGER.info("브론즈 적재 완료 [%s] — load_date=%s inserted=%d",
                spec.dataset, load_date, inserted)
    return {"dataset": spec.dataset, "inserted": inserted, "skipped": False}


# ── task 3: 검증 (수집 rows == Trino count) ──────────────────────────────────────
def verify_master(spec_key: str, **context) -> dict:
    spec = SPECS[spec_key]
    ti = context["ti"]
    landed = ti.xcom_pull(task_ids=f"land_{spec.dataset}")
    load_date = landed["load_date"]
    expected_rows = landed["rows"]

    cursor, catalog, schema = _trino_cursor()
    qualified_table = _qualified_table(catalog, schema, spec)

    cursor.execute(
        f"SELECT count(*) FROM {qualified_table} WHERE load_date = {sql_string(load_date)}"
    )
    total = cursor.fetchone()[0]

    suffix = f" (기대 ~{spec.expected_rows})" if spec.expected_rows is not None else ""
    LOGGER.info(
        "검증 [%s] — load_date=%s trino_count=%d 수집rows=%d%s",
        spec.dataset, load_date, total, expected_rows, suffix,
    )
    if total != expected_rows:
        raise RuntimeError(
            f"검증 실패 [{spec.dataset}] — load_date={load_date} "
            f"trino_count={total} != 수집rows={expected_rows}"
        )
    return {"dataset": spec.dataset, "trino_count": total, "expected_rows": expected_rows}


with DAG(
    dag_id="transit_master_bronze",
    description="서울 열린데이터 마스터 2종(subwayStationMaster·GetParkInfo)을 주간 R2 랜딩 후 "
                "Iceberg 브론즈에 load_date 단위 멱등 적재. silver 는 ASAC-DBT.",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["transit", "master", "bronze"],
) as dag:
    for _spec in SPECS.values():
        # 에러 콜백(#77) — 원천별 source_system(기존 transit DAG 값과 정합).
        _record_problem = problem_failure_callback(
            domain="transit", source_system=_spec.source_system
        )
        # 실행 메트릭(#188) — 콜러블을 track 으로 래핑(레코드는 dags/metrics/ 에 적재).
        _land = PythonOperator(
            task_id=f"land_{_spec.dataset}",
            python_callable=track(layer="bronze", domain="transit")(land_master),
            op_kwargs={"spec_key": _spec.dataset},
            on_failure_callback=[_record_problem],
        )
        _load = PythonOperator(
            task_id=f"load_{_spec.dataset}",
            python_callable=track(layer="bronze", domain="transit")(load_master),
            op_kwargs={"spec_key": _spec.dataset},
            on_failure_callback=[_record_problem],
        )
        _verify = PythonOperator(
            task_id=f"verify_{_spec.dataset}",
            python_callable=track(layer="bronze", domain="transit")(verify_master),
            op_kwargs={"spec_key": _spec.dataset},
            on_failure_callback=[_record_problem],
        )
        _land >> _load >> _verify
