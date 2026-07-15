"""서울 버스 ELT (transit 도메인) — TOPIS 버스 도착/위치 → XML 원본 R2 적재 + Iceberg bronze.

지하철(transit_subway_bronze)과 같은 도메인/패턴이나 차이:
  - 소스 = 서울 TOPIS 버스(ws.bus.go.kr), 키 = PUBLIC_DATA_API_KEY(URL 인코딩 필요)
  - 응답이 XML → 원본 그대로 보존(R2 ext=xml, Iceberg raw=XML varchar). 파싱은 silver(dbt).
  - 단위 = 노선(busRouteId). 이 DAG = Bronze 한정.

수집 스코프(#212): `bus_arrival` 은 **수집하지 않는다** — silver 가 소비하지 않아
(설계 v2, bus_source.md §5) 호출 예산을 위치로 재배분(720→360콜/일).
코드·테이블·파서는 그대로 유지 — SOURCES 에서 해당 항목만 주석 처리했다.
재개: 아래 SOURCES 의 주석을 해제하면 즉시 복귀.
⚠️ 실시간 데이터는 소급 수집 불가 — 중단 구간은 영구 이력 공백으로 남는다.
"""

import os
import re
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# 동봉 패키지 import (Airflow 3.x 는 dags 하위폴더를 sys.path 에 자동 추가 안 함)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback
from common.runmetrics import track

from seoul_transit import config
from seoul_transit.bus import collect_bus_raw
from seoul_transit.r2_landing import land

CATALOG = os.environ.get("TRINO_ICEBERG_CATALOG", "iceberg")
SCHEMA = os.environ.get("TRANSIT_SCHEMA", "transit")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

DOMAIN = os.environ.get("TRANSIT_DOMAIN", "transit")
SOURCE = os.environ.get("BUS_SOURCE", "seoul_bus")

# bronze 테이블 -> dataset
SOURCES = {
    # "bronze_bus_arrival": "bus_arrival",  # 수집 제외(#212) — silver 미소비, 재개 시 주석 해제
    "bronze_bus_position": "bus_position",
}

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_transit_problem = problem_failure_callback(domain=DOMAIN, source_system=SOURCE)


def sql_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def sql_str(value) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def current_dag_run_id() -> str:
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "unknown")


def _land_objects(dataset: str, raws: list, run_id: str) -> None:
    """bronze: 노선별 XML 원본을 page-NNNN.xml 로 적재 (원본 보존, silver 변환 없음)."""
    routes = [r["route"] for r in raws]
    res = land(
        stage="raw", domain=DOMAIN, source=SOURCE, dataset=dataset,
        pages=[r["raw"] for r in raws],
        endpoint=raws[0]["endpoint"] if raws else "", kind=dataset,
        rows=sum(max(r["rows"], 0) for r in raws), run_id=run_id,
        request_params={"busRouteId": routes}, ext="xml",
    )
    print(f"object landed [{dataset}] routes={len(routes)}: bronze={res['manifest_key']}")


def _load_bronze(table: str, dataset: str, raws: list, dag_run_id: str) -> int:
    """노선별 XML 원본을 Iceberg bronze 로 1행=1노선 적재 (raw=XML varchar)."""
    import trino.dbapi

    cat, sch, tbl = sql_identifier(CATALOG), sql_identifier(SCHEMA), sql_identifier(table)
    qualified = f"{cat}.{sch}.{tbl}"
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=CATALOG, schema=SCHEMA,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cat}.{sch}")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified} (
            source varchar,
            dataset varchar,
            bus_route_id varchar,
            ts_collected varchar,
            rows_cnt integer,
            raw varchar,
            ingested_at timestamp(6),
            dag_run_id varchar
        ) WITH (format = 'PARQUET')
        """
    )
    if not raws:
        print(f"{qualified}: 0 rows (skip insert)")
        return 0

    # XML 원본은 노선당 수백 KB → 한 INSERT 에 합치면 Trino 쿼리길이 한도(100만자) 초과.
    # 노선당 1행씩 개별 INSERT 로 분할 (QUERY_TEXT_TOO_LARGE 회피).
    cols = "source, dataset, bus_route_id, ts_collected, rows_cnt, raw, ingested_at, dag_run_id"
    for r in raws:
        row = (
            "("
            f"{sql_str(SOURCE)}, "
            f"{sql_str(dataset)}, "
            f"{sql_str(r['route'])}, "
            f"{sql_str(r['ts_collected'])}, "
            f"{int(r['rows'])}, "
            f"{sql_str(r['raw'])}, "
            f"TIMESTAMP {sql_str(ingested_at)}, "
            f"{sql_str(dag_run_id)}"
            ")"
        )
        cur.execute(f"INSERT INTO {qualified} ({cols}) VALUES {row}")
    print(f"{qualified}: inserted {len(raws)} rows (노선별 1행, raw=XML)")
    return len(raws)


def ingest_bus() -> dict:
    key = config.load_bus_key()  # URL 인코딩된 서비스키
    dag_run_id = current_dag_run_id()
    counts = {}
    for table, dataset in SOURCES.items():
        raws = collect_bus_raw(key, dataset)
        _land_objects(dataset, raws, dag_run_id)
        counts[dataset] = _load_bronze(table, dataset, raws, dag_run_id)
    print(f"ingest counts: {counts}")
    return counts


with DAG(
    dag_id="transit_bus_bronze",
    description="서울 TOPIS 버스 도착/위치 → R2 XML 원본 + Iceberg bronze. silver 는 ASAC-DBT.",
    start_date=datetime(2026, 1, 1),
    schedule=config.schedule_for("bus", "*/20 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["seoul", "transit", "bus", "ingest", "bronze", "xml", "trino", "iceberg"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_bus",
        # 실행 메트릭(#188) — 최소 침습 콜러블 래핑.
        python_callable=track(layer="bronze", domain="transit")(ingest_bus),
        on_failure_callback=record_transit_problem,
    )
