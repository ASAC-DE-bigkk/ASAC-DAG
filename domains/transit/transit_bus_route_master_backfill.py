"""버스 노선 마스터 소급 백필 DAG (#765) — R2 raw XML 재파싱 → bronze 재적재.

파서가 3필드(busRouteId·busRouteNm·routeType)만 취하던 시절의 주간 스냅샷에도
시간표 필드(첫차·막차·배차간격·기점·종점 등)가 원본 XML 에 그대로 있다 —
bus_route_master 는 주 경계 purge 예외(보존 대상)라 raw 가 전부 남아 있다.
이 DAG 은 raw 스냅샷 전량을 현행 파서로 재파싱해 load_date 단위 멱등
재적재한다(reference 는 건드리지 않는다 — collector 영향 0).

수동 전용(schedule=None). 같은 load_date 재실행은 그 날짜만 교체(멱등).
"""

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback
from common.runmetrics import track

from seoul_transit import bus_routes, config

DOMAIN = config.TRANSIT_DOMAIN
SOURCE = config.BUS_SOURCE
DATASET = "bus_route_master"
RAW_PREFIX = f"raw/{DOMAIN}/{SOURCE}/{DATASET}/"

record_transit_problem = problem_failure_callback(domain=DOMAIN, source_system=SOURCE)


def _snapshot_labels_from_key(key: str) -> tuple[str, str]:
    """raw 키 → (load_date, ingest_ts). 규약: .../load_date=<d>/ingest_ts=<t>/page-*.xml"""
    parts = dict(
        seg.split("=", 1) for seg in key.split("/") if "=" in seg
    )
    load_date, ingest_ts = parts.get("load_date"), parts.get("ingest_ts")
    if not load_date or not ingest_ts:
        raise ValueError(f"raw 키에서 load_date/ingest_ts 추출 실패: {key}")
    return load_date, ingest_ts


def backfill_route_master_bronze() -> dict:
    """R2 raw 스냅샷 전량 재파싱 → load_date 별 멱등 재적재.

    - load_date 가 여러 ingest_ts 를 가지면 최신 ingest_ts 만 쓴다(그 날짜의 최종본).
    - 위생 가드(MIN_ROUTES)에 걸리는 스냅샷은 건너뛰고 보고만 한다(부분 응답 보존물).
    """
    import trino.dbapi

    from seoul_transit.r2_landing import get_bytes, list_keys

    keys = [k for k in list_keys(RAW_PREFIX) if k.endswith(".xml")]
    if not keys:
        raise RuntimeError(f"raw 스냅샷 없음 — prefix={RAW_PREFIX}")

    # load_date → 최신 ingest_ts 키 1개
    latest_by_date: dict[str, tuple[str, str]] = {}
    for key in keys:
        load_date, ingest_ts = _snapshot_labels_from_key(key)
        held = latest_by_date.get(load_date)
        if held is None or ingest_ts > held[0]:
            latest_by_date[load_date] = (ingest_ts, key)

    dev = os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"
    catalog = os.environ.get("TRINO_ICEBERG_CATALOG") or ("iceberg_dev" if dev else "iceberg")
    schema = os.environ.get("TRANSIT_SCHEMA", "transit")
    qualified = f"{catalog}.{schema}.{bus_routes.BUS_ROUTE_MASTER_TABLE}"
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog, schema=schema,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    cur = conn.cursor()
    cur.execute(bus_routes.master_ddl(qualified))
    cur.fetchall()
    for stmt in bus_routes.master_migration_sql(qualified):
        cur.execute(stmt)
        cur.fetchall()

    run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual")
    loaded, skipped = [], []
    for load_date in sorted(latest_by_date):
        ingest_ts, key = latest_by_date[load_date]
        xml = get_bytes(key).decode("utf-8")
        try:
            routes = bus_routes.parse_routes(xml)
        except RuntimeError as exc:
            skipped.append((load_date, str(exc)))
            continue
        rows = bus_routes.build_master_rows(routes, config.BUS_TIER1_TYPES)
        _, collected_at = bus_routes.snapshot_labels(ingest_ts)
        for stmt in bus_routes.master_load_sql(
            qualified, rows,
            load_date=load_date, collected_at=collected_at, dag_run_id=run_id,
        ):
            cur.execute(stmt)
            cur.fetchall()
        loaded.append((load_date, len(rows)))
        print(f"backfill {load_date}: {len(rows)}노선 ({key})")

    for load_date, reason in skipped:
        print(f"backfill skip {load_date}: {reason}")
    if not loaded:
        raise RuntimeError(f"백필 적재 0건 — skip={len(skipped)}")
    return {"loaded": dict(loaded), "skipped": len(skipped)}


with DAG(
    dag_id="transit_bus_route_master_backfill",
    description="R2 raw 스냅샷 재파싱 → bronze_bus_route_master 시간표 필드 소급 백필(#765). 수동 전용.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["seoul", "transit", "bus", "master", "backfill"],
) as dag:
    PythonOperator(
        task_id="backfill_route_master_bronze",
        python_callable=track(layer="bronze", domain="transit")(backfill_route_master_bronze),
        on_failure_callback=record_transit_problem,
    )
