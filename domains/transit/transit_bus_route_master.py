"""버스 노선 마스터 DAG (#369) — getBusRouteList 주간 스냅샷 → R2 raw + reference 갱신.

transit_bus_bronze(collector, BUS_ROUTES=ALL)가 소비하는
reference/transit/bus_routes/latest.json 을 갱신한다. 노선 개폐(신설·폐지)는
주 단위면 충분 — 실시간 아님.

⚠️ 부트스트랩: collector 를 ALL 모드로 켜기 전에 이 DAG 을 최초 1회 수동 트리거.
위생 가드(bus_routes.MIN_ROUTES)로 부분/빈 응답이면 reference 를 덮지 않는다.
Iceberg 브론즈 테이블 적재는 silver 소비가 생기면 후속(현재 소비자는 collector 뿐).
"""

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# 동봉 패키지 import (Airflow 3.x 는 dags 하위폴더를 sys.path 에 자동 추가 안 함)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback
from common.runmetrics import track

from seoul_transit import bus_routes, config
from seoul_transit.r2_landing import land, put_json

DOMAIN = config.TRANSIT_DOMAIN
SOURCE = config.BUS_SOURCE
DATASET = "bus_route_master"

record_transit_problem = problem_failure_callback(domain=DOMAIN, source_system=SOURCE)


def current_dag_run_id() -> str:
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "unknown")


def refresh_bus_routes() -> dict:
    """1콜 수집 → 파싱·위생가드 → R2 raw 스냅샷 랜딩 → reference 갱신(원자적 덮어쓰기)."""
    key = config.load_bus_key()
    run_id = current_dag_run_id()

    xml = bus_routes.fetch_route_list_xml(key)
    routes = bus_routes.parse_routes(xml)  # 가드 통과 후에만 아래 진행 — 빈 스냅샷 차단

    landed = land(
        stage="raw", domain=DOMAIN, source=SOURCE, dataset=DATASET,
        pages=[xml], endpoint="getBusRouteList", kind=DATASET,
        rows=len(routes), run_id=run_id, load_pattern="snapshot_replace",
        request_params={}, ext="xml",
    )
    put_json(
        config.BUS_ROUTES_REFERENCE_KEY,
        bus_routes.build_reference(routes, run_id=run_id, ingest_ts=landed["ingest_ts"]),
    )
    excluded = sum(1 for r in routes if r.get("routeType") in config.BUS_ROUTE_TYPES_EXCLUDE)
    print(
        f"bus routes refreshed: total={len(routes)} "
        f"(수집 대상 {len(routes) - excluded}, 제외타입 {sorted(config.BUS_ROUTE_TYPES_EXCLUDE)}={excluded}) "
        f"→ {config.BUS_ROUTES_REFERENCE_KEY}"
    )
    return {"total": len(routes), "collect_scope": len(routes) - excluded}


with DAG(
    dag_id="transit_bus_route_master",
    description="TOPIS 노선목록 주간 스냅샷 → R2 raw + collector reference(#369). "
                "collector ALL 모드 부트스트랩 필수.",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["seoul", "transit", "bus", "master", "reference"],
) as dag:
    refresh = PythonOperator(
        task_id="refresh_bus_routes",
        # 실행 메트릭(#188)
        python_callable=track(layer="bronze", domain="transit")(refresh_bus_routes),
        on_failure_callback=record_transit_problem,
    )
