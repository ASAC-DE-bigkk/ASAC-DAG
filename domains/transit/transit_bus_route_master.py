"""버스 노선 마스터 DAG (#369) — getBusRouteList 주간 스냅샷 → R2 raw + reference 갱신.

transit_bus_bronze(collector, BUS_ROUTES=ALL)가 소비하는
reference/transit/bus_routes/latest.json 을 갱신한다. 노선 개폐(신설·폐지)는
주 단위면 충분 — 실시간 아님.

⚠️ 부트스트랩: collector 를 ALL 모드로 켜기 전에 이 DAG 을 최초 1회 수동 트리거.
위생 가드(bus_routes.MIN_ROUTES)로 부분/빈 응답이면 reference 를 덮지 않는다.

태스크 2 (#471): reference → bronze_bus_route_master 적재(routeType·tier). tier 는
collector 수집 정책(BUS_TIER1_TYPES)으로 여기서 계산해 넣고, ASAC-DBT
dim_transit_bus_route_tier 가 관측 역산 대신 이 tier 를 직접 조인한다.
"""

import os
import re
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


def _trino_target() -> tuple[str, str]:
    """(catalog, schema) — transit_master_bronze 와 동일 규약(#367): dev/prod 는 카탈로그로
    분리, 스키마는 공용 transit."""
    dev = os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"
    catalog = (
        os.environ.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev") if dev
        else os.environ.get("TRINO_ICEBERG_CATALOG", "iceberg")
    )
    return catalog, os.environ.get("TRANSIT_SCHEMA", "transit")


def load_route_master_bronze() -> dict:
    """reference(latest.json)의 노선 → bronze_bus_route_master 전체 교체 적재.

    reference 를 원천으로 삼는다(refresh 태스크가 방금 갱신). tier 는 collector 수집
    정책(config.BUS_TIER1_TYPES)으로 여기서 계산해 넣는다 — ASAC-DBT dim_transit_bus_route_tier
    가 관측 역산 대신 이 tier 를 직접 조인한다(#471). 주간 스냅샷이라 load_date 이력을
    쌓지 않고 매주 갈아끼운다(노선 개폐 반영).
    """
    import trino.dbapi

    from seoul_transit.r2_landing import get_json

    ref = get_json(config.BUS_ROUTES_REFERENCE_KEY)
    rows = bus_routes.build_master_rows(ref.get("routes", []), config.BUS_TIER1_TYPES)
    if len(rows) < bus_routes.MIN_ROUTES:
        # reference 가 어떤 이유로 얇아졌으면(부분 응답 잔존 등) 기존 bronze 를 덮지 않는다.
        raise RuntimeError(
            f"노선 마스터 적재 가드 — reference {len(rows)}개 < 최소 {bus_routes.MIN_ROUTES}"
        )

    # ingest_ts 는 land()가 항상 채우지만(정상 경로), 옛 포맷·수동 조작으로 결손되면
    # 슬라이싱이 깨진 timestamp 리터럴('-- ::')을 만들어 INSERT 가 실패한다 — 명확히 막는다.
    ingest_ts = str(ref.get("ingest_ts", ""))  # 예: 20260721T134500Z (UTC)
    if not re.fullmatch(r"\d{8}T\d{6}Z", ingest_ts):
        raise RuntimeError(
            f"reference.ingest_ts 형식 오류({ingest_ts!r}) — YYYYMMDDTHHMMSSZ 기대. 적재 중단"
        )
    load_date = f"{ingest_ts[0:4]}-{ingest_ts[4:6]}-{ingest_ts[6:8]}"
    collected_at = (
        f"{load_date} {ingest_ts[9:11]}:{ingest_ts[11:13]}:{ingest_ts[13:15]}.000000"
    )
    run_id = current_dag_run_id()

    catalog, schema = _trino_target()
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
    for stmt in bus_routes.master_load_sql(
        qualified, rows, load_date=load_date, collected_at=collected_at, dag_run_id=run_id,
    ):
        cur.execute(stmt)
        cur.fetchall()  # DELETE·INSERT 실행 확정

    tier1 = sum(1 for r in rows if r["tier"] == 1)
    print(
        f"bronze_bus_route_master 적재: {len(rows)}노선 "
        f"(tier1={tier1}, tier2={len(rows) - tier1}) load_date={load_date}"
    )
    return {"total": len(rows), "tier1": tier1}


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
    load_bronze = PythonOperator(
        task_id="load_route_master_bronze",
        python_callable=track(layer="bronze", domain="transit")(load_route_master_bronze),
        on_failure_callback=record_transit_problem,
    )

    refresh >> load_bronze
