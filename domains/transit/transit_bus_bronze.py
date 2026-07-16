"""서울 버스 수집(collector) — TOPIS 위치 XML → R2 raw 랜딩 + loader pending 마커.

#369 수집·적재 분리: 이 DAG 은 **R2 랜딩까지만** 한다. Iceberg bronze 적재는
transit_bronze_loader 가 pending 마커를 소비해 수행. dag_id 는 이력 연속성을 위해 유지.

수집 스코프(#369): BUS_ROUTES=ALL(기본) → 노선 마스터 reference(주간 갱신)에서
서울 전 노선(~728, 인천7·경기8 제외) 로드, 스레드풀 병렬 호출.
⚠️ 부트스트랩: transit_bus_route_master 를 최초 1회 실행해야 reference 가 생긴다.

수집 제외(#212 유지): `bus_arrival` 은 silver 미소비로 수집하지 않는다.
코드·테이블·파서는 유지 — SOURCES 에서 해당 항목만 주석 처리. 재개 시 주석 해제 + PR.
⚠️ 실시간 데이터는 소급 수집 불가 — 중단 구간은 영구 이력 공백으로 남는다.
"""

import json
import os
import sys
from datetime import datetime, timedelta

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

from seoul_transit import config, loader
from seoul_transit.bus import collect_bus_raw
from seoul_transit.r2_landing import land

# 경로 세그먼트는 config 로 중앙화(#369 리뷰) — maintenance 보존 경로와 공유.
DOMAIN = config.TRANSIT_DOMAIN
SOURCE = config.BUS_SOURCE

# bronze 테이블 -> dataset
SOURCES = {
    # "bronze_bus_arrival": "bus_arrival",  # 수집 제외(#212) — silver 미소비, 재개 시 주석 해제
    "bronze_bus_position": "bus_position",
}

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_transit_problem = problem_failure_callback(domain=DOMAIN, source_system=SOURCE)


def current_dag_run_id() -> str:
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "unknown")


def _land_objects(dataset: str, raws: list, run_id: str) -> dict:
    """R2 raw 랜딩 — 노선별 XML 을 **번들 1객체(JSONL)** 로 적재(#369 번들링).

    노선당 1객체(page-NNNN.xml, 728 PUT ≈ 4.5분)가 주기 단축의 병목이라 런당
    1 PUT(~10초)으로 묶는다. JSONL 1행 = 1노선 {busRouteId, rows, raw(xml)} —
    원본 XML 은 그대로 보존되고 bronze 테이블(1행=1노선)도 불변. 파싱은 loader.
    """
    routes = [r["route"] for r in raws]
    bundle = "\n".join(
        json.dumps(
            {"busRouteId": r["route"], "rows": r["rows"], "raw": r["raw"]},
            ensure_ascii=False,
        )
        for r in raws
    )
    res = land(
        stage="raw", domain=DOMAIN, source=SOURCE, dataset=dataset,
        pages=[bundle],
        endpoint=raws[0]["endpoint"] if raws else "", kind=dataset,
        rows=sum(max(r["rows"], 0) for r in raws), run_id=run_id,
        request_params={"busRouteId": routes, "bundle": "jsonl"}, ext="jsonl",
    )
    print(f"object landed [{dataset}] routes={len(routes)} bundled=1: raw={res['manifest_key']}")
    return res


def ingest_bus() -> dict:
    key = config.load_bus_key()  # URL 인코딩된 서비스키
    dag_run_id = current_dag_run_id()
    counts = {}
    for _table, dataset in SOURCES.items():
        raws = collect_bus_raw(key, dataset)
        landed = _land_objects(dataset, raws, dag_run_id)
        loader.enqueue_pending(
            dataset=dataset, source=SOURCE, landed=landed, run_id=dag_run_id,
            ts_collected=raws[0]["ts_collected"] if raws else None,
        )
        counts[dataset] = len(raws)
    print(f"collect counts: {counts}")
    return counts


with DAG(
    dag_id="transit_bus_bronze",
    description="서울 TOPIS 버스 위치(전 노선) → R2 XML 랜딩 + loader 마커. 적재는 transit_bronze_loader.",
    start_date=datetime(2026, 1, 1),
    schedule=config.schedule_for("bus", "*/3 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["seoul", "transit", "bus", "ingest", "collector", "xml", "r2"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_bus",
        # 실행 메트릭(#188) — 최소 침습 콜러블 래핑.
        python_callable=track(layer="bronze", domain="transit")(ingest_bus),
        on_failure_callback=record_transit_problem,
    )
