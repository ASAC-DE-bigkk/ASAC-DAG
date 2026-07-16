"""서울 공영주차 수집(collector) — GetParkingInfo → R2 raw 랜딩 + loader pending 마커.

#369 수집·적재 분리: 이 DAG 은 **R2 랜딩까지만** 한다. Iceberg bronze 적재는
transit_bronze_loader 가 pending 마커를 소비해 수행. dag_id 는 이력 연속성을 위해 유지.
단일 호출(123개 전체) — 스코프 확대 대상 아님(이미 전체).
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
from seoul_transit.alerts import warn_if_empty
from seoul_transit.parking import collect_parking
from seoul_transit.r2_landing import land

# 경로 세그먼트는 config 로 중앙화(#369 리뷰) — maintenance 보존 경로와 공유.
DOMAIN = config.TRANSIT_DOMAIN
SOURCE = config.PARKING_SOURCE
DATASET = "parking"

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_transit_problem = problem_failure_callback(domain=DOMAIN, source_system=SOURCE)


def current_dag_run_id() -> str:
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "unknown")


def _land_objects(res: dict, run_id: str) -> dict:
    """R2 raw 랜딩(원본 응답 JSON). 랜딩 결과 반환."""
    res_b = land(
        stage="raw", domain=DOMAIN, source=SOURCE, dataset=DATASET,
        pages=[json.dumps(res["raw"], ensure_ascii=False)],
        endpoint=res["endpoint"], kind=DATASET, rows=res["rows"],
        run_id=run_id, request_params=res["request_params"], ext="json",
    )
    print(f"object landed [{DATASET}] rows={res['rows']}: raw={res_b['manifest_key']}")
    return res_b


def ingest_parking() -> dict:
    key = config.load_key()
    dag_run_id = current_dag_run_id()
    res = collect_parking(key)
    landed = _land_objects(res, dag_run_id)
    loader.enqueue_pending(
        dataset=DATASET, source=SOURCE, landed=landed, run_id=dag_run_id,
        ts_collected=res["ts_collected"],
    )
    n = res["rows"]
    # 0행 이상 경보(#229) — 주차는 24시간 데이터가 정상이라 quiet hours 미적용.
    warn_if_empty(DATASET, n, dag_run_id)
    print(f"collect counts: {{'parking': {n}}}")
    return {"parking": n}


with DAG(
    dag_id="transit_parking_bronze",
    description="서울 공영주차 실시간 점유(GetParkingInfo) → R2 랜딩 + loader 마커. 적재는 transit_bronze_loader.",
    start_date=datetime(2026, 1, 1),
    schedule=config.schedule_for("parking", "*/5 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["seoul", "transit", "parking", "ingest", "collector", "r2"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_parking",
        # 실행 메트릭(#188) — 최소 침습 콜러블 래핑.
        python_callable=track(layer="bronze", domain="transit")(ingest_parking),
        on_failure_callback=record_transit_problem,
    )
