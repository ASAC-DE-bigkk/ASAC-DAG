"""서울 지하철 수집(collector) — 실시간 API → R2 raw 랜딩 + loader pending 마커.

#369 수집·적재 분리: 이 DAG 은 **R2 랜딩까지만** 한다. Iceberg bronze 적재는
transit_bronze_loader(저빈도) 가 pending 마커를 소비해 수행 — 고빈도(1분) 수집에서
Trino 소형 INSERT·스냅샷 폭증을 피한다. dag_id 는 이력 연속성을 위해 유지.

수집 스코프(#369): SUBWAY_STATIONS=ALL(기본) → 도착 일괄 API 1콜로 전 역(~2,954행).
역 목록 env 로 역별 호출 폴백 가능.

수집 제외(#212 유지): `subway_position` 은 silver 미소비로 수집하지 않는다.
코드·테이블·파서는 유지 — SOURCES 에서 해당 항목만 주석 처리. 재개 시 주석 해제 + PR.
⚠️ 실시간 데이터는 소급 수집 불가 — 중단 구간은 영구 이력 공백으로 남는다.
"""

import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# 동봉 패키지(seoul_transit)는 이 DAG 파일과 같은 폴더(dags/domains/transit/)에 있다.
# Airflow 3.x 는 dags 하위 디렉터리를 sys.path 에 자동 추가하지 않고(plugins 마운트도 없음),
# 그래서 자기 폴더를 직접 path 에 올려 패키지를 import 한다.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback
from common.runmetrics import track

from seoul_transit import config, loader
from seoul_transit.alerts import warn_if_empty
from seoul_transit.r2_landing import land
from seoul_transit.subway import collect_subway

# R2 객체 적재 (raw) — 팀 <stage>/<domain>/<source> 규약. 경로 세그먼트는 config 로
# 중앙화(#369 리뷰) — maintenance 의 보존 집행 경로와 같은 값을 공유한다.
DOMAIN = config.TRANSIT_DOMAIN
SOURCE = config.SUBWAY_SOURCE

# bronze 테이블 -> dataset 매핑
SOURCES = {
    "bronze_subway_arrival": "subway_arrival",
    # "bronze_subway_position": "subway_position",  # 수집 제외(#212) — silver 미소비, 재개 시 주석 해제
}

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_transit_problem = problem_failure_callback(domain=DOMAIN, source_system=SOURCE)


def current_dag_run_id() -> str:
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "unknown")


def _land_objects(dataset: str, raws: list, run_id: str) -> dict:
    """R2 raw 랜딩: target(역/호선)별 원본 응답을 page-NNNN 으로. 랜딩 결과 반환.

    (변환/정제는 ASAC-DBT silver, Iceberg 적재는 transit_bronze_loader.)
    """
    targets = [r["request_params"]["target"] for r in raws]
    rows_cap = raws[0]["request_params"]["rows"] if raws else None
    res = land(
        stage="raw", domain=DOMAIN, source=SOURCE, dataset=dataset,
        pages=[json.dumps(r["raw"], ensure_ascii=False) for r in raws],
        endpoint=raws[0]["endpoint"] if raws else "", kind=dataset,
        rows=sum(r["rows"] for r in raws), run_id=run_id,
        request_params={"targets": targets, "rows": rows_cap}, ext="json",
    )
    print(f"object landed [{dataset}] targets={len(targets)}: raw={res['manifest_key']}")
    return res


def ingest_subway() -> dict:
    key = config.load_key()
    dag_run_id = current_dag_run_id()
    counts = {}
    for _table, dataset in SOURCES.items():
        # ALL 모드면 일괄 1콜, 아니면 target(역/호선)당 1콜 — 이중 호출 없음.
        res = collect_subway(key, dataset)
        raws = res["raws"]
        landed = _land_objects(dataset, raws, dag_run_id)
        loader.enqueue_pending(
            dataset=dataset, source=SOURCE, landed=landed, run_id=dag_run_id,
            ts_collected=raws[0]["ts_collected"] if raws else None,
        )
        counts[dataset] = sum(r["rows"] for r in raws)
        # 0행 이상 경보(#229) — 단, 심야 미운행 시간대(quiet hours)의 0행은 정상 취급.
        warn_if_empty(dataset, counts[dataset], dag_run_id, quiet_ok=True)
    print(f"collect counts: {counts}")
    return counts


with DAG(
    dag_id="transit_subway_bronze",
    description="지하철 실시간(일괄 ALL) → R2 raw 랜딩 + loader 마커. 적재는 transit_bronze_loader.",
    start_date=datetime(2026, 1, 1),
    schedule=config.schedule_for("subway", "*/3 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["seoul", "transit", "subway", "ingest", "collector", "r2"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_subway",
        # 실행 메트릭(#188) — 최소 침습 콜러블 래핑.
        python_callable=track(layer="bronze", domain="transit")(ingest_subway),
        on_failure_callback=record_transit_problem,
    )
