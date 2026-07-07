"""Airflow DAG: citydata(통합 도시데이터) bronze 적재 (#192).

서울시 실시간 도시데이터 **통합 API(citydata)** 121개 장소를 10분마다 수집한다.
``population_bronze``(인구 전용)와 같은 태스크 경계:

    fetch_raw    API 호출 + **수집 즉시 gzip** 해 R2 raw 아카이브 (~177KB→~25KB,
                 전 블록 원본 보존 — 어떤 블록이든 사후 재처리 가능)
    load_bronze  raw(.json.gz)를 읽어 **allowlist 블록만** (장소×블록) 행으로
                 Iceberg ``bronze_seoul_citydata`` 멱등 적재
    report       run 리포트(커버리지·압축 통계)를 R2 에 기록 (all_done)

기본 블록: 인구·실시간상권(신한카드)·지하철/버스 승하차·따릉이·날씨/대기질.
겹침 블록(도로/주차/도착정보/문화행사)은 raw 에만 — 각 도메인 원천이 canonical(#192).

파라미터 (트리거 시 덮어쓰기 가능):
  target      "dev" | "prod"      (기본 dev)
  max_areas   장소 상한(테스트/샘플)  기본 None(전체 121)
  blocks      bronze 적재 블록 목록   기본 DEFAULT_BRONZE_BLOCKS
  write_report  run 리포트 기록 여부   기본 True
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator

# 이 파일의 디렉토리(domains/population)를 sys.path에 넣어 `ppltn_ingest.*`를 import.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402

from ppltn_ingest.common.config import RunContext  # noqa: E402
from ppltn_ingest.source.citydata import DEFAULT_BRONZE_BLOCKS  # noqa: E402
from ppltn_ingest.source.citydata_ingest import (  # noqa: E402
    CitydataIngestOptions,
    build_citydata_run_report,
    fetch_and_land_citydata,
    load_citydata_bronze_from_raw,
    write_citydata_run_report,
)

KST = "Asia/Seoul"

record_population_problem = problem_failure_callback(
    domain="population", source_system="seoul_citydata")

DEFAULT_PARAMS = {
    "target": "dev",
    "max_areas": None,
    "blocks": list(DEFAULT_BRONZE_BLOCKS),
    "write_report": True,
}


def _run_context(context) -> RunContext:
    """공유 RunContext(재시도 = 같은 파티션). fetch_raw 만 계산, 하류는 XCom 재사용."""
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run is not None else context.get("run_id", "manual")
    end = context.get("data_interval_end") or context.get("logical_date")
    if end is None:
        return RunContext.create(run_id=run_id)
    return RunContext(
        load_date=end.in_timezone(KST).strftime("%Y-%m-%d"),
        ingest_ts=end.in_timezone("UTC").strftime("%Y%m%dT%H%M%SZ"),
        run_id=run_id,
    )


def _fetch_raw(**context) -> dict:
    params = context["params"]
    ctx = _run_context(context)
    max_areas = params.get("max_areas")
    opts = CitydataIngestOptions(max_areas=int(max_areas) if max_areas else None)

    results = fetch_and_land_citydata(ctx, target=params["target"], opts=opts)
    landed = [r for r in results if r["ok"]]
    failed = [r for r in results if not r["ok"]]
    gz_mb = sum(r.get("gz_bytes", 0) for r in results) / 1024 / 1024
    print(f"[citydata bronze] raw landed {len(landed)}/{len(results)} · "
          f"failed={len(failed)} · gzip {gz_mb:.1f}MB")
    for f in failed[:10]:
        print(f"  ⚠ {f['area_nm']}: {f['error']}")

    if not landed:
        raise AirflowException(f"citydata bronze: 전체 {len(results)}개 장소 수집 실패")
    return {
        "ctx": {"load_date": ctx.load_date, "ingest_ts": ctx.ingest_ts, "run_id": ctx.run_id},
        "results": results,
    }


def _load_bronze(**context) -> int:
    params = context["params"]
    fetched = context["ti"].xcom_pull(task_ids="fetch_raw")
    ctx = RunContext(**fetched["ctx"])
    inserted = load_citydata_bronze_from_raw(
        ctx, results=fetched["results"], target=params["target"],
        blocks=tuple(params.get("blocks") or DEFAULT_BRONZE_BLOCKS))
    print(f"[citydata bronze] bronze_rows_inserted={inserted}")
    return inserted


def _report(**context) -> None:
    params = context["params"]
    fetched = context["ti"].xcom_pull(task_ids="fetch_raw")
    if not fetched:
        print("[citydata bronze] 리포트 없음(fetch_raw 미완료) — 스킵")
        return
    if not params.get("write_report"):
        return
    inserted = context["ti"].xcom_pull(task_ids="load_bronze") or 0
    ctx = RunContext(**fetched["ctx"])
    report = build_citydata_run_report(fetched["results"], ctx, inserted=inserted)
    try:
        key = write_citydata_run_report(report, target=params["target"])
        print(f"[citydata bronze] run report -> {key}")
    except Exception as exc:  # noqa: BLE001 -- 리포트 실패가 run 판정을 가리지 않게
        print(f"[citydata bronze] run report 적재 실패(무시): {exc}")


with DAG(
    dag_id="population_citydata_bronze",
    description="Collect Seoul citydata (unified, 121 areas) gzip raw to R2 + block-split Iceberg bronze.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
    params=DEFAULT_PARAMS,
    tags=["ingest", "population", "citydata", "bronze", "r2", "iceberg"],
) as dag:
    fetch_raw = PythonOperator(
        task_id="fetch_raw", python_callable=_fetch_raw,
        on_failure_callback=record_population_problem)
    load_bronze = PythonOperator(
        task_id="load_bronze", python_callable=_load_bronze,
        on_failure_callback=record_population_problem)
    report = PythonOperator(
        task_id="report", python_callable=_report, trigger_rule="all_done",
        on_failure_callback=record_population_problem)

    fetch_raw >> load_bronze >> report
