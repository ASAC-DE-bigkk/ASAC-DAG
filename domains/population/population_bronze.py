"""Airflow DAG: population 도메인 bronze(원본) 적재.

서울시 실시간 도시데이터 인구혼잡도(citydata_ppltn) 121개 장소를 5분마다 수집한다.
태스크는 "다시 만들 수 없는 것"과 "다시 만들 수 있는 것" 경계로 나뉜다:

    fetch_raw    API 호출 + R2 ``raw/population/`` 아카이브 (실시간 응답은 재현 불가
                 → 받는 즉시 박제까지 한 태스크)
    load_bronze  R2 raw를 다시 읽어 Iceberg bronze에 **원본 payload + 메타데이터**로
                 멱등 적재 (bronze만 실패하면 API 재호출 없이 이 태스크만 재시도)
    report       run 리포트를 R2에 기록 (all_done)

태스크 간에는 payload가 아니라 **raw 객체 키 목록 + 실행 컨텍스트**만 XCom으로 넘긴다.
필드 분해는 silver/dbt 몫(``ppltn_ingest`` 참고).

시크릿은 컨테이너 환경변수에서 온다(compose의 ``env_file: .env``가 ``SEOUL_API_KEY_PPLT``,
``R2_DEV_*`` 주입) -- 값은 여기 없다. dev/prod는 카탈로그(iceberg_dev/iceberg)와
버킷(seoul-dev/seoul)으로 가른다.

파라미터 (트리거 시 덮어쓰기 가능):
  target      "dev" | "prod"   (기본 dev -> iceberg_dev / seoul-dev)
  max_areas   장소 상한(테스트/샘플)          기본 None(전체 121)
  write_report  run 리포트를 R2에 남길지        기본 True
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
from ppltn_ingest.source.config import SOURCE_ID  # noqa: E402
from ppltn_ingest.source.ingest import (  # noqa: E402
    IngestOptions,
    build_run_report,
    fetch_and_land,
    load_bronze_from_raw,
    write_run_report,
)

KST = "Asia/Seoul"

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_population_problem = problem_failure_callback(
    domain="population", source_system=SOURCE_ID)

DEFAULT_PARAMS = {
    "target": "dev",
    "max_areas": None,
    "write_report": True,
}


def _run_context(context) -> RunContext:
    """공유 RunContext를 만든다(재시도 = 같은 파티션).

    스케줄 run은 ``data_interval_end``를 쓰고, 수동 트리거처럼 data interval이 없는
    run(Airflow 3에서 흔함)은 ``logical_date``, 그것도 없으면 현재 시각으로 폴백한다.
    fetch_raw에서만 호출하고 하류 태스크는 XCom의 ctx를 재사용한다 -- 폴백(now)이
    태스크마다 다른 ts를 만드는 것을 막는다.
    """
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
    """121개 장소를 조회해 R2 raw에 적재. 성공 0건이면 task 실패(fail loud).

    반환(XCom): {"ctx": RunContext dict, "results": 장소별 결과(payload 미포함)}.
    """
    params = context["params"]
    ctx = _run_context(context)
    max_areas = params.get("max_areas")
    opts = IngestOptions(max_areas=int(max_areas) if max_areas else None)

    results = fetch_and_land(ctx, target=params["target"], opts=opts)
    landed = [r for r in results if r["ok"]]
    failed = [r for r in results if not r["ok"]]
    print(
        f"[population bronze] raw landed {len(landed)}/{len(results)} · failed={len(failed)}"
    )
    for f in failed[:10]:
        print(f"  ⚠ {f['area_nm']}: {f['error']}")

    if not landed:
        raise AirflowException(f"population bronze: 전체 {len(results)}개 장소 수집 실패")
    return {
        "ctx": {"load_date": ctx.load_date, "ingest_ts": ctx.ingest_ts, "run_id": ctx.run_id},
        "results": results,
    }


def _load_bronze(**context) -> int:
    """R2 raw를 읽어 Iceberg bronze에 멱등 적재(같은 ingest_ts delete-then-insert)."""
    params = context["params"]
    fetched = context["ti"].xcom_pull(task_ids="fetch_raw")
    ctx = RunContext(**fetched["ctx"])
    inserted = load_bronze_from_raw(ctx, results=fetched["results"], target=params["target"])
    print(f"[population bronze] bronze_rows_inserted={inserted}")
    return inserted


def _report(**context) -> None:
    """run 리포트를 R2에 남긴다(일배치 리포트/알림 DAG의 소스). all_done로 항상 실행."""
    params = context["params"]
    fetched = context["ti"].xcom_pull(task_ids="fetch_raw")
    if not fetched:
        print("[population bronze] 리포트 없음(fetch_raw 미완료) — 스킵")
        return
    if not params.get("write_report"):
        return
    inserted = context["ti"].xcom_pull(task_ids="load_bronze") or 0
    ctx = RunContext(**fetched["ctx"])
    report = build_run_report(fetched["results"], ctx, inserted=inserted, dry_run=False)
    try:
        key = write_run_report(report, target=params["target"])
        print(f"[population bronze] run report -> {key}")
    except Exception as exc:  # noqa: BLE001 -- 리포트 적재 실패가 run 판정을 가리지 않게
        print(f"[population bronze] run report 적재 실패(무시): {exc}")


with DAG(
    dag_id="population_bronze",
    description="Collect Seoul citydata_ppltn (121 areas) raw payload to R2 + Iceberg bronze.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
    params=DEFAULT_PARAMS,
    tags=["ingest", "population", "bronze", "r2", "iceberg"],
) as dag:
    fetch_raw = PythonOperator(
        task_id="fetch_raw",
        python_callable=_fetch_raw,
        on_failure_callback=record_population_problem,
    )
    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=_load_bronze,
        on_failure_callback=record_population_problem,
    )
    report = PythonOperator(
        task_id="report",
        python_callable=_report,
        trigger_rule="all_done",
        on_failure_callback=record_population_problem,
    )

    fetch_raw >> load_bronze >> report
