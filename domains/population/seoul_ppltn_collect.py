"""Airflow DAG: population 도메인 bronze(원본) 적재.

서울시 실시간 도시데이터 인구혼잡도(citydata_ppltn) 121개 장소를 5분마다 수집해,
원본 응답을 R2 ``bronze/population/`` 아래 아카이브하고 Iceberg bronze 테이블에
**원본 payload + 메타데이터**로 적재한다(필드 분해는 silver/dbt 몫; ``ppltn_ingest`` 참고).

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

from ppltn_ingest.common.config import RunContext  # noqa: E402
from ppltn_ingest.source.ingest import (  # noqa: E402
    IngestOptions,
    ingest_all,
    write_run_report,
)

KST = "Asia/Seoul"

DEFAULT_PARAMS = {
    "target": "dev",
    "max_areas": None,
    "write_report": True,
}


def _run_context(context) -> RunContext:
    """공유 RunContext를 만든다(재시도 = 같은 파티션).

    스케줄 run은 ``data_interval_end``를 쓰고, 수동 트리거처럼 data interval이 없는
    run(Airflow 3에서 흔함)은 ``logical_date``, 그것도 없으면 현재 시각으로 폴백한다.
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


def _ingest(**context) -> dict:
    """121개 장소를 적재하고 정량 리포트를 반환한다. 성공 0건이면 task 실패(fail loud)."""
    params = context["params"]
    ctx = _run_context(context)
    max_areas = params.get("max_areas")
    opts = IngestOptions(max_areas=int(max_areas) if max_areas else None)

    report = ingest_all(ctx, target=params["target"], opts=opts)
    cov = report["coverage"]
    print(
        f"[population bronze] coverage {cov['landed']}/{cov['expected']} ({cov['coverage_pct']}%) · "
        f"bronze_rows={report['bronze_rows_inserted']} · failed={cov['failed']}"
    )
    for f in report["failures"][:10]:
        print(f"  ⚠ {f['area_nm']}: {f['error']}")

    if cov["landed"] == 0:
        raise AirflowException(f"population bronze: 전체 {cov['expected']}개 장소 수집 실패")
    return report


def _report(**context) -> None:
    """run 리포트를 R2에 남긴다(일배치 리포트/알림 DAG의 소스). all_done로 항상 실행."""
    params = context["params"]
    report = context["ti"].xcom_pull(task_ids="ingest")
    if not report:
        print("[population bronze] 리포트 없음(ingest 미완료) — 스킵")
        return
    if not params.get("write_report"):
        return
    try:
        key = write_run_report(report, target=params["target"])
        print(f"[population bronze] run report -> {key}")
    except Exception as exc:  # noqa: BLE001 -- 리포트 적재 실패가 run 판정을 가리지 않게
        print(f"[population bronze] run report 적재 실패(무시): {exc}")


with DAG(
    dag_id="seoul_ppltn_collect",
    description="Collect Seoul citydata_ppltn (121 areas) raw payload to R2 + Iceberg bronze.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
    params=DEFAULT_PARAMS,
    tags=["ingest", "population", "bronze", "r2", "iceberg"],
) as dag:
    ingest = PythonOperator(task_id="ingest", python_callable=_ingest)
    report = PythonOperator(task_id="report", python_callable=_report, trigger_rule="all_done")

    ingest >> report
