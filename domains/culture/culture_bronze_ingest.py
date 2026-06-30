"""Airflow DAG: culture 도메인 bronze(원본) 적재 -> R2.

일배치. 채택한 culture 데이터셋을 KOPIS / 서울 열린데이터에서 받아 원본 API 응답을
R2 ``bronze/culture/`` 아래에 적재한다(``culture_ingest`` 참고). 데이터셋마다
매핑 태스크 1개라서, 한 데이터셋 실패가 격리되고 재시도 가능하며 그리드에서 바로 보인다.

시크릿은 컨테이너 환경변수에서 온다(compose의 ``env_file: .env``가
``KOPIS_SERVICE_KEY``, ``SEOUL_OPENAPI_KEY``, ``R2_DEV_*``를 주입) -- 값은 여기 없다.

파라미터 (트리거 시 덮어쓰기 가능):
  target          "dev" | "prod"            (기본 dev -> 버킷 seoul-dev)
  datasets        적재할 데이터셋 슬러그 일부; 빈 값 -> 활성 전체
  date_from/to    YYYYMMDD; 비면 -> 롤링 [end-lookback_days, end]
  lookback_days   날짜창 크기 (boxoffice는 <=31)                       기본 31
  include_detail  KOPIS 상세 엔드포인트도 크롤(상한 있음)               기본 True
  max_detail      상세 크롤당 id 상한                                  기본 200
  kopis_rows      KOPIS 목록 페이지 크기                               기본 100
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator

# 이 파일의 디렉토리(domains/culture)를 sys.path에 넣어 `culture_ingest.*`를 import.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from culture_ingest.common.config import RunContext  # noqa: E402
from culture_ingest.source.datasets import enabled_datasets  # noqa: E402
from culture_ingest.source.ingest import IngestOptions, ingest_one  # noqa: E402

KST = "Asia/Seoul"

DEFAULT_PARAMS = {
    "target": "dev",
    "datasets": [],  # 데이터셋 슬러그 일부; 빈 값 = 활성 전체
    "date_from": "",
    "date_to": "",
    "lookback_days": 31,
    "include_detail": True,
    "max_detail": 200,
    "kopis_rows": 100,
}


def _plan(**context) -> list[dict]:
    """적재할 데이터셋마다 op_kwargs dict 하나씩을 만들고, 실행 컨텍스트를 공유한다.

    모든 매핑 태스크는 같은 ``ingest_ts``(실행의 data interval에서 유도)에 적재되므로,
    재시도한 실행은 같은 파티션을 덮어쓴다.
    """
    params = context["params"]
    end = context["data_interval_end"]
    load_date = end.in_timezone(KST).strftime("%Y-%m-%d")
    ingest_ts = end.in_timezone("UTC").strftime("%Y%m%dT%H%M%SZ")
    run_id = context["dag_run"].run_id

    # 날짜창: 명시 안 하면 [end-lookback_days, end] 롤링 윈도우 사용.
    date_from = params["date_from"]
    date_to = params["date_to"]
    if not (date_from and date_to):
        date_to = end.in_timezone(KST).strftime("%Y%m%d")
        date_from = end.in_timezone(KST).subtract(days=int(params["lookback_days"])).strftime("%Y%m%d")

    # 데이터셋 필터: include_detail 꺼지면 상세 제외, datasets 지정 시 그 부분집합만.
    include_detail = bool(params["include_detail"])
    wanted = set(params.get("datasets") or [])
    names = [
        ds.name
        for ds in enabled_datasets()
        if (include_detail or ds.kind != "kopis_detail")
        and (not wanted or ds.name in wanted)
    ]
    print(f"plan: {len(names)} datasets, window {date_from}~{date_to}, ingest_ts={ingest_ts}")
    return [
        {
            "name": name,
            "target": params["target"],
            "load_date": load_date,
            "ingest_ts": ingest_ts,
            "run_id": run_id,
            "date_from": date_from,
            "date_to": date_to,
            "include_detail": include_detail,
            "max_detail": int(params["max_detail"]),
            "kopis_rows": int(params["kopis_rows"]),
        }
        for name in names
    ]


def _ingest(
    name: str,
    target: str,
    load_date: str,
    ingest_ts: str,
    run_id: str,
    date_from: str,
    date_to: str,
    include_detail: bool,
    max_detail: int,
    kopis_rows: int,
    **context,
) -> dict:
    """데이터셋 1개를 적재 (매핑 태스크 1개). 실패 시 AirflowException으로 그 태스크만 실패."""
    ctx = RunContext(load_date=load_date, ingest_ts=ingest_ts, run_id=run_id)
    opts = IngestOptions(
        date_from=date_from,
        date_to=date_to,
        kopis_rows=kopis_rows,
        max_detail=max_detail,
        include_detail=include_detail,
    )
    result = ingest_one(name, ctx=ctx, opts=opts, target=target)
    print(f"{name}: pages={result.pages} rows={result.rows} bytes={result.bytes_written} {result.error}")
    if result.error and "skipped" not in result.error:
        raise AirflowException(f"{name} failed: {result.error}")
    return result.summary()


def _report(**context) -> None:
    """모든 매핑 태스크 결과를 모아 적재 요약을 로그로 남긴다(all_done로 항상 실행)."""
    rows = context["ti"].xcom_pull(task_ids="ingest_dataset") or []
    rows = [r for r in rows if r]
    landed = [r for r in rows if not r["error"]]
    total_rows = sum(r["rows"] for r in landed)
    print(f"culture bronze ingest done: {len(landed)}/{len(rows)} datasets landed, {total_rows} rows")


with DAG(
    dag_id="culture_bronze_ingest",
    description="Land culture domain raw source data (KOPIS + Seoul OA) to R2 bronze/culture.",
    start_date=pendulum.datetime(2026, 6, 1, tz=KST),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["ingest", "culture", "bronze", "r2"],
) as dag:
    # 1) plan: 적재할 데이터셋 목록과 공유 ingest_ts를 계산.
    plan = PythonOperator(task_id="plan", python_callable=_plan)

    # 2) ingest_dataset: plan 결과를 동적 매핑해 데이터셋마다 태스크 1개씩 병렬 실행.
    ingest_dataset_task = PythonOperator.partial(
        task_id="ingest_dataset",
        python_callable=_ingest,
    ).expand(op_kwargs=plan.output)

    # 3) report: 일부 데이터셋이 실패해도(all_done) 항상 요약을 남김.
    report = PythonOperator(task_id="report", python_callable=_report, trigger_rule="all_done")

    plan >> ingest_dataset_task >> report
