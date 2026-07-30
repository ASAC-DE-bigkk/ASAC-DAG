"""Airflow DAG: culture 시설 상세 주간 refresh (#206).

kopis_facility_detail 은 SCD2 정적 dim(freshness SLA 8일)이라 자정 일배치에서
분리(datasets.refresh="weekly")하고, 이 DAG 가 매주 일요일 05:30 KST 에
culture_bronze 를 시설 목록+상세 전수(max_detail=2000)로 트리거한다.

  - 자정이 아닌 시각: KOPIS 자정 간헐 400(#201) 창 회피, 자정 호출 -200/일
  - 목록을 같이 태움: detail 이 같은 run 에 랜딩된 목록에서 id 재사용(#146),
    신규 시설이 목록→상세 같은 주기에 편입
  - 리포트·SLO·볼륨 HWM·Discord·에러 콜백은 culture_bronze 것을 그대로 재사용
    (베이스라인은 다중 리포트 병합이라 이 부분 run 이 자정런 HWM 을 안 가림)

파라미터 (트리거 시 덮어쓰기 가능):
  target   "dev" | "prod"   (기본 = 런타임 env — culture_bronze 로 전달)
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param

# 이 파일의 디렉토리(domains/culture)를 sys.path에 넣어 `culture_ingest.*`를 import.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import TARGET_CHOICES, default_target  # noqa: E402

from culture_ingest.source.datasets import WEEKLY_FACILITY_REFRESH_CONF  # noqa: E402

KST = "Asia/Seoul"

record_culture_problem = problem_failure_callback(domain="culture")

with DAG(
    dag_id="culture_facility_refresh",
    description="Weekly full crawl of KOPIS facility detail via culture_bronze trigger (#206).",
    start_date=pendulum.datetime(2026, 6, 1, tz=KST),
    schedule="30 5 * * 0",  # 매주 일요일 05:30 KST — 자정 400 창(#201)·maintenance(04:30)와 시차
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    # 배포 env 를 따른다(ASK-Seoul#66). 이 값은 아래 conf 로 culture_bronze 에 그대로 전달되므로
    # 하드코딩 "dev" 였으면 prod 스택에서 주간 전수 재크롤이 dev 버킷에 쓰려다 실패한다.
    params={
        "target": Param(
            default=default_target(),
            type="string",
            enum=list(TARGET_CHOICES),
            description="culture_bronze 로 전달할 환경. 기본값은 런타임 env(ASK_SEOUL_TARGET/DBT_TARGET).",
        )
    },
    tags=["ingestion", "culture", "kopis", "weekly"],
) as dag:
    trigger_full_crawl = TriggerDagRunOperator(
        task_id="trigger_full_crawl",
        trigger_dag_id="culture_bronze",
        # conf 는 culture_bronze 의 params 를 run 단위로 덮어쓴다(수동 트리거와 동일 경로).
        conf={**WEEKLY_FACILITY_REFRESH_CONF, "target": "{{ params.target }}"},
        on_failure_callback=record_culture_problem,
    )
