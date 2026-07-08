"""Airflow DAG: citydata silver/gold 변환 (dbt) (#192, ASAC-DBT#69).

수집 DAG(``population_citydata_bronze``, 10분)가 적재한 블록 bronze 를 **10분마다
(수집 5분 오프셋)** dbt 로 grain 별 silver 5종 + gold 2종으로 변환하고 테스트한다.
전 모델 incremental(merge) — 재생성이 없어 스냅샷/파일 누적이 최소화되고,
주간 ``population_maintenance`` 가 나머지(expire_snapshots·optimize·metadata 정리)를 맡는다.

silver 는 grain(1행의 의미)별 분리: 상권(장소×시각)·업종상세(×업종)·승하차(×수단)·
따릉이(×대여소)·대기질. gold: 크로스 신호 최신 스냅샷(place_latest — 혼잡도 조인) +
일 소비 인사이트(cmrcl_daily). 상세는 ASAC-DBT#69.

파라미터 (트리거 시 덮어쓰기 가능):
  target   "dev" | "prod"   (기본 dev)
"""

from __future__ import annotations

import os
import shlex
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402

KST = "Asia/Seoul"

record_population_problem = problem_failure_callback(
    domain="population", source_system="seoul_citydata")

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/population"

# citydata 계열 모델만 선택 — 인구(silver_seoul_ppltn 계열)는 population_transform 소유.
CITYDATA_MODELS = (
    "dim_seoul_area "
    "silver_citydata_cmrcl silver_citydata_cmrcl_rsb silver_citydata_transit_ppltn "
    "silver_citydata_sbike silver_citydata_air "
    "gold_citydata_place_latest gold_citydata_cmrcl_daily"
)

DEFAULT_PARAMS = {"target": "dev"}


def _dbt(args: str) -> str:
    """dbt 하위명령을 population 프로젝트/프로파일로 실행하는 bash 스니펫."""
    project = shlex.quote(DBT_PROJECT)
    return (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"{shlex.quote(DBT_BIN)} {args} --target {{{{ params.target }}}} --no-use-colors"
    )


with DAG(
    dag_id="population_citydata_transform",
    description="Transform citydata bronze -> grain-split silvers + cross-signal golds via dbt (every 10 min).",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="5-59/10 * * * *",  # 수집(*/10) 5분 뒤 — 최신 bronze 반영
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["transform", "population", "citydata", "silver", "gold", "dbt"],
) as dag:
    # 공용 패키지(asac_axes) 설치 — 멱등(이미 있으면 재사용 수준으로 저렴).
    deps = BashOperator(
        task_id="dbt_deps",
        bash_command=_dbt("deps"),
        on_failure_callback=record_population_problem,
    )

    # citydata silver/gold 빌드 — dbt 가 dim → silver → gold 의존성 순서를 보장.
    run_models = BashOperator(
        task_id="dbt_run",
        bash_command=_dbt(f"run --select {CITYDATA_MODELS}"),
        on_failure_callback=record_population_problem,
    )

    # citydata 모델 품질 테스트만 선택 실행.
    test_models = BashOperator(
        task_id="dbt_test",
        bash_command=_dbt(f"test --select {CITYDATA_MODELS}"),
        on_failure_callback=record_population_problem,
    )

    deps >> run_models >> test_models
