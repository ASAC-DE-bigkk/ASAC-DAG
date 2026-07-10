"""Airflow DAG: citydata silver/gold 변환 (dbt) (#192, ASAC-DBT#69).

수집 DAG(``citydata_bronze``, 5분)가 적재한 블록 bronze 를 **5분마다(수집 직후 오프셋)**
dbt 로 grain 별 silver + gold 로 변환하고 테스트한다. **인구(seoul_ppltn) + citydata
(seoul_citydata) 전 모델**을 한 번에 빌드한다 — 인구 silver 도 이제 citydata bronze
(LIVE_PPLTN_STTS)에서 파생한다. 전 모델 incremental — 재생성이 없어 스냅샷/파일 누적이
최소화되고, 주간 ``citydata_maintenance`` 가 나머지(expire/optimize/metadata 정리)를 맡는다.

silver 는 grain(1행의 의미)별 분리: 인구(장소×시각)·상권(장소×시각)·업종상세(×업종)·
승하차(×수단)·따릉이(×대여소)·대기질. gold: 크로스 신호 최신 스냅샷(place_latest) +
인구 시계열·일 소비 인사이트. 상세는 ASAC-DBT#69.

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

record_citydata_problem = problem_failure_callback(
    domain="citydata", source_system="seoul_citydata")

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/citydata"

DEFAULT_PARAMS = {"target": "dev"}


def _dbt(args: str) -> str:
    """dbt 하위명령을 citydata 프로젝트/프로파일로 실행하는 bash 스니펫."""
    project = shlex.quote(DBT_PROJECT)
    return (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"{shlex.quote(DBT_BIN)} {args} --target {{{{ params.target }}}} --no-use-colors"
    )


with DAG(
    dag_id="citydata_transform",
    description="Transform citydata bronze -> **인구 + citydata** silver/gold via dbt (every 5 min). 단일 변환.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="2-59/5 * * * *",  # 수집(*/5) 직후 — 최신 bronze 반영
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["transform", "citydata", "population", "silver", "gold", "dbt"],
) as dag:
    # 공용 패키지(asac_axes) 설치 — 멱등(이미 있으면 재사용 수준으로 저렴).
    deps = BashOperator(
        task_id="dbt_deps",
        bash_command=_dbt("deps"),
        on_failure_callback=record_citydata_problem,
    )

    # 참조 seed(area_geo + asac_axes crosswalk/boundary) 적재. asac_axes 의
    # seoul_gu_boundary 는 우리 그래프에서 아무 모델도 참조하지 않아(gu_code 는
    # dim_admin_dong 라이브 코드에서 취득) 스키마 위생상 로드에서 제외한다 (#267).
    seed_refs = BashOperator(
        task_id="dbt_seed",
        bash_command=_dbt("seed --exclude asac_axes.seoul_gu_boundary"),
        on_failure_callback=record_citydata_problem,
    )

    # **인구 + citydata 전 모델** 빌드. asac_axes 중 dim_admin_dong 은 **빌드**한다 —
    # dim_seoul_area 가 행안부 라이브 코드용으로 참조(#115, common.bronze_admin_dong_master
    # 원천). 미사용 dim_beop_admin_link 만 제외.
    run_models = BashOperator(
        task_id="dbt_run",
        bash_command=_dbt("run --exclude asac_axes.dim_beop_admin_link"),
        on_failure_callback=record_citydata_problem,
    )

    # 품질 테스트 — 우리 모델만(패키지 자체 테스트는 패키지 CI 소관).
    test_models = BashOperator(
        task_id="dbt_test",
        bash_command=_dbt("test --exclude package:asac_axes"),
        on_failure_callback=record_citydata_problem,
    )

    deps >> seed_refs >> run_models >> test_models
