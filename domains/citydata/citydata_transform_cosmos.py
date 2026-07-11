"""Airflow DAG (PoC): citydata 변환을 Cosmos 로 — 모델 1개 = 태스크 1개 + 모델별 즉시 테스트.

기존 ``citydata_transform``(BashOperator 로 dbt run/test 통짜)의 Cosmos 전환 시제품.
**모노프로젝트 루트**(/opt/airflow/dbt, PoC 브랜치)를 가리키며, 티어(fast/slow/agg,
#283)는 DbtTaskGroup 3개 + 기존 벽시계 게이트로 유지한다.

Cosmos 가 주는 것:
  * manifest 계보 → Airflow 태스크 그래프 (UI 에서 dbt lineage 가 그대로 보임)
  * TestBehavior.AFTER_EACH — 모델 빌드 직후 그 모델 테스트만 실행 → 오염 조기 차단,
    깨진 지점 하류만 멈춤, 모델 단위 재시도
  * dbt ls 셀렉터 그대로(RenderConfig.select) — 티어 목록 재사용

PoC 한계(본 전환 때 해결): target 이 dev 고정(ProfileConfig), dbt deps/seed 는
기존 BashOperator 유지. 기존 DAG 와 병행 배치(dag_id 별도) — 기본 일시정지.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import Asset

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, LoadMode, TestBehavior

_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.assets import CITYDATA_BRONZE_ASSET  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402

KST_TZ = ZoneInfo("Asia/Seoul")

record_citydata_problem = problem_failure_callback(
    domain="citydata", source_system="seoul_citydata")

# 모노프로젝트 루트 (PoC — 기존 domains/citydata 개별 프로젝트가 아님)
DBT_ROOT = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"

# 티어별 모델 (citydata_transform 과 동일, #283)
FAST_SELECT = [
    "dim_admin_dong", "dim_seoul_area",
    "silver_seoul_ppltn", "silver_citydata_transit_ppltn", "silver_citydata_sbike",
    "gold_seoul_ppltn_by_time", "gold_citydata_place_latest",
]
SLOW_SELECT = ["silver_citydata_cmrcl", "silver_citydata_cmrcl_rsb", "silver_citydata_air"]
AGG_SELECT = [
    "gold_seoul_ppltn_daily", "gold_citydata_cmrcl_daily",
    "gold_citydata_purchasing_power_daily", "gold_citydata_ppltn_x_culture_daily",
    "gold_citydata_transit_x_incident_hourly",
]

project_config = ProjectConfig(DBT_ROOT)
profile_config = ProfileConfig(
    profile_name="asac_seoul",
    target_name="dev",  # PoC 고정 — 본 전환 시 params 연동 검토
    profiles_yml_filepath=f"{DBT_ROOT}/profiles.yml",
)
# dbt 는 전용 venv 에 있어(메인 파이썬에 미설치) in-process(DBT_RUNNER) 불가 —
# SUBPROCESS 모드로 venv 실행파일을 호출한다.
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_BIN,
    invocation_mode=InvocationMode.SUBPROCESS,
)


def _tier_group(group_id: str, select: list[str]) -> DbtTaskGroup:
    """티어 1개 = DbtTaskGroup 1개. 모델별 run→test 쌍으로 렌더된다."""
    return DbtTaskGroup(
        group_id=group_id,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=select,
            test_behavior=TestBehavior.AFTER_EACH,
        ),
        default_args={"retries": 1, "retry_delay": timedelta(minutes=2),
                      "on_failure_callback": record_citydata_problem},
    )


def _is_slow_window(**_) -> bool:
    return datetime.now(KST_TZ).minute % 10 < 5


def _is_agg_window(**_) -> bool:
    return datetime.now(KST_TZ).minute < 5


with DAG(
    dag_id="citydata_transform_cosmos",
    description="[PoC] citydata transform via Cosmos — model-level tasks + per-model tests (mono dbt root).",
    start_date=datetime(2026, 1, 1, tzinfo=KST_TZ),
    schedule=[Asset(CITYDATA_BRONZE_ASSET)],
    catchup=False,
    max_active_runs=1,
    tags=["poc", "cosmos", "transform", "citydata", "dbt"],
) as dag:
    deps_seed = BashOperator(
        task_id="dbt_deps_seed",
        bash_command=(
            "set -euo pipefail\n"
            f"cd {DBT_ROOT}\n"
            f"export DBT_PROFILES_DIR={DBT_ROOT} DBT_PROJECT_DIR={DBT_ROOT}\n"
            f"{DBT_BIN} deps --target dev --no-use-colors\n"
            f"{DBT_BIN} seed --exclude asac_axes.seoul_gu_boundary --target dev --no-use-colors"
        ),
        on_failure_callback=record_citydata_problem,
    )

    fast = _tier_group("fast", FAST_SELECT)

    gate_slow = ShortCircuitOperator(task_id="gate_slow_10min", python_callable=_is_slow_window)
    slow = _tier_group("slow", SLOW_SELECT)

    gate_agg = ShortCircuitOperator(task_id="gate_agg_hourly", python_callable=_is_agg_window)
    agg = _tier_group("agg", AGG_SELECT)

    deps_seed >> fast
    fast >> gate_slow >> slow
    slow >> gate_agg >> agg
