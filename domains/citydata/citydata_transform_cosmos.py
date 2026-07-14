"""Airflow DAG: citydata 변환을 Cosmos 로 — 모델 1개 = 태스크 1개 + 모델별 즉시 테스트.

기존 ``citydata_transform``(BashOperator 로 dbt run/test 통짜)의 Cosmos 전환본. 통짜는
한 태스크가 8모델을 묶어 돌려 **1개 실패 시 재시도가 이미 성공한 형제까지 재실행** →
R2 delete+insert 이중 삽입(중복)의 방아쇠가 됐다(#309/운영이슈). Cosmos 는 모델마다
태스크를 쪼개 **실패한 모델만 재시도**하므로 그 경로가 사라진다.

**단독 citydata 프로젝트**(domains/citydata, profile seoul_ppltn)를 가리킨다 — 팀
모노프로젝트(#143)와 독립. 팀 이미지가 astronomer-cosmos 를 담게 되면 그대로 동작한다.

구조: 기존과 동일한 Asset 트리거 + fast/slow 티어(벽시계 게이트) 유지. 각 티어는
DbtTaskGroup 으로, ``TestBehavior.AFTER_EACH`` 라 모델 빌드 직후 그 모델 테스트만 돈다.

⚠ 기존 ``citydata_transform`` 과 **동시 실행 금지**(같은 테이블 write) — 이 DAG 는
``is_paused_upon_creation=True`` 로 기본 정지. 검증 후 기존 DAG 를 끄고 이 DAG 를 켠다.
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

from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import InvocationMode, LoadMode, TestBehavior

_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.assets import CITYDATA_BRONZE_ASSET  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402

KST_TZ = ZoneInfo("Asia/Seoul")

# 단독 citydata dbt 프로젝트 (모노 루트 아님).
DBT_PROJECT = "/opt/airflow/dbt/domains/citydata"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"

record_citydata_problem = problem_failure_callback(
    domain="citydata", source_system="seoul_citydata", dbt_project_dir=DBT_PROJECT)

# 티어별 모델 — 골드의 "자연 주기"로 배치.
# fast(5분): 실시간 grain(현재 스냅샷·시간대별) + 시간 grain 크로스(날씨·교통돌발). commerce
#   는 view 라 재생성이 순간이라 fast 에 둔다(항상 최신·R2 write 없음).
# slow(10분): 일 grain 집계(daily) — 하루치라 5분마다 재계산할 필요 없음 + 무거운 cmrcl/air silver.
FAST_SELECT = [
    "dim_admin_dong", "dim_seoul_area",
    "silver_citydata_ppltn", "silver_citydata_transit_ppltn", "silver_citydata_sbike",
    "gold_citydata_ppltn_by_time", "gold_citydata_place_latest",
    "gold_citydata_ppltn_x_weather_hourly", "gold_citydata_ppltn_x_commerce_dong",
    "gold_citydata_transit_x_incident_hourly",
]
SLOW_SELECT = [
    "silver_citydata_cmrcl", "silver_citydata_cmrcl_rsb", "silver_citydata_air",
    "gold_citydata_ppltn_daily", "gold_citydata_cmrcl_daily",
    "gold_citydata_purchasing_power_daily", "gold_citydata_ppltn_x_culture_daily",
]

# DBT_MANIFEST 로드 — 파싱 시점에 dbt 를 돌리지 않고 target/manifest.json 을 읽어 그래프를
# 만든다. dbt 가 전용 venv 에만 있어(메인 env 에 dbt-trino 없음) DBT_LS(in-process ls)가
# 불가하므로 manifest 방식이 견고하다. manifest 는 transform 이 주기적으로 갱신한다.
project_config = ProjectConfig(
    DBT_PROJECT,
    manifest_path=f"{DBT_PROJECT}/target/manifest.json",
)
profile_config = ProfileConfig(
    profile_name="seoul_ppltn",
    target_name="dev",  # 실험 단계 고정 — 본 전환 시 params 연동 검토
    profiles_yml_filepath=f"{DBT_PROJECT}/profiles.yml",
)
# dbt 는 전용 venv 에 있어(메인 파이썬에 미설치) in-process 불가 → SUBPROCESS 로 venv 호출.
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_BIN,
    invocation_mode=InvocationMode.SUBPROCESS,
)


def _dbt(args: str) -> str:
    return (
        "set -euo pipefail\n"
        f"cd {DBT_PROJECT}\n"
        f"export DBT_PROFILES_DIR={DBT_PROJECT} DBT_PROJECT_DIR={DBT_PROJECT}\n"
        f"{DBT_BIN} {args} --target dev --no-use-colors"
    )


def _tier_group(group_id: str, select: list[str]) -> DbtTaskGroup:
    """티어 1개 = DbtTaskGroup 1개. 모델별 run→test(AFTER_EACH) 태스크로 렌더."""
    return DbtTaskGroup(
        group_id=group_id,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=select,
            test_behavior=TestBehavior.AFTER_EACH,
        ),
        # cosmos 는 모델 실행 시 프로젝트를 tmp 로 복사하는데 dbt_packages(asac_axes)를 안
        # 가져온다 → 각 태스크가 dbt deps 를 먼저 돌려 패키지를 설치하게 한다.
        operator_args={"install_deps": True},
        default_args={"retries": 1, "retry_delay": timedelta(minutes=2),
                      "on_failure_callback": record_citydata_problem},
    )


def _is_slow_window(**_) -> bool:
    """slow 티어(10분) 게이트 — 기존 citydata_transform 과 동일 벽시계 근사."""
    return datetime.now(KST_TZ).minute % 10 < 5


with DAG(
    dag_id="citydata_transform_cosmos",
    description="citydata transform via Cosmos — 모델별 태스크 + 모델별 테스트. 기존 통짜 DAG 대체(중복 재발 차단).",
    start_date=datetime(2026, 1, 1, tzinfo=KST_TZ),
    schedule=[Asset(CITYDATA_BRONZE_ASSET)],
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,  # 기존 DAG 와 동시 write 금지 — 검증 후 스왑
    tags=["transform", "citydata", "cosmos", "dbt"],
) as dag:
    # deps/seed 는 Cosmos 가 다루지 않아 BashOperator 유지. 미사용 seoul_gu_boundary 제외(#267).
    deps_seed = BashOperator(
        task_id="dbt_deps_seed",
        bash_command=_dbt("deps") + "\n" + _dbt("seed --exclude asac_axes.seoul_gu_boundary"),
        on_failure_callback=record_citydata_problem,
    )

    fast = _tier_group("fast", FAST_SELECT)
    gate_slow = ShortCircuitOperator(
        task_id="gate_slow_10min", python_callable=_is_slow_window,
        on_failure_callback=record_citydata_problem,
    )
    slow = _tier_group("slow", SLOW_SELECT)

    deps_seed >> fast >> gate_slow >> slow
