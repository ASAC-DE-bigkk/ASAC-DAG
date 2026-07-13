"""Airflow DAG: citydata silver/gold 변환 (dbt) (#192, ASAC-DBT#69, #274).

수집 DAG(``citydata_bronze``)가 **Asset**(``iceberg://citydata/bronze``)을 발행하면
그때 기동한다(크론 오프셋 폐기 — bronze 완료 이벤트에 묶어 "덜 끝난 bronze 를 읽는"
경합 제거, #274). **인구(seoul_ppltn) + citydata(seoul_citydata) 전 모델**을 빌드하되
소스 갱신 주기에 맞춰 두 티어로 나눈다:

  * fast (매 bronze ≈ 5분): 인구·승하차·따릉이 silver + 실시간 골드. 5분마다 새 값.
  * slow (10분): 상권·업종·대기질 silver + 일/시간 골드. 10분마다 새 값이라 한 번 걸러
    돌려 헛계산·스냅샷 churn 을 줄인다. gate 는 벽시계 minute%10 근사(멱등이라 무해).

크로스티어 의존(예: purchasing_power[slow] → ppltn[fast])은 run_fast 선행으로 항상 최신을
읽고, ``--select`` 라 fast 모델을 재빌드하지 않는다. dag_id 는 유지 → maintenance pause 유효.

파라미터: target "dev" | "prod" (기본 dev)
"""

from __future__ import annotations

import os
import shlex
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import Asset

# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.assets import CITYDATA_BRONZE_ASSET  # noqa: E402

KST = "Asia/Seoul"
KST_TZ = ZoneInfo(KST)

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/citydata"

# dbt 실패 시 run_results.json 을 파싱해 "어떤 모델/테스트가 왜" 를 알림에 넣는다(#304).
record_citydata_problem = problem_failure_callback(
    domain="citydata", source_system="seoul_citydata", dbt_project_dir=DBT_PROJECT)

DEFAULT_PARAMS = {"target": "dev"}

# ── 티어별 모델 (soure 갱신 주기 정렬) ──────────────────────────────────────────
# fast: 5분마다 새 값. dim 은 상시 최신 유지(저렴)라 fast 에 둔다.
FAST_MODELS = " ".join([
    "dim_admin_dong", "dim_seoul_area",
    "silver_seoul_ppltn", "silver_citydata_transit_ppltn", "silver_citydata_sbike",
    "gold_seoul_ppltn_by_time", "gold_citydata_place_latest",
])
# slow: 10분마다 새 값 + 일/시간 집계 골드.
SLOW_MODELS = " ".join([
    "silver_citydata_cmrcl", "silver_citydata_cmrcl_rsb", "silver_citydata_air",
    "gold_seoul_ppltn_daily", "gold_citydata_cmrcl_daily",
    "gold_citydata_purchasing_power_daily", "gold_citydata_ppltn_x_culture_daily",
    "gold_citydata_transit_x_incident_hourly",
])


def transform_schedule():
    """기본 Asset 트리거. 테스트용으로 env 로 크론/None 오버라이드 가능."""
    override = os.environ.get("ASK_SEOUL_CITYDATA_TRANSFORM_SCHEDULE")
    if override is not None:
        return override or None
    return [Asset(CITYDATA_BRONZE_ASSET)]


def _is_slow_window(**_) -> bool:
    """slow 티어(10분) 진입 게이트. bronze 는 */5 로 기동(분 0,5,10..)하므로 벽시계 분이
    10의 배수 구간이면 slow 를 돌린다. 지연·지터로 가끔 5분 간격으로 돌아도 delete+insert
    멱등이라 결과는 동일(스냅샷만 한 번 더)."""
    return datetime.now(KST_TZ).minute % 10 < 5


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
    description="Transform citydata bronze -> 인구+citydata silver/gold via dbt. Asset 트리거 + fast(5분)/slow(10분) 티어링.",
    start_date=datetime(2026, 1, 1, tzinfo=KST_TZ),
    schedule=transform_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["transform", "citydata", "population", "silver", "gold", "dbt"],
) as dag:
    # 공용 패키지(asac_axes) 설치 — 멱등(이미 있으면 저렴).
    deps = BashOperator(
        task_id="dbt_deps",
        bash_command=_dbt("deps"),
        on_failure_callback=record_citydata_problem,
    )

    # 참조 seed 적재. 미사용 seoul_gu_boundary 는 제외(#267).
    seed_refs = BashOperator(
        task_id="dbt_seed",
        bash_command=_dbt("seed --exclude asac_axes.seoul_gu_boundary"),
        on_failure_callback=record_citydata_problem,
    )

    # fast 티어 — 매 bronze(≈5분). 5분 주기 신호 + 실시간 골드 + dim.
    run_fast = BashOperator(
        task_id="dbt_run_fast",
        bash_command=_dbt(f"run --select {FAST_MODELS}"),
        on_failure_callback=record_citydata_problem,
    )
    test_fast = BashOperator(
        task_id="dbt_test_fast",
        bash_command=_dbt(f"test --select {FAST_MODELS} --exclude package:asac_axes"),
        on_failure_callback=record_citydata_problem,
    )

    # slow 티어 — 10분마다. 10분 주기 신호 + 일/시간 골드. gate 통과 시에만.
    gate_slow = ShortCircuitOperator(
        task_id="gate_slow_10min",
        python_callable=_is_slow_window,
        on_failure_callback=record_citydata_problem,
    )
    run_slow = BashOperator(
        task_id="dbt_run_slow",
        bash_command=_dbt(f"run --select {SLOW_MODELS}"),
        on_failure_callback=record_citydata_problem,
    )
    test_slow = BashOperator(
        task_id="dbt_test_slow",
        bash_command=_dbt(f"test --select {SLOW_MODELS} --exclude package:asac_axes"),
        on_failure_callback=record_citydata_problem,
    )

    # fast 선행 → slow 골드가 fast silver(ppltn 등)를 최신으로 읽는다.
    deps >> seed_refs >> run_fast >> test_fast
    run_fast >> gate_slow >> run_slow >> test_slow
