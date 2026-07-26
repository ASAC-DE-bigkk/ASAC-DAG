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
from common.ops.run_sink import record_run  # noqa: E402

KST_TZ = ZoneInfo("Asia/Seoul")

# 단독 citydata dbt 프로젝트 (모노 루트 아님).
DBT_PROJECT = "/opt/airflow/dbt/domains/citydata"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"

record_citydata_problem = problem_failure_callback(
    domain="citydata", source_system="seoul_citydata", dbt_project_dir=DBT_PROJECT)

# run-metadata(ops.run_metadata) — 성공·실패 모두 1행 append(태스크 단위). 기존 problem
# 콜백과 병행하며, best-effort(기록 실패는 태스크 판정 안 가림).
_run_ok = record_run("citydata", "transform", status="success")
_run_fail = record_run("citydata", "transform", status="failed")

# 티어는 dbt **태그**로 관리 — 모델명 하드코딩 리스트 대신. 각 모델의 tier 는 그 모델
# schema.yml `config: tags: [fast|slow]` 에 있고(SQL=비즈니스로직 / yml=문서·메타 분리
# 원칙), 패키지 모델 dim_admin_dong 은 dbt_project.yml 에서 태깅한다. 모델을 추가할 때
# **태그만 붙이면 이 DAG 는 안 고쳐도 됨** → 리스트-모델 드리프트 0, 단일 진실원천.
# `dbt run --select tag:fast` 수동 실행도 동일 결과(cosmos ↔ 수동 일관).
# 티어 배치 근거(골드의 "자연 주기"):
#   fast(5분): 실시간 grain(현재 스냅샷·시간대별) + 시간 grain 크로스(날씨·교통돌발).
#   slow(10분): 일 grain 집계(daily, 5분마다 재계산 불필요) + 무거운 cmrcl/air silver.
# 주의: 태그는 manifest 에 반영돼야 선택됨 → 모델/태그 변경 시 manifest 재생성 필요.
FAST_SELECT = ["tag:fast"]
SLOW_SELECT = ["tag:slow"]
# daily(매일 자정): 패턴 골드(dow_hour·forecast·demographics) — 과거 누적 평균이라 한시간새
# 안 바뀜, 하루 1회 재빌드면 충분. 무거운 by_time 전체 스캔을 하루 1회로 제한 → OOM 최소화.
# grain 은 시간(0~23)이지만 '갱신주기'는 일. 서빙(8시 DAILY)보다 앞서 자정에 실행.
DAILY_SELECT = ["tag:daily"]
# hourly(매시 :05~:09): 시간 grain 골드(ppltn_hourly·x_weather·x_incident) — 시간 버킷이라
# 매시 1회면 충분(현재-시각 실시간은 fast 스냅샷이 담당). fast(5분)에서 빼 OOM 완화.
HOURLY_SELECT = ["tag:hourly"]

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
        # retries=0 — silver 는 incremental delete+insert(table 은 전체 재빌드가 5분 예산 초과라
        # 불가)라 재시도가 R2 비원자성으로 이중삽입 중복을 유발한다. 재시도 대신 다음 5분 run 의
        # 룩백이 실패 window 를 재계산해 self-heal 한다. gold 는 table 이라 재시도 무관.
        # 대가: 일시 race(seed 재빌드 등)마다 알림이 뜰 수 있으나 self-clearing 이다.
        # retries=0 유지(위 주석의 중복 방지 근거). run-metadata 는 성공·실패 모두 기록.
        default_args={
            "retries": 0,
            "on_success_callback": _run_ok,
            "on_failure_callback": [record_citydata_problem, _run_fail],
        },
    )


def _is_slow_window(**_) -> bool:
    """slow 티어(10분) 게이트 — 기존 citydata_transform 과 동일 벽시계 근사."""
    return datetime.now(KST_TZ).minute % 10 < 5


def _is_daily_window(**_) -> bool:
    """daily 티어 게이트 — 매일 0시 15~19분 KST 만(slow·hourly 창과 stagger). 패턴 골드."""
    now = datetime.now(KST_TZ)
    return now.hour == 0 and 15 <= now.minute < 20


def _is_hourly_window(**_) -> bool:
    """hourly 티어 게이트 — 매시 '첫 실행 1회'(분 무관, slow 의 :00~:04 는 양보).
    과거엔 '매시 5~9분' 5분 창이었으나, transform 이 asset 트리거라 불규칙한 분(예: :18·:37·:56)에
    돌아 창을 자주 빗나가 hourly 티어가 몇 시간씩 안 도는 버그(2026-07-26). Variable 로 '이 시간에
    이미 돌았나'를 보고 시간당 정확히 1회 통과 → 타이밍에 안 흔들린다. downstream 실패 시엔 다음
    시간 run 이 self-heal(hourly 골드는 table+replace)."""
    from airflow.models import Variable

    now = datetime.now(KST_TZ)
    if now.minute < 5:
        return False  # :00~:04 는 slow 창 — stagger 양보
    key = "citydata_transform_hourly_last_hour"
    cur = now.strftime("%Y-%m-%dT%H")  # 시간 버킷
    if Variable.get(key, default_var="") == cur:
        return False  # 이 시간엔 이미 실행함
    Variable.set(key, cur)
    return True


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
    gate_daily = ShortCircuitOperator(
        task_id="gate_daily_midnight", python_callable=_is_daily_window,
        on_failure_callback=record_citydata_problem,
    )
    daily = _tier_group("daily", DAILY_SELECT)
    gate_hourly = ShortCircuitOperator(
        task_id="gate_hourly", python_callable=_is_hourly_window,
        on_failure_callback=record_citydata_problem,
    )
    hourly = _tier_group("hourly", HOURLY_SELECT)

    # fast 완료 후 게이트 분기(독립·stagger 로 상호 비겹침) — slow(:00~04)·hourly(:05~09)·daily(0시:15~19).
    deps_seed >> fast >> gate_slow >> slow
    fast >> gate_hourly >> hourly
    fast >> gate_daily >> daily
