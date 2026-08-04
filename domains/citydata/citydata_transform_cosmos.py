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

# 이 파일의 디렉토리(domains/citydata)를 sys.path 에 넣어 콜백에서 `citydata_ingest.*`(D1 writer)를
# import 가능하게 — bronze 와 동일. 이게 없으면 run 관측 콜백이 ModuleNotFoundError 로 조용히 죽는다.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.assets import CITYDATA_BRONZE_ASSET  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402

KST_TZ = ZoneInfo("Asia/Seoul")

# 단일 env 노브 — prod 컷오버 = DBT_TARGET=prod 한 줄, 롤백 = 제거(#556).
# 미설정 시 기본 "dev"(불변). 파싱 시점에 읽는다 — Cosmos ProfileConfig/RenderConfig 는
# DAG 파싱 시점에 고정되는 정적 설정이라, 태스크 실행 시점 params 로는 dbt --target 을
# 바꿀 수 없다(Cosmos 가 렌더한 태스크 커맨드에 이미 박혀있음). 그래서 여기만 env 로 읽는다.
_TARGET = os.environ.get("DBT_TARGET", "prod")

# 단독 citydata dbt 프로젝트 (모노 루트 아님).
DBT_PROJECT = "/opt/airflow/dbt/domains/citydata"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"

record_citydata_problem = problem_failure_callback(
    domain="citydata", source_system="seoul_citydata", dbt_project_dir=DBT_PROJECT)

# run 관측 — bronze(_run_ok_d1)와 같은 **D1 직접 emit**. 왜 record_run(R2 경유)이 아니라 직접인가
# (모두 dev 로컬 실측, ASK-Seoul#78):
#  1) 관측 결과 transform 의 D1 run 은 34건 전부 layer=NULL/failed — 유효 silver/gold·성공 0.
#     record_run("citydata","transform") 은 layer="transform" 을 쓰는데 Layer enum 은 bronze/silver/gold
#     뿐이라 그 R2→D1 배치 적재 경로에서 유효 계층으로 안 남았다.
#  2) bronze 는 record_run(R2) 에 더해 _run_ok_d1 로 **D1 에 직접** 실시간 한 줄을 쓴다 — transform 엔
#     그게 없었다. 그래서 배치·계층 문제를 우회해 **모델명으로 layer 판별(silver_*/gold_*) + D1 직접 적재**.
# 실패 콜백은 리스트 대신 **단일 함수로 합성** — 콜백 리스트의 실행 순서·보장을 걸지 않으려는 방어적 선택.
def _model_layer(task_id: str):
    """Cosmos 모델 run 태스크(``tier.<model>.run``) → Layer. 모르면 None(추측 금지)."""
    parts = str(task_id).split(".")
    if len(parts) < 2 or parts[-1] != "run":   # 모델 run 만 — test/gate/deps 제외
        return None
    from common.ops.contract import Layer  # noqa: PLC0415
    model = parts[-2]
    if model.startswith("silver_"):
        return Layer.SILVER
    if model.startswith("gold_"):
        return Layer.GOLD
    return None


def _emit_run_d1(context, status) -> None:
    """모델 run 1건 = ``_ops_run_event`` D1 한 줄(bronze _run_ok_d1 과 동형). fail-open(C-2)."""
    ti = context["ti"]
    layer = _model_layer(ti.task_id)
    if layer is None:
        return
    try:
        from common.ops.contract import Grain, OpsCategory, build_ops_event  # noqa: PLC0415
        from citydata_ingest.source.d1_run_event_writer import make_d1_run_event_writer  # noqa: PLC0415
        target = (context.get("params") or {}).get("target", "dev")
        record = build_ops_event(
            OpsCategory.RUNS, domain="citydata", layer=layer,
            grain=Grain.AIRFLOW_TASK, status=status,
            dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id,
            try_number=ti.try_number, environment=target)
        make_d1_run_event_writer()(record)
    except Exception as exc:  # noqa: BLE001 — C-2 fail-open(관측 실패가 변환을 안 죽인다)
        import logging  # noqa: PLC0415
        logging.getLogger(__name__).warning(
            "[ops] transform run D1 적재 실패(무시): %s", type(exc).__name__)


def _run_d1_success(context) -> None:
    from common.ops.contract import RunStatus  # noqa: PLC0415
    _emit_run_d1(context, RunStatus.SUCCESS)


def _fail_callback(context) -> None:
    """problem 문서 + run D1 을 **단일 함수로 합성** — 콜백 리스트 순서에 의존 안 하려는 방어."""
    from common.ops.contract import RunStatus  # noqa: PLC0415
    try:
        record_citydata_problem(context)
    except Exception:  # noqa: BLE001 — 한 콜백 실패가 다른 콜백을 막지 않게
        pass
    _emit_run_d1(context, RunStatus.FAILED)

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
    target_name=_TARGET,  # DBT_TARGET env 노브 — 컷오버(#556). 기본 dev(불변).
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
        f"{DBT_BIN} {args} --target {_TARGET} --no-use-colors"
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
            # 단일 콜백만 — 콜백 리스트 순서 의존 회피. run 관측은 모델별 layer 로 D1 직접 emit.
            "on_success_callback": _run_d1_success,
            "on_failure_callback": _fail_callback,
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


def _not_maintenance(**_) -> bool:
    """maintenance 진행 중이면(Variable citydata_maintenance_active=1) transform 전체 skip.

    주간 유지보수(citydata_maintenance)가 optimize 로 데이터파일을 재작성하는 동안 transform 의
    delete+insert 가 겹치면 Iceberg 커밋 충돌이 난다. 그 창에서만 transform 을 막는다 —
    maintenance DAG 가 플래그를 set(pause)/clear(resume, trigger_rule=all_done 로 항상 clear)."""
    from airflow.models import Variable

    return Variable.get("citydata_maintenance_active", default_var="0") != "1"


with DAG(
    dag_id="citydata_transform_cosmos",
    description="citydata transform via Cosmos — 모델별 태스크 + 모델별 테스트. 기존 통짜 DAG 대체(중복 재발 차단).",
    start_date=datetime(2026, 1, 1, tzinfo=KST_TZ),
    schedule=[Asset(CITYDATA_BRONZE_ASSET)],
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,  # 기존 DAG 와 동시 write 금지 — 검증 후 스왑
    # record_run(runs/ 관측)이 이 target 을 읽어 runs 경로/버킷을 정함 — dbt --target 과
    # 같은 DBT_TARGET 노브를 따르게 해 관측과 실제 빌드 대상이 어긋나지 않게 한다.
    params={"target": _TARGET},
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

    # 최상위 게이트: 주간 maintenance 진행 중이면 transform 전체 skip(optimize↔delete+insert 충돌 방지).
    gate_maint = ShortCircuitOperator(
        task_id="gate_not_maintenance", python_callable=_not_maintenance,
        on_failure_callback=record_citydata_problem,
    )

    # fast 완료 후 게이트 분기(독립·stagger 로 상호 비겹침) — slow(:00~04)·hourly(:05~09)·daily(0시:15~19).
    gate_maint >> deps_seed >> fast >> gate_slow >> slow
    fast >> gate_hourly >> hourly
    fast >> gate_daily >> daily
