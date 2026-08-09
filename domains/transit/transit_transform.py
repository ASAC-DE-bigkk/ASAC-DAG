"""Airflow DAG: transit silver transform via dbt (#190).

The transit bronze DAGs (subway/bus/parking/master) land raw API payloads in R2
and publish Iceberg bronze runs. This transform DAG runs the ASAC-DBT transit
project (``domains/transit``) as a single ``dbt build`` — seeds, dim snapshots,
silver incrementals, and the contract tests in one DAG-ordered pass — and keeps
silver retries independent from bronze API collection.

이 파일은 transform DAG 두 개를 담는다(#443 예고 분리):
- ``transit_transform`` (*/15): 사용자향 '지금' 카드·증분 모델·silver·dim·게이트
  테스트 — ``--exclude 'tag:heavy tag:hourly'``. 감시성 테스트(tag:hourly)는
  시간당 1회만 ShortCircuit 분기(``dbt_test_rest``)로 실행한다(테스트 티어링 A안).
- ``transit_transform_heavy`` (5,35 오프셋 30분): 아카이브 전량 재집계 프로파일 4종 —
  ``--select tag:heavy``, artifact 는 ``--target-path target_heavy`` 로 격리

dbt 실행 방식은 타 도메인 transform DAG(weather/traffic)의 관례를 그대로 따른다:
컨테이너 dbt 바이너리(``/home/airflow/dbt-venv/bin/dbt``), 마운트된 dbt 프로젝트
(``/opt/airflow/dbt/domains/transit``), ``DBT_PROJECT_DIR``/``DBT_PROFILES_DIR``
주입, ``--target {dev|prod}`` 분기. 계약 게이트는 여전히 ``build`` 하나가 세운다
(#190): 그레인 unique·키/시간축 not_null·미래 event_at 차단(freshness 계약,
assert_silver_transit_no_future_event_at) 등 tag:gate 테스트는 매 15분
build 안에서 DAG 순서로 돌고, 실패는 build 의 비영 종료 → 태스크 실패로 이어진다.
range/enum/coverage/warn 등 감시성 테스트(tag:hourly)만 시간당 1회 분기로 뺐다 —
B안 스코핑(ASAC-DBT#418)이 test 각각을 싸게 만든 뒤에도 남는 '테스트 개수 × 96회/일'
의 단일노드 쿼리 조율 오버헤드를 줄인다. 분류 규약은 dbt_project.yml 헤더 주석 참조.

빌드 후 ``run_results.json`` 을 파싱해 모델 단위 실행 메트릭(layer=silver, #188)을
R2 에 적재한다(``dump_dbt_run_results``). 실패한 빌드에서도 모델/테스트 결과를
남길 수 있게 메트릭 적재 태스크는 ``all_done`` 으로 돈다.

⚠ 리프 마스킹(#526): Airflow 는 DagRun 상태를 **리프 태스크**로 판정하므로, all_done
메트릭 태스크가 유일한 리프면 dbt_build 실패가 run success 로 가려진다(7/21~ 실측
143연속 "성공한 좀비"). weather/traffic transform 6개가 쓰는 관례를 그대로 따라
메트릭 태스크를 ``as_teardown(on_failure_fail_dagrun=False)`` 로 선언한다 —
teardown 은 DagRun 판정에서 제외되어 dbt_build 가 실질 리프가 되고(build 실패 =
run 실패 복원), build 실패에서도 메트릭은 계속 적재된다(#188 의도 보존).
테스트 티어링(A안) 이후 실질 리프는 dbt_build → (시간당) dbt_test_rest 체인이다 —
build 실패는 downstream upstream_failed 로, test_rest 실패는 리프 실패로 각각
run 실패가 되고, 시간당 분기의 skip 은 성공 판정을 해치지 않는다(#526 의도 유지).
설계 배경: domains/weather/docs/superpowers/specs/2026-07-14-weather-transform-
teardown-provenance-design.md
"""

from __future__ import annotations

import os
import shlex
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)

# 동봉 패키지(seoul_transit)는 이 DAG 파일과 같은 폴더(dags/domains/transit/)에 있다.
# Airflow 3.x 는 dags 하위 디렉터리를 sys.path 에 자동 추가하지 않으므로 직접 올린다.
_DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if _DAG_DIR not in sys.path:
    sys.path.insert(0, _DAG_DIR)
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다.
_DAGS_ROOT = os.path.dirname(os.path.dirname(_DAG_DIR))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.pools import TRINO_TRANSIT_HEAVY_POOL  # noqa: E402
from common.runmetrics import dump_dbt_run_results  # noqa: E402
from seoul_transit import config  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/transit"
# 도메인 전용 Trino 직렬화 pool (slot 1) — 정의는 common/pools.py(단일 진실 공급원,
# airflow-init 이 pools import 로 생성). weather/traffic 의 도메인별 heavy pool 관례.
# fresh/heavy 두 transform 의 dbt_deps·dbt_build 가 같은 pool 을 쓰므로 단일노드
# Trino 에서 두 빌드가 절대 겹쳐 돌지 않고(실측: dev 동시 실행 시 heavy 168s → 약 8분),
# 공유 dbt_packages/ 를 다시 쓰는 deps 의 교차 경합도 함께 차단된다.
# dbt 는 run_results.json 을 프로젝트의 target-path(dbt_project.yml: target) 아래에 쓴다.
RUN_RESULTS_PATH = os.path.join(DBT_PROJECT, "target", "run_results.json")
# heavy DAG 는 target-path 를 격리한다 — 두 transform DAG 가 같은 프로젝트를 공유하므로
# 기본 target/ 을 같이 쓰면 run_results.json 등 artifact 를 서로 덮어써 메트릭이 섞인다.
HEAVY_TARGET_PATH = "target_heavy"
HEAVY_RUN_RESULTS_PATH = os.path.join(DBT_PROJECT, HEAVY_TARGET_PATH, "run_results.json")
# 시간당 1회 감시성 테스트 분기(dbt_test_rest)도 target-path 를 격리한다 — 같은 런에서
# fresh build 의 run_results.json 을 teardown 메트릭이 읽기 전에 test 가 덮어쓰는
# 경합을 막는다(heavy 격리와 동일 사유).
REST_TARGET_PATH = "target_test_hourly"
REST_RUN_RESULTS_PATH = os.path.join(DBT_PROJECT, REST_TARGET_PATH, "run_results.json")

DOMAIN = os.environ.get("TRANSIT_DOMAIN", "transit")

def _make_params() -> dict:
    """DAG 별 params — fresh/heavy 두 DAG 가 Param 인스턴스를 공유하지 않게 매번 새로 만든다."""
    return {
        "target": Param(
            # 배포 env 를 따른다(#575) — dev 박스(DBT_TARGET=dev)에선 기존과 동일한 dev 기본
            # (실수로 prod 를 치지 않는 안전 게이트 유지), prod 스택에선 자동 prod.
            # 하드코딩 "dev" 는 prod 컷오버 시 iceberg_dev CATALOG_NOT_FOUND 로 전 런 실패했다.
            default=os.environ.get("DBT_TARGET", "dev"),
            type="string",
            enum=["dev", "prod"],
            description="dbt target profile name.",
        )
    }


DEFAULT_PARAMS = _make_params()

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
# transform 은 단일 외부 소스 API 를 호출하지 않고 subway/bus/parking 브론즈를 함께
# 소비하므로 단일 source_system 이 성립하지 않는다 → weather/traffic transform 관례대로 생략.
record_transit_problem = problem_failure_callback(domain=DOMAIN)


def transform_schedule() -> str:
    """*/15 기본, env TRANSIT_TRANSFORM_SCHEDULE 로 오버라이드(seoul_transit.config 재사용).

    15분으로 당긴 근거(#443): 사용자향 '지금' 카드(G1)는 지하철 3분·주차 5분 수집인데
    @hourly 면 최대 1시간 묵은 값을 보여준다. 실측 dbt build 소요는 344초(2026-07-20,
    dev 전 모델+테스트)라 15분 창에 들어간다.

    #443 이 예고한 무거운 모델 분리는 시행됨: 아카이브 전량 재집계 프로파일 4종
    (리듬·주차·노선·event_access, tag:heavy)은 transit_transform_heavy 가 느린 주기로
    따로 빌드하고, 이 DAG 는 --exclude tag:heavy 로 제외한다. max_active_runs=1 이라
    초과분은 큐잉되며 겹쳐 돌지는 않는다.
    """
    return config.schedule_for("transit_transform", "*/15 * * * *")


def transform_heavy_schedule() -> str:
    """heavy 프로파일 재빌드 주기 — 5,35분 기본, env TRANSIT_TRANSFORM_HEAVY_SCHEDULE 오버라이드.

    30분 주기이되 */30 이 아니라 5,35 오프셋인 이유: fresh(*/15)와 heavy 를 나눈 목적이
    단일노드 Trino 상시부하 완화인데, */30 이면 매 :00/:30 에 fresh 런과 반드시 동시
    트리거되어 순간 부하가 오히려 커진다(dbt deps 의 dbt_packages/ 동시 쓰기 충돌도 회피).
    fresh 실측 소요(#443: 344초, heavy 4종 빠지면 그 이하)가 5분 오프셋 안에 대체로
    끝나므로 겹침이 최소화된다.
    """
    return config.schedule_for("transit_transform_heavy", "5,35 * * * *")


def transform_gate_open(now=None) -> bool:
    """TRANSIT_TRANSFORM_NOT_BEFORE(KST) 이전이면 변환을 돌리지 않는다.

    버스 수집 정책 전환(BUS_COLLECT_NOT_BEFORE, 2026-07-21 09:00)에 맞춰 gold 아카이브를
    드롭하고 개시일(dbt var transit_archive_start_at) 이후부터 새로 쌓기로 했다. 그 사이
    아카이브 테이블은 **의도적으로 비어 있는데**, 공통축 커버리지 테스트는 빈 테이블을
    실패로 규정한다(asac_axes #48: "빈 테이블(total=0)도 실패로 본다"). 게이트 없이 돌면
    데이터가 들어올 때까지 매시 빌드가 실패해 경보만 쌓인다 — 그래서 첫 수집이 랜딩된
    뒤부터 돌도록 늦춘다(수집 09:00 → 변환 09:30).

    시각이 지나면 무해한 no-op. 빈 값이면 게이트 없음.
    """
    from datetime import datetime

    raw = os.environ.get("TRANSIT_TRANSFORM_NOT_BEFORE", "2026-07-21T09:30").strip()
    if not raw:
        return True
    gate = datetime.fromisoformat(raw)
    if gate.tzinfo is None:
        gate = gate.replace(tzinfo=config.KST)
    return (now or datetime.now(config.KST)) >= gate


def check_transform_gate(**_context) -> None:
    """게이트가 닫혀 있으면 downstream(dbt) 을 건너뛴다.

    fresh·heavy 두 DAG 의 공통 첫 태스크라, maintenance 창(#748) 차단도 여기서 함께
    본다 — optimize 가 silver·gold 파일을 재작성하는 동안 merge 커밋이 겹치면 Iceberg
    충돌이 나므로 그 창의 run 만 skip 한다(citydata gate_not_maintenance 관례).
    플래그는 transit_maintenance 가 set(pause)/clear(resume, all_done 로 항상 clear)한다.
    """
    from airflow.exceptions import AirflowSkipException
    from airflow.models import Variable

    from seoul_transit.maintenance import MAINT_FLAG

    if not transform_gate_open():
        raise AirflowSkipException(
            "변환 재개 게이트 이전 — gold 아카이브 개시일 전까지 대기 "
            f"(TRANSIT_TRANSFORM_NOT_BEFORE={os.environ.get('TRANSIT_TRANSFORM_NOT_BEFORE', '2026-07-21T09:30')})"
        )
    if Variable.get(MAINT_FLAG, default_var="0") == "1":
        raise AirflowSkipException(
            f"transit_maintenance 진행 중({MAINT_FLAG}=1) — "
            "silver·gold optimize 와 merge 커밋 충돌 방지를 위해 이 run 은 skip"
        )


def rest_tests_due(**_context) -> bool:
    """감시성 테스트(tag:hourly) 분기를 시간당 정확히 1회만 통과시킨다(ShortCircuit).

    citydata ``_is_hourly_window`` 관례 재사용: Variable 로 '이 시간 버킷에 이미
    돌았나'를 본다. 분 창(minute 범위) 방식은 citydata 에서 트리거 분이 불규칙할 때
    창을 자주 빗나가 hourly 티어가 몇 시간씩 안 도는 버그가 있었다(2026-07-26) —
    transit 은 */15 cron 이라 덜하지만 max_active_runs=1 큐잉 지연으로 같은 문제가
    재현될 수 있어 동일하게 Variable 방식을 쓴다. Variable set 후 downstream(test)이
    실패해도 태스크 재시도·다음 시간 run 이 재검증한다(self-heal).

    한계(감수): set 직후 이 태스크 자체가 죽으면 재시도가 False 를 보고 그 시간 분은
    감시 테스트가 결손된다 — 다음 시간 run 이 self-heal 하므로 최대 1시간 결손이며,
    citydata 도 같은 트레이드오프를 감수한다.
    """
    from airflow.models import Variable

    now = datetime.now(KST)
    key = "transit_transform_hourly_tests_last_hour"
    cur = now.strftime("%Y-%m-%dT%H")  # 시간 버킷
    if Variable.get(key, default_var="") == cur:
        return False  # 이 시간엔 이미 실행함
    Variable.set(key, cur)
    return True


def dbt_command(
    args: str,
    *,
    select: str | None = None,
    exclude: str | None = None,
    target_path: str | None = None,
) -> str:
    """dbt 실행 bash — weather/traffic transform 과 동일한 조립(경로·env·target 분기).

    select/exclude 는 fresh/heavy 분리용 노드 셀렉터(``--select``/``--exclude``).
    target_path 는 artifact(run_results.json 등) 격리용 ``--target-path`` — 같은 프로젝트를
    공유하는 두 transform DAG 가 기본 target/ 을 덮어쓰지 않게 한다. 기본값(전부 None)은
    현행 동작과 동일.
    """
    project = shlex.quote(DBT_PROJECT)
    command = f"{shlex.quote(DBT_BIN)} {args}"
    if select:
        command += f" --select {shlex.quote(select)}"
    if exclude:
        command += f" --exclude {shlex.quote(exclude)}"
    if target_path:
        command += f" --target-path {shlex.quote(target_path)}"
    return (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"{command} --target '{{{{ params.target }}}}' --no-use-colors"
    )


def publish_silver_metrics(
    run_results_path: str = RUN_RESULTS_PATH,
    *,
    cleanup_after: bool = False,
    **context,
) -> dict:
    """dbt build 산출 run_results.json → 모델 단위 실행 메트릭(layer=silver, #188)을 R2 적재.

    - target(dev/prod) 은 params 에서 받아 sink 버킷(R2_DEV_* dev 우선) 을 정합시킨다.
    - run_results.json 부재(예: deps 실패로 build 미도달) 는 태스크를 실패시키지 않고 skip —
      메트릭 적재 실패가 이미 실패한 빌드 위에 잡음을 더하지 않게 한다.
    - cleanup_after: 게시 성공 직후 run_results.json 을 삭제한다. 시간당 테스트 분기
      전용 — ShortCircuit 은 teardown 을 skip 대상에서 제외하므로(아래 배선 주석 참조)
      분기 skip 런에도 teardown 이 돌며, 파일을 지워 두지 않으면 지난 시간의 stale
      결과를 매 15분 재게시한다. 삭제해 두면 skip 런은 '파일 없음 skip' no-op 이 된다.
    """
    if not os.path.exists(run_results_path):
        print(f"run_results.json 없음 — 메트릭 적재 skip: {run_results_path}")
        return {"skipped": True, "rows": 0}
    target = (context.get("params") or {}).get("target")
    records = dump_dbt_run_results(run_results_path, domain=DOMAIN, target=target)
    print(f"silver 메트릭 적재: {len(records)} records (domain={DOMAIN}, target={target})")
    if cleanup_after:
        os.remove(run_results_path)
        print(f"게시 완료 후 artifact 제거(stale 재게시 방지): {run_results_path}")
    return {"rows": len(records), "skipped": False}


with DAG(
    dag_id="transit_transform",
    description="Transform transit bronze -> silver via dbt build (#190).",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=transform_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "transit", "transform", "silver", "dbt"],
) as dag:
    # 재개 게이트(gold 아카이브 개시일 대기) — 닫혀 있으면 skip 으로 downstream 차단.
    # 실패가 아니라 skip 이라 경보를 만들지 않는다.
    gate = PythonOperator(
        task_id="check_transform_gate",
        python_callable=check_transform_gate,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_command("deps"),
        pool=TRINO_TRANSIT_HEAVY_POOL,
        on_failure_callback=record_transit_problem,
    )

    # 계약 게이트: seed+dim+silver+게이트 테스트(tag:gate·무태그)를 DAG 순서로 build.
    # dbt 테스트 실패는 build 의 비영 종료 → 이 태스크 실패 → DAG 런 실패.
    # tag:heavy(아카이브 전량 재집계 프로파일 4종)는 transit_transform_heavy 담당,
    # tag:hourly(감시성 테스트)는 아래 시간당 분기(dbt_test_rest) 담당이라 제외 —
    # 그레인/키 무결성은 여전히 매 15분 여기서 차단된다(테스트 티어링 A안).
    # (forecast_card 는 상류 citydata ppltn_forecast 삭제로 제품 제거 — ASAC-DBT#432)
    # ⚠ fresh 에 남는 parking_full_risk 는 heavy 테이블(parking_profile)을 SELECT 한다 —
    # 신규 환경/골드 드롭 직후엔 첫 heavy 런(:05/:35) 전까지 이 모델이 TABLE_NOT_FOUND 로
    # 실패할 수 있다(1 heavy 주기 내 자가 회복, 부트스트랩은 1회 수동 full build 권장).
    dbt_build = BashOperator(
        task_id="dbt_build",
        # 공백 구분 union 셀렉터 — dbt 는 인자 안 공백을 여러 기준의 합집합으로 파싱한다.
        bash_command=dbt_command("build", exclude="tag:heavy tag:hourly"),
        pool=TRINO_TRANSIT_HEAVY_POOL,
        on_failure_callback=record_transit_problem,
    )

    # 모델 단위 실행 메트릭(#188) — build 실패에서도 적재. teardown 선언(#526)으로
    # DagRun 판정에서 제외해 dbt_build 를 실질 리프로 만든다(weather/traffic 관례).
    publish_metrics = PythonOperator(
        task_id="publish_silver_metrics",
        python_callable=publish_silver_metrics,
        on_failure_callback=record_transit_problem,
    ).as_teardown(on_failure_fail_dagrun=False)

    # 테스트 티어링(A안): 감시성 테스트(tag:hourly)는 시간당 1회만. build(게이트 테스트
    # 포함)가 성공한 뒤에만 진행하므로 모델·그레인 무결성 검증 주기는 그대로 15분이다.
    # citydata 3-tier 의 ShortCircuit 게이트 패턴과 동일 — 분기 미해당 런은 실패가
    # 아니라 skip 이라 경보를 만들지 않고 DagRun 성공 판정도 해치지 않는다.
    # 단 게이트 태스크 자체가 실패하면(예: Variable API 오류) dbt_test_rest 가
    # upstream_failed 리프가 되어 run 실패다 — 감시 결손을 조용히 넘기지 않는 방향.
    gate_test_hourly = ShortCircuitOperator(
        task_id="gate_test_hourly",
        python_callable=rest_tests_due,
        on_failure_callback=record_transit_problem,
    )

    # 나머지 테스트 실행 — 같은 직렬화 pool(단일노드 Trino 보호). 매시 :00 런에서
    # heavy(:05,:35)와 인접하지만 pool slot 1 이 동시 실행을 차단한다(대기만 발생).
    dbt_test_rest = BashOperator(
        task_id="dbt_test_rest",
        bash_command=dbt_command(
            "test", select="tag:hourly", exclude="tag:heavy",
            target_path=REST_TARGET_PATH,
        ),
        pool=TRINO_TRANSIT_HEAVY_POOL,
        on_failure_callback=record_transit_problem,
    )

    # 시간당 테스트 결과도 #188 메트릭으로 적재(heavy 와 동일 관례). teardown 이라
    # DagRun 판정에서 빠지고, dbt_test_rest 실패에서도 결과를 남긴다.
    # ⚠ ShortCircuit 은 teardown 을 skip 대상에서 제외한다(providers-standard
    # get_tasks_to_skip 의 is_teardown 제외) — 이 태스크는 분기 skip 런에도 매번
    # 실행된다. cleanup_after 로 게시 직후 run_results.json 을 지워, skip 런에선
    # '파일 없음 skip' no-op 이 되게 한다(지난 시간 결과의 stale 재게시 방지).
    publish_rest_metrics = PythonOperator(
        task_id="publish_rest_test_metrics",
        python_callable=publish_silver_metrics,
        op_kwargs={"run_results_path": REST_RUN_RESULTS_PATH, "cleanup_after": True},
        on_failure_callback=record_transit_problem,
    ).as_teardown(on_failure_fail_dagrun=False)

    gate >> dbt_deps >> dbt_build >> publish_metrics
    dbt_build >> gate_test_hourly >> dbt_test_rest >> publish_rest_metrics


# ── heavy: 아카이브 전량 재집계 프로파일 4종을 느린 주기로 분리 ─────────────────────
# gold_transit_dong_rhythm / parking_profile / bus_route_comfort / event_access 는
# materialized='table' 전량 재빌드라 transit_transform 이 최대 Trino 소비자가 되는
# 주범이었다(#443 docstring 이 예고한 분리). 15분 신선도가 불필요한 프로파일이므로
# 30분 주기(5,35 오프셋)로 뺀다. 사용자향 '지금' 카드·증분 모델은 fresh(*/15) 유지.
#
# 의존성: heavy 모델의 상류 silver/dim 은 fresh DAG 가 15분마다 갱신하므로
# --select tag:heavy (상류 제외)로 기존 silver 를 재사용한다. +tag:heavy(조상 포함)는
# heavy 런도 무겁게 만들므로 쓰지 않는다 — 최초 배포 시 상류가 비어 있으면 1회 수동
# full build 로 부트스트랩할 것.
with DAG(
    dag_id="transit_transform_heavy",
    description="Rebuild heavy transit profile golds (tag:heavy) every 30min (#443 follow-up).",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=transform_heavy_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params=_make_params(),
    tags=["ask_seoul", "transit", "transform", "gold", "dbt", "heavy"],
) as heavy_dag:
    heavy_gate = PythonOperator(
        task_id="check_transform_gate",
        python_callable=check_transform_gate,
    )

    heavy_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_command("deps"),
        pool=TRINO_TRANSIT_HEAVY_POOL,
        on_failure_callback=record_transit_problem,
    )

    # heavy 4종 + 그 테스트만 build. --target-path 격리로 fresh DAG 의 target/ artifact
    # (run_results.json)를 덮어쓰지 않는다.
    heavy_dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=dbt_command(
            "build", select="tag:heavy", target_path=HEAVY_TARGET_PATH
        ),
        pool=TRINO_TRANSIT_HEAVY_POOL,
        on_failure_callback=record_transit_problem,
    )

    # 메트릭 적재(#188)·teardown 선언(#526) 은 fresh 와 동일 관례 — 격리된 target_heavy/
    # 의 run_results.json 을 읽는다.
    heavy_publish_metrics = PythonOperator(
        task_id="publish_silver_metrics",
        python_callable=publish_silver_metrics,
        op_kwargs={"run_results_path": HEAVY_RUN_RESULTS_PATH},
        on_failure_callback=record_transit_problem,
    ).as_teardown(on_failure_fail_dagrun=False)

    heavy_gate >> heavy_dbt_deps >> heavy_dbt_build >> heavy_publish_metrics
