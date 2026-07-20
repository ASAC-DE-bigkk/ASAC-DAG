"""Airflow DAG: transit silver transform via dbt (#190).

The transit bronze DAGs (subway/bus/parking/master) land raw API payloads in R2
and publish Iceberg bronze runs. This transform DAG runs the ASAC-DBT transit
project (``domains/transit``) as a single ``dbt build`` — seeds, dim snapshots,
silver incrementals, and the contract tests in one DAG-ordered pass — and keeps
silver retries independent from bronze API collection.

dbt 실행 방식은 타 도메인 transform DAG(weather/traffic)의 관례를 그대로 따른다:
컨테이너 dbt 바이너리(``/home/airflow/dbt-venv/bin/dbt``), 마운트된 dbt 프로젝트
(``/opt/airflow/dbt/domains/transit``), ``DBT_PROJECT_DIR``/``DBT_PROFILES_DIR``
주입, ``--target {dev|prod}`` 분기. 차이는 run/test 를 쪼개지 않고 ``build`` 하나로
계약 게이트를 세운다는 점(seed+dim+silver+test 전체) — dbt 테스트 실패는 build 의
비영 종료로 이어져 태스크가 실패한다.

빌드 후 ``run_results.json`` 을 파싱해 모델 단위 실행 메트릭(layer=silver, #188)을
R2 에 적재한다(``dump_dbt_run_results``). 실패한 빌드에서도 모델/테스트 결과를
남길 수 있게 메트릭 적재 태스크는 ``all_done`` 으로 돈다 — 빌드 태스크 자체의 실패가
DAG 런 실패(계약 게이트)를 그대로 보존하므로 게이트는 훼손되지 않는다.
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
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

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
from common.runmetrics import dump_dbt_run_results  # noqa: E402
from seoul_transit import config  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/transit"
# dbt 는 run_results.json 을 프로젝트의 target-path(dbt_project.yml: target) 아래에 쓴다.
RUN_RESULTS_PATH = os.path.join(DBT_PROJECT, "target", "run_results.json")

DOMAIN = os.environ.get("TRANSIT_DOMAIN", "transit")

DEFAULT_PARAMS = {
    "target": Param(
        default="dev",
        type="string",
        enum=["dev", "prod"],
        description="dbt target profile name.",
    )
}

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
# transform 은 단일 외부 소스 API 를 호출하지 않고 subway/bus/parking 브론즈를 함께
# 소비하므로 단일 source_system 이 성립하지 않는다 → weather/traffic transform 관례대로 생략.
record_transit_problem = problem_failure_callback(domain=DOMAIN)


def transform_schedule() -> str:
    """*/15 기본, env TRANSIT_TRANSFORM_SCHEDULE 로 오버라이드(seoul_transit.config 재사용).

    15분으로 당긴 근거(#443): 사용자향 '지금' 카드(G1)는 지하철 3분·주차 5분 수집인데
    @hourly 면 최대 1시간 묵은 값을 보여준다. 실측 dbt build 소요는 344초(2026-07-20,
    dev 전 모델+테스트)라 15분 창에 들어간다.

    ⚠️ 여유가 무한하지 않다: 프로파일 3종(리듬·주차·노선)은 아카이브 전량을 매 런
    재집계하고 event_access 는 행사×역·주차 거리 계산을 매 런 반복한다 — 아카이브가
    쌓이면 소요가 늘어난다. build 가 15분에 근접하면 무거운 모델을 별도 주기(tag 선택)로
    분리할 것. max_active_runs=1 이라 초과분은 큐잉되며 겹쳐 돌지는 않는다.
    """
    return config.schedule_for("transit_transform", "*/15 * * * *")


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
    """게이트가 닫혀 있으면 downstream(dbt) 을 건너뛴다."""
    from airflow.exceptions import AirflowSkipException

    if not transform_gate_open():
        raise AirflowSkipException(
            "변환 재개 게이트 이전 — gold 아카이브 개시일 전까지 대기 "
            f"(TRANSIT_TRANSFORM_NOT_BEFORE={os.environ.get('TRANSIT_TRANSFORM_NOT_BEFORE', '2026-07-21T09:30')})"
        )


def dbt_command(args: str) -> str:
    """dbt 실행 bash — weather/traffic transform 과 동일한 조립(경로·env·target 분기)."""
    project = shlex.quote(DBT_PROJECT)
    return (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"{shlex.quote(DBT_BIN)} {args} --target '{{{{ params.target }}}}' --no-use-colors"
    )


def publish_silver_metrics(run_results_path: str = RUN_RESULTS_PATH, **context) -> dict:
    """dbt build 산출 run_results.json → 모델 단위 실행 메트릭(layer=silver, #188)을 R2 적재.

    - target(dev/prod) 은 params 에서 받아 sink 버킷(R2_DEV_* dev 우선) 을 정합시킨다.
    - run_results.json 부재(예: deps 실패로 build 미도달) 는 태스크를 실패시키지 않고 skip —
      메트릭 적재 실패가 이미 실패한 빌드 위에 잡음을 더하지 않게 한다.
    """
    if not os.path.exists(run_results_path):
        print(f"run_results.json 없음 — 메트릭 적재 skip: {run_results_path}")
        return {"skipped": True, "rows": 0}
    target = (context.get("params") or {}).get("target")
    records = dump_dbt_run_results(run_results_path, domain=DOMAIN, target=target)
    print(f"silver 메트릭 적재: {len(records)} records (domain={DOMAIN}, target={target})")
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
        on_failure_callback=record_transit_problem,
    )

    # 계약 게이트: seed+dim+silver+test 전체를 DAG 순서로 build. dbt 테스트 실패는
    # build 의 비영 종료 → 이 태스크 실패 → DAG 런 실패.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=dbt_command("build"),
        on_failure_callback=record_transit_problem,
    )

    # 모델 단위 실행 메트릭(#188). build 실패에서도 모델/테스트 결과를 남기도록 all_done.
    publish_metrics = PythonOperator(
        task_id="publish_silver_metrics",
        python_callable=publish_silver_metrics,
        trigger_rule=TriggerRule.ALL_DONE,
        on_failure_callback=record_transit_problem,
    )

    gate >> dbt_deps >> dbt_build >> publish_metrics
