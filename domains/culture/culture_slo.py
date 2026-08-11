"""culture_slo — run_report·dag_runs → SLO bronze 적재 + dbt tag:slo 빌드 (05:00 KST).

설계 §3: 본류(bronze 03:00 → Asset transform ~04:00) 완료 뒤·facility_refresh(05:30)
앞 슬롯. Asset outlet 없음(스케줄 구동) — 본류(culture_bronze/culture_transform) 무수정 원칙.
로더 IO(트리노/Airflow)는 culture_ingest.slo.io, 순수 로직은 culture_ingest.slo.loader.

체인: load_slo_bronze → dbt_slo → notify_slo_success

알림은 양쪽이 다 있어야 조용함을 읽을 수 있다(#777 과 같은 규칙): 실패는
`common.errors.airflow` 의 에러 embed(실패 노드·사유 포함), 성공은 체인 끝 태스크.
둘 다 best-effort 라 알림이 못 나가도 마트 결과는 그대로다.
"""
from __future__ import annotations

import os
import shlex
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)
from common.discord.notify import send_payload  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import TARGET_CHOICES, default_target  # noqa: E402
from common.ops import Layer  # noqa: E402
from common.ops.observability import ops_default_args  # noqa: E402

from culture_ingest.common import transform_notify  # noqa: E402

KST = "Asia/Seoul"

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/culture"

# `dbt_project_dir` 을 주면 실패 알림에 **어떤 모델·테스트가 왜** 깨졌는지가 붙는다(#161).
# culture_transform(#777)과 같은 이유·같은 형이다 — dbt CLI 를 BashOperator 로 직접 돌아
# 결과가 `target/run_results.json` 에 그대로 남으므로 XCom 키 형이 아니라 project_dir 형.
record_culture_problem = problem_failure_callback(
    domain="culture", dbt_project_dir=DBT_PROJECT)

#: `dbt build` 결과 스냅샷을 가리키는 태스크 id — 복사(bash)와 읽기(notify)가 이 하나를 공유한다.
SLO_SNAPSHOT_TASK = "dbt_slo"
# target 기본값은 배포 env 를 따른다(ASK-Seoul#66) — 하드코딩 "dev" 였으면 prod 에서
# dev 버킷의 run_report 를 읽으려다 매일 05:00 런이 실패한다.
DEFAULT_PARAMS = {
    "target": Param(
        default=default_target(),
        type="string",
        enum=list(TARGET_CHOICES),
        description="run_report·dag_runs 를 읽을 환경. 기본값은 런타임 env(ASK_SEOUL_TARGET/DBT_TARGET).",
    )
}


def _dbt(args: str, *, snapshot_for: str | None = None) -> str:
    """``snapshot_for`` 를 주면 성공 직후 그 태스크의 ``run_results.json`` 을 따로 복사한다.

    culture_transform 과 이름이 겹치지 않는다(`run_results.dbt_slo.json`) — 같은 프로젝트
    디렉터리를 쓰는 두 DAG 이 서로의 스냅샷을 덮어쓰면 알림이 남의 결과를 말하게 된다.
    """
    project = shlex.quote(DBT_PROJECT)
    command = (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"{shlex.quote(DBT_BIN)} {args} --target {{{{ params.target }}}} --no-use-colors"
    )
    if snapshot_for:
        snapshot = transform_notify.snapshot_path(DBT_PROJECT, snapshot_for)
        command += f"\ncp -f target/run_results.json {shlex.quote(snapshot)}"
    return command


def _notify_slo_success(**context: object) -> None:
    """SLO 마트를 어떻게 만들었는지 Discord 로 알린다(성공 경로).

    `dbt build` 는 run 과 test 를 **한 번에** 돌아 결과가 한 파일에 같이 남는다. 요약 함수는
    노드 종류(model/test)로 갈라 보므로 같은 payload 를 둘 다에 넘기면 되고, 소요만 합이
    두 배가 되지 않도록 명시로 넘긴다.
    """
    results = transform_notify.read_run_results(
        transform_notify.snapshot_path(DBT_PROJECT, SLO_SNAPSHOT_TASK))
    if results is None:
        return

    dag_run = context.get("dag_run")
    params = context.get("params") or {}
    models = transform_notify.summarize_models(results)
    payload = transform_notify.build_transform_payload(
        models,
        transform_notify.summarize_tests(results),
        label="SLO 마트",
        target=str(params.get("target")) if isinstance(params, dict) else None,
        dag_run_id=getattr(dag_run, "run_id", None),
        elapsed=models.get("elapsed"),
    )
    try:
        send_payload(payload, domain=transform_notify.DOMAIN)
    except Exception as exc:  # noqa: BLE001 — 알림 실패가 마트 성공을 뒤집지 않는다
        print(f"[culture slo] discord 알림 실패(무시): {type(exc).__name__}")


def _load_slo_bronze(**context):
    """R2 리포트 + Airflow dag_run 을 SLO bronze 2표에 적재(멱등)."""
    from culture_ingest.slo.io import load_dag_runs, load_run_reports

    target = context["params"]["target"]
    n_reports = load_run_reports(target=target)
    n_dag_runs = load_dag_runs(target=target)
    print(f"slo bronze: run_report+{n_reports}행, dag_runs={n_dag_runs}행")


with DAG(
    dag_id="culture_slo",
    description="run_report·dag_runs → SLO bronze 적재 + dbt tag:slo (05:00 KST)",
    start_date=pendulum.datetime(2026, 7, 16, tz=KST),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2, "retry_delay": timedelta(minutes=2),
        # ASK-Seoul#78 — 이 DAG 의 모든 태스크가 실행 기록을 남긴다. 실패 상세
        # (RFC 9457 Problem JSON)를 밀어내지 않고 뒤에 붙는다(교체가 아니라 추가).
        # SLO bronze 적재를 앞세우지만 이 DAG 의 산출물은 SLO 마트라 gold 로 등록한다.
        **ops_default_args("culture", Layer.GOLD, on_failure=record_culture_problem),
    },
    params=DEFAULT_PARAMS,
    tags=["culture", "slo", "observability"],
) as dag:
    load_slo_bronze = PythonOperator(
        task_id="load_slo_bronze",
        python_callable=_load_slo_bronze,
    )
    dbt_slo = BashOperator(
        task_id=SLO_SNAPSHOT_TASK,
        bash_command=_dbt("build --select tag:slo", snapshot_for=SLO_SNAPSHOT_TASK),
    )
    # 성공 쪽 말(#161 은 실패만 말한다) — 05:00 런이 조용할 때 "잘 돌았다"와 "안 돌았다"를
    # 가를 수 있게 한다. 알림이 실패해도 이 태스크는 성공으로 둔다(마트는 이미 만들어졌다).
    notify_success = PythonOperator(
        task_id="notify_slo_success", python_callable=_notify_slo_success)

    load_slo_bronze >> dbt_slo >> notify_success
