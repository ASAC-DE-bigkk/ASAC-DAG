"""culture_slo — run_report·dag_runs → SLO bronze 적재 + dbt tag:slo 빌드 (05:00 KST).

설계 §3: 본류(bronze 03:00 → Asset transform ~04:00) 완료 뒤·facility_refresh(05:30)
앞 슬롯. Asset outlet 없음(스케줄 구동) — 본류(culture_bronze/culture_transform) 무수정 원칙.
로더 IO(트리노/Airflow)는 culture_ingest.slo.io, 순수 로직은 culture_ingest.slo.loader.
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
from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import TARGET_CHOICES, default_target  # noqa: E402
from common.ops import Layer  # noqa: E402
from common.ops.observability import ops_default_args  # noqa: E402

KST = "Asia/Seoul"
record_culture_problem = problem_failure_callback(domain="culture")

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/culture"
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


def _dbt(args: str) -> str:
    project = shlex.quote(DBT_PROJECT)
    return (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"{shlex.quote(DBT_BIN)} {args} --target {{{{ params.target }}}} --no-use-colors"
    )


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
        task_id="dbt_slo",
        bash_command=_dbt("build --select tag:slo"),
    )
    load_slo_bronze >> dbt_slo
