"""Airflow DAG: population silver/gold 변환 (dbt).

수집 DAG(``seoul_ppltn_collect``, 5분)가 적재한 bronze를 **15분마다** dbt로 silver/gold
변환하고 테스트한다. 수집(5분)과 변환(15분)을 분리해, 5분마다 전체 재생성하는 낭비를
피하면서 원천 갱신 주기(대략 5~15분)에 맞춘 신선도를 유지한다.

dbt 프로젝트는 compose가 마운트한 ``/opt/airflow/dbt/domains/population``을 쓴다
(ASAC-DBT 레포). dbt 실행 바이너리는 이미지의 전용 venv(``/home/airflow/dbt-venv``).
target(dev/prod)은 카탈로그(iceberg_dev/iceberg)를 가르며 기본 dev.

파라미터 (트리거 시 덮어쓰기 가능):
  target   "dev" | "prod"   (기본 dev)
"""

from __future__ import annotations

import shlex
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

KST = "Asia/Seoul"

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/population"

DEFAULT_PARAMS = {"target": "dev"}


def _dbt(args: str) -> str:
    """dbt 하위명령을 population 프로젝트/프로파일로 실행하는 bash 스니펫.

    compose 기본 DBT_PROJECT_DIR/DBT_PROFILES_DIR(elt_smoke)을 population으로 덮어쓴다.
    target은 DAG 파라미터에서 온다(기본 dev).
    """
    project = shlex.quote(DBT_PROJECT)
    return (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"{shlex.quote(DBT_BIN)} {args} --target {{{{ params.target }}}} --no-use-colors"
    )


with DAG(
    dag_id="seoul_ppltn_transform",
    description="Transform population bronze -> silver/gold via dbt (every 15 min).",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["transform", "population", "silver", "gold", "dbt"],
) as dag:
    # silver/gold 모델 빌드 (table 재생성).
    run_models = BashOperator(
        task_id="dbt_run",
        bash_command=_dbt("run --select silver_seoul_ppltn gold_seoul_ppltn_by_time"),
    )

    # 데이터 품질 테스트 (assert_silver_not_empty 등).
    test_models = BashOperator(
        task_id="dbt_test",
        bash_command=_dbt("test"),
    )

    run_models >> test_models
