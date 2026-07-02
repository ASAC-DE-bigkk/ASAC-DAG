"""Airflow DAG: population silver/gold 변환 (dbt).

수집 DAG(``population_bronze``, 5분)가 적재한 bronze를 **5분마다** dbt로 silver/gold
변환하고 테스트한다. silver가 incremental(merge)이라 매 run은 최근 수집분만 처리하고
(전체 재스캔 없음), gold는 silver를 소비하는 얇은 파생이라 table 재생성으로 둔다.
silver와 gold는 dbt가 ref() 의존성 순서로 한 run에서 함께 빌드한다.

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
    dag_id="population_transform",
    description="Transform population bronze -> silver/gold via dbt (every 5 min).",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["transform", "population", "silver", "gold", "dbt"],
) as dag:
    # 참조 데이터(121장소 좌표/영역 seed) 적재 -- 121행이라 매 run 갱신해도 싸고 멱등.
    seed_refs = BashOperator(
        task_id="dbt_seed",
        bash_command=_dbt("seed"),
    )

    # silver(incremental merge) + gold(table 재생성, seed 조인) 빌드.
    run_models = BashOperator(
        task_id="dbt_run",
        bash_command=_dbt("run --select silver_seoul_ppltn gold_seoul_ppltn_by_time"),
    )

    # 데이터 품질 테스트 (assert_silver_not_empty 등).
    test_models = BashOperator(
        task_id="dbt_test",
        bash_command=_dbt("test"),
    )

    seed_refs >> run_models >> test_models
