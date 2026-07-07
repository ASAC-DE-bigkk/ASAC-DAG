"""Airflow DAG: culture silver/gold 변환 (dbt) — Asset 트리거.

``culture_bronze``의 load_bronze가 bronze Iceberg를 갱신하며 Asset을 발행하면
이 DAG이 자동 기동한다(cron 우연 결합 없음 — bronze 성공 시에만 변환).

체인: dbt_source_freshness → dbt_seed → dbt_run → dbt_test
  * source freshness — sources.yml의 계약(경고 30h/에러 48h)을 실측. error만 실패.
  * seed — sema_branch_location(분관 좌표)·sejong_location + asac_axes 패키지 seed(행정동 크로스워크·경계).
  * run/test — silver 9종 + gold 3종 빌드 후 계약 테스트.

dbt 프로젝트는 compose가 마운트한 ``/opt/airflow/dbt/domains/culture``(ASAC-DBT),
실행 바이너리는 이미지 전용 venv. target(dev/prod)은 카탈로그를 가른다(기본 dev).

파라미터 (트리거 시 덮어쓰기 가능):
  target   "dev" | "prod"   (기본 dev)
"""

from __future__ import annotations

import os
import shlex
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402

from culture_ingest.common.config import CULTURE_BRONZE_ASSET  # noqa: E402

KST = "Asia/Seoul"

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
# dbt 변환 DAG 라 외부 소스가 없어 source_system 은 생략.
record_culture_problem = problem_failure_callback(domain="culture")

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/domains/culture"

DEFAULT_PARAMS = {"target": "dev"}


def _dbt(args: str) -> str:
    """dbt 하위명령을 culture 프로젝트/프로파일로 실행하는 bash 스니펫."""
    project = shlex.quote(DBT_PROJECT)
    return (
        "set -euo pipefail\n"
        f"cd {project}\n"
        f"export DBT_PROFILES_DIR={project} DBT_PROJECT_DIR={project}\n"
        f"{shlex.quote(DBT_BIN)} {args} --target {{{{ params.target }}}} --no-use-colors"
    )


with DAG(
    dag_id="culture_transform",
    description="Transform culture bronze -> silver/gold via dbt, triggered by bronze Asset.",
    start_date=pendulum.datetime(2026, 6, 1, tz=KST),
    schedule=[Asset(CULTURE_BRONZE_ASSET)],
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    params=DEFAULT_PARAMS,
    tags=["transform", "culture", "silver", "gold", "dbt"],
) as dag:
    # bronze 신선도 게이트 — 48h 넘게 낡았으면 여기서 멈추고 수집부터 고치게 한다.
    freshness = BashOperator(task_id="dbt_source_freshness",
                             bash_command=_dbt("source freshness"),
                             on_failure_callback=record_culture_problem)

    seed = BashOperator(task_id="dbt_seed", bash_command=_dbt("seed"),
                        on_failure_callback=record_culture_problem)

    # asac_axes 패키지 자체 모델(dim_admin_dong)은 타 레포 로더(#154) 의존이라 이 환경에서 ERROR —
    # culture 변환은 자기 모델만 빌드/테스트한다 (패키지 seed·매크로·제네릭 테스트는 계속 사용).
    run_models = BashOperator(task_id="dbt_run",
                              bash_command=_dbt("run --exclude package:asac_axes"),
                              on_failure_callback=record_culture_problem)

    test_models = BashOperator(task_id="dbt_test",
                               bash_command=_dbt("test --exclude package:asac_axes"),
                               on_failure_callback=record_culture_problem)

    freshness >> seed >> run_models >> test_models
