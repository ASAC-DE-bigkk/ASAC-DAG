"""commerce_localdata_transform — silver dbt 변환 오케스트레이션 (run → test).

bronze 적재(commerce_load_bronze, 04:00 KST) 이후 dbt/domains/commerce 의 silver 모델을
전량 재빌드하고 테스트한다. dbt 는 Airflow 이미지의 별도 venv(`DBT_BIN`, 기본
/home/airflow/dbt-venv/bin/dbt — common_dbt_smoke 와 동일 계약)로 실행한다.

정책(재빌드·단위 제어 — dbt/domains/commerce/docs/rebuild-and-ops.md):
- silver 는 bronze 의 순수 함수(materialized=table 전량 재빌드) → 이 DAG 는 상태가 없고
  몇 번을 재실행해도 결과가 같다(멱등). 실패 시 그냥 재실행.
- 특정 데이터셋/일자/run 제외(삭제)는 dbt 프로젝트의 vars(exclude_*)로, 특정 데이터셋
  재적재는 bronze 워터마크 파일로 제어한다 — 이 DAG 는 항상 "현재 설정대로 전량"만 수행.
- gold 단계는 Step 9 구현 시 run/test 태스크 2개를 뒤에 추가한다.

  dbt_run_silver ─> dbt_test_silver
"""
from __future__ import annotations

import shlex
import sys
from pathlib import Path

# 자립(portable): 자기 카테고리의 include 를 import 경로에 올린다.
sys.path.insert(0, str(Path(__file__).resolve().parent / "include"))
# 공통 패키지(dags/common) — commerce_core.storage 가 common.storage 를 쓴다(#109).
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from commerce_core.env import load_commerce_env  # noqa: E402

load_commerce_env()

from security import install_security  # noqa: E402

install_security()

import os  # noqa: E402

import pendulum  # noqa: E402
from airflow.decorators import dag  # noqa: E402
from airflow.providers.standard.operators.bash import BashOperator  # noqa: E402

# dbt 실행 계약(호스트 이미지 env 우선, 없으면 기본값) — common_dbt_smoke 와 동일 형태.
DBT_PROJECT_DIR = os.getenv("COMMERCE_DBT_PROJECT_DIR", "/opt/airflow/dbt/domains/commerce")
DBT_BIN = os.getenv("DBT_BIN", "dbt")
# 기본 dev(iceberg_dev/seoul-dev). prod 전환은 .env.commerce 또는 compose env 로.
DBT_TARGET = os.getenv("COMMERCE_DBT_TARGET") or os.getenv("DBT_TARGET", "dev")
SILVER_SELECT = "silver_license_history silver_license_current"

_DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "retry_delay": pendulum.duration(minutes=5)}


def _dbt_command(args: str) -> str:
    """고정 인자만 조립(외부 입력 없음). 값은 전부 shlex.quote — 셸 주입 여지 차단."""
    return (
        "set -euo pipefail\n"
        f"cd {shlex.quote(DBT_PROJECT_DIR)}\n"
        f"DBT_PROFILES_DIR={shlex.quote(DBT_PROJECT_DIR)} "
        f"{shlex.quote(DBT_BIN)} --no-use-colors --target {shlex.quote(DBT_TARGET)} {args}"
    )


@dag(dag_id="commerce_localdata_transform", schedule="0 5 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["seoul", "commerce", "silver", "dbt"], doc_md=__doc__)
def commerce_localdata_transform():
    run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=_dbt_command(f"run --select {SILVER_SELECT}"),
    )
    test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=_dbt_command(f"test --select {SILVER_SELECT}"),
    )
    run_silver >> test_silver


commerce_localdata_transform()
