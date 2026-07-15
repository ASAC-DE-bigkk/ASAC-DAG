"""commerce_load_gold — gold **집계·인사이트 전용**(레이어 재분류 #70, PROJECT.md §4).

silver(05:00 — 원형 정리본 entity/detail 까지 적재) 이후 06:00 KST. 레이어 정의(사용자 확정):
silver = 결측 처리·표준화·중복 제거·**테이블 단위 정리·JOIN 모델링(원형)**, gold = **업무 목적별
집계·지표·인사이트만**. 원형 파이프라인(entity·entity_history·detail)은 commerce_load_silver 에
편승했고, 이 DAG 는 silver 원형을 입력으로 집계 테이블만 빌드·누적한다.

  dbt_gold(Cosmos — gold_license_dong_summary run+test) ──> report_gold

- 집계 확장 시 models/gold/ 에 모델 추가 + GOLD_SELECT 에 등록(D1 export 후보는 PROJECT.md §4.3).
- 유지보수(신설 테이블 optimize/expire/orphan)는 silver DAG 의 maintain_gold_tables 가 담당.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from commerce_core.env import load_commerce_env  # noqa: E402

load_commerce_env()

from security import install_security  # noqa: E402

install_security()

import os  # noqa: E402

import pendulum  # noqa: E402
from airflow.decorators import dag, task  # noqa: E402
from cosmos import (  # noqa: E402
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode, TestBehavior  # noqa: E402

log = logging.getLogger(__name__)

_DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "retry_delay": pendulum.duration(minutes=5)}

DBT_PROJECT_DIR = os.getenv("COMMERCE_DBT_PROJECT_DIR", "/opt/airflow/dbt/domains/commerce")
DBT_BIN = os.getenv("DBT_BIN", "/home/airflow/dbt-venv/bin/dbt")
DBT_TARGET = os.getenv("COMMERCE_DBT_TARGET") or os.getenv("DBT_TARGET", "dev")
# gold = 집계·지표만(#70). 원형(entity/detail)은 silver DAG 소속.
GOLD_SELECT = [
    "gold_license_dong_summary",
    "gold_license_flow_daily",
    "gold_license_flow_monthly",
    "gold_license_flow_yearly",
    "gold_license_status_duration",
    "gold_env_facility_operation",
    "gold_license_lifespan",
    "gold_license_cohort_survival",
    "gold_license_seasonality",
    "gold_license_stock_age_band",
    "gold_license_gu_specialization",
    "gold_license_churn_yearly",
    "gold_license_data_quality",
    "gold_license_status_transition",
    "gold_license_dong_category_matrix",
    "gold_license_change_activity",
    "gold_detail_area_profile",
    "gold_license_multi_site",
    "gold_license_geo_grid",
    "gold_license_address_succession",
    "gold_license_phone_succession",
    "gold_detail_uptae_mix",
]

_profile_config = ProfileConfig(
    profile_name="commerce",
    target_name=DBT_TARGET,
    profiles_yml_filepath=Path(DBT_PROJECT_DIR) / "profiles.yml",
)
_project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_DIR)
_execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path=DBT_BIN,
)
_render_config = RenderConfig(
    select=GOLD_SELECT,
    test_behavior=TestBehavior.AFTER_EACH,
    load_method=LoadMode.DBT_LS,
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path=DBT_BIN,
)


@dag(dag_id="commerce_load_gold", schedule="0 6 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["seoul", "commerce", "gold", "iceberg", "aggregate"], doc_md=__doc__)
def commerce_load_gold():
    @task(trigger_rule="all_done")
    def report_gold(**ctx) -> dict:
        """집계 빌드 결과 Discord 리포트 — 집계 테이블 현황(행수, -1=미빌드/실패). 실패해도 보고."""
        from datetime import datetime, timezone

        from gold import report

        dr = ctx.get("dag_run")
        elapsed = ((datetime.now(timezone.utc) - dr.start_date).total_seconds()
                   if dr and getattr(dr, "start_date", None) else None)
        return report.send_gold_report(elapsed_seconds=elapsed)

    dbt_gold = DbtTaskGroup(
        group_id="dbt_gold",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        render_config=_render_config,
        operator_args={"install_deps": False},
    )

    dbt_gold >> report_gold()


commerce_load_gold()
