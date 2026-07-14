"""commerce_load_gold_refresh — gold(Iceberg) **강제 전량 재구축**(트리거 전용).

정기 `commerce_load_gold`(06:00 · 증분)와 별개로, 워터마크/기존 상태와 무관하게 gold Iceberg
계층 전체를 새로 구축하는 온디맨드 DAG. 용도: gold 스키마/카탈로그 개편 반영 · 강제 새로고침 ·
(후속) D1 export 전 정합 재구축. 서빙 레이어 정책: docs/PROJECT.md §4.

정기 DAG 와의 차이:
- `schedule=None` — **트리거로만** 실행(스케줄 없음, catchup 없음).
- 코어(dbt): Cosmos `full_refresh=True` — 3개 모델 전량 재빌드(incremental 포함).
- detail: `run_load_details(force_full=True)` — 각 detail DELETE 후 전량 재적재
  (INSERT INTO SELECT 문장 원자성 그대로 — 부분 상태 없음).

  build_catalog ──> dbt_gold(full-refresh) ──> load_details(full) ──> report_gold
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
GOLD_SELECT = ["gold_license_entity", "gold_license_entity_history", "gold_license_dong_summary"]

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


@dag(dag_id="commerce_load_gold_refresh", schedule=None,
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["seoul", "commerce", "gold", "iceberg", "manual", "refresh"], doc_md=__doc__)
def commerce_load_gold_refresh():
    @task
    def build_catalog() -> dict:
        """실측 → 카탈로그 규칙 → gold_catalog(Iceberg) 갱신 — 정기 DAG 와 동일(멱등)."""
        from commerce_core import registry
        from gold import catalog_rules, loader, measure

        fields = measure.measure_fields()
        meta = {d.short: {"fmt": d.fmt} for d in registry.enabled_for_schedule("daily")}
        cat = catalog_rules.build_catalog(fields, meta)
        _prev_details, prev = loader.read_catalog()
        if prev and prev != cat["version"]:
            log.warning("카탈로그 드리프트: %s → %s", prev, cat["version"])
        loader.upsert_catalog(cat["details"], cat["version"])
        return {"version": cat["version"], "datasets": len(fields),
                "clusters": sum(1 for d in cat["details"] if d["kind"] == "detail_cluster"),
                "singles": sum(1 for d in cat["details"] if d["kind"] == "detail_single"),
                "drift": bool(prev and prev != cat["version"])}

    @task
    def load_details_full() -> dict:
        """detail 전량 재적재 — 각 테이블 DELETE 후 재구축(force_full)."""
        from gold import loader

        details, version = loader.read_catalog()
        if not details:
            log.info("카탈로그 비어 있음 — build_catalog 선행 필요")
            return {"loaded": {}}
        loaded = loader.run_load_details(details, force_full=True)
        return {"loaded": loaded, "catalog_version": version, "mode": "full_reload"}

    @task(trigger_rule="all_done")
    def report_gold(**ctx) -> dict:
        """전량 재구축 결과 Discord 리포트 — 실패해도 반드시 보고."""
        from datetime import datetime, timezone

        from gold import report

        ti = ctx["ti"]
        catalog = ti.xcom_pull(task_ids="build_catalog")
        load = ti.xcom_pull(task_ids="load_details_full")
        dr = ctx.get("dag_run")
        elapsed = ((datetime.now(timezone.utc) - dr.start_date).total_seconds()
                   if dr and getattr(dr, "start_date", None) else None)
        return report.send_gold_report(catalog=catalog, load=load, elapsed_seconds=elapsed)

    dbt_gold = DbtTaskGroup(
        group_id="dbt_gold",
        project_config=_project_config,
        profile_config=_profile_config,
        execution_config=_execution_config,
        render_config=_render_config,
        operator_args={"install_deps": False, "full_refresh": True},
    )

    build_catalog() >> dbt_gold >> load_details_full() >> report_gold()


commerce_load_gold_refresh()
