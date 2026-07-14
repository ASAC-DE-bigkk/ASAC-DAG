"""commerce_load_gold — gold **Iceberg** 적재: 코어(dbt/Cosmos) + 카탈로그 구동 detail.

silver(05:00) 이후 06:00 KST. 서빙 레이어 정책(docs/PROJECT.md §4 — 2026-07-14 개편):
서빙 Postgres 폐기, gold 는 bronze/silver 와 동일한 **Iceberg 카탈로그**에 만들고 선별 소수
테이블만 D1(SQLite)로 export(예정). 기존 RDB 관계형 모델링은 Iceberg 로 승계한다:

- **코어(공통 컬럼, dbt)**: gold_license_entity(현재) · gold_license_entity_history(이력) ·
  gold_license_dong_summary(행정동 집계, D1 1순위) — Cosmos 가 모델당 run+test 로 실행.
- **detail(API 별 상이 컬럼, 카탈로그 구동)**: bronze record_json 실측 → 엄격 클러스터 규칙
  (Jaccard>=0.7 ∧ 멤버>=3 ∧ 공유>=8) → `gold_catalog`(Iceberg) 갱신 → detail 테이블
  (`commerce_<domain>_detail`) DDL ensure + **멤버별 증분 INSERT INTO SELECT**(Trino 단독,
  행이 Python 을 거치지 않음). 워터마크 = detail 테이블 자체의 멤버별 max(collected_at).

  build_catalog ──> dbt_gold(Cosmos) ──> load_details ──> report_gold

식별자: 자연키 (dataset, opnsfteamcode, mgtno) — bigserial/entity_seq 없음(§4.2 D1 특성).
리니지: 코어는 Cosmos 네이티브 OL, detail 은 물리명 stitch(같은 웨어하우스). 뷰 320 미승계.
객체 명세: dbt/domains/commerce/docs/DB/gold/(재편 중) · 카탈로그 규칙: include/gold/.
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

# dbt 실행 계약 — commerce_load_silver 와 동일(venv dbt · SUBPROCESS · 기존 profiles 재사용).
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


@dag(dag_id="commerce_load_gold", schedule="0 6 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["seoul", "commerce", "gold", "iceberg"], doc_md=__doc__)
def commerce_load_gold():
    @task
    def build_catalog() -> dict:
        """실측(bronze record_json) → 카탈로그 규칙 → gold_catalog(Iceberg) 갱신(+드리프트 감지)."""
        from commerce_core import registry
        from gold import catalog_rules, loader, measure

        fields = measure.measure_fields()
        meta = {d.short: {"fmt": d.fmt} for d in registry.enabled_for_schedule("daily")}
        cat = catalog_rules.build_catalog(fields, meta)

        _prev_details, prev = loader.read_catalog()
        if prev and prev != cat["version"]:
            log.warning("카탈로그 드리프트: %s → %s (신규 API/필드 반영)", prev, cat["version"])
        loader.upsert_catalog(cat["details"], cat["version"])
        summary = {"version": cat["version"], "datasets": len(fields),
                   "clusters": sum(1 for d in cat["details"] if d["kind"] == "detail_cluster"),
                   "singles": sum(1 for d in cat["details"] if d["kind"] == "detail_single"),
                   "drift": bool(prev and prev != cat["version"])}
        log.info("catalog: %s", summary)
        return summary

    @task
    def load_details() -> dict:
        """gold_catalog 를 읽어 detail 테이블 DDL ensure + 멤버별 증분 적재(Trino 단독)."""
        from gold import loader

        details, version = loader.read_catalog()
        if not details:
            log.info("카탈로그 비어 있음 — build_catalog 선행 필요")
            return {"loaded": {}}
        loaded = loader.run_load_details(details)
        return {"loaded": loaded, "catalog_version": version}

    @task(trigger_rule="all_done")
    def report_gold(**ctx) -> dict:
        """실행시간 + 카탈로그 + 코어 현황 + detail 적재행 Discord 리포트(#218). 실패해도 보고."""
        from datetime import datetime, timezone

        from gold import report

        ti = ctx["ti"]
        catalog = ti.xcom_pull(task_ids="build_catalog")
        load = ti.xcom_pull(task_ids="load_details")
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
        operator_args={"install_deps": False},
    )

    build_catalog() >> dbt_gold >> load_details() >> report_gold()


commerce_load_gold()
