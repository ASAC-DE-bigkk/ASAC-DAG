"""commerce_load_gold_refresh — 서빙 DB(Postgres) **강제 전량 재적재**(트리거 전용).

정기 `commerce_load_gold`(06:00 · 마커 증분/조기 스킵)와 별개로, **마커와 무관하게** silver 의
현재 데이터를 서빙 DB 에 새로 적재하는 온디맨드 DAG. 용도:

- **새 서빙 DB 부트스트랩**: 다른 팀원이 자기 서빙 Postgres 를 띄우고 현재 gold 데이터를 즉시
  받고 싶을 때(정기 스케줄을 기다리지 않고 트리거).
- **강제 새로고침**: 마커/워터마크 상태와 무관하게 silver → 서빙 DB 를 전량 재적재하고 싶을 때.

정기 DAG 와의 차이:
- `schedule=None` — **트리거로만** 실행(스케줄 없음, catchup 없음).
- `loader.run_load(force_full=True)` — 조기 스킵·마커 창을 모두 건너뛰고 전 객체를 전량 삭제 후
  재적재(청크 경로, OOM 바운드). 완료 후 마커는 최신 hi 로 전진(정기 DAG 와 상태 일관).
- **entity_seq 매핑(`commerce_entity_key`)은 보존** — 재적재/초기화에서 절대 삭제하지 않는다
  (같은 업소가 같은 번호를 유지, refactor-guide §4). 별도 DB 신규 테이블 생성 없음.

  build_catalog ──> load_gold_full ──> build_code_values ──> report_gold

객체 명세: dbt/domains/commerce/docs/DB/gold/ · 카탈로그 규칙: include/gold/ · 배경: change-log #64.
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
from airflow.sdk import Asset  # noqa: E402

log = logging.getLogger(__name__)

_DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "retry_delay": pendulum.duration(minutes=5)}

# OpenLineage inlets/outlets — 정기 gold DAG 와 동일 표기(silver→gold 엣지 방출). 배경: commerce_load_gold.
_TRINO_AUTH = f"{os.getenv('TRINO_HOST', 'trino')}:{os.getenv('TRINO_PORT', '8080')}"
_CATALOG = (os.getenv("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
            if os.getenv("DBT_TARGET", "dev").strip().lower() == "dev"
            else os.getenv("TRINO_ICEBERG_CATALOG", "iceberg"))
_SCHEMA = os.getenv("COMMERCE_SCHEMA", "commerce")
_PG_AUTH = f"{os.getenv('COMMERCE_GOLD_PG_HOST', 'serving-postgres')}:{os.getenv('COMMERCE_GOLD_PG_PORT', '5432')}"
_PG_DB = os.getenv("COMMERCE_GOLD_PG_DB", "serving")
_GOLD_INLETS = [
    Asset(f"trino://{_TRINO_AUTH}/{_CATALOG}/{_SCHEMA}/silver_license_history"),
    Asset(f"trino://{_TRINO_AUTH}/{_CATALOG}/{_SCHEMA}/silver_license_current"),
]
_GOLD_OUTLETS = [
    Asset(f"postgres://{_PG_AUTH}/{_PG_DB}/public/commerce_business_entity"),
    Asset(f"postgres://{_PG_AUTH}/{_PG_DB}/public/commerce_business_entity_history"),
]


@dag(dag_id="commerce_load_gold_refresh", schedule=None,
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["seoul", "commerce", "gold", "serving", "manual", "refresh"], doc_md=__doc__)
def commerce_load_gold_refresh():
    @task
    def build_catalog() -> dict:
        """실측(bronze record_json) → 카탈로그 규칙 → commerce_catalog 갱신(+드리프트 감지).

        정기 gold 의 build_catalog 와 동일 — 재적재 전 카탈로그/코어·마커 테이블 존재 보장(멱등)."""
        from commerce_core import registry
        from gold import catalog_rules, ddl, loader, measure, pg

        fields = measure.measure_fields()
        meta = {d.short: {"fmt": d.fmt} for d in registry.enabled_for_schedule("daily")}
        cat = catalog_rules.build_catalog(fields, meta)

        pgconn = pg.connect()
        try:
            with pgconn.cursor() as cur:
                for _n, sql in ddl.create_core_sql():
                    cur.execute(sql)  # security: allow-sql - 코드 상수 DDL
            pgconn.commit()
            with pgconn.cursor() as cur:
                cur.execute("select distinct catalog_version from commerce_catalog limit 1")
                row = cur.fetchone()
            prev = row[0] if row else None
            if prev and prev != cat["version"]:
                log.warning("카탈로그 드리프트: %s → %s (신규 API/필드 반영)", prev, cat["version"])
            loader.upsert_catalog(pgconn, cat["details"], cat["version"])
        finally:
            pgconn.close()
        summary = {"version": cat["version"], "datasets": len(fields),
                   "clusters": sum(1 for d in cat["details"] if d["kind"] == "detail_cluster"),
                   "singles": sum(1 for d in cat["details"] if d["kind"] == "detail_single"),
                   "drift": bool(prev and prev != cat["version"])}
        log.info("catalog(refresh): %s", summary)
        return summary

    @task(inlets=_GOLD_INLETS, outlets=_GOLD_OUTLETS)
    def load_gold_full() -> dict:
        """카탈로그를 읽어 DDL ensure → **마커 무관 전량 재적재**(force_full) → DONE.

        정기 DAG 의 load_gold 와 달리 조기 스킵·마커 창을 건너뛰고 전 객체를 삭제 후 재적재한다
        (loader.run_load(force_full=True)). entity_seq 매핑(commerce_entity_key)은 보존."""
        from gold import loader, pg

        pgconn = pg.connect()
        try:
            with pgconn.cursor() as cur:
                cur.execute("select object, kind, members, payload_columns from commerce_catalog "
                            "where kind in ('detail_cluster', 'detail_single') order by object")
                rows = cur.fetchall()
        finally:
            pgconn.close()
        if not rows:
            log.info("카탈로그 비어 있음 — build_catalog 선행 필요")
            return {"loaded": {}}
        details = [{"object": r[0], "kind": r[1], "members": (r[2] or "").split(),
                    "payload": (r[3] or "").split()} for r in rows]
        dataset_map = {}
        for d in details:
            etype = d["object"].removeprefix("commerce_").removesuffix("_detail")
            for m in d["members"]:
                dataset_map[m] = {"entity_type": etype, "detail_table": d["object"]}
        return loader.run_load(details, dataset_map, force_full=True)

    @task
    def build_code_values() -> dict:
        """detail 저카디널리티 값 집계(commerce_code_value, 멱등) — load 이후 실행."""
        from gold import code_values, pg

        pgconn = pg.connect()
        try:
            return code_values.build_code_values(pgconn)
        finally:
            pgconn.close()

    @task(trigger_rule="all_done")
    def report_gold(**ctx) -> dict:
        """실행시간 + 카탈로그 + 객체별 적재행 Discord 리포트(#218). 실패해도 반드시 보고."""
        from datetime import datetime, timezone

        from gold import report

        ti = ctx["ti"]
        catalog = ti.xcom_pull(task_ids="build_catalog")
        load = ti.xcom_pull(task_ids="load_gold_full")
        dr = ctx.get("dag_run")
        elapsed = ((datetime.now(timezone.utc) - dr.start_date).total_seconds()
                   if dr and getattr(dr, "start_date", None) else None)
        return report.send_gold_report(catalog=catalog, load=load, elapsed_seconds=elapsed)

    build_catalog() >> load_gold_full() >> build_code_values() >> report_gold()


commerce_load_gold_refresh()
