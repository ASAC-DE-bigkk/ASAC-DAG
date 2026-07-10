"""commerce_load_gold — gold 카탈로그 생성 + 카탈로그 기반 서빙 DB(Postgres) 증분 적재.

silver(05:00) 이후 06:00 KST. **카탈로그와 DB 가 함께 존재**한다는 전제(사용자 확정):
task1 이 카탈로그(DB 테이블 `commerce_catalog`)를 만들고, task2 가 그 카탈로그를 읽어
없는 table/view 를 생성(task 초기)한 뒤 증분 데이터를 적재한다.

  build_catalog ──> load_gold

- build_catalog: 적재된 bronze `record_json` 키를 dataset 별 실측(Trino) → 엄격 클러스터 규칙
  (Jaccard>=0.7 ∧ 멤버>=3 ∧ 공유>=8) + 도메인 명명맵 → `commerce_catalog` 갱신(version) +
  버전 변경(드리프트) 감지 로그.
- load_gold: 카탈로그를 읽어 **DDL ensure**(core·dim·detail·view — IF NOT EXISTS) →
  marker(`commerce_load_run_marker`, collected_at 워터마크) → **중단 방어**(워터마크 이후
  잔존행 선삭제, marker 없으면 전량 재적재) → silver 신규 버전만 적재 → **완료 후 DONE**.

이력: silver history(append-only)의 버전행을 그대로 승계 — 값 변경 = 버전 누적(A→B→A 보존).
위치 매핑 키(gu_code·admin_dong_code)와 시간축(updatedt)은 entity/history/view 에 상시 노출.
객체 명세: dbt/domains/commerce/docs/DB/gold/ (tables.md·views.md) · 카탈로그 규칙: include/gold/.
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

import pendulum  # noqa: E402
from airflow.decorators import dag, task  # noqa: E402

log = logging.getLogger(__name__)

_DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "retry_delay": pendulum.duration(minutes=5)}


@dag(dag_id="commerce_load_gold", schedule="0 6 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["seoul", "commerce", "gold", "serving"], doc_md=__doc__)
def commerce_load_gold():
    @task
    def build_catalog() -> dict:
        """실측(bronze record_json) → 카탈로그 규칙 → commerce_catalog 갱신(+드리프트 감지)."""
        from commerce_core import registry
        from gold import catalog_rules, ddl, loader, measure, pg

        fields = measure.measure_fields()
        meta = {d.short: {"fmt": d.fmt} for d in registry.enabled_for_schedule("daily")}
        cat = catalog_rules.build_catalog(fields, meta)

        pgconn = pg.connect()
        try:
            with pgconn.cursor() as cur:            # 카탈로그/마커 테이블 선생성(멱등)
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
        log.info("catalog: %s", summary)
        return summary

    @task
    def load_gold() -> dict:
        """카탈로그(DB)를 읽어 DDL ensure → marker 증분 → 중단 방어 → 적재 → DONE."""
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
        return loader.run_load(details, dataset_map)

    @task(trigger_rule="all_done")
    def report_gold(**ctx) -> dict:
        """실행시간 + 카탈로그 + 객체별 적재행 Discord 리포트(#218). 실패해도 반드시 보고."""
        from datetime import datetime, timezone

        from gold import report

        ti = ctx["ti"]
        catalog = ti.xcom_pull(task_ids="build_catalog")   # 실패 시 None → 실패 리포트
        load = ti.xcom_pull(task_ids="load_gold")
        dr = ctx.get("dag_run")
        elapsed = ((datetime.now(timezone.utc) - dr.start_date).total_seconds()
                   if dr and getattr(dr, "start_date", None) else None)
        return report.send_gold_report(catalog=catalog, load=load, elapsed_seconds=elapsed)

    build_catalog() >> load_gold() >> report_gold()


commerce_load_gold()
