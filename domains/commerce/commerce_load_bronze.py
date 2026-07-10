"""commerce_load_bronze — raw 증분(R2) → **Iceberg bronze 적재** 라인 (수집과 분리).

raw 수집(commerce_collect_raw/recollect)과 **완전히 분리**된 적재 전용 DAG. raw 는 R2 오브젝트
랜딩(불변), 이 DAG 가 그걸 읽어 Trino/PyIceberg 로 Iceberg 원본층 테이블에 적재한다.

정책(사용자 확정 · docs/pipeline/medallion-implementation-plan.md):
- 상태는 **파일 기반(RDB 없음)**, raw 와 격리된 공간(`{COMMERCE_BRONZE_STATE_LAYER}`).
  워터마크(데이터셋별 마지막 적재 run) + pending(complete 없어 미적재 일자, 현재-2일 재감시·3일 폐기).
- 워터마크 없음 → 처음부터 전체 재적재(**PyIceberg**, 대용량 커밋 1회). 있음 → **Trino** 증분.
  한 번에 모든 날짜를 적재하지 않고 실행당 최대 날짜 수로 바운드(catch-up 은 다음 실행이 이어감).
- Iceberg/parquet + 상태파일 삭제 후 재적재 가능(raw 불변). 적재 실패 run 은 워터마크를 전진시키지
  않아 다음 실행이 재시도.

  resolve_plan ─> ensure_warehouse ─> load_one.expand ─> finalize(워터마크/pending/manifest)
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "include"))
# 공통 패키지(dags/common) — commerce_core.storage 가 common.storage 를 쓴다(#109).
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from commerce_core.env import load_commerce_env  # noqa: E402

load_commerce_env()

from security import install_security  # noqa: E402

install_security()

import os  # noqa: E402

import pendulum  # noqa: E402
from airflow.decorators import dag, task  # noqa: E402
from airflow.models.param import Param  # noqa: E402
from airflow.utils.trigger_rule import TriggerRule  # noqa: E402

from bronze import load_plan, load_state, warehouse  # noqa: E402
from commerce_core import registry  # noqa: E402
from commerce_core.settings import get_settings  # noqa: E402
from commerce_core.storage import get_storage  # noqa: E402

log = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))

COLLECTIBLE_SHORTS = [d.short for d in registry.enabled_for_schedule("daily")]
_DEFAULT_LOOKBACK_DAYS = int(os.getenv("COMMERCE_LOAD_LOOKBACK_DAYS", "3") or "3")
_DEFAULT_ARGS = {"owner": "data-eng", "retries": 2, "retry_delay": pendulum.duration(minutes=3)}
_PARAMS = {"lookback_days": Param(default=_DEFAULT_LOOKBACK_DAYS, type="integer",
           description="최근 N일 창(today-N~today)의 미적재 run 만 적재(0=무제한). "
                       "ENV: COMMERCE_LOAD_LOOKBACK_DAYS. #223 이전엔 '가장 이른 N날짜'였음(신규셋 배제 버그).")}


@task
def resolve_plan(**ctx) -> dict:
    """워터마크/pending + raw run 목록으로 적재 계획 산출(무엇을 어느 엔진으로)."""
    storage = get_storage()
    prefix = get_settings().storage_prefix
    today = datetime.now(KST).strftime("%Y-%m-%d")
    lookback_days = int(ctx["params"].get("lookback_days") or 0)
    plan = load_plan.resolve_load_plan(
        storage, prefix=prefix, datasets=list(COLLECTIBLE_SHORTS),
        watermark=load_state.read_watermark(storage, prefix),
        pending=load_state.read_pending(storage, prefix),
        today=today, lookback_days=lookback_days or None)
    log.info("계획: units=%d(lookback=%s일), pending 유지=%d 폐기=%d, no_watermark=%s",
             len(plan["units"]), lookback_days or "∞", len(plan["pending_keep"]),
             len(plan["pending_expired"]), plan["no_watermark"])
    return plan


@task
def plan_units(plan: dict) -> list[dict]:
    """계획에서 적재 단위 리스트만 추출 — expand 매핑 입력(XCom 커스텀 키 매핑 불가 회피)."""
    return list(plan.get("units", []))


@task
def resolve_load_date() -> str:
    """bronze load_date(파티션) = 적재 실행일(KST)."""
    return datetime.now(KST).strftime("%Y-%m-%d")


@task
def ensure_warehouse(plan: dict) -> bool:
    """적재할 units 이 있을 때만 Iceberg 스키마/테이블 IF NOT EXISTS(Trino DDL, 경량)."""
    if not plan.get("units"):
        log.info("적재 대상 없음 — warehouse 준비 생략.")
        return False
    warehouse.ensure_schema_and_tables()
    return True


@task(map_index_template="{{ short }}", max_active_tis_per_dagrun=1)
def load_one(unit: dict, load_date: str, **ctx) -> dict:
    """적재 단위 1건(엔진 분기). raw 를 재읽어 멱등 적재 → 실패 시 태스크 재시도.

    **직렬화**(max_active_tis_per_dagrun=1): 152종이 동일 Iceberg 테이블에 병렬 커밋하면
    낙관적 동시성 충돌(CommitFailedException)로 대부분 실패한다. 한 번에 한 단위만 적재한다.
    """
    try:
        from airflow.sdk import get_current_context
    except ImportError:
        from airflow.operators.python import get_current_context
    get_current_context()["short"] = unit.get("short", "?")
    return warehouse.load_unit(get_storage(), unit, load_date=load_date)


@task(trigger_rule=TriggerRule.ALL_DONE)
def iceberg_maintenance(_loaded) -> list[dict]:
    """적재 후 Iceberg 유지보수(optimize/expire/orphan) — **매일**(#226). bronze 테이블 대상.

    (테이블,op) 단위 멱등 — 중단 후 재실행해도 안전(재개 표준). 결과는 finalize 가 DAG 리포트의
    '🧹 유지보수' 섹션으로 흡수. 자원(소요시간/CPU/peak RAM)은 Trino 쿼리 stats 로 캡처.
    ALL_DONE — 적재 일부 실패해도 유지보수는 수행.
    """
    from bronze import maintenance

    days = int(os.getenv("COMMERCE_ICEBERG_EXPIRE_DAYS", "7") or "7")
    # bronze 가 쓰는 두 테이블 모두 유지보수 — manifest 는 delete-then-insert 로 커밋이 계속 쌓이므로
    # 유지보수에서 빠지면 스냅샷/메타데이터가 무한 축적돼 R2 Data Catalog 메타 불일치를 유발(실측 원인).
    return maintenance.run_table_maintenance(
        tables=("bronze_localdata_license", "bronze_collection_run_manifest"), expire_days=days)


@task(trigger_rule=TriggerRule.ALL_DONE)
def finalize(plan: dict, load_results: list[dict], maint_results: list[dict] | None = None) -> dict:
    """워터마크 전진(적재 성공분까지) · pending 갱신 · manifest 발행 · 리포트(+유지보수 섹션)."""
    storage = get_storage()
    prefix = get_settings().storage_prefix
    results = [r for r in (load_results or []) if r]
    succeeded = {(r["short"], r["run_id"]) for r in results}
    planned = {(u["short"], u["run_id"]) for u in plan.get("units", [])}
    failed = planned - succeeded

    current = load_state.read_watermark(storage, prefix)
    final_wm = load_plan.commit_watermark(
        current, resolved_runs=plan.get("resolved_runs", {}), failed_run_ids=failed)
    load_state.write_watermark(storage, prefix, final_wm)
    load_state.write_pending(storage, prefix, plan.get("pending_keep", []))

    manifest = warehouse.write_manifest(results) if results else {"published": 0, "datasets": 0}
    metrics = {"loaded_units": len(results), "failed_units": len(failed),
               "published": manifest.get("published", 0),
               "pending": len(plan.get("pending_keep", [])),
               "expired": len(plan.get("pending_expired", [])),
               "finalized_at": datetime.now(timezone.utc).isoformat()}
    log.info("finalize: %s", metrics)
    if failed:
        log.warning("적재 실패 run(다음 실행 재시도): %s", sorted(failed))
    if plan.get("pending_expired"):
        log.warning("pending 폐기(3일 경과, complete 없음): %s", plan["pending_expired"])

    # DAG 단위 완료 리포트 → Discord(common.discord, #218). API(short)·category(한글) 단위.
    try:
        from commerce_core import run_report   # 지연 임포트

        # 신규 = rows_loaded(증분 파일 실제 적재분), 전체 = rows_expected(=increment_count).
        rr = [{"short": r["short"], "status": "ok" if r.get("is_publishable") else "failed",
               "new": r.get("rows_loaded", 0), "total": r.get("rows_expected", 0),
               "task": "load_one"} for r in results]
        rr += [{"short": sh, "status": "failed", "error": "적재 실패(다음 실행 재시도)",
                "task": "load_one"} for (sh, _run) in failed]
        if rr:
            extra = [run_report.maintenance_section(maint_results)] if maint_results else None
            metrics["report"] = run_report.send_run_report(
                dag_id="commerce_load_bronze", run_id=metrics["finalized_at"],
                observed_date=datetime.now(KST).strftime("%Y-%m-%d"),
                stage="bronze_load", results=rr, extra_sections=extra)
    except Exception as exc:  # noqa: BLE001
        log.warning("run report 스킵(무시): %s", type(exc).__name__)
    return metrics


@dag(dag_id="commerce_load_bronze", schedule="0 4 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS, tags=["seoul", "commerce", "bronze"],
     doc_md=__doc__, params=_PARAMS)
def commerce_load_bronze():
    plan = resolve_plan()
    units = plan_units(plan)
    load_date = resolve_load_date()
    prep = ensure_warehouse(plan)
    loaded = load_one.partial(load_date=load_date).expand(unit=units)
    prep >> loaded
    maint = iceberg_maintenance(loaded)          # 적재 후 유지보수(#226) → 리포트 섹션
    finalize(plan, loaded, maint)


commerce_load_bronze()
