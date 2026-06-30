"""seoul_commerce — 서울 인허가(commerce) **원본 수집(bronze) 라인** (DB·외부 매니페스트 없음).

이 DAG 라인은 bronze(원본 수집)만 담당한다. silver 가공은 여기서 분리되어 있고
가공 로직은 include/silver/ 에 그대로 보존되어 있다(별도 오케스트레이션 없음).

DAG 2개(공통 태스크 공유):
  - **seoul_commerce_daily**     : 수집 대상 전체를 매 실행 수집(@daily).
  - **seoul_commerce_recollect** : 최근 run 에서 미완료(incomplete/미시도)인 API만 재수집(주기적).
                                   재수집 대상이 없으면 수집 진행 안 함(빈 매핑 → run 폴더 미생성).

  resolve_observed_date ─┐
  make_bronze_run_id ────┤
  check_api_key (gate) ──┤
  (plan_all|find_incomplete) ─┴─> ingest_one.expand ──> finalize_run ─> (_RUN 마커/metrics)

- **API 단위 진행 가시성**: `ingest_one` 은 Dynamic Task Mapping + 각 매핑 인스턴스를
  **API(short) 이름으로 라벨링**(`map_index_template`) → Airflow Grid/Graph 에서
  성공/실패/대기 job 을 API 별로 확인. 결과 요약은 run 폴더의 마커(`_markers/*`)에도 남는다.
- bronze: 1회 page_size(≤1000)씩 끝까지 순회 + 완전성 점검. **DAG 실행 1회 = run_id 폴더 1개**,
  API당 1파일(NDJSON) + API별 마커(completed|incomplete). 상태는 그 마커가 전부.
- 데이터셋 간 실패 격리: ingest 가 비인증 오류를 status=failed(=incomplete 마커)로 반환.

코드: dags/domains/commerce/include/ · 레지스트리: config/dataset_registry.yaml
저장 경로/마커: docs/pipeline/bronze/caveats.md · docs/architecture/storage.md
재수집/알림: docs/operations/recollect-and-alerts.md
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 자립(portable): 자기 카테고리의 include 를 import 경로에 올린다.
sys.path.insert(0, str(Path(__file__).resolve().parent / "include"))

# 번들 자립용 env 적재(프로세스 env 우선). 자세히: docs/configuration/configuration.md
from common.env import load_commerce_env  # noqa: E402

load_commerce_env()

# 보안: env 적재 직후 로그 시크릿 마스킹 설치(이후 모든 commerce 로그/예외에서 키 자동 마스킹).
# 종합검증/처리 로직: docs/security/security.md
from security import assert_iso_date, install_log_redaction  # noqa: E402

install_log_redaction()

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

try:  # Airflow 3.x Task SDK
    from airflow.sdk import get_current_context
except ImportError:  # Airflow 2.x
    from airflow.operators.python import get_current_context

from bronze import bronze_tasks, markers
from common import paths, registry
from common.settings import get_settings
from common.storage import get_storage

log = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))

# 현재 인허가(LOCALDATA) 대상은 전부 daily(전부 enabled_for_schedule("daily")).
# monthly/irregular 는 인허가 외(위치정보/현황)만 있어 비활성 — config/non_license_datasets.yaml 로 격리.
COLLECTIBLE = registry.enabled_for_schedule("daily")
COLLECTIBLE_SHORTS = [d.short for d in COLLECTIBLE]
PENDING = registry.pending_for_schedule("daily")

_DEFAULT_ARGS = {"owner": "data-eng", "retries": 2, "retry_delay": pendulum.duration(minutes=3)}
_PARAMS = {"observed_date": Param(default="", type="string",
           description="논리 수집일 override (YYYY-MM-DD, silver 파티션). 비우면 run 의 ds.")}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_ds(ctx) -> str:
    """run 의 논리 수집일(YYYY-MM-DD, silver 파티션).

    Airflow 3.x 는 수동 Trigger 시 logical_date 가 None 일 수 있고, 그때는 ds/ts/
    data_interval_* 매크로가 컨텍스트에 주입되지 않아 ctx["ds"] 가 KeyError 가 난다.
    → ds → data_interval_start → logical_date → dag_run → 현재(KST) 순으로 폴백.
    """
    ds = ctx.get("ds")
    if ds:
        return ds
    dt = ctx.get("data_interval_start") or ctx.get("logical_date")
    if dt is None:
        dag_run = ctx.get("dag_run")
        dt = getattr(dag_run, "logical_date", None) or getattr(dag_run, "run_after", None)
    if dt is not None:
        dt = dt.astimezone(KST) if dt.tzinfo else dt
        return dt.strftime("%Y-%m-%d")
    return datetime.now(KST).strftime("%Y-%m-%d")


# ── 공통 태스크(두 DAG 공유) ──────────────────────────────────────────────────
@task
def resolve_observed_date(**ctx) -> str:
    override = (ctx["params"].get("observed_date") or "").strip()
    if override:
        assert_iso_date(override)   # 경로 주입 방지 + 계약(YYYY-MM-DD). 잘못된 입력은 즉시 실패.
    observed_date = override or _run_ds(ctx)
    log.info("observed_date=%s (collectible=%d, pending=%d)",
             observed_date, len(COLLECTIBLE_SHORTS), len(PENDING))
    for d in PENDING:
        log.warning("SKIP %s (%s): service_name 미설정", d.short, d.oa_id)
    return observed_date


@task
def make_bronze_run_id() -> str:
    """이 실행의 run_id 폴더명(실행시각, KST, 밀리초). 한 번 계산해 모든 ingest 가 공유."""
    now = datetime.now(KST)
    run_id = now.strftime("%Y-%m-%d_%H%M%S_") + f"{now.microsecond // 1000:03d}"
    log.info("bronze run_id=%s", run_id)
    return run_id


@task
def check_api_key() -> dict:
    return bronze_tasks.verify_api_key()


@task
def plan_all_targets() -> list[str]:
    """daily: 수집 대상 전체(중복 제거는 silver)."""
    log.info("plan(daily): 수집 대상 %d종", len(COLLECTIBLE_SHORTS))
    return list(COLLECTIBLE_SHORTS)


@task
def find_incomplete_targets() -> list[str]:
    """recollect: 최근 run 에서 completed 가 아닌(미완료/미시도) API만. 없으면 빈 리스트 → 수집 안 함."""
    settings = get_settings()
    storage = get_storage()
    latest = markers.latest_run_id(storage, settings.storage_prefix)
    targets = markers.incomplete_targets(storage, settings.storage_prefix, latest, COLLECTIBLE_SHORTS)
    log.info("recollect: latest_run=%s, 재수집 대상=%d %s", latest, len(targets), targets)
    return targets


@task(map_index_template="{{ short }}")          # Airflow Grid/Graph 에 API(short)로 라벨
def ingest_one(short: str, observed_date: str, bronze_run_id: str, **ctx) -> dict:
    get_current_context()["short"] = short        # 매핑 인스턴스 라벨 = API 이름
    return bronze_tasks.fetch_dataset_to_bronze(
        registry.by_short(short), observed_date, ctx["run_id"], bronze_run_id)


@task(trigger_rule=TriggerRule.ALL_DONE)
def finalize_run(bronze_run_id: str, observed_date: str, summaries: list[dict]) -> dict:
    summaries = [s for s in (summaries or []) if s]
    if not summaries:   # 수집 대상 없음(recollect) → run 마커 생략 = run 폴더 미생성
        log.info("수집 대상 없음 — run 마커 생략(수집 진행 안 함).")
        return {"bronze_run_id": bronze_run_id, "observed_date": observed_date,
                "datasets_total": 0, "skipped": True}
    settings = get_settings()
    storage = get_storage()
    ok = [s for s in summaries if s.get("status") == "ok"]
    incomplete = [s["short"] for s in summaries if s.get("status") != "ok"]
    run_status = (paths.MARKER_COMPLETED if not incomplete else paths.MARKER_INCOMPLETE)
    metrics = {"bronze_run_id": bronze_run_id, "observed_date": observed_date,
               "run_status": run_status, "datasets_total": len(summaries),
               "datasets_ok": len(ok), "datasets_incomplete": len(incomplete),
               "incomplete_shorts": incomplete,
               "pages": sum(s.get("pages_written", 0) for s in summaries),
               "rows": sum(s.get("rows_total", 0) for s in summaries),
               "finalized_at": _utcnow_iso()}
    storage.write_json(paths.bronze_run_marker_key(
        prefix=settings.storage_prefix, run_id=bronze_run_id, status=run_status), metrics)
    log.info("run 마커(_RUN.%s): %s", run_status, metrics)
    if incomplete:
        log.warning("미완료(다음 실행/recollect 재수집): %s", incomplete)
    return metrics


def _wire(targets):
    """공통 흐름(bronze 전용): targets(list[str]) → ingest → finalize."""
    observed_date = resolve_observed_date()
    bronze_run_id = make_bronze_run_id()
    gate = check_api_key()
    bronze = ingest_one.partial(
        observed_date=observed_date, bronze_run_id=bronze_run_id).expand(short=targets)
    gate >> bronze
    finalize_run(bronze_run_id, observed_date, bronze)


@dag(dag_id="seoul_commerce_daily", schedule="@daily",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS, tags=["seoul", "commerce", "daily"],
     doc_md=__doc__, params=_PARAMS)
def seoul_commerce_daily():
    _wire(plan_all_targets())


@dag(dag_id="seoul_commerce_recollect", schedule="0 */6 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS, tags=["seoul", "commerce", "recollect"],
     doc_md=__doc__, params=_PARAMS)
def seoul_commerce_recollect():
    # 최근 run 의 미완료 API만 재수집. 대상 없으면 빈 매핑 → 수집 진행 안 함.
    _wire(find_incomplete_targets())


seoul_commerce_daily()
seoul_commerce_recollect()
