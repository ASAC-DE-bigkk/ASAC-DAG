"""commerce_collect_watchdog — 파이프라인 정상 실행 감지 → Discord (#218).

수집(`commerce_collect_raw`)뿐 아니라 **후속 레이어(`commerce_load_silver`·`commerce_load_gold`)까지**
금일 정상 실행됐는지 점검한다.
- 수집: 오늘 날짜 bronze `_RUN` 마커 존재(R2).
- silver/gold: Airflow DagRun 상태(오늘 KST 성공 run 여부). 이력이 아예 없으면(미활성) 건너뛴다
  (아직 켜지 않은 DAG 오탐 방지).
- 전부 정상: '파이프라인 정상' 성공 알림을 **하루 1회만**(가드 마커). 문제: '미완료 경고'를 매 점검 재발송.

전송은 공통 Discord(best-effort). 가드 마커는 raw 밖 운영 상태 존 —
COMMERCE_WATCHDOG_STATE_LAYER(기본 `state/commerce/watchdog`, prod 는 `ops/control/state/commerce/watchdog`).
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from commerce_core.env import load_commerce_env  # noqa: E402

load_commerce_env()

from security import install_security  # noqa: E402

install_security()

import pendulum  # noqa: E402
from airflow.decorators import dag, task  # noqa: E402

from commerce_core import paths  # noqa: E402
from commerce_core.observability import ops_default_args  # noqa: E402
from commerce_core.settings import get_settings  # noqa: E402
from commerce_core.storage import get_storage  # noqa: E402
from common.discord import COLOR_FAIL, COLOR_OK, send_embed  # noqa: E402
from common.ops import Layer  # noqa: E402

log = logging.getLogger(__name__)
_DEFAULT_ARGS = {"owner": "data-eng", **ops_default_args(Layer.RAW)}
KST = timezone(timedelta(hours=9))
_DOMAIN = "commerce"
_COLLECT_DAG = "commerce_collect_raw"
# 후속 레이어 DAG(순서) — 라벨은 리포트용
_LAYERS = [("commerce_load_silver", "silver 변환"), ("commerce_load_gold", "gold 적재")]
# 가드 마커 레이어(#60 약속② — 가변 상태는 raw 밖 ops 존). 미설정 시 구 위치 폴백(하위호환).
_WATCHDOG_STATE_LAYER = os.getenv("COMMERCE_WATCHDOG_STATE_LAYER", "state/commerce/watchdog")


def _pfx(prefix: str, tail: str) -> str:
    return f"{prefix}/{tail}" if prefix else tail


def _today_collect_ran(storage, prefix: str, today: str) -> bool:
    """오늘 `load_date=<today>` 마커 파티션에 `_RUN.completed|incomplete` 가 있으면 수집 실행됨.

    마커 존(COMMERCE_MARKERS_LAYER) 기준 — 파일명 판정이라 구(run 폴더 `_markers/`)·신 위치 모두 동작.
    """
    date_prefix = paths.markers_date_prefix(prefix=prefix, date=today)
    return any(k.rsplit("/", 1)[-1].startswith("_RUN.") for k in storage.list_keys(date_prefix))


def _layer_state(dag_id: str, today: str) -> str:
    """오늘(KST) 해당 DAG 상태: 'success'|'failed'|'running'|... | 'missing'(이력만) | 'inactive'(이력 없음).

    Airflow 메타 DagRun 을 직접 조회(LocalExecutor — 태스크에서 DB 접근 가능). 조회 실패는 'query_error'.
    """
    try:
        from airflow.models import DagRun
        from airflow.utils.session import create_session
        with create_session() as session:
            runs = (session.query(DagRun)
                    .filter(DagRun.dag_id == dag_id)
                    .order_by(DagRun.start_date.desc()).limit(30).all())
            if not runs:
                return "inactive"
            for r in runs:
                st = getattr(r, "start_date", None)
                if st and st.astimezone(KST).strftime("%Y-%m-%d") == today:
                    return str(getattr(r, "state", "") or "unknown")
            return "missing"
    except Exception as exc:  # noqa: BLE001 — 감지 실패가 태스크를 죽이지 않게
        log.warning("watchdog: %s DagRun 조회 실패(무시): %s", dag_id, type(exc).__name__)
        return "query_error"


@task
def check_pipeline() -> dict:
    """수집 + silver + gold 금일 정상 실행 점검 → 정상(1회)/문제(매회) 알림."""
    storage = get_storage()
    prefix = get_settings().storage_prefix
    now = datetime.now(KST)
    today = now.strftime("%Y-%m-%d")

    collected = _today_collect_ran(storage, prefix, today)
    layers = {dag_id: _layer_state(dag_id, today) for dag_id, _ in _LAYERS}

    problems = []
    if not collected:
        problems.append(f"`{_COLLECT_DAG}`(수집) — 금일 run 미감지")
    for dag_id, label in _LAYERS:
        st = layers[dag_id]
        if st in ("success", "inactive", "query_error"):   # 정상/미활성/조회불가(오탐 방지)는 경고 제외
            continue
        problems.append(f"`{dag_id}`({label}) — 금일 성공 run 없음(state={st})")

    if problems:
        send_embed(
            f"❌ [commerce] 파이프라인 경고 — {today}",
            "금일 정상 실행이 감지되지 않았습니다:\n- " + "\n- ".join(problems)
            + "\n\n실행 순서(수집 → bronze 적재 → silver 변환 → gold 적재)와 각 DAG 상태를 확인하세요."
            " (본 경고는 감지될 때까지 주기적으로 재발송됩니다.)",
            color=COLOR_FAIL, domain=_DOMAIN)
        log.warning("watchdog: %s 파이프라인 미완료 %s", today, problems)
        return {"date": today, "collected": collected, "layers": layers, "ok": False}

    # 전부 정상 → 하루 1회만 성공 알림(중복 방지 가드) — 위치는 env 레이어(#60 약속②)
    guard = _pfx(prefix, f"{_WATCHDOG_STATE_LAYER}/pipeline_{today}.ok")
    if storage.exists(guard):
        log.info("watchdog: %s 파이프라인 정상 — 성공 알림 이미 전송(스킵)", today)
        return {"date": today, "collected": collected, "layers": layers, "ok": True, "notified": False}
    active = [f"{lab}" for did, lab in _LAYERS if layers[did] == "success"]
    sent = send_embed(
        f"✅ [commerce] 파이프라인 정상 — {today}",
        "수집 · " + " · ".join(active or ["(후속 레이어 미활성)"]) + " 금일 정상 실행 확인.",
        color=COLOR_OK, domain=_DOMAIN)
    if sent:
        storage.write_text(guard, now.isoformat())
    return {"date": today, "collected": collected, "layers": layers, "ok": True, "notified": bool(sent)}


@dag(dag_id="commerce_collect_watchdog", schedule="0 8,12,16,20 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["seoul", "commerce", "watchdog"], doc_md=__doc__)
def commerce_collect_watchdog():
    check_pipeline()


commerce_collect_watchdog()
