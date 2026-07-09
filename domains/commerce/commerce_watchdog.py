"""commerce_collect_watchdog — 금일 수집 실행 감지 → Discord (#218).

`commerce_collect_raw` 가 **당일 실행됐는지**(오늘 날짜 bronze `_RUN` 마커 존재)를 주기적으로 점검한다.
- 실행 확인: '정상 실행' 성공 알림을 **하루 1회만** 보낸다(당일 성공 가드 마커로 중복 방지).
- 실행 이력 없음: 매 점검마다 '수집 미실행' **에러 경고**를 보낸다(주기적 재발송).

전송은 공통 Discord(common.discord). best-effort — 조회/알림 실패가 태스크를 죽이지 않는다.
가드 마커는 bronze 원본 밖(운영 상태 경로 `state/commerce/watchdog/`)에 둔다(§2.2 bronze 격리 유지).
"""
from __future__ import annotations

import logging
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

from commerce_core.settings import get_settings  # noqa: E402
from commerce_core.storage import get_storage  # noqa: E402
from common.discord import COLOR_FAIL, COLOR_OK, send_embed  # noqa: E402

log = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))
_DOMAIN = "commerce"
_TARGET_DAG = "commerce_collect_raw"
_RAW_LAYER = "raw/commerce"   # 수집 원본 루트(=paths.RAW_LAYER)


def _pfx(prefix: str, tail: str) -> str:
    return f"{prefix}/{tail}" if prefix else tail


def _today_run_exists(storage, prefix: str, y: str, m: str, d: str) -> bool:
    """오늘 날짜(Y/M/D) run 폴더에 `_RUN.completed|incomplete` 마커가 하나라도 있으면 수집 실행됨."""
    date_prefix = _pfx(prefix, f"{_RAW_LAYER}/{y}/{m}/{d}/")
    return any("/_markers/_RUN." in k for k in storage.list_keys(date_prefix))


@task
def check_today_collection() -> dict:
    """오늘 수집 실행 여부 점검 → 실행=성공(1회)/미실행=에러(매회) 알림."""
    storage = get_storage()
    prefix = get_settings().storage_prefix
    now = datetime.now(KST)
    y, m, d = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    date_str = now.strftime("%Y-%m-%d")
    ran = _today_run_exists(storage, prefix, y, m, d)
    ok_key = _pfx(prefix, f"state/commerce/watchdog/{date_str}.ok")

    if ran:
        if storage.exists(ok_key):     # 당일 성공 알림 이미 전송 → 스킵(중복 방지)
            log.info("watchdog: %s 수집 확인 — 성공 알림 이미 전송(스킵)", date_str)
            return {"date": date_str, "ran": True, "notified": False}
        sent = send_embed(f"✅ [commerce] 수집 감지 — {date_str} 정상 실행",
                          f"`{_TARGET_DAG}` 금일 수집 run 이 확인되었습니다.",
                          color=COLOR_OK, domain=_DOMAIN)
        if sent:                        # 전송 성공 시에만 가드 기록(실패 시 다음 점검 재시도)
            storage.write_text(ok_key, now.isoformat())
        return {"date": date_str, "ran": True, "notified": bool(sent)}

    # 미실행 → 매 점검마다 에러 경고(주기적 재발송)
    send_embed(f"❌ [commerce] 수집 미실행 경고 — {date_str}",
               f"`{_TARGET_DAG}` 금일 수집 run 이 감지되지 않았습니다. 실행 상태를 확인하세요."
               " (본 경고는 실행이 감지될 때까지 주기적으로 재발송됩니다.)",
               color=COLOR_FAIL, domain=_DOMAIN)
    log.warning("watchdog: %s 수집 미실행 — 에러 경고 전송", date_str)
    return {"date": date_str, "ran": False, "notified": True}


@dag(dag_id="commerce_collect_watchdog", schedule="0 8,12,16,20 * * *",
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, tags=["seoul", "commerce", "watchdog"], doc_md=__doc__)
def commerce_collect_watchdog():
    check_today_collection()


commerce_collect_watchdog()
