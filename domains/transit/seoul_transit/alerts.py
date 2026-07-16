"""수집 이상 Discord 경보 — 공통 common.discord(#161) 재사용 (#229 보완).

에러 코드가 있는 응답은 api.raise_for_result 가 실패로 올려 기존 #77 실패 콜백 →
#161 Discord 에러 embed 가 자동 발동한다. 이 모듈은 그 **밖의 경우** —
에러 코드는 없는데 **0행**인 '의심 빈응답' — 을 WARN 으로 알린다. hard error 로 단정할
수 없어 태스크를 실패시키진 않되(정상 빈 스냅샷일 수도), 조용히 지나가지 않게 가시화한다.

webhook 폴백: `TRANSIT_DISCORD_WEBHOOK_URL` → `DISCORD_WEBHOOK_URL`.
미설정이면 send_embed 가 조용히 스킵(무해). 전송 실패도 best-effort 로 삼킨다.
수집 태스크는 실행당 1회이므로 알림 폭풍 가드 없이 최대 1건/런.
"""
from __future__ import annotations

import logging
from datetime import datetime, time

from common.discord import COLOR_WARN, send_embed

from .config import KST, TRANSIT_QUIET_HOURS

LOGGER = logging.getLogger(__name__)


def _parse_quiet(spec: str) -> tuple[time, time] | None:
    """"HH:MM-HH:MM" → (start, end). 형식 오류·빈 값이면 None(억제 비활성)."""
    try:
        start_s, end_s = spec.split("-")
        sh, sm = (int(x) for x in start_s.strip().split(":"))
        eh, em = (int(x) for x in end_s.strip().split(":"))
        return time(sh, sm), time(eh, em)
    except (ValueError, AttributeError):
        return None


def in_quiet_hours(now: datetime | None = None) -> bool:
    """지금(KST)이 무경보 창(TRANSIT_QUIET_HOURS) 안인가. 자정 걸침 지원."""
    window = _parse_quiet(TRANSIT_QUIET_HOURS)
    if window is None:
        return False
    start, end = window
    t = (now or datetime.now(KST)).astimezone(KST).time()
    if start <= end:
        return start <= t < end
    return t >= start or t < end  # 예: 23:00-05:00


def warn_if_empty(dataset: str, rows: int, run_id: str, *, domain: str = "transit",
                  quiet_ok: bool = False) -> bool:
    """dataset 수집이 0행이면 Discord WARN 전송. rows>0 이면 no-op. 전송 성공 여부 반환.

    quiet_ok=True 인 소스(지하철 등 심야 미운행)는 무경보 창(KST, TRANSIT_QUIET_HOURS)
    동안 0행 WARN 을 억제한다 — 새벽 정상 0행의 반복 경보 방지. 주차처럼 24시간
    데이터가 정상인 소스는 quiet_ok=False(기본) 유지. 반환값은 테스트/로그용.
    """
    if rows and rows > 0:
        return False
    if quiet_ok and in_quiet_hours():
        LOGGER.info("[%s] %s 수집 0행 — 무경보 창(%s) 내 정상 취급(run_id=%s)",
                    domain, dataset, TRANSIT_QUIET_HOURS, run_id)
        return False
    LOGGER.warning("[%s] %s 수집 0행 — 비정상 의심(run_id=%s)", domain, dataset, run_id)
    return send_embed(
        title=f"⚠️ {domain} · {dataset} 수집 0행 (비정상 의심)",
        description=(
            f"`{dataset}` 실시간 수집이 **0행**입니다. 에러 코드는 없었으나 정상 스냅샷이 "
            f"비었는지 확인이 필요합니다 — 키/쿼터/원천 이상 가능. "
            f"실시간은 소급 불가하니 방치 시 이력 공백이 됩니다.\n"
            f"run_id=`{run_id}`"
        ),
        color=COLOR_WARN,
        domain=domain,
    )
