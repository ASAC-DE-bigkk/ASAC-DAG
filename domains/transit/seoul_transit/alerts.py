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

from common.discord import COLOR_WARN, send_embed

LOGGER = logging.getLogger(__name__)


def warn_if_empty(dataset: str, rows: int, run_id: str, *, domain: str = "transit") -> bool:
    """dataset 수집이 0행이면 Discord WARN 전송. rows>0 이면 no-op. 전송 성공 여부 반환.

    반환값은 테스트/로그용 — 호출측 분기용이 아니다.
    """
    if rows and rows > 0:
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
