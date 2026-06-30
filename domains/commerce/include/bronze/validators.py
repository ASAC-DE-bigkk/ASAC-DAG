"""bronze 완전성 점검 — '끝까지 순회했는가 + 건수 일치'를 판정(순수 함수, 테스트 대상)."""
from __future__ import annotations

from common.schemas import STATUS_OK, STATUS_PARTIAL


def assess_completeness(*, rows_total: int, list_total_count: int,
                        stopped_by_cap: bool) -> tuple[bool, bool, str]:
    """수집 결과의 완전성을 판정.

    Returns: (complete, verified, status)
      - verified: 수집 건수 >= 선언 전체건수(list_total_count). 전체 0이면 0건이어야 True.
      - complete: cap 없이 끝까지 + verified.
      - status: complete 면 "ok", 아니면 "partial".
    """
    if list_total_count:
        verified = rows_total >= list_total_count
    else:
        verified = rows_total == 0
    complete = (not stopped_by_cap) and verified
    return complete, verified, (STATUS_OK if complete else STATUS_PARTIAL)
