"""silver 스키마 검증 — 정규화 레코드가 공통 컬럼 계약을 지키는지 점검(순수 함수)."""
from __future__ import annotations

from common.schemas import COMMON_COLUMNS


def validate_normalized(records: list[dict]) -> dict:
    """정규화 레코드 점검.

    Returns 리포트: 전체/누락컬럼/UPDATEDT 결측 건수. 누락 컬럼이 있으면 ok=False.
    """
    missing_cols = set()
    updatedt_null = 0
    for r in records:
        for c in COMMON_COLUMNS:
            if c not in r:
                missing_cols.add(c)
        if not (r.get("UPDATEDT") or "").strip():
            updatedt_null += 1
    return {
        "records": len(records),
        "missing_columns": sorted(missing_cols),
        "updatedt_null": updatedt_null,
        "ok": not missing_cols,
    }
