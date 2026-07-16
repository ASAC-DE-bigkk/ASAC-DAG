"""공영주차 실시간(GetParkingInfo) 수집 — 원본 응답만 반환 (#369 수집·적재 분리).

raw = 원본 API 응답 그대로(R2 보존용). 행 파싱은 loader 가 재파싱(_envelope_rows —
ts_source=NOW_PRK_VHCL_UPDT_TM, 실시간 응답엔 좌표 없음 → lat/lon=None).
단일 호출로 전체(123개) — 지하철처럼 다중 target 루프 불필요.
"""

from . import config
from .api import get, openapi_url, raise_for_result
from .records import now_kst


def collect_parking(key: str) -> dict:
    """GetParkingInfo 1회 호출 → {raw, rows, endpoint, request_params, ts_collected}."""
    tc = now_kst()
    d = get(openapi_url(key, "GetParkingInfo", 1, config.PARKING_ROWS))
    # 200 + 에러/최상위 RESULT(키 만료·쿼터 등)를 실패로 올린다(#229) — 아니면 0행 마스킹.
    raise_for_result(d, "GetParkingInfo")
    rows = d.get("GetParkingInfo", {}).get("row", [])
    return {
        "raw": d, "rows": len(rows),
        "endpoint": "GetParkingInfo", "request_params": {"rows": config.PARKING_ROWS},
        "ts_collected": tc,
    }
