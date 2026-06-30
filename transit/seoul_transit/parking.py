"""공영주차 실시간(GetParkingInfo) → 원본 보존 엔벨로프.

raw = 주차장 행 그대로. ts_source = NOW_PRK_VHCL_UPDT_TM(갱신시각).
단일 호출로 전체(123개) — 지하철처럼 다중 target 루프 불필요.
⚠️ 실시간 API 엔 좌표 없음(LAT/LOT 부재) → lat/lon=None. 좌표는 마스터(GetParkInfo) 후속.
"""

from . import config
from .api import get, openapi_url
from .records import envelope, now_kst


def collect_parking(key: str) -> dict:
    """GetParkingInfo 1회 호출 → {records, raw, rows, endpoint, request_params}.

    호출 1회로 records(silver/bronze테이블) + raw(bronze객체) 동시 확보.
    """
    tc = now_kst()
    d = get(openapi_url(key, "GetParkingInfo", 1, config.PARKING_ROWS))
    rows = d.get("GetParkingInfo", {}).get("row", [])
    records = [
        envelope(
            "parking", r,
            ts_source=r.get("NOW_PRK_VHCL_UPDT_TM"), ts_collected=tc,
            lat=r.get("LAT") or None, lon=r.get("LOT") or None,  # 실시간엔 없음(=None)
        )
        for r in rows
    ]
    return {
        "records": records, "raw": d, "rows": len(rows),
        "endpoint": "GetParkingInfo", "request_params": {"rows": config.PARKING_ROWS},
    }
