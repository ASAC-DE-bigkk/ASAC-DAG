"""지하철 도착·위치 → 원본 보존 엔벨로프.

raw = 원본 행 그대로. ts_source = recptnDt. 좌표는 응답에 없어 None.
도착↔위치 join 키 = trainNo (raw 안에 있음).
"""

from . import config
from .api import get, subway_url
from .records import envelope, now_kst

# dataset -> (service, list_key, target_attr)
_DATASETS = {
    "subway_arrival": ("realtimeStationArrival", "realtimeArrivalList", "SUBWAY_STATION"),
    "subway_position": ("realtimePosition", "realtimePositionList", "SUBWAY_LINE"),
}


def fetch_subway_raw(key: str, dataset: str) -> dict:
    """원본 API 응답 dict 와 메타 반환 (R2 raw 적재용, 가공 없음).

    반환: {raw, rows, endpoint, request_params}
    """
    service, list_key, target_attr = _DATASETS[dataset]
    target = getattr(config, target_attr)
    rows_cap = config.ARRIVAL_ROWS if dataset == "subway_arrival" else config.POSITION_ROWS
    d = get(subway_url(key, service, rows_cap, target))
    return {
        "raw": d,
        "rows": len(d.get(list_key, [])),
        "endpoint": service,
        "request_params": {"target": target, "rows": rows_cap},
    }


def collect_subway_arrival(key: str) -> list:
    """역명 기준 실시간 도착정보."""
    tc = now_kst()
    url = subway_url(key, "realtimeStationArrival", config.ARRIVAL_ROWS, config.SUBWAY_STATION)
    d = get(url)
    return [
        envelope("subway_arrival", r, ts_source=r.get("recptnDt"), ts_collected=tc)
        for r in d.get("realtimeArrivalList", [])
    ]


def collect_subway_position(key: str) -> list:
    """호선 기준 실시간 열차 위치."""
    tc = now_kst()
    url = subway_url(key, "realtimePosition", config.POSITION_ROWS, config.SUBWAY_LINE)
    d = get(url)
    return [
        envelope("subway_position", r, ts_source=r.get("recptnDt"), ts_collected=tc)
        for r in d.get("realtimePositionList", [])
    ]
