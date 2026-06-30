"""지하철 도착·위치 → 원본 보존 엔벨로프.

raw = 원본 행 그대로. ts_source = recptnDt. 좌표는 응답에 없어 None.
도착↔위치 join 키 = trainNo (도착 btrainNo == 위치 trainNo).

다중 target 수집: 도착=여러 역, 위치=여러 호선. target 당 1회만 호출(이중 호출 없음) —
한 번의 응답에서 silver용 envelope 와 bronze용 원본을 같이 뽑는다.
"""

from . import config
from .api import get, subway_url
from .records import envelope, now_kst

# dataset -> (service, list_key, targets_attr, rows_attr, source)
_DATASETS = {
    "subway_arrival":  ("realtimeStationArrival", "realtimeArrivalList",  "SUBWAY_STATIONS", "ARRIVAL_ROWS",  "subway_arrival"),
    "subway_position": ("realtimePosition",       "realtimePositionList", "SUBWAY_LINES",    "POSITION_ROWS", "subway_position"),
}


def collect_subway(key: str, dataset: str) -> dict:
    """dataset(arrival/position)을 설정된 전 target(역/호선)에 1회씩 호출.

    반환:
      {"records": [envelope, ...],                               # silver / bronze 테이블용
       "raws":    [{raw, rows, endpoint, request_params}, ...]}  # target별 원본 (bronze 객체용)
    """
    service, list_key, targets_attr, rows_attr, source = _DATASETS[dataset]
    rows_cap = getattr(config, rows_attr)
    tc = now_kst()
    records, raws = [], []
    for target in getattr(config, targets_attr):
        d = get(subway_url(key, service, rows_cap, target))
        rows = d.get(list_key, [])
        records.extend(
            envelope(source, r, ts_source=r.get("recptnDt"), ts_collected=tc) for r in rows
        )
        raws.append({
            "raw": d,
            "rows": len(rows),
            "endpoint": service,
            "request_params": {"target": target, "rows": rows_cap},
        })
    return {"records": records, "raws": raws}
