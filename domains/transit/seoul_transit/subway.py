"""지하철 도착·위치 수집 — 원본 응답(raws)만 반환 (#369 수집·적재 분리).

raw = 원본 API 응답 그대로(R2 보존용). silver/bronze 행 파싱은 loader 가
R2 원본을 재파싱한다(loader._envelope_rows) — collector 는 행을 만들지 않는다.
도착↔위치 join 키 = trainNo (도착 btrainNo == 위치 trainNo).

다중 target 수집: 도착=여러 역, 위치=여러 호선. target 당 1회만 호출(이중 호출 없음).
전역 모드(#369): SUBWAY_STATIONS=ALL 이면 도착은 일괄 API 1콜로 전 역을 수집한다.
"""

from . import config
from .api import get, raise_for_result, subway_all_url, subway_url
from .records import now_kst

# dataset -> (service, list_key, targets_attr, rows_attr)
_DATASETS = {
    "subway_arrival":  ("realtimeStationArrival", "realtimeArrivalList",  "SUBWAY_STATIONS", "ARRIVAL_ROWS"),
    "subway_position": ("realtimePosition",       "realtimePositionList", "SUBWAY_LINES",    "POSITION_ROWS"),
}


def collect_subway(key: str, dataset: str) -> dict:
    """dataset(arrival/position)을 설정된 전 target(역/호선)에 1회씩 호출.

    반환: {"raws": [{raw, rows, endpoint, request_params, ts_collected}, ...]}
    (target별 원본 — R2 랜딩용. 행 파싱은 loader 소관.)
    """
    service, list_key, targets_attr, rows_attr = _DATASETS[dataset]
    targets = getattr(config, targets_attr)
    if dataset == "subway_arrival" and targets == ["ALL"]:
        return _collect_arrival_all(key)
    rows_cap = getattr(config, rows_attr)
    tc = now_kst()
    raws = []
    for target in targets:
        d = get(subway_url(key, service, rows_cap, target))
        # 200 + 에러 엔벨로프(키 만료·쿼터 등)를 실패로 올린다(#229) — 아니면 0행 마스킹.
        raise_for_result(d, service, target)
        raws.append({
            "raw": d,
            "rows": len(d.get(list_key, [])),
            "endpoint": service,
            "request_params": {"target": target, "rows": rows_cap},
            "ts_collected": tc,
        })
    return {"raws": raws}


def _collect_arrival_all(key: str) -> dict:
    """도착 일괄(ALL) 1콜 → 전 역 (#369). 반환 형태는 collect_subway 와 동일.

    응답 ~2MB(실측 1.9MB) — 기본 20s 대신 60s timeout.
    """
    service, list_key = "realtimeStationArrival", "realtimeArrivalList"
    tc = now_kst()
    d = get(subway_all_url(key), timeout=60)
    # 200 + 에러 엔벨로프(키 만료·쿼터 등)를 실패로 올린다(#229) — 아니면 0행 마스킹.
    raise_for_result(d, service, "ALL")
    raws = [{
        "raw": d,
        "rows": len(d.get(list_key, [])),
        "endpoint": service,
        "request_params": {"target": "ALL", "rows": None},
        "ts_collected": tc,
    }]
    return {"raws": raws}
