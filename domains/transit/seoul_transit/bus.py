"""서울 TOPIS 버스 도착·위치 → XML 원본 보존.

지하철과 달리 응답이 XML 이라 파싱하지 않고 원본 그대로 적재(가공은 silver=dbt).
둘 다 노선(busRouteId) 단위 — 지하철 호선 루프와 동일 패턴.
연계키 = vehId/plainNo(차량번호). 위치(buspos)에 gpsX/gpsY 좌표 있음.
"""

import re

from . import config
from .api import bus_url, get_text
from .records import now_kst

# dataset -> (ws.bus.go.kr 경로, list 태그)
_DATASETS = {
    "bus_arrival":  ("arrive/getArrInfoByRouteAll", "itemList"),  # 노선 전 정류장 도착예정
    "bus_position": ("buspos/getBusPosByRtid",      "itemList"),  # 노선 운행 차량(좌표)
}

_HEADER_CD = re.compile(r"<headerCd>(\d+)</headerCd>")


def collect_bus_raw(key_enc: str, dataset: str) -> list:
    """BUS_ROUTES 전 노선을 1회씩 호출 → 노선별 XML 원본 리스트.

    반환: [{raw(xml str), rows, route, endpoint, request_params, ts_collected}, ...]
    headerCd!=0(정상 아님)이면 해당 노선 row 는 rows=-1 로 표시(원본은 그대로 보존).
    """
    path, list_tag = _DATASETS[dataset]
    endpoint = path.split("/")[-1]
    tc = now_kst()
    out = []
    for route in config.BUS_ROUTES:
        xml = get_text(bus_url(key_enc, path, busRouteId=route))
        cd = _HEADER_CD.search(xml)
        ok = cd and cd.group(1) == "0"
        out.append({
            "raw": xml,
            "rows": xml.count(f"<{list_tag}>") if ok else -1,
            "route": route,
            "endpoint": endpoint,
            "request_params": {"busRouteId": route},
            "ts_collected": tc,
        })
    return out
