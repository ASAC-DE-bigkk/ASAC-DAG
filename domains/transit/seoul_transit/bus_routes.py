"""버스 노선 마스터(getBusRouteList) — 전 노선 스냅샷 + collector reference (#369).

수집 스코프가 하드코딩 5노선 → 전 노선으로 확대되며, collector(ALL 모드)가 소비할
reference/transit/bus_routes/latest.json 을 여기서 만든다. routeType 전체(인천7·경기8
포함)를 저장하고 제외 필터는 collector 측(BUS_ROUTE_TYPES_EXCLUDE) — 재수집 없이 조정.

2026-07-15 실측: 전체 1,364노선(경기 598·인천 38), 응답 ~600KB XML 1콜.
순수 로직(파싱·가드)만 — R2 랜딩·오케스트레이션은 transit_bus_route_master.py.
"""
from __future__ import annotations

import os
import re

from .api import bus_url, get_text

_HEADER_CD = re.compile(r"<headerCd>(\d+)</headerCd>")
_HEADER_MSG = re.compile(r"<headerMsg>([^<]*)</headerMsg>")
_ITEM = re.compile(r"<itemList>(.*?)</itemList>", re.S)
_FIELDS = {
    "busRouteId": re.compile(r"<busRouteId>([^<]*)</busRouteId>"),
    "busRouteNm": re.compile(r"<busRouteNm>([^<]*)</busRouteNm>"),
    "routeType": re.compile(r"<routeType>([^<]*)</routeType>"),
}

# 위생 가드 — 전체 노선이 이보다 적으면 부분/빈 응답 의심(실측 1,364). 빈 스냅샷으로
# reference 를 덮으면 collector 전체가 스코프를 잃는다 → 이전 reference 유지가 안전.
MIN_ROUTES = int(os.environ.get("BUS_ROUTE_MIN_COUNT", "500"))


def fetch_route_list_xml(key_enc: str) -> str:
    """노선목록 1콜(~600KB) — 원본 XML 반환(R2 보존용)."""
    return get_text(bus_url(key_enc, "busRouteInfo/getBusRouteList"), timeout=60)


def parse_routes(xml: str) -> list[dict]:
    """XML → [{busRouteId, busRouteNm, routeType}, ...]. headerCd!=0 이면 raise."""
    cd = _HEADER_CD.search(xml)
    if not cd or cd.group(1) != "0":
        msg = _HEADER_MSG.search(xml)
        raise RuntimeError(
            f"getBusRouteList 응답 오류 — headerCd={cd.group(1) if cd else '?'} "
            f"msg={msg.group(1) if msg else '?'}"
        )
    routes = []
    for block in _ITEM.finditer(xml):
        item = block.group(1)
        route = {}
        for field, pattern in _FIELDS.items():
            m = pattern.search(item)
            route[field] = m.group(1).strip() if m else None
        if route.get("busRouteId"):
            routes.append(route)
    if len(routes) < MIN_ROUTES:
        raise RuntimeError(
            f"노선 마스터 위생 가드 — {len(routes)}개 < 최소 {MIN_ROUTES} "
            f"(부분/빈 응답 의심, 이전 reference 유지)"
        )
    return routes


def build_reference(routes: list[dict], *, run_id: str, ingest_ts: str) -> dict:
    """collector 가 읽는 reference 본문 — 전 노선 + 생성 메타."""
    return {
        "routes": routes,
        "total": len(routes),
        "run_id": run_id,
        "ingest_ts": ingest_ts,
    }
