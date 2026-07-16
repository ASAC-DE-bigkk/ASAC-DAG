"""서울 TOPIS 버스 도착·위치 → XML 원본 보존.

지하철과 달리 응답이 XML 이라 파싱하지 않고 원본 그대로 적재(가공은 silver=dbt).
둘 다 노선(busRouteId) 단위 — 지하철 호선 루프와 동일 패턴.
연계키 = vehId/plainNo(차량번호). 위치(buspos)에 gpsX/gpsY 좌표 있음.

전역 모드(#369): BUS_ROUTES=ALL 이면 노선 마스터 reference(주간 갱신)에서 전 노선을
로드해 BUS_ROUTE_TYPES_EXCLUDE(기본 인천7·경기8)를 제외하고, 스레드-로컬 코어로
병렬 호출한다(HttpCore 는 스레드 안전이 아님 — api.get_text_mt).
"""

import logging
import re
from concurrent.futures import ThreadPoolExecutor

from . import config
from .api import bus_url, get_text_mt
from .records import now_kst

LOGGER = logging.getLogger(__name__)

# dataset -> (ws.bus.go.kr 경로, list 태그)
_DATASETS = {
    "bus_arrival":  ("arrive/getArrInfoByRouteAll", "itemList"),  # 노선 전 정류장 도착예정
    "bus_position": ("buspos/getBusPosByRtid",      "itemList"),  # 노선 운행 차량(좌표)
}

_HEADER_CD = re.compile(r"<headerCd>(\d+)</headerCd>")

# 부분 실패 허용선 — 전 노선(수백 개) 수집에서 개별 노선의 일시 오류(재시도 소진)로
# 런 전체를 죽이지 않는다. 단 이 비율을 넘으면 원천/키 이상으로 보고 실패시킨다.
_MAX_FAILURE_RATIO = 0.1


def resolve_routes() -> list[str]:
    """수집 대상 busRouteId 목록.

    - BUS_ROUTES 가 명시 목록이면 그대로(부분 수집·롤백용).
    - "ALL" 이면 노선 마스터 reference(transit_bus_route_master 주간 갱신)에서 로드,
      BUS_ROUTE_TYPES_EXCLUDE 타입 제외. reference 부재 시 명확히 실패한다 —
      조용한 폴백(부분 스코프)이 더 위험(#212 이력 공백 교훈). 부트스트랩:
      transit_bus_route_master 를 최초 1회 수동 트리거.
    """
    if config.BUS_ROUTES != ["ALL"]:
        return list(config.BUS_ROUTES)
    from .r2_landing import get_json

    try:
        reference = get_json(config.BUS_ROUTES_REFERENCE_KEY)
    except Exception as exc:
        raise RuntimeError(
            f"BUS_ROUTES=ALL 인데 노선 마스터 reference 없음/로드 실패 "
            f"({config.BUS_ROUTES_REFERENCE_KEY}) — transit_bus_route_master 를 먼저 "
            f"1회 실행해 부트스트랩 필요"
        ) from exc
    routes = [
        r["busRouteId"] for r in reference.get("routes", [])
        if r.get("routeType") not in config.BUS_ROUTE_TYPES_EXCLUDE
    ]
    if not routes:
        raise RuntimeError(
            "노선 마스터 reference 에서 대상 노선 0개 — reference 손상 또는 "
            f"BUS_ROUTE_TYPES_EXCLUDE({sorted(config.BUS_ROUTE_TYPES_EXCLUDE)}) 과도 의심"
        )
    return routes


def _per_worker_rate() -> float | None:
    """워커별 req/s 상한 — 합산이 BUS_RATE_LIMIT 이 되도록 배분(#369 스로틀)."""
    if config.BUS_RATE_LIMIT <= 0:
        return None
    return config.BUS_RATE_LIMIT / max(1, config.BUS_COLLECT_WORKERS)


def _fetch_route(key_enc: str, path: str, list_tag: str, endpoint: str,
                 route: str, tc: str) -> dict:
    xml = get_text_mt(bus_url(key_enc, path, busRouteId=route), rate_limit=_per_worker_rate())
    cd = _HEADER_CD.search(xml)
    ok = cd and cd.group(1) == "0"
    return {
        "raw": xml,
        "rows": xml.count(f"<{list_tag}>") if ok else -1,
        "route": route,
        "endpoint": endpoint,
        "request_params": {"busRouteId": route},
        "ts_collected": tc,
    }


def collect_bus_raw(key_enc: str, dataset: str, routes: list[str] | None = None) -> list:
    """대상 전 노선을 1회씩 호출 → 노선별 XML 원본 리스트 (page 순서 = 노선 순서).

    반환: [{raw(xml str), rows, route, endpoint, request_params, ts_collected}, ...]
    headerCd!=0(정상 아님)이면 해당 노선 row 는 rows=-1 로 표시(원본은 그대로 보존).
    전송 오류(재시도 소진)는 노선 단위로 제외하되, 실패 비율이 _MAX_FAILURE_RATIO 를
    넘으면 원천/키 이상으로 보고 런을 실패시킨다.
    """
    path, list_tag = _DATASETS[dataset]
    endpoint = path.split("/")[-1]
    tc = now_kst()
    routes = routes if routes is not None else resolve_routes()

    out, failures = [], []
    workers = max(1, config.BUS_COLLECT_WORKERS)
    if workers == 1 or len(routes) <= 1:
        results = (
            _try_fetch(key_enc, path, list_tag, endpoint, r, tc, failures) for r in routes
        )
        out = [r for r in results if r is not None]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = pool.map(
                lambda r: _try_fetch(key_enc, path, list_tag, endpoint, r, tc, failures),
                routes,
            )
            out = [r for r in results if r is not None]

    if failures:
        allowed = max(5, int(len(routes) * _MAX_FAILURE_RATIO))
        LOGGER.warning(
            "[bus:%s] 노선 %d/%d 수집 실패(허용 %d): %s%s",
            dataset, len(failures), len(routes), allowed,
            ",".join(f[0] for f in failures[:10]),
            " ..." if len(failures) > 10 else "",
        )
        # 전량 실패는 허용선과 무관하게 즉시 실패 — 바닥값(5) 때문에 소규모 명시
        # 목록(롤백 모드 ≤5노선)의 100% 실패가 빈 번들로 성공 마감되는 것을 차단
        # (#212/#229 가 막으려던 무경보 0행 마스킹 — 리뷰 #369).
        if not out:
            raise RuntimeError(
                f"버스 {dataset} 전 노선({len(routes)}) 수집 실패 — 키/원천 이상 "
                f"(첫 오류: {failures[0][1]})"
            )
        if len(failures) > allowed:
            raise RuntimeError(
                f"버스 {dataset} 수집 실패 노선 {len(failures)}/{len(routes)} — "
                f"허용선({allowed}) 초과, 원천/키 이상 의심 (첫 오류: {failures[0][1]})"
            )
    return out


def _try_fetch(key_enc, path, list_tag, endpoint, route, tc, failures) -> dict | None:
    try:
        return _fetch_route(key_enc, path, list_tag, endpoint, route, tc)
    except Exception as exc:  # noqa: BLE001 — 노선 단위 격리, 비율 초과는 상위에서 raise
        failures.append((route, f"{type(exc).__name__}: {exc}"))
        return None
