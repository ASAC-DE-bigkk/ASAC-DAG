"""버스 전 노선 수집(#369) 단위 테스트 — 노선 마스터 파싱·스코프 해석·병렬 수집 격리."""
import importlib
import sys
from pathlib import Path

import pytest

_TRANSIT = Path(__file__).resolve().parents[1]        # domains/transit
_DAGS = Path(__file__).resolve().parents[3]           # dags 루트
for p in (str(_DAGS), str(_TRANSIT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from seoul_transit import bus, bus_routes, config  # noqa: E402


def _routes_xml(n_ok=3, header_cd="0"):
    items = "".join(
        f"<itemList><busRouteId>10010{i:04d}</busRouteId>"
        f"<busRouteNm>노선{i}</busRouteNm><routeType>{3 if i % 2 else 8}</routeType></itemList>"
        for i in range(n_ok)
    )
    return f"<msgHeader><headerCd>{header_cd}</headerCd><headerMsg>정상</headerMsg></msgHeader>{items}"


# ── 노선 마스터 파싱 ─────────────────────────────────────────────────────────────
def test_parse_routes_extracts_fields(monkeypatch):
    monkeypatch.setattr(bus_routes, "MIN_ROUTES", 2)
    routes = bus_routes.parse_routes(_routes_xml(3))
    assert len(routes) == 3
    assert routes[1] == {"busRouteId": "100100001", "busRouteNm": "노선1", "routeType": "3"}


def test_parse_routes_header_error_raises():
    with pytest.raises(RuntimeError, match="headerCd=8"):
        bus_routes.parse_routes(_routes_xml(3, header_cd="8"))


def test_parse_routes_min_count_guard(monkeypatch):
    # 부분/빈 응답이 reference 를 덮지 않게 — 이전 스냅샷 유지가 안전.
    monkeypatch.setattr(bus_routes, "MIN_ROUTES", 500)
    with pytest.raises(RuntimeError, match="위생 가드"):
        bus_routes.parse_routes(_routes_xml(3))


# ── 수집 스코프 해석 ─────────────────────────────────────────────────────────────
def test_resolve_routes_explicit_list_bypasses_reference(monkeypatch):
    monkeypatch.setattr(config, "BUS_ROUTES", ["100100025", "100100454"])
    assert bus.resolve_routes() == ["100100025", "100100454"]


def test_resolve_routes_all_filters_excluded_types(monkeypatch):
    from seoul_transit import r2_landing
    monkeypatch.setattr(config, "BUS_ROUTES", ["ALL"])
    monkeypatch.setattr(config, "BUS_ROUTE_TYPES_EXCLUDE", {"7", "8"})
    monkeypatch.setattr(r2_landing, "get_json", lambda key: {"routes": [
        {"busRouteId": "1", "routeType": "3"},
        {"busRouteId": "2", "routeType": "8"},   # 경기 → 제외
        {"busRouteId": "3", "routeType": "7"},   # 인천 → 제외
        {"busRouteId": "4", "routeType": "4"},
    ]})
    assert bus.resolve_routes() == ["1", "4"]


def test_resolve_routes_all_without_reference_fails_loudly(monkeypatch):
    from seoul_transit import r2_landing
    monkeypatch.setattr(config, "BUS_ROUTES", ["ALL"])
    def boom(key):
        raise FileNotFoundError(key)
    monkeypatch.setattr(r2_landing, "get_json", boom)
    with pytest.raises(RuntimeError, match="transit_bus_route_master"):
        bus.resolve_routes()  # 조용한 폴백 금지 — 부분 스코프가 더 위험


def test_resolve_routes_all_empty_after_filter_fails(monkeypatch):
    from seoul_transit import r2_landing
    monkeypatch.setattr(config, "BUS_ROUTES", ["ALL"])
    monkeypatch.setattr(config, "BUS_ROUTE_TYPES_EXCLUDE", {"3"})
    monkeypatch.setattr(r2_landing, "get_json",
                        lambda key: {"routes": [{"busRouteId": "1", "routeType": "3"}]})
    with pytest.raises(RuntimeError, match="0개"):
        bus.resolve_routes()


# ── 병렬 수집 — 노선 단위 실패 격리 + 허용선 ─────────────────────────────────────
def _fake_fetch(fail_routes):
    def fetch(url, timeout=20, rate_limit=None):
        route = url.rsplit("=", 1)[1]
        if route in fail_routes:
            raise ConnectionError(f"boom {route}")
        return f"<msgHeader><headerCd>0</headerCd></msgHeader><itemList>{route}</itemList>"
    return fetch


def test_collect_bus_raw_isolates_single_route_failure(monkeypatch):
    monkeypatch.setattr(bus, "get_text_mt", _fake_fetch({"r02"}))
    routes = [f"r{i:02d}" for i in range(10)]
    out = bus.collect_bus_raw("key", "bus_position", routes=routes)
    assert len(out) == 9
    assert all(r["route"] != "r02" for r in out)
    assert all(r["rows"] == 1 for r in out)


def test_collect_bus_raw_fails_over_failure_ratio(monkeypatch):
    routes = [f"r{i:03d}" for i in range(100)]
    monkeypatch.setattr(bus, "get_text_mt", _fake_fetch(set(routes[:20])))  # 20% 실패
    with pytest.raises(RuntimeError, match="허용선"):
        bus.collect_bus_raw("key", "bus_position", routes=routes)


def test_collect_bus_raw_preserves_route_page_order(monkeypatch):
    # loader 가 manifest 노선 순서 = 페이지 순서를 전제 — 병렬화가 순서를 깨면 안 된다.
    monkeypatch.setattr(bus, "get_text_mt", _fake_fetch(set()))
    routes = [f"r{i:02d}" for i in range(20)]
    out = bus.collect_bus_raw("key", "bus_position", routes=routes)
    assert [r["route"] for r in out] == routes


# ── 티어링 (#440) ────────────────────────────────────────────────────────────────
def _ref(monkeypatch, routes):
    from seoul_transit import r2_landing
    monkeypatch.setattr(r2_landing, "get_json", lambda key: {"routes": routes})
    monkeypatch.setattr(config, "BUS_ROUTES", ["ALL"])


def test_resolve_routes_tier1_only_filters_types(monkeypatch):
    _ref(monkeypatch, [
        {"busRouteId": "1", "routeType": "3"},   # 간선 → tier1
        {"busRouteId": "2", "routeType": "6"},   # 광역 → tier1
        {"busRouteId": "3", "routeType": "4"},   # 지선 → tier2
        {"busRouteId": "4", "routeType": "15"},  # 심야 → tier2
        {"busRouteId": "5", "routeType": "8"},   # 경기 → 제외
    ])
    assert bus.resolve_routes(include_tier2=False) == ["1", "2"]
    assert bus.resolve_routes(include_tier2=True) == ["1", "2", "3", "4"]


def test_resolve_routes_explicit_list_ignores_tiering(monkeypatch):
    monkeypatch.setattr(config, "BUS_ROUTES", ["100100025"])
    assert bus.resolve_routes(include_tier2=False) == ["100100025"]


# ── 쿼터/인증 가드 (#440 — headerCd 무경보 구멍 차단) ────────────────────────────
def _fake_fetch_cd(cd_by_route):
    def fetch(url, timeout=20, rate_limit=None):
        route = url.rsplit("=", 1)[1]
        cd = cd_by_route.get(route, "0")
        body = "<itemList>x</itemList>" if cd == "0" else ""
        return f"<msgHeader><headerCd>{cd}</headerCd></msgHeader>{body}"
    return fetch


def test_quota_guard_raises_on_majority_key_faults(monkeypatch):
    routes = [f"r{i}" for i in range(10)]
    monkeypatch.setattr(bus, "get_text_mt", _fake_fetch_cd({r: "7" for r in routes[:6]}))
    with pytest.raises(RuntimeError, match="키 이상"):
        bus.collect_bus_raw("key", "bus_position", routes=routes)


def test_quota_guard_tolerates_no_data_cd4(monkeypatch):
    # 미운행 '결과 없음'(cd=4)은 다수여도 정상 — 쿼터 가드 미발동, rows=-1 보존
    routes = [f"r{i}" for i in range(10)]
    monkeypatch.setattr(bus, "get_text_mt", _fake_fetch_cd({r: "4" for r in routes[:8]}))
    out = bus.collect_bus_raw("key", "bus_position", routes=routes)
    assert len(out) == 10
    assert sum(1 for r in out if r["rows"] == -1) == 8


def test_collect_bus_raw_total_failure_always_raises(monkeypatch):
    # 리뷰 #369: 허용선 바닥값(5) 때문에 소규모 명시 목록(롤백 모드)의 100% 실패가
    # 빈 번들로 성공 마감되던 결함 — 전량 실패는 허용선과 무관하게 raise.
    routes = [f"r{i}" for i in range(5)]
    monkeypatch.setattr(bus, "get_text_mt", _fake_fetch(set(routes)))
    with pytest.raises(RuntimeError, match="전 노선"):
        bus.collect_bus_raw("key", "bus_position", routes=routes)


# ── 스로틀 (#369 — 총 상한을 워커별로 배분) ──────────────────────────────────────
def test_per_worker_rate_divides_total(monkeypatch):
    monkeypatch.setattr(config, "BUS_RATE_LIMIT", 20.0)
    monkeypatch.setattr(config, "BUS_COLLECT_WORKERS", 8)
    assert bus._per_worker_rate() == pytest.approx(2.5)


def test_per_worker_rate_disabled_when_nonpositive(monkeypatch):
    # 0/음수 = 무제한(None) — HttpCore 에 그대로 주면 ValueError 이므로 여기서 흡수
    monkeypatch.setattr(config, "BUS_RATE_LIMIT", 0.0)
    assert bus._per_worker_rate() is None


# ── config 기본값 계약 (#369) ────────────────────────────────────────────────────
def test_config_defaults_full_scope(monkeypatch):
    for var in ("SUBWAY_STATIONS", "BUS_ROUTES", "BUS_ROUTE_TYPES_EXCLUDE"):
        monkeypatch.delenv(var, raising=False)
    cfg = importlib.reload(config)
    try:
        assert cfg.SUBWAY_STATIONS == ["ALL"]
        assert cfg.BUS_ROUTES == ["ALL"]
        assert cfg.BUS_ROUTE_TYPES_EXCLUDE == {"7", "8"}
    finally:
        importlib.reload(config)  # 다른 테스트가 실제 env 기준 config 를 보게 복원
