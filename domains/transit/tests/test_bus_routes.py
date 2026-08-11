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


def _routes_xml(n_ok=3, header_cd="0", timetable=False):
    def item(i):
        extra = (
            f"<firstBusTm>2026081104{i:02d}00</firstBusTm><lastBusTm>2026081122{i:02d}00</lastBusTm>"
            f"<term>1{i}</term><stStationNm>기점{i}</stStationNm><edStationNm>종점{i}</edStationNm>"
            f"<corpNm>회사{i}</corpNm><length>14{i}</length>"
        ) if timetable else ""
        return (
            f"<itemList><busRouteId>10010{i:04d}</busRouteId>"
            f"<busRouteNm>노선{i}</busRouteNm><routeType>{3 if i % 2 else 8}</routeType>{extra}</itemList>"
        )
    items = "".join(item(i) for i in range(n_ok))
    return f"<msgHeader><headerCd>{header_cd}</headerCd><headerMsg>정상</headerMsg></msgHeader>{items}"


# ── 노선 마스터 파싱 ─────────────────────────────────────────────────────────────
def test_parse_routes_extracts_fields(monkeypatch):
    monkeypatch.setattr(bus_routes, "MIN_ROUTES", 2)
    routes = bus_routes.parse_routes(_routes_xml(3))
    assert len(routes) == 3
    # 시간표 필드가 없는 응답(구 픽스처)에서도 키는 있고 값은 None — 하위 호환.
    assert routes[1]["busRouteId"] == "100100001"
    assert routes[1]["busRouteNm"] == "노선1"
    assert routes[1]["routeType"] == "3"
    assert routes[1]["firstBusTm"] is None and routes[1]["term"] is None


def test_parse_routes_extracts_timetable_fields(monkeypatch):
    # #765 — 첫차/막차/배차간격/기점·종점/운수사/노선길이는 원본 문자열 그대로 취한다.
    monkeypatch.setattr(bus_routes, "MIN_ROUTES", 2)
    routes = bus_routes.parse_routes(_routes_xml(3, timetable=True))
    assert routes[1]["firstBusTm"] == "20260811040100"
    assert routes[1]["lastBusTm"] == "20260811220100"
    assert routes[1]["term"] == "11"
    assert routes[1]["stStationNm"] == "기점1"
    assert routes[1]["edStationNm"] == "종점1"
    assert routes[1]["corpNm"] == "회사1"
    assert routes[1]["length"] == "141"


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


# ── bronze 적재 (#471 — routeType·tier 원천화) ──────────────────────────────────
def test_tier_for_maps_tier1_types_only():
    t1 = {"3", "6"}
    assert bus_routes.tier_for("3", t1) == 1   # 간선
    assert bus_routes.tier_for("6", t1) == 1   # 광역
    assert bus_routes.tier_for("4", t1) == 2   # 지선 → tier2
    assert bus_routes.tier_for("11", t1) == 2  # 심야 → tier2
    assert bus_routes.tier_for(None, t1) == 2  # 미상 → tier2(안전한 방향)
    assert bus_routes.tier_for("", t1) == 2


def test_build_master_rows_sets_tier_and_keeps_source_route_type():
    routes = [
        {"busRouteId": "100100001", "busRouteNm": "간선A", "routeType": "3"},
        {"busRouteId": "100100002", "busRouteNm": "지선B", "routeType": "4"},
        {"busRouteId": None, "busRouteNm": "결측", "routeType": "3"},  # id 없으면 제외
    ]
    rows = bus_routes.build_master_rows(routes, {"3", "6"})
    assert len(rows) == 2
    assert rows[0]["bus_route_id"] == "100100001"
    assert rows[0]["bus_route_nm"] == "간선A"
    assert rows[0]["route_type"] == "3" and rows[0]["tier"] == 1
    # 시간표 필드(#765): 구 reference(3필드 시절)에도 None 으로 안전
    assert rows[0]["first_bus_tm"] is None and rows[0]["term"] is None
    assert rows[1]["tier"] == 2 and rows[1]["route_type"] == "4"


def test_build_master_rows_carries_timetable_fields():
    routes = [{
        "busRouteId": "100100412", "busRouteNm": "6001", "routeType": "1",
        "firstBusTm": "20260811043000", "lastBusTm": "20260811225000", "term": "13",
        "stStationNm": "인천공항", "edStationNm": "동대문", "corpNm": "공항리무진", "length": "146",
    }]
    row = bus_routes.build_master_rows(routes, {"3", "6"})[0]
    assert row["first_bus_tm"] == "20260811043000"
    assert row["last_bus_tm"] == "20260811225000"
    assert row["term"] == "13"
    assert row["start_station_nm"] == "인천공항"
    assert row["end_station_nm"] == "동대문"
    assert row["corp_nm"] == "공항리무진"
    assert row["route_length"] == "146"


def test_master_replace_sql_is_delete_then_insert_with_escaping():
    rows = [{"bus_route_id": "100100001", "bus_route_nm": "정촌'행",  # 작은따옴표 이스케이프
             "route_type": "3", "tier": 1}]
    stmts = bus_routes.master_load_sql(
        "iceberg_dev.transit.bronze_bus_route_master", rows,
        load_date="2026-07-21", collected_at="2026-07-21 13:45:00.000000",
        dag_run_id="scheduled__x",
    )
    assert stmts[0] == "DELETE FROM iceberg_dev.transit.bronze_bus_route_master WHERE load_date = '2026-07-21'"  # 그 load_date 만
    assert "INSERT INTO" in stmts[1]
    assert "'정촌''행'" in stmts[1]                       # '' 이스케이프
    assert ", 1, " in stmts[1]                           # tier 는 정수 리터럴(따옴표 없음)
    assert "timestamp '2026-07-21 13:45:00.000000'" in stmts[1]


def test_snapshot_labels_split_kst_label_from_utc_lineage():
    """load_date 는 KST(#78 P-1), collected_at 은 UTC 계보 시각 — 기준이 갈린다.

    16:30Z 는 KST 로 다음 날 01:30 이다. 두 값을 같은 문자열에서 잘라 쓰면
    'KST 날짜 + UTC 시각' 이라는 존재하지 않는 시각이 bronze 에 박힌다.
    """
    load_date, collected_at = bus_routes.snapshot_labels("20260805T163000Z")
    assert load_date == "2026-08-06"                      # KST 실행일
    assert collected_at == "2026-08-05 16:30:00.000000"   # UTC 원본


def test_snapshot_labels_rejects_malformed_ingest_ts():
    # 형식이 깨지면 '-- ::' 같은 리터럴이 INSERT 로 흘러가므로 여기서 끊는다.
    with pytest.raises(ValueError, match="ingest_ts"):
        bus_routes.snapshot_labels("2026-08-05")


def test_master_replace_sql_refuses_empty_rows():
    # 빈 rows 로는 DELETE 만 남아 전건 소실 위험 — 호출 자체를 막는다.
    with pytest.raises(ValueError):
        bus_routes.master_load_sql(
            "t", [], load_date="2026-07-21", collected_at="2026-07-21 00:00:00.000000",
            dag_run_id="x",
        )


def test_master_ddl_types_route_codes_varchar_tier_integer():
    ddl = bus_routes.master_ddl("iceberg_dev.transit.bronze_bus_route_master")
    assert "route_type varchar" in ddl   # 코드류 varchar(선행 0 보존)
    assert "tier integer" in ddl
    assert "bus_route_id varchar" in ddl
    assert "first_bus_tm varchar" in ddl  # 시간표 필드(#765) — 원본 보존 varchar
    assert "route_length varchar" in ddl


def test_master_migration_sql_adds_timetable_columns_idempotently():
    # 구 테이블(3필드 시절) 보강 — IF NOT EXISTS 라 신규 생성 후에도 무해(멱등).
    stmts = bus_routes.master_migration_sql("iceberg_dev.transit.bronze_bus_route_master")
    assert len(stmts) == 7
    assert all("ADD COLUMN IF NOT EXISTS" in s for s in stmts)
    assert any("first_bus_tm varchar" in s for s in stmts)
    assert any("term varchar" in s for s in stmts)


def test_master_replace_sql_includes_timetable_columns():
    rows = [{"bus_route_id": "100100412", "bus_route_nm": "6001", "route_type": "1",
             "tier": 2, "first_bus_tm": "20260811043000", "last_bus_tm": None,
             "term": "13", "start_station_nm": "인천공항", "end_station_nm": "동대문",
             "corp_nm": None, "route_length": "146"}]
    stmts = bus_routes.master_load_sql(
        "t", rows, load_date="2026-08-09", collected_at="2026-08-09 00:00:11.000000",
        dag_run_id="x",
    )
    assert "first_bus_tm, last_bus_tm, term, start_station_nm, end_station_nm" in stmts[1]
    assert "'20260811043000'" in stmts[1]
    assert "'인천공항'" in stmts[1]
    # None/빈값은 NULL 리터럴
    assert ", NULL, '13'," in stmts[1].replace("\n", " ")
