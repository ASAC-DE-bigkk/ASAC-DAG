"""loader(#369) 순수 로직 단위 테스트 — 파싱·청크·마커·DDL.

collector/loader 분리의 핵심 계약:
- R2 원본 재파싱 결과가 기존 collector 인라인 적재와 동일한 행을 만든다.
- INSERT 청크가 문자 캡을 지킨다(Trino QUERY_TEXT_TOO_LARGE 회피).
- pending 마커 키가 **같은 dataset 안에서** 사전순 = 시간순이고, dataset 을 가로지르는
  처리 순서는 sort_markers 가 세운다(#719).
"""
import json
import sys
from pathlib import Path

import pytest

_TRANSIT = Path(__file__).resolve().parents[1]        # domains/transit
_DAGS = Path(__file__).resolve().parents[3]           # dags 루트
for p in (str(_DAGS), str(_TRANSIT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from seoul_transit import config, loader  # noqa: E402


def _marker(dataset, **over):
    m = loader.make_marker(
        dataset=dataset, source=over.pop("source", "src"),
        manifest_key="raw/transit/x/_manifest.json",
        run_id=over.pop("run_id", "run-1"),
        ts_collected=over.pop("ts_collected", "2026-07-15 12:00:00"),
    )
    m.update(over)
    return m


# ── 마커 ─────────────────────────────────────────────────────────────────────────
def test_make_marker_maps_table_and_shape():
    m = _marker("subway_arrival")
    assert m["table"] == "bronze_subway_arrival" and m["shape"] == "envelope"
    m = _marker("bus_position")
    assert m["table"] == "bronze_bus_position" and m["shape"] == "bus"


def test_make_marker_rejects_unknown_dataset():
    with pytest.raises(ValueError):
        loader.make_marker(dataset="nope", source="s", manifest_key="k",
                           run_id="r", ts_collected="t")


def test_pending_key_sorts_chronologically_and_sanitizes():
    early = loader.pending_key("parking", "20260715T110000Z", "manual__2026-07-15T11:00:00+00:00")
    late = loader.pending_key("parking", "20260715T120000Z", "scheduled__2026-07-15T12:00:00+00:00")
    assert early < late
    assert "+" not in early and ":" not in early.rsplit("/", 1)[1]


def test_sort_markers_orders_across_datasets_by_time():
    """#719 회귀 — 나열의 사전순은 dataset 이름순이라 시간순이 아니다.

    2026-08-06 prod: 한 런이 bus_position 19건 전량 → parking 53건 전량 →
    subway_arrival 90건 순으로 처리됐다. 키의 `<dataset>/` 세그먼트가 ingest_ts 보다
    앞서기 때문. 정렬을 거치면 실제 수집 시각 순으로 섞여야 한다.
    """
    keys = [
        loader.pending_key("subway_arrival", "20260806T100000Z", "r1"),   # 가장 오래됨
        loader.pending_key("parking", "20260806T110000Z", "r2"),
        loader.pending_key("bus_position", "20260806T120000Z", "r3"),     # 가장 최신
    ]
    # 사전순(= 기존 동작)은 dataset 이름순 — 가장 최신인 버스가 맨 앞으로 온다.
    assert [k.split("/")[-2] for k in sorted(keys)] == [
        "bus_position", "parking", "subway_arrival"]
    # 정렬을 거치면 수집 시각 순.
    assert [k.split("/")[-2] for k in loader.sort_markers(keys)] == [
        "subway_arrival", "parking", "bus_position"]


def test_sort_markers_puts_unparsable_first_and_is_stable():
    """ingest_ts 를 못 읽는 키는 굶기지 않고 먼저 드레인한다. 동시각은 키 사전순."""
    odd = config.LOADER_PENDING_PREFIX + "parking/legacy-marker.json"
    same_a = loader.pending_key("parking", "20260806T110000Z", "a")
    same_b = loader.pending_key("subway_arrival", "20260806T110000Z", "b")
    ordered = loader.sort_markers([same_b, same_a, odd])
    assert ordered[0] == odd
    assert ordered[1:] == sorted([same_a, same_b])


def test_marker_ingest_ts_parses_and_rejects():
    at = loader.marker_ingest_ts(loader.pending_key("parking", "20260806T114501Z", "r"))
    assert at is not None
    assert (at.year, at.month, at.day, at.hour, at.minute) == (2026, 8, 6, 11, 45)
    assert at.tzinfo is not None                       # 나이 계산이 naive 비교로 깨지지 않게
    assert loader.marker_ingest_ts("prefix/parking/not-a-stamp__r.json") is None


def test_pending_prefix_comes_from_the_ops_gate():
    """규약 앞부분은 관문이 만든다(#78 P-5 · §1) — 손으로 조립하지 않는다.

    하위유형(state)이 도메인(transit)보다 앞이어야 한다. 이 순서가 뒤집히면 운영 중인
    마커가 나열 접두에서 빠져 loader·maintenance 양쪽에서 안 보이는 고립이 된다(#547).
    """
    from common.ops import ControlSubtype, OpsCategory, category_prefix

    assert config.LOADER_PENDING_PREFIX.startswith(
        category_prefix(OpsCategory.CONTROL, control=ControlSubtype.STATE, domain="transit"))
    assert config.LOADER_PENDING_PREFIX == "ops/control/state/transit/loader_pending/"
    # 쓰기와 읽기가 같은 값을 쓴다 — 갈리면 마커 고립.
    assert loader.pending_key("parking", "20260715T110000Z", "r").startswith(
        config.LOADER_PENDING_PREFIX)
    # 구경로 드레인은 유지된다(#547 — 소진 확인 전 제거 금지).
    assert config.LOADER_PENDING_LEGACY_PREFIXES


# ── envelope 파싱 (지하철·주차) ───────────────────────────────────────────────────
def test_build_rows_subway_arrival():
    page = json.dumps({
        "errorMessage": {"code": "INFO-000"},
        "realtimeArrivalList": [
            {"statnNm": "강남", "recptnDt": "2026-07-15 12:00:00", "btrainNo": "1"},
            {"statnNm": "잠실", "recptnDt": "2026-07-15 12:00:05", "btrainNo": "2"},
        ],
    }, ensure_ascii=False).encode("utf-8")
    rows = loader.build_rows(_marker("subway_arrival"), {}, [page])
    assert len(rows) == 2
    source, ts_source, ts_collected, lat, lon, raw = rows[0]
    # source 컬럼 = dataset 명 — 분리 전 인라인 적재 값과 연속(marker source 아님)
    assert source == "subway_arrival" and ts_source == "2026-07-15 12:00:00"
    assert ts_collected == "2026-07-15 12:00:00" and lat is None and lon is None
    assert json.loads(raw)["statnNm"] == "강남"


def test_build_rows_parking_extracts_nested_rows():
    page = json.dumps({
        "GetParkingInfo": {
            "RESULT": {"CODE": "INFO-000"},
            "row": [{"PKLT_NM": "시청", "NOW_PRK_VHCL_UPDT_TM": "2026-07-15 11:59:00"}],
        },
    }, ensure_ascii=False).encode("utf-8")
    rows = loader.build_rows(_marker("parking"), {}, [page])
    assert len(rows) == 1
    assert rows[0][1] == "2026-07-15 11:59:00"          # ts_source
    assert json.loads(rows[0][5])["PKLT_NM"] == "시청"  # raw


# ── 버스 파싱 (1페이지=1노선 XML) ─────────────────────────────────────────────────
def test_build_rows_bus_position_counts_items_and_aligns_routes():
    ok_xml = "<msgHeader><headerCd>0</headerCd></msgHeader><itemList>a</itemList><itemList>b</itemList>"
    bad_xml = "<msgHeader><headerCd>4</headerCd></msgHeader>"
    manifest = {"request_params": {"busRouteId": ["100100025", "100100454"]}}
    rows = loader.build_rows(
        _marker("bus_position"), manifest,
        [ok_xml.encode("utf-8"), bad_xml.encode("utf-8")],
    )
    assert [r[2] for r in rows] == ["100100025", "100100454"]  # bus_route_id 정합
    assert rows[0][4] == 2 and rows[1][4] == -1                # rows_cnt / 오류 -1
    assert rows[0][5] == ok_xml                                # raw 원본 보존


def test_build_rows_bus_route_page_mismatch_raises():
    manifest = {"request_params": {"busRouteId": ["1"]}}
    with pytest.raises(ValueError):
        loader.build_rows(_marker("bus_position"), manifest, [b"<a/>", b"<b/>"])


# ── 버스 번들(JSONL, #369 번들링) ─────────────────────────────────────────────────
def test_build_rows_bus_bundle_jsonl():
    # collector(_land_objects)와 동일한 직렬화 — 1행=1노선 {busRouteId, rows, raw}
    ok_xml = "<msgHeader><headerCd>0</headerCd></msgHeader><itemList>가</itemList>"
    bundle = "\n".join([
        json.dumps({"busRouteId": "100100025", "rows": 1, "raw": ok_xml}, ensure_ascii=False),
        json.dumps({"busRouteId": "100100454", "rows": -1, "raw": "<h/>"}, ensure_ascii=False),
        "",  # 빈 줄 허용
    ])
    manifest = {"request_params": {"busRouteId": ["100100025", "100100454"], "bundle": "jsonl"}}
    rows = loader.build_rows(_marker("bus_position"), manifest, [bundle.encode("utf-8")])
    assert [r[2] for r in rows] == ["100100025", "100100454"]
    assert rows[0][4] == 1 and rows[1][4] == -1   # rows_cnt 는 collector 계산값 신뢰
    assert rows[0][5] == ok_xml                    # 원본 XML(유니코드 포함) 보존


def test_build_rows_bus_legacy_pages_still_supported():
    # 번들 전환 시점에 남아 있던 구형(노선당 1페이지) 마커도 계속 처리 가능해야 한다
    manifest = {"request_params": {"busRouteId": ["1"]}}
    rows = loader.build_rows(
        _marker("bus_position"), manifest,
        [b"<msgHeader><headerCd>0</headerCd></msgHeader><itemList>x</itemList>"],
    )
    assert rows[0][4] == 1


# ── 청크 INSERT ──────────────────────────────────────────────────────────────────
def test_chunk_values_respects_char_cap():
    values = ["(" + "x" * 99 + ")"] * 10  # 원소당 101자
    chunks = list(loader.chunk_values(values, max_chars=300))
    assert all(sum(len(v) for v in c) <= 300 or len(c) == 1 for c in chunks)
    assert sum(len(c) for c in chunks) == 10
    assert max(len(c) for c in chunks) == 2  # 300 캡 → 2개씩


def test_chunk_values_oversized_single_element_passes_alone():
    big = "(" + "x" * 1000 + ")"
    chunks = list(loader.chunk_values(["(a)", big, "(b)"], max_chars=100))
    assert [len(c) for c in chunks] == [1, 1, 1]
    assert chunks[1] == [big]


def test_value_literal_shapes():
    env = loader.value_literal(
        ("s", None, "tc", None, None, "raw{'q'}"),
        ingested_at="2026-07-15 03:00:00.000000", dag_run_id="r-1", shape="envelope",
    )
    assert env.startswith("('s', NULL, 'tc', NULL, NULL,") and "TIMESTAMP" in env
    bus = loader.value_literal(
        ("s", "bus_position", "100100025", "tc", -1, "<xml/>"),
        ingested_at="2026-07-15 03:00:00.000000", dag_run_id="r-1", shape="bus",
    )
    assert ", -1, " in bus  # rows_cnt 는 정수 리터럴


def test_create_table_ddl_matches_legacy_schema():
    env_ddl = loader.create_table_ddl("c.s.t", "envelope")
    for col in ("source varchar", "ts_source", "lat", "lon", "raw varchar",
                "ingested_at timestamp(6)", "dag_run_id"):
        assert col in env_ddl
    bus_ddl = loader.create_table_ddl("c.s.t", "bus")
    for col in ("bus_route_id", "rows_cnt integer"):
        assert col in bus_ddl
