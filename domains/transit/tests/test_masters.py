"""transit 마스터 수집 순수 로직 단위 테스트 (#162).

가짜 Transport(공통 HttpCore 주입) + 가짜 Storage 로 실호출 없이 검증:
- iter_master: collected>=list_total_count / short page 종료, 페이지 폭주 가드
- RESULT.CODE 처리: INFO-200(데이터 없음) 종료, 그 외 코드 RuntimeError(코드+메시지 노출)
- 최상위 RESULT(서비스 엔벨로프 없음): INFO-200→빈 스냅샷 종료, 그 외→코드 포함 raise
- land_master: R2 경로 규약(raw/transit/<source_system>/<dataset>/…) + manifest, 빈 스냅샷 raise
- load_action: load_date 멱등/자가치유 결정(insert/skip/reload)
- column_names: 원천 필드(대문자) → 소문자 스네이크 컬럼
- 경로 키(PathKey) 가 로그/예외에서 redact 되고 manifest 에 남지 않음
- 실제 스펙(subwayStationMaster·GetParkInfo) 계약 회귀
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

from common.http.contract import TransportResponse  # noqa: E402
from common.security import redact  # noqa: E402
from common.storage import Storage  # noqa: E402
from seoul_transit import masters  # noqa: E402
from seoul_transit.masters import MasterSpec  # noqa: E402

_KEY = "SEOULOPENAPI-pathkey-abcdef0123456789longsecret"

# 테스트용 소형 스펙 — per_page 를 작게 잡아 페이지네이션 경로를 탄다.
_SPEC = MasterSpec(
    dataset="unit_master",
    service="GetParkInfo",
    source_system="seoul_parking",
    table="bronze_unit_master",
    fields=("PKLT_CD", "PKLT_NM", "LAT", "LOT"),
    expected_rows=3,  # 랜딩 테스트 3행과 일치 — manifest 전달 검증이 None==None 으로 무력화되지 않게
)


class FakeTransport:
    """스크립트된 응답을 순서대로 돌려주는 가짜 Transport(계약: contract.Transport)."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    def send(self, method, url, *, params, headers, timeout):
        self.calls.append({"method": method, "url": url,
                           "params": dict(params or {}), "headers": dict(headers or {})})
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class RepeatingTransport:
    """항상 같은 응답 — 종료 조건 미충족(폭주 가드) 테스트용."""

    def __init__(self, response):
        self.response = response
        self.calls = []

    def send(self, method, url, *, params, headers, timeout):
        self.calls.append({"url": url})
        return self.response


class FakeStorage(Storage):
    """put 을 메모리에 담는 가짜 Storage(write_json 은 base 구현 경유)."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def write_bytes(self, key, data):
        self.objects[key] = data

    def read_bytes(self, key):
        return self.objects[key]

    def exists(self, key):
        return key in self.objects

    def list_keys(self, prefix):
        return sorted(k for k in self.objects if k.startswith(prefix))

    def delete(self, key):
        self.objects.pop(key, None)


def _page(rows, *, total, code="INFO-000", service=_SPEC.service):
    body = {service: {
        "list_total_count": total,
        "RESULT": {"CODE": code, "MESSAGE": "정상 처리되었습니다"},
        "row": rows,
    }}
    return TransportResponse(status=200,
                             content=json.dumps(body, ensure_ascii=False).encode("utf-8"))


def _top_level_result(code, message="처리 결과", *, service=_SPEC.service):
    """서비스 엔벨로프({service:{...}}) 없이 **최상위** RESULT 만 있는 응답(인증/데이터없음)."""
    body = {"RESULT": {"CODE": code, "MESSAGE": message}}
    assert service not in body  # 서비스 키가 정말로 없어야 최상위 경로를 탄다
    return TransportResponse(status=200,
                             content=json.dumps(body, ensure_ascii=False).encode("utf-8"))


def _row(cd, nm="주차장", lat="37.5", lot="127.0"):
    return {"PKLT_CD": cd, "PKLT_NM": nm, "LAT": lat, "LOT": lot}


def _client(transport):
    return masters.build_client(masters.build_core(transport=transport), _KEY)


# ── column_names 매핑 ─────────────────────────────────────────────────────────────
def test_column_names_lowercase_snake():
    assert masters.column_names(_SPEC) == ["pklt_cd", "pklt_nm", "lat", "lot"]
    assert masters.column_names(masters.SUBWAY_STATION_MASTER) == [
        "bldn_id", "bldn_nm", "route", "lat", "lot"]


# ── 실제 스펙 계약 회귀(서비스명·행수·계보 정합) ─────────────────────────────────
def test_specs_registered_and_contract():
    assert set(masters.SPECS) == {"subway_station_master", "park_info_master"}
    sub = masters.SUBWAY_STATION_MASTER
    park = masters.PARK_INFO_MASTER
    assert sub.service == "subwayStationMaster" and sub.source_system == "seoul_subway"
    assert sub.table == "bronze_subway_station_master"
    assert park.service == "GetParkInfo" and park.source_system == "seoul_parking"
    assert park.table == "bronze_park_info_master"
    # GetParkInfo(마스터) 는 실시간 GetParkingInfo 와 혼동 금지.
    assert park.service != "GetParkingInfo"
    # 좌표 필드가 두 원천 모두에 있어야 한다(공간연계 목적).
    assert {"LAT", "LOT"} <= set(sub.fields)
    assert {"LAT", "LOT"} <= set(park.fields)


# ── iter_master 종료 조건 ────────────────────────────────────────────────────────
def test_iter_master_terminates_on_total_reached():
    # per_page=2, total=3: [2행, 1행] → 2페이지 누적 3 == total 에서 종료.
    p1 = _page([_row("A"), _row("B")], total=3)
    p2 = _page([_row("C")], total=3)
    client = _client(FakeTransport([p1, p2]))
    pages = list(masters.iter_master(client, _SPEC, per_page=2))
    assert [p[0] for p in pages] == [1, 2]
    assert sum(len(rows) for _, _, rows in pages) == 3


def test_iter_master_terminates_on_short_page():
    # total 이 실제보다 큼(4) → collected>=total 로는 못 끝냄. short page(1<2)로 종료.
    p1 = _page([_row("A"), _row("B")], total=4)
    p2 = _page([_row("C")], total=4)
    client = _client(FakeTransport([p1, p2]))
    pages = list(masters.iter_master(client, _SPEC, per_page=2))
    assert [p[0] for p in pages] == [1, 2]
    assert sum(len(rows) for _, _, rows in pages) == 3


def test_iter_master_page_overflow_guard():
    # 항상 full page(2행)이고 total 무한대 → 종료 안 됨 → _MAX_PAGES 초과 시 RuntimeError.
    full = _page([_row("A"), _row("B")], total=10_000)
    client = masters.build_client(masters.build_core(transport=RepeatingTransport(full)), _KEY)
    with pytest.raises(RuntimeError, match="폭주"):
        list(masters.iter_master(client, _SPEC, per_page=2))


def test_iter_master_no_data_code_stops_empty():
    # INFO-200(데이터 없음) → yield 없이 조용히 종료.
    p1 = _page([], total=0, code="INFO-200")
    client = _client(FakeTransport([p1]))
    assert list(masters.iter_master(client, _SPEC, per_page=2)) == []


def test_iter_master_error_code_raises():
    p1 = _page([], total=0, code="INFO-100")  # 인증키 오류류
    client = _client(FakeTransport([p1]))
    with pytest.raises(RuntimeError, match="INFO-100"):
        list(masters.iter_master(client, _SPEC, per_page=2))


def test_iter_master_error_code_exposes_message():
    # 엔벨로프 내부 RESULT 오류코드는 MESSAGE 도 예외에 노출돼야 한다(운영 진단용).
    body = {_SPEC.service: {"list_total_count": 0,
                            "RESULT": {"CODE": "ERROR-500", "MESSAGE": "서버 오류"},
                            "row": []}}
    resp = TransportResponse(status=200,
                             content=json.dumps(body, ensure_ascii=False).encode("utf-8"))
    client = _client(FakeTransport([resp]))
    with pytest.raises(RuntimeError, match="MESSAGE=서버 오류"):
        list(masters.iter_master(client, _SPEC, per_page=2))


# ── 최상위 RESULT(서비스 엔벨로프 부재) 처리 ─────────────────────────────────────
def test_iter_master_top_level_no_data_stops_empty():
    # 서비스 엔벨로프 없이 최상위 RESULT INFO-200 → 빈 스냅샷으로 조용히 종료.
    client = _client(FakeTransport([_top_level_result("INFO-200", "데이터가 없습니다")]))
    assert list(masters.iter_master(client, _SPEC, per_page=2)) == []


def test_iter_master_top_level_error_raises_with_code_and_message():
    # 최상위 RESULT 인증오류(INFO-100) → 코드+메시지 포함 RuntimeError(엔벨로프 없음 아님).
    client = _client(FakeTransport([_top_level_result("INFO-100", "인증키가 유효하지 않습니다")]))
    with pytest.raises(RuntimeError, match=r"INFO-100.*인증키가 유효하지 않습니다"):
        list(masters.iter_master(client, _SPEC, per_page=2))


def test_iter_master_formless_response_raises_envelope_missing():
    # 서비스 키도, 최상위 RESULT.CODE 도 없는 완전 무형식 → 기존 '엔벨로프 없음' 에러 유지.
    resp = TransportResponse(status=200, content=b'{"unexpected": true}')
    client = _client(FakeTransport([resp]))
    with pytest.raises(RuntimeError, match="엔벨로프 없음"):
        list(masters.iter_master(client, _SPEC, per_page=2))


# ── load_action: load_date 멱등/자가치유 결정 ────────────────────────────────────
def test_load_action_first_load_inserts():
    assert masters.load_action(existing_rows=0, landed_rows=784) == "insert"


def test_load_action_true_idempotent_skips():
    assert masters.load_action(existing_rows=784, landed_rows=784) == "skip"


def test_load_action_partial_or_drift_reloads():
    # partial(배치 중간 실패로 300행만 남음) 또는 당일 원천 변동(2204→2210) → reload.
    assert masters.load_action(existing_rows=300, landed_rows=784) == "reload"
    assert masters.load_action(existing_rows=2204, landed_rows=2210) == "reload"


# ── land_master 경로 규약 + manifest ─────────────────────────────────────────────
def test_land_master_path_convention_and_manifest():
    store = FakeStorage()
    pages = [
        (1, b'{"a": 1}', [{"x": 1}, {"x": 2}]),
        (2, b'{"a": 2}', [{"x": 3}]),
    ]
    result = masters.land_master(
        iter(pages), _SPEC, run_id="run-xyz",
        storage=store, load_date="2026-07-06", ingest_ts="20260706T120000Z",
    )
    # transit 문서 규약: raw/transit/<source_system>/<dataset>/… (source 세그먼트 포함).
    base = ("raw/transit/seoul_parking/unit_master"
            "/load_date=2026-07-06/ingest_ts=20260706T120000Z")
    assert result["object_keys"] == [f"{base}/page-0001.json", f"{base}/page-0002.json"]
    assert result["manifest_key"] == f"{base}/_manifest.json"
    assert result["rows"] == 3

    manifest = json.loads(store.read_bytes(f"{base}/_manifest.json").decode("utf-8"))
    assert manifest["dataset"] == "unit_master"
    assert manifest["source_system"] == "seoul_parking"
    assert manifest["service"] == "GetParkInfo"
    assert manifest["pages"] == 2
    assert manifest["rows"] == 3
    assert manifest["run_id"] == "run-xyz"
    assert manifest["object_keys"] == result["object_keys"]
    # 완결 확인서 필드(ASK-Seoul#60 약속③, #547).
    assert manifest["status"] == "ok"
    assert manifest["completed_at"]  # ISO8601 존재만 — 시각 자체는 실행 시점 의존
    assert manifest["expected_rows"] == 3  # _SPEC.expected_rows 실값 전달 확인
    # R1 순서 — 확인서가 마지막 쓰기여야 한다(쓰다 만 랜딩에 확인서가 없도록).
    assert list(store.objects)[-1] == result["manifest_key"]


def test_land_master_empty_snapshot_raises():
    # 마스터는 절대 0행일 수 없다 — 빈 스냅샷은 원천 이상 신호 → RuntimeError, manifest 미기록.
    store = FakeStorage()
    with pytest.raises(RuntimeError, match="빈 마스터 스냅샷"):
        masters.land_master(
            iter([(1, b'{"row": []}', [])]), _SPEC, run_id="run-empty",
            storage=store, load_date="2026-07-06", ingest_ts="20260706T120000Z",
        )
    # 빈 스냅샷은 _manifest.json 도 남기지 않아야 한다(green 오염 방지).
    assert not any(k.endswith("_manifest.json") for k in store.objects)


# ── rows_from_document ────────────────────────────────────────────────────────────
def test_rows_from_document_reads_envelope():
    doc = json.loads(_page([_row("A"), _row("B")], total=2).content.decode("utf-8"))
    rows = masters.rows_from_document(doc, _SPEC)
    assert [r["PKLT_CD"] for r in rows] == ["A", "B"]


def test_rows_from_document_missing_envelope_raises():
    with pytest.raises(RuntimeError):
        masters.rows_from_document({"other": {}}, _SPEC)


# ── 경로 키(PathKey) 미노출 ───────────────────────────────────────────────────────
def test_path_key_is_redacted_in_url():
    transport = FakeTransport([_page([_row("A")], total=1)])
    client = _client(transport)
    masters.fetch_page(client, _SPEC, 1, 1000)
    url = transport.calls[0]["url"]
    # 서울식은 키가 URL 경로에 박힌다(원본 url 엔 존재) — 하지만 redact 후엔 사라져야 한다.
    assert _KEY in url
    scrubbed = redact(url)
    assert _KEY not in scrubbed
    assert "REDACTED" in scrubbed


def test_key_not_in_manifest():
    store = FakeStorage()
    masters.land_master(
        iter([(1, b'{"row": [{"PKLT_CD": "1"}]}', [{"PKLT_CD": "1"}])]),
        _SPEC, run_id="run-1", storage=store,
    )
    blob = b"".join(store.objects.values())
    assert _KEY.encode("utf-8") not in blob
