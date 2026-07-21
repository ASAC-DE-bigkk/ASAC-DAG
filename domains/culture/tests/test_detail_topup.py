"""#466 야간 facility detail top-up(missing 모드) 테스트."""
from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import Landing, LocalSink
from culture_ingest.source.datasets import BY_NAME
from culture_ingest.source.ingest import IngestOptions, ingest_dataset
from culture_ingest.source.clients import Page


def _xml_facility_page(*ids: str) -> bytes:
    rows = "".join(f"<db><mt10id>{i}</mt10id><fcltynm>시설{i}</fcltynm></db>" for i in ids)
    return f'<?xml version="1.0" encoding="UTF-8"?><dbs>{rows}</dbs>'.encode()


class _DetailOnlyKopis:
    """detail 만 허용 — 목록 API(list_ids)는 allow_list_ids 있을 때만 응답하는 스텁."""

    def __init__(self, allow_list_ids: list[str] | None = None):
        self.detail_ids: list[str] = []
        self._fallback_ids = allow_list_ids
        self.list_ids_limit = "unset"

    def detail(self, path: str, identifier: str) -> Page:
        self.detail_ids.append(identifier)
        return Page(index=1, body=_xml_facility_page(identifier), row_count=1, ext="xml")

    def list_ids(self, path, base_params, id_field, limit):
        if self._fallback_ids is None:
            raise AssertionError("목록이 랜딩돼 있으면 API 재조회가 없어야 함(#146)")
        self.list_ids_limit = limit
        return list(self._fallback_ids)


class _Clients:
    def __init__(self, kopis):
        self.kopis = kopis
        self.seoul = None


def _landing(tmp_path) -> Landing:
    ctx = RunContext(load_date="2026-07-21", ingest_ts="20260721T000000Z", run_id="test")
    return Landing(LocalSink(str(tmp_path)), "raw/culture", ctx)


def _land_facility_list(landing: Landing, *ids: str) -> None:
    prefix = landing.prefix_for("kopis", "kopis_facility")
    key = landing.write_page(prefix, "page-0001.xml", _xml_facility_page(*ids), "xml")
    landing.write_manifest(prefix, {"dataset": "kopis_facility", "rows": len(ids),
                                    "object_keys": [key]})


DS = BY_NAME["kopis_facility_detail"]


def _opts(**kw) -> IngestOptions:
    base = dict(include_detail=True, max_detail=200, detail_mode="missing")
    base.update(kw)
    return IngestOptions(**base)


def test_missing_mode_fetches_only_unknown_ids(tmp_path):
    landing = _landing(tmp_path)
    _land_facility_list(landing, "FC001", "FC002", "FC003", "FC004")
    kopis = _DetailOnlyKopis()
    res = ingest_dataset(DS, _Clients(kopis), landing,
                         _opts(known_detail_ids=["FC001", "FC003"]))
    assert not res.error
    assert kopis.detail_ids == ["FC002", "FC004"]  # 안티조인: 목록 − 기존


def test_missing_mode_caps_after_diff(tmp_path):
    # cap 은 차집합 **이후** — 목록 후미의 신규 시설을 cap 이 가리면 안 된다(#466)
    landing = _landing(tmp_path)
    _land_facility_list(landing, "FC001", "FC002", "FC003", "FC004")
    kopis = _DetailOnlyKopis()
    ingest_dataset(DS, _Clients(kopis), landing,
                   _opts(max_detail=1, known_detail_ids=["FC001", "FC002", "FC003"]))
    assert kopis.detail_ids == ["FC004"]  # 기존 3건을 건너뛰고 신규 1건에 cap 적용


def test_missing_mode_skips_when_no_missing(tmp_path):
    landing = _landing(tmp_path)
    _land_facility_list(landing, "FC001", "FC002")
    kopis = _DetailOnlyKopis()
    res = ingest_dataset(DS, _Clients(kopis), landing,
                         _opts(known_detail_ids=["FC001", "FC002"]))
    assert res.error == "skipped (detail top-up: no missing ids)"
    assert kopis.detail_ids == []  # API 호출 0


def test_missing_mode_skips_when_known_ids_unavailable(tmp_path):
    # plan 의 bronze 조회 실패(fail-open) → known=None → top-up skip, 런은 계속
    landing = _landing(tmp_path)
    _land_facility_list(landing, "FC001")
    res = ingest_dataset(DS, _Clients(_DetailOnlyKopis()), landing,
                         _opts(known_detail_ids=None))
    assert res.error == "skipped (detail top-up: bronze id set unavailable)"


def test_missing_mode_falls_back_to_api_when_list_not_landed(tmp_path):
    # 매핑 태스크 병렬이라 목록 미착지가 정상 경로에서 발생(E2E 실측 7/21) —
    # full 모드처럼 API 재조회로 폴백하되, 안티조인은 그대로 적용한다. limit=None(전체).
    landing = _landing(tmp_path)  # 목록 랜딩 없음
    kopis = _DetailOnlyKopis(allow_list_ids=["FC001", "FC002", "FC003"])
    res = ingest_dataset(DS, _Clients(kopis), landing,
                         _opts(known_detail_ids=["FC001", "FC002"]))
    assert not res.error
    assert kopis.detail_ids == ["FC003"]  # 폴백 목록에도 안티조인 적용
    assert kopis.list_ids_limit is None   # 전체 목록 — cap 은 차집합 후


def test_full_mode_keeps_current_behavior(tmp_path):
    # 주간 전수(full)는 안티조인 없이 목록 앞에서부터 max_detail 개 — 현행 동작 보존
    landing = _landing(tmp_path)
    _land_facility_list(landing, "FC001", "FC002", "FC003")
    kopis = _DetailOnlyKopis()
    res = ingest_dataset(DS, _Clients(kopis), landing,
                         _opts(detail_mode="full", max_detail=2,
                               known_detail_ids=["FC001"]))  # full 은 known 무시
    assert not res.error
    assert kopis.detail_ids == ["FC001", "FC002"]


# ── plan-측 기존 ID 로드 (Trino, fail-open) ──────────────────────────────────

from culture_ingest.source.ingest import load_existing_detail_ids


class _FakeTrinoWarehouse:
    """BronzeWarehouse 실인터페이스 미러 — execute 는 .client 에 있다(E2E 실측으로 확정)."""

    def __init__(self, rows=None, error: Exception | None = None):
        self._rows = rows or []
        self._error = error
        self.sql: str | None = None
        self.client = self  # 실물처럼 .client.execute 경로 제공

    def qualified(self, dataset: str) -> str:
        return f"iceberg.culture.bronze_{dataset}"

    def execute(self, sql: str):
        self.sql = sql
        if self._error:
            raise self._error
        return self._rows


def test_load_existing_detail_ids_returns_distinct_ids():
    wh = _FakeTrinoWarehouse(rows=[["FC001"], ["FC002"], [None]])
    ids = load_existing_detail_ids("dev", warehouse=wh)
    assert ids == ["FC001", "FC002"]  # None/빈 값 행은 제거
    assert "json_extract_scalar(record_json, '$.mt10id')" in wh.sql
    assert "bronze_kopis_facility_detail" in wh.sql


def test_load_existing_detail_ids_fails_open():
    wh = _FakeTrinoWarehouse(error=RuntimeError("trino down"))
    assert load_existing_detail_ids("dev", warehouse=wh) is None  # fail-open → top-up skip


def test_missing_mode_ignores_non_flagged_detail(tmp_path):
    # missing_only_nightly=False 인 다른 detail(공연 상세)은 missing 모드여도 현행 동작
    landing = _landing(tmp_path)
    prefix = landing.prefix_for("kopis", "kopis_performance")
    body = ('<?xml version="1.0"?><dbs><db><mt20id>PF001</mt20id>'
            "<prfnm>공연</prfnm></db></dbs>").encode()
    key = landing.write_page(prefix, "page-0001.xml", body, "xml")
    landing.write_manifest(prefix, {"dataset": "kopis_performance", "rows": 1,
                                    "object_keys": [key]})
    kopis = _DetailOnlyKopis()
    res = ingest_dataset(BY_NAME["kopis_performance_detail"], _Clients(kopis), landing,
                         _opts(known_detail_ids=[]))
    assert not res.error
    assert kopis.detail_ids == ["PF001"]
