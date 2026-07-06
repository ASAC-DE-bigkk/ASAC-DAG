"""#146 — KOPIS 자정 대량 400 대응 2건.

① 400/429 1회 백오프 재시도: '일시(자정 rate-limit)'와 '지속(진짜 범위 밖/오류)'을
   구분한다. 7/4·7/5 실증: rate-limit 400 이 #84 오버슛 처리에 '목록 끝'으로 오인돼
   목록이 1페이지(100행)에서 조용히 절단됐다.
② detail 크롤의 목록 재조회 제거: 같은 run 에 이미 랜딩된 sibling 목록 raw 에서
   id 를 재사용(자정 호출 감축). 목록이 아직 안 랜딩됐으면 기존 API 재조회로 폴백.
"""
from __future__ import annotations

import json

import pytest
import requests

from culture_ingest.common.config import RunContext
from culture_ingest.common.http import Page
from culture_ingest.common.landing import Landing, LocalSink
from culture_ingest.source.clients import KopisClient
from culture_ingest.source.datasets import BY_NAME
from culture_ingest.source.ingest import IngestOptions, ingest_dataset


def _xml_page(*ids: str) -> bytes:
    rows = "".join(f"<db><mt20id>{i}</mt20id></db>" for i in ids)
    return f"<dbs>{rows}</dbs>".encode()


class _Resp:
    def __init__(self, status: int, body: bytes = b""):
        self.status_code = status
        self.content = body

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.HTTPError(f"{self.status_code} Client Error")
            err.response = self
            raise err


class _SeqSession:
    """cpage 별 응답 '시퀀스'를 돌려주는 스텁 — 같은 페이지 재요청 시 다음 원소."""

    def __init__(self, sequences: dict[int, list[_Resp]]):
        self._seq = {k: list(v) for k, v in sequences.items()}
        self.calls: list[int] = []

    def get(self, url, params=None, timeout=None):
        page = params["cpage"]
        self.calls.append(page)
        seq = self._seq[page]
        return seq.pop(0) if len(seq) > 1 else seq[0]


def _client(sequences: dict[int, list[_Resp]]) -> KopisClient:
    c = KopisClient(service_key="test-key-123")
    c.session = _SeqSession(sequences)
    c.retry_delay_sec = 0  # 테스트에서 sleep 제거
    return c


# ── ① 400 재시도 (일시 vs 지속 구분) ──────────────────────────────────────────

def test_transient_400_retried_and_list_continues():
    """자정 rate-limit 흉내: 2페이지 첫 요청 400 → 재시도 성공 → 목록이 계속된다."""
    c = _client({
        1: [_Resp(200, _xml_page("A", "B"))],
        2: [_Resp(400), _Resp(200, _xml_page("C", "D"))],  # 일시 400 후 회복
        3: [_Resp(200, _xml_page("E"))],                    # 짧은 페이지 = 끝
    })
    pages = list(c.list_pages("pblprfr", {}, rows=2, max_pages=None))
    assert [p.row_count for p in pages] == [2, 2, 1], "일시 400 이 목록을 끊으면 안 됨"
    assert c.session.calls == [1, 2, 2, 3]


def test_persistent_overshoot_400_still_ends_list():
    """진짜 범위 밖(지속 400)은 재시도 후에도 400 → #84 대로 목록 끝."""
    c = _client({
        1: [_Resp(200, _xml_page("A", "B"))],
        2: [_Resp(200, _xml_page("C", "D"))],
        3: [_Resp(400)],  # 지속 400 (재요청에도 같은 응답)
    })
    pages = list(c.list_pages("pblprfr", {}, rows=2, max_pages=None))
    assert [p.row_count for p in pages] == [2, 2]
    assert c.session.calls == [1, 2, 3, 3]  # 재시도로 2회 확인 후 종료


def test_first_page_persistent_400_raises():
    """1페이지 지속 400 = 잘못된 파라미터/키 — 재시도 후에도 실패면 raise."""
    c = _client({1: [_Resp(400)]})
    with pytest.raises(requests.HTTPError):
        list(c.list_pages("pblprfr", {}, rows=2, max_pages=None))
    assert c.session.calls == [1, 1]


def test_transient_429_also_retried():
    c = _client({
        1: [_Resp(429), _Resp(200, _xml_page("A"))],
    })
    pages = list(c.list_pages("pblprfr", {}, rows=2, max_pages=None))
    assert [p.row_count for p in pages] == [1]


# ── ② detail 목록 재조회 제거 (랜딩된 raw 재사용 + API 폴백) ──────────────────

class _DetailOnlyKopis:
    """detail 만 허용 — 목록 API(list_ids/list_pages)가 불리면 실패시키는 스텁."""

    def __init__(self, allow_list_ids: list[str] | None = None):
        self.detail_ids: list[str] = []
        self._fallback_ids = allow_list_ids

    def detail(self, path: str, identifier: str) -> Page:
        self.detail_ids.append(identifier)
        return Page(index=1, body=_xml_page(identifier), row_count=1, ext="xml")

    def list_ids(self, *a, **k):
        if self._fallback_ids is None:
            raise AssertionError("목록 재조회(list_ids)가 호출되면 안 됨(#146)")
        return list(self._fallback_ids)


class _Clients:
    def __init__(self, kopis):
        self.kopis = kopis
        self.seoul = None


def _landing(tmp_path) -> Landing:
    ctx = RunContext(load_date="2026-07-06", ingest_ts="20260706T000000Z", run_id="test")
    return Landing(LocalSink(str(tmp_path)), "raw/culture", ctx)


def _land_sibling_list(landing: Landing, *ids: str) -> None:
    """kopis_performance 목록 raw(페이지+매니페스트)를 같은 run 에 미리 랜딩."""
    prefix = landing.prefix_for("kopis", "kopis_performance")
    key = landing.write_page(prefix, "page-0001.xml", _xml_page(*ids), "xml")
    landing.write_manifest(prefix, {"dataset": "kopis_performance", "rows": len(ids),
                                    "object_keys": [key]})


def test_detail_reuses_landed_list_ids(tmp_path):
    landing = _landing(tmp_path)
    _land_sibling_list(landing, "PF001", "PF002", "PF003")
    kopis = _DetailOnlyKopis(allow_list_ids=None)  # list_ids 호출 시 즉사
    res = ingest_dataset(BY_NAME["kopis_performance_detail"], _Clients(kopis), landing,
                         IngestOptions(include_detail=True, max_detail=200))
    assert not res.error
    assert kopis.detail_ids == ["PF001", "PF002", "PF003"]


def test_detail_respects_max_detail_from_landed_ids(tmp_path):
    landing = _landing(tmp_path)
    _land_sibling_list(landing, "PF001", "PF002", "PF003")
    kopis = _DetailOnlyKopis(allow_list_ids=None)
    ingest_dataset(BY_NAME["kopis_performance_detail"], _Clients(kopis), landing,
                   IngestOptions(include_detail=True, max_detail=2))
    assert kopis.detail_ids == ["PF001", "PF002"]


def test_detail_falls_back_to_api_when_list_not_landed(tmp_path):
    landing = _landing(tmp_path)  # sibling 목록 랜딩 없음
    kopis = _DetailOnlyKopis(allow_list_ids=["PF900"])
    res = ingest_dataset(BY_NAME["kopis_performance_detail"], _Clients(kopis), landing,
                         IngestOptions(include_detail=True, max_detail=200))
    assert not res.error
    assert kopis.detail_ids == ["PF900"], "목록 미랜딩 시 기존 API 경로로 폴백해야 함"
