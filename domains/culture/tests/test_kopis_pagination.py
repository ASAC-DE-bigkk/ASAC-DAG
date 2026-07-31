"""KOPIS 목록 페이지네이션 오버슛 400 처리 (#84 · #152 Transport 경계).

총 행수가 페이지 크기의 정확한 배수면 마지막 페이지가 꽉 차고, 루프가 다음
페이지를 조회한다. KOPIS는 범위 밖 페이지에 HTTP 400을 주는데 이건 '목록 끝'
신호지 오류가 아니다 — 단, 1페이지의 400(잘못된 파라미터/키)은 진짜 오류.
"""
import pytest

from common.http.contract import TransportResponse
from common.http.core import HttpCore
from common.http.errors import HttpProblemError
from culture_ingest.source.clients import KopisClient


def _xml_page(n_rows: int) -> bytes:
    return ("<dbs>" + "<db><x/></db>" * n_rows + "</dbs>").encode()


def _resp(status: int, body: bytes = b"") -> TransportResponse:
    return TransportResponse(status=status, content=body)


class _FakeTransport:
    """cpage 별로 준비된 응답을 돌려주는 Transport 스텁."""

    def __init__(self, responses: dict[int, TransportResponse]):
        self._responses = responses
        self.calls: list[int] = []

    def send(self, method, url, *, params, headers, timeout):
        page = params["cpage"]
        self.calls.append(page)
        return self._responses[page]


def _client(responses: dict[int, TransportResponse]) -> tuple[KopisClient, _FakeTransport]:
    transport = _FakeTransport(responses)
    core = HttpCore(source="kopis", transport=transport, rate_limit=None,
                    sleep=lambda s: None)
    c = KopisClient(service_key="test-key", retry_delay_sec=0, core=core)
    return c, transport


def test_overshoot_400_after_full_pages_ends_list():
    # 페이지 1·2가 rows(=2)만큼 꽉 참 → 3페이지 조회 시 400 = 목록 끝(정상 종료)
    c, transport = _client({1: _resp(200, _xml_page(2)),
                            2: _resp(200, _xml_page(2)),
                            3: _resp(400)})
    pages = list(c.list_pages("pblprfr", {}, rows=2, max_pages=None))
    assert [p.row_count for p in pages] == [2, 2]
    # #146: 400 은 1회 재시도로 '지속' 확인 → #201: cpage=1 이 살아있는지 확인 후 끝 판정
    assert transport.calls == [1, 2, 3, 3, 1]


def test_first_page_400_still_raises():
    # 1페이지 400 = 잘못된 파라미터/키 — 삼키면 안 된다
    c, _ = _client({1: _resp(400)})
    with pytest.raises(HttpProblemError):
        list(c.list_pages("pblprfr", {}, rows=2, max_pages=None))


def test_partial_last_page_unaffected():
    # 마지막 페이지가 덜 찬 기존 경로는 오버슛 조회 없이 그대로 종료
    c, transport = _client({1: _resp(200, _xml_page(2)),
                            2: _resp(200, _xml_page(1))})
    pages = list(c.list_pages("pblprfr", {}, rows=2, max_pages=None))
    assert [p.row_count for p in pages] == [2, 1]
    assert transport.calls == [1, 2]  # 3페이지 조회 안 함
