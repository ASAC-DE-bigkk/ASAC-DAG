"""KOPIS 목록 페이지네이션 오버슛 400 처리 (#84).

총 행수가 페이지 크기의 정확한 배수면 마지막 페이지가 꽉 차고, 루프가 다음
페이지를 조회한다. KOPIS는 범위 밖 페이지에 HTTP 400을 주는데 이건 '목록 끝'
신호지 오류가 아니다 — 단, 1페이지의 400(잘못된 파라미터/키)은 진짜 오류.
"""
import requests

from culture_ingest.source.clients import KopisClient


def _xml_page(n_rows: int) -> bytes:
    return ("<dbs>" + "<db><x/></db>" * n_rows + "</dbs>").encode()


class _Resp:
    def __init__(self, status: int, body: bytes = b""):
        self.status_code = status
        self.content = body

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.HTTPError(f"{self.status_code} Client Error")
            err.response = self
            raise err


class _FakeSession:
    """cpage 별로 준비된 응답을 돌려주는 세션 스텁."""

    def __init__(self, responses: dict[int, _Resp]):
        self._responses = responses
        self.calls: list[int] = []

    def get(self, url, params=None, timeout=None):
        page = params["cpage"]
        self.calls.append(page)
        return self._responses[page]


def _client(responses: dict[int, _Resp]) -> KopisClient:
    c = KopisClient(service_key="test-key")
    c.session = _FakeSession(responses)
    return c


def test_overshoot_400_after_full_pages_ends_list():
    # 페이지 1·2가 rows(=2)만큼 꽉 참 → 3페이지 조회 시 400 = 목록 끝(정상 종료)
    c = _client({1: _Resp(200, _xml_page(2)),
                 2: _Resp(200, _xml_page(2)),
                 3: _Resp(400)})
    pages = list(c.list_pages("pblprfr", {}, rows=2, max_pages=None))
    assert [p.row_count for p in pages] == [2, 2]
    assert c.session.calls == [1, 2, 3]  # 오버슛 조회까지는 감


def test_first_page_400_still_raises():
    # 1페이지 400 = 잘못된 파라미터/키 — 삼키면 안 된다
    c = _client({1: _Resp(400)})
    try:
        list(c.list_pages("pblprfr", {}, rows=2, max_pages=None))
        raise AssertionError("HTTPError가 나야 한다")
    except requests.HTTPError:
        pass


def test_partial_last_page_unaffected():
    # 마지막 페이지가 덜 찬 기존 경로는 오버슛 조회 없이 그대로 종료
    c = _client({1: _Resp(200, _xml_page(2)),
                 2: _Resp(200, _xml_page(1))})
    pages = list(c.list_pages("pblprfr", {}, rows=2, max_pages=None))
    assert [p.row_count for p in pages] == [2, 1]
    assert c.session.calls == [1, 2]  # 3페이지 조회 안 함
