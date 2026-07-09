"""#196 — KcisaClient(area2 페이징) + kcisa XML 파싱 테스트."""
from __future__ import annotations

from culture_ingest.common.records import parse_records

_KCISA_XML = (
    b"<response><body><items>"
    b"<item><serviceName>\xec\xa0\x84\xec\x8b\x9c</serviceName><seq>1</seq>"
    b"<title>A</title><place>P</place><gpsX>127.0</gpsX><gpsY>37.5</gpsY></item>"
    b"<item><serviceName>\xea\xb3\xb5\xec\x97\xb0</serviceName><seq>2</seq>"
    b"<title>B</title><place>Q</place></item>"
    b"</items><totalCount>2</totalCount></body></response>"
)


def test_parse_kcisa_items():
    recs = parse_records("kcisa", _KCISA_XML, "item", "area2")
    assert len(recs) == 2
    assert recs[0]["seq"] == "1" and recs[0]["title"] == "A"
    assert recs[0]["gpsX"] == "127.0"
    assert recs[1]["seq"] == "2"


def test_parse_kcisa_empty():
    body = b"<response><body><items></items><totalCount>0</totalCount></body></response>"
    assert parse_records("kcisa", body, "item", "area2") == []


# ── KcisaClient area2 페이징 (가짜 Transport = 진짜 HttpCore·QueryKey 통과) ──────
from common.http.contract import TransportResponse
from common.http.core import HttpCore
from culture_ingest.source.clients import KcisaClient, KcisaError


class _FakeKcisaTransport:
    """PageNo 별로 item 을 돌려주는 가짜 KCISA. total 개까지 채우고 그 뒤는 빈 페이지."""

    def __init__(self, total: int):
        self.total = total
        self.seen_keys: list[str] = []

    def send(self, method, url, *, params, headers, timeout):
        # QueryKey 가 serviceKey 를 params 에 병합한다 — 노출 표면 확인용 수집.
        self.seen_keys.append(params.get("serviceKey", ""))
        page = int(params["PageNo"]); rows = int(params["numOfrows"])
        start = (page - 1) * rows
        n = max(0, min(rows, self.total - start))
        items = "".join(f"<item><seq>{start + i}</seq><title>T{start + i}</title></item>"
                        for i in range(n))
        body = (f"<response><body><items>{items}</items>"
                f"<totalCount>{self.total}</totalCount>"
                f"<resultCode>00</resultCode></body></response>")
        return TransportResponse(status=200, content=body.encode())


def _client(transport):
    core = HttpCore(source="kcisa", transport=transport, rate_limit=None, sleep=lambda s: None)
    return KcisaClient("SECRET_KEY_123", core=core)


def test_kcisa_pages_until_empty():
    t = _FakeKcisaTransport(total=453)
    pages = list(_client(t).list_pages("area2", {"sido": "서울"}, rows=200, max_pages=None))
    assert sum(p.row_count for p in pages) == 453
    assert [p.index for p in pages] == [1, 2, 3]   # 200+200+53, 그 다음 빈 페이지에서 종료


def test_kcisa_stops_on_short_page():
    t = _FakeKcisaTransport(total=150)  # 첫 페이지(200)에 150 → rows 미만이라 즉시 종료
    pages = list(_client(t).list_pages("area2", {"sido": "서울"}, rows=200, max_pages=None))
    assert len(pages) == 1 and pages[0].row_count == 150


def test_kcisa_ends_on_empty_page_when_exact_multiple():
    """total 이 rows 의 정확한 배수면 마지막 꽉 찬 페이지 다음의 빈 페이지에서 끝난다 —
    KOPIS(오버슛 400)와 달리 KCISA 는 status 200·빈 item 을 주므로 이 경로가 종료 계약."""
    t = _FakeKcisaTransport(total=400)  # 200+200 후 3페이지는 빈 페이지(0건)
    pages = list(_client(t).list_pages("area2", {"sido": "서울"}, rows=200, max_pages=None))
    assert [p.index for p in pages] == [1, 2] and sum(p.row_count for p in pages) == 400
    assert len(t.seen_keys) == 3  # 빈 3페이지까지 실제로 요청해 종료를 확인


def test_kcisa_error_masks_key():
    class _AuthErr:
        def send(self, method, url, *, params, headers, timeout):
            return TransportResponse(status=200, content=(
                b"<OpenAPI_ServiceResponse><cmmMsgHeader>"
                b"<returnReasonCode>30</returnReasonCode>"
                b"<returnAuthMsg>SERVICE_KEY_IS_NOT_REGISTERED</returnAuthMsg>"
                b"</cmmMsgHeader></OpenAPI_ServiceResponse>"))
    try:
        list(_client(_AuthErr()).list_pages("area2", {"sido": "서울"}, rows=200, max_pages=None))
        assert False, "KcisaError 가 나야 함"
    except KcisaError as exc:
        assert "SECRET_KEY_123" not in str(exc)
