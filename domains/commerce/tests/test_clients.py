"""서울 OpenAPI 응답 파서(parse_page) 단위 테스트 — 네트워크 불필요."""
import json

import pytest

from bronze.clients import (
    SeoulApiError, SeoulAuthError, parse_page,
)


def _env(service, rows, code="INFO-000", total=None):
    block = {"list_total_count": total if total is not None else len(rows),
             "RESULT": {"CODE": code, "MESSAGE": "x"}, "row": rows}
    return json.dumps({service: block}).encode("utf-8")


def test_parse_ok_returns_rows_and_total():
    p = parse_page(_env("S", [{"A": "1"}], total=600), "S")
    assert p.code == "INFO-000" and p.rows == [{"A": "1"}] and p.total_count == 600


def test_parse_no_data_is_empty_page():
    p = parse_page(_env("S", [], code="INFO-200"), "S")
    assert p.rows == [] and p.code == "INFO-200"


def test_parse_auth_error_raises_autherror():
    raw = json.dumps({"RESULT": {"CODE": "INFO-100", "MESSAGE": "bad key"}}).encode()
    with pytest.raises(SeoulAuthError):
        parse_page(raw, "S")


def test_parse_non_auth_error_raises_apierror():
    raw = _env("S", [], code="INFO-400")
    with pytest.raises(SeoulApiError):
        parse_page(raw, "S")


def test_parse_invalid_json_raises():
    with pytest.raises(SeoulApiError):
        parse_page(b"not json", "S")


def test_parse_non_numeric_total_raises_apierror_not_valueerror():
    """스키마 드리프트(list_total_count 비정수)도 SeoulApiError 계약 유지(#78 리뷰).

    원시 ValueError 가 새면 resolve.py 의 `except SeoulApiError` 를 관통해 CLI 가 죽는다.
    """
    raw = _env("S", [{"A": "1"}], total="N/A")
    with pytest.raises(SeoulApiError) as ei:
        parse_page(raw, "S")
    assert ei.value.code == "ERROR-PARSE"


# ── fetch_page 재시도(2026-07-21 실측: 200+비-JSON 과부하 응답 → dataset incomplete) ──
class _FakeCore:
    """HttpCore 대역 — get() 이 미리 정한 본문 시퀀스를 순서대로 반환."""
    def __init__(self, bodies):
        self._bodies = list(bodies)
        self.calls = 0

    def get(self, url, **kw):
        from common.http.contract import TransportResponse
        body = self._bodies[min(self.calls, len(self._bodies) - 1)]
        self.calls += 1
        return TransportResponse(status=200, content=body, headers={})


def _client(bodies, **kw):
    from bronze.clients import SeoulOpenApiClient
    c = SeoulOpenApiClient(key="k", base_url="http://x",
                           parse_backoff_seconds=0.0, **kw)
    c._core = _FakeCore(bodies)
    return c


def test_fetch_page_retries_error_parse_then_succeeds(monkeypatch):
    monkeypatch.setattr("bronze.clients.time.sleep", lambda *_: None)
    good = _env("S", [{"A": "1"}], total=1)
    c = _client([b"<html>overloaded</html>", b"", good], parse_retries=3)
    p = c.fetch_page("S", 1, 1)
    assert p.rows == [{"A": "1"}]
    assert c._core.calls == 3          # 비-JSON 2회 재시도 후 성공


def test_fetch_page_error_parse_exhausts_and_raises():
    c = _client([b"<html>down</html>"], parse_retries=2)
    with pytest.raises(SeoulApiError) as e:
        c.fetch_page("S", 1, 1)
    assert e.value.code == "ERROR-PARSE"
    assert c._core.calls == 3          # 1 + 2 재시도


def test_fetch_page_auth_error_not_retried():
    auth = _env("S", [], code="INFO-100")   # 인증 오류 봉투
    c = _client([auth, auth], parse_retries=3)
    with pytest.raises(SeoulAuthError):
        c.fetch_page("S", 1, 1)
    assert c._core.calls == 1          # 인증 오류는 즉시 실패(재시도 금지)
