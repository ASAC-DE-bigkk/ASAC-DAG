"""#197 — KobisClient(JSON 단일 GET) 단위 테스트.

테스트 대체 경계 = Transport(#152). session mock 금지 — 진짜 HttpCore 를 통과시키고
가짜 Transport 로 응답을 주입한다. 키는 QueryKey 로 params 에 병합되므로 fake
transport 가 params 에서 key 를 읽는다(URL 문자열엔 안 들어감, #144).
"""
from __future__ import annotations

import json

from common.http.contract import TransportResponse
from common.http.core import HttpCore
from culture_ingest.source.clients import KobisClient, KobisError

# 실키 금지 — 길이만 충족하는 가짜 값.
FAKE_KOBIS = "FAKEKOBISKEY0123456789ABCDEF0123"


def _daily_json(names):
    return json.dumps({
        "boxOfficeResult": {
            "boxofficeType": "일별 박스오피스",
            "showRange": "20260707~20260707",
            "dailyBoxOfficeList": [
                {"rank": str(i + 1), "movieCd": f"2026000{i}", "movieNm": nm, "audiCnt": "100"}
                for i, nm in enumerate(names)
            ],
        }
    }).encode("utf-8")


class _FakeKobisTransport:
    """params 를 기록하고 정해진 JSON 을 돌려주는 가짜 전송 계층."""

    def __init__(self, body):
        self.body = body
        self.last_params = None

    def send(self, method, url, *, params, headers, timeout):
        self.last_params = dict(params or {})
        return TransportResponse(status=200, content=self.body)


def _client(transport):
    core = HttpCore(source="kobis", transport=transport, rate_limit=None, sleep=lambda s: None)
    return KobisClient(FAKE_KOBIS, core=core), transport


def test_daily_boxoffice_parses_top10():
    cli, tr = _client(_FakeKobisTransport(_daily_json(["A", "B", "C"])))
    page = cli.daily_boxoffice("20260707")
    assert page.row_count == 3
    assert page.ext == "json"
    # 키는 params 로 병합(#144) — URL 문자열엔 없다.
    assert tr.last_params.get("key") == FAKE_KOBIS
    assert tr.last_params.get("targetDt") == "20260707"


def test_seoul_passes_wideareacd():
    cli, tr = _client(_FakeKobisTransport(_daily_json(["A"])))
    cli.daily_boxoffice("20260707", "0105001")
    assert tr.last_params.get("wideAreaCd") == "0105001"


def test_nation_omits_wideareacd():
    cli, tr = _client(_FakeKobisTransport(_daily_json(["A"])))
    cli.daily_boxoffice("20260707")
    assert "wideAreaCd" not in tr.last_params


def test_fault_raises_and_masks_key():
    # 키를 literal 등록 → daily_boxoffice 가 fault 를 redact 로 감싸는 배선을 검증
    # (등록됐는데도 평문이 남으면 redact 호출 누락).
    from common.security.redaction import get_default_redactor, register_secret
    register_secret(FAKE_KOBIS)
    try:
        body = json.dumps({"faultInfo": {"message": f"invalid key {FAKE_KOBIS}"}}).encode("utf-8")
        cli, _ = _client(_FakeKobisTransport(body))
        import pytest
        with pytest.raises(KobisError) as ei:
            cli.daily_boxoffice("20260707")
        assert FAKE_KOBIS not in str(ei.value)
    finally:
        red = get_default_redactor()
        if FAKE_KOBIS in red._literals:  # noqa: SLF001 -- 테스트 정리
            red._literals.remove(FAKE_KOBIS)
