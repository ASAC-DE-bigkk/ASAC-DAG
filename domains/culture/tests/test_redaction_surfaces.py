"""#144 — 인증키가 culture 에러 표면 4곳에 새지 않는지 검증.

표면: ①clients 예외 메시지 ②ingest result.error ③run_report ④notify payload.
2026-07-04 라이브 누출(KOPIS `service=<key>` 평문)이 근거. KOPIS의 bare `service=` 는
공통 structural 패턴 미매치(실측)라 **literal 등록**이 방어의 핵심이다.
"""
from __future__ import annotations

import json

import pytest
import requests

from common.security.redaction import (
    PLACEHOLDER,
    get_default_redactor,
    redact,
    register_secret,
)
from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import Landing, LocalSink
from culture_ingest.common.notify import build_report_payload
from culture_ingest.source.clients import KopisClient
from culture_ingest.source.datasets import BY_NAME
from culture_ingest.source.ingest import IngestOptions, build_run_report, ingest_dataset

# 실키 금지 — 길이만 literal 등록 조건(≥6)을 충족하는 가짜 값.
FAKE_KOPIS = "FAKEKOPISKEY1234567890"
FAKE_SEOUL = "FAKESEOULKEY0987654321"

LEAKY_URL = f"http://www.kopis.or.kr/openApi/restful/prfplc?service={FAKE_KOPIS}&cpage=1"


@pytest.fixture(autouse=True)
def _fake_key_registered():
    """가짜 키를 기본 redactor 에 등록하고 테스트 후 제거(전역 오염 방지)."""
    register_secret(FAKE_KOPIS)
    register_secret(FAKE_SEOUL)
    yield
    red = get_default_redactor()
    for k in (FAKE_KOPIS, FAKE_SEOUL):
        if k in red._literals:  # noqa: SLF001 -- 테스트 정리 용도
            red._literals.remove(k)


# ── ① clients 예외 경계 ────────────────────────────────────────────────────────

class _Resp400:
    """service= 키가 박힌 URL 로 HTTPError 를 던지는 requests 응답 흉내."""

    status_code = 400
    content = b""

    def __init__(self, url: str):
        self.url = url

    def raise_for_status(self):
        raise requests.HTTPError(
            f"400 Client Error: Bad Request for url: {self.url}", response=self
        )


class _Session400:
    def get(self, url, params=None, timeout=None):
        qs = "&".join(f"{k}={v}" for k, v in (params or {}).items())
        return _Resp400(f"{url}?{qs}")


def test_kopis_client_httperror_masks_key():
    cli = KopisClient(FAKE_KOPIS)
    cli.session = _Session400()
    with pytest.raises(requests.HTTPError) as ei:
        cli._get("pblprfr", {"cpage": 1})
    msg = str(ei.value)
    assert FAKE_KOPIS not in msg, "예외 메시지에 KOPIS 키 평문"
    assert PLACEHOLDER in msg


# ── ② ingest result.error ─────────────────────────────────────────────────────

class _LeakyKopis:
    """scrub 을 거치지 않은(우회된) 예외를 흉내 — 2차 방어(ingest 층) 검증용."""

    def list_pages(self, *a, **k):
        raise requests.HTTPError(f"400 Client Error: Bad Request for url: {LEAKY_URL}")


class _Clients:
    kopis = _LeakyKopis()
    seoul = None


def test_ingest_result_error_masks_key(tmp_path):
    ds = BY_NAME["kopis_festival"]
    ctx = RunContext(load_date="2026-07-06", ingest_ts="20260706T000000Z", run_id="test")
    landing = Landing(LocalSink(str(tmp_path)), "raw/culture", ctx)
    res = ingest_dataset(
        ds, _Clients(), landing,
        IngestOptions(date_from="20260601", date_to="20260706"),
    )
    assert res.error, "에러가 result.error 로 수집돼야 함"
    assert FAKE_KOPIS not in res.error, "result.error 에 KOPIS 키 평문"
    assert PLACEHOLDER in res.error


# ── ③ run_report ──────────────────────────────────────────────────────────────

def _summary_with_leak() -> dict:
    return {
        "name": "kopis_festival",
        "source": "kopis",
        "endpoint": "prffest",
        "rows": 0,
        "pages": 0,
        "error": f"HTTPError: 400 Client Error: Bad Request for url: {LEAKY_URL}",
        "checks": {},
    }


def test_run_report_masks_key():
    ctx = RunContext(load_date="2026-07-06", ingest_ts="20260706T000000Z", run_id="test")
    report = build_run_report([_summary_with_leak()], ctx, expected_total=1)
    dumped = json.dumps(report, ensure_ascii=False)
    assert FAKE_KOPIS not in dumped, "run_report 에 KOPIS 키 평문"


# ── ④ notify payload ──────────────────────────────────────────────────────────

def test_notify_payload_masks_key():
    report = {
        "load_date": "2026-07-06",
        "ingest_ts": "20260706T000000Z",
        "run_id": "test",
        "coverage": {"landed": 0, "expected": 1},
        "total_rows": 0,
        "slo_passed": False,
        "violations": [],
        "failed_datasets": [
            {"dataset": "kopis_festival",
             "error": f"HTTPError: 400 ... url: {LEAKY_URL}"},
        ],
        "datasets": [
            {"name": "kopis_festival", "rows": 0,
             "error": f"HTTPError: 400 ... url: {LEAKY_URL}"},
        ],
    }
    payload = build_report_payload(report)
    dumped = json.dumps(payload, ensure_ascii=False)
    assert FAKE_KOPIS not in dumped, "Discord payload 에 KOPIS 키 평문"


# ── literal 등록 배선(build_clients) ──────────────────────────────────────────

def test_build_clients_registers_source_keys(monkeypatch):
    """build_clients 가 읽은 키를 redactor literal 로 등록해야
    bare `service=` (structural 미매치) 도 가려진다."""
    red = get_default_redactor()
    for k in (FAKE_KOPIS, FAKE_SEOUL):  # fixture 등록분 제거 후 배선 자체를 검증
        if k in red._literals:  # noqa: SLF001
            red._literals.remove(k)
    monkeypatch.setenv("KOPIS_SERVICE_KEY", FAKE_KOPIS)
    monkeypatch.setenv("SEOUL_API_KEY_CULT", FAKE_SEOUL)

    from culture_ingest.source.ingest import build_clients
    build_clients()

    assert FAKE_KOPIS not in redact(f"url?service={FAKE_KOPIS}&cpage=1")
    assert FAKE_SEOUL not in redact(f"GET /{FAKE_SEOUL}/json/culturalEventInfo/1/1000/")
