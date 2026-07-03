"""security 패키지 — 마스킹/로그필터/입력검증/정적감사/종합검증 + bronze 누출 통합 테스트.

핵심 위험을 경로별로 실증한다:
  - 로그/예외/구조체에서 시크릿 마스킹(literal + structural)
  - 로깅 핸들러를 통한 실제 출력 마스킹(메시지/인자/트레이스백)
  - bronze 마커 JSON(at-rest)으로 키가 새지 않음(end-to-end)
  - 입력(observed_date) 경로 주입 차단
  - `run_security_verification()` 단일 포인트가 번들에서 통과
"""
import io
import json
import logging

import pytest

from security import (
    PLACEHOLDER, assert_iso_date, assert_safe_segment, is_iso_date,
    is_safe_segment, run_security_verification,
)
from security.log_filter import (
    _FILTER_ATTR, SecretRedactingFilter, install_log_redaction,
    is_log_redaction_installed,
)
from security.redaction import Redactor, collect_secret_values

_KEY = "abcdEFGH1234567890abcdEFGH1234567890zzzz"   # 가짜 서울 인증키(40자)
_SEOUL_URL = f"http://openapi.seoul.go.kr:8088/{_KEY}/json/LOCALDATA_072404/1/1/"


# ── redaction: literal ──────────────────────────────────────────────────────────
def test_literal_redaction_masks_registered_secret():
    red = Redactor([_KEY])
    out = red.redact_text(f"boom key={_KEY} done")
    assert _KEY not in out and PLACEHOLDER in out


def test_short_value_not_redacted_as_literal():
    red = Redactor(["v1"])           # 길이 < 최소 → literal 등록 안 됨(오탐 방지)
    assert red.redact_text("schema v1 ok") == "schema v1 ok"


# ── redaction: structural(실제 값 몰라도 형태로) ────────────────────────────────
def test_seoul_full_url_path_key_masked_without_literal():
    red = Redactor()                 # env/literal 없음
    out = red.redact_text(_SEOUL_URL)
    assert _KEY not in out and "/json/LOCALDATA_072404/1/1/" in out


def test_requests_exception_path_form_masked():
    red = Redactor()
    msg = f"Max retries exceeded with url: /{_KEY}/json/SVC/1/1/ (Caused by ConnectError)"
    assert _KEY not in red.redact_text(msg)


# 값을 분할/조립해 *소스에 평문 시크릿 리터럴이 남지 않게* 한다(자기 감사에 안 걸리도록).
_SK = "sk-" + "abcdef123456"
_PRIV = "PRIV_VALUE_" + "abcdef123456"
_AKIA = "AKIA" + "IOSFODNN7EXAMPLE"
_TOK = "zzzz" + "zzzzzzzz"


@pytest.mark.parametrize("text,secret", [
    ("Authorization: Bearer " + _SK, _SK),
    ("aws_secret_access_key=" + _PRIV, _PRIV),
    (_AKIA + " used", _AKIA),
    ("token=" + _TOK + " next", _TOK),
])
def test_structural_patterns_mask(text, secret):
    assert secret not in Redactor().redact_text(text)


def test_redact_recurses_dict_and_list():
    red = Redactor([_KEY])
    obj = {"error": _SEOUL_URL, "rows": 7, "nested": [f"token={_KEY}", "ok"]}
    out = red.redact(obj)
    assert _KEY not in json.dumps(out) and out["rows"] == 7


# ── secret 수집(이름 기준 + deny + 길이 + placeholder) ───────────────────────────
def test_collect_secret_values_rules():
    env = {
        "SEOUL_API_KEY_COMM": _KEY,                  # 수집
        "R2_SECRET_ACCESS_KEY": "secretvalue123456",  # 수집
        "R2_ACCESS_KEY_ID": "accesskeyid123456",    # 수집
        "SEOUL_OPEN_API_BASE_URL": "http://x/y",    # deny(_URL)
        "R2_ENDPOINT": "https://e",                 # 이름 비시크릿
        "STORAGE_BACKEND": "local",                 # 비시크릿
        "R2_REGION": "auto",                        # 짧음/비시크릿
        "SOME_KEY": "${R2_X}",                      # placeholder(${...}) 제외
    }
    vals = collect_secret_values(env)
    assert _KEY in vals and "secretvalue123456" in vals and "accesskeyid123456" in vals
    assert "http://x/y" not in vals and "local" not in vals and "${R2_X}" not in vals


# ── 로깅 필터: 실제 핸들러 출력 마스킹 ──────────────────────────────────────────
def _logger_with_filter(name, redactor):
    buf = io.StringIO()
    h = logging.StreamHandler(buf)
    h.addFilter(SecretRedactingFilter(redactor))
    lg = logging.getLogger(name)
    lg.handlers = [h]
    lg.propagate = False
    lg.setLevel(logging.INFO)
    return lg, buf


def test_log_filter_masks_message_and_args():
    red = Redactor([_KEY])
    lg, buf = _logger_with_filter("t.sec.msg", red)
    lg.warning("fetch failed for %s", _SEOUL_URL)
    assert _KEY not in buf.getvalue() and PLACEHOLDER in buf.getvalue()


def test_log_filter_masks_traceback():
    red = Redactor([_KEY])
    lg, buf = _logger_with_filter("t.sec.exc", red)
    try:
        raise ValueError(f"boom {_SEOUL_URL}")
    except ValueError:
        lg.exception("collect failed")
    assert _KEY not in buf.getvalue()


def test_install_log_redaction_idempotent_and_detected():
    install_log_redaction()
    assert is_log_redaction_installed()
    root = logging.getLogger()
    before = sum(getattr(f, _FILTER_ATTR, False) for f in root.filters)
    install_log_redaction()          # 두 번째 호출은 중복 부착하지 않는다
    after = sum(getattr(f, _FILTER_ATTR, False) for f in root.filters)
    assert after == before


# ── 입력 검증(경로 주입 차단) ───────────────────────────────────────────────────
@pytest.mark.parametrize("val,ok", [
    ("2026-06-30", True), ("2026-6-30", False), ("../etc", False), ("", False),
])
def test_is_iso_date(val, ok):
    assert is_iso_date(val) is ok


@pytest.mark.parametrize("val,ok", [
    ("2026-06-30", True), ("observed_date=1", True), ("../x", False),
    ("a/b", False), ("a\\b", False), ("-rf", False), ("", False), ("..", False),
])
def test_is_safe_segment(val, ok):
    assert is_safe_segment(val) is ok


def test_assert_helpers_raise_on_injection():
    with pytest.raises(ValueError):
        assert_iso_date("../../etc/passwd")
    with pytest.raises(ValueError):
        assert_safe_segment("a/../b")
    assert assert_iso_date("2026-06-30") == "2026-06-30"


# ── 정적 감사 + 종합검증 단일 포인트 ────────────────────────────────────────────
def test_static_audit_all_pass_on_bundle():
    from security.audit import run_static_audit
    from security.verify import BUNDLE_ROOT
    failed = [(f.check, f.detail) for f in run_static_audit(BUNDLE_ROOT) if not f.ok]
    assert not failed, failed


def test_run_security_verification_no_blocking():
    report = run_security_verification(runtime_checks=False)
    assert not report.blocking, report.render()
    assert "결과:" in report.render()


def test_verification_with_runtime_passes_after_install():
    from security import install_security
    from security.stdio_guard import (
        uninstall_excepthook_redaction, uninstall_stdout_redaction,
    )
    status = install_security()
    try:
        assert status["log_redaction"] and status["stdout_redaction"] \
            and status["excepthook_redaction"], status
        report = run_security_verification(runtime_checks=True)
        assert report.ok, report.render()
    finally:   # 다른 테스트의 캡처/훅 상태를 오염시키지 않게 원복
        uninstall_stdout_redaction()
        uninstall_excepthook_redaction()


# ── end-to-end: bronze 마커(at-rest)로 키가 새지 않음 ───────────────────────────
def test_bronze_marker_error_is_redacted(tmp_path, monkeypatch):
    """네트워크 예외 메시지에 키가 박혀도 bronze 마커 JSON 에 평문 키가 남지 않아야 한다."""
    monkeypatch.setenv("SEOUL_API_KEY_COMM", _KEY)
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COMMERCE_STORAGE_PREFIX", "")
    from security.redaction import refresh_env_secrets
    refresh_env_secrets()            # 가짜 키를 기본 redactor 에 등록

    from bronze import bronze_tasks
    from commerce_core.schemas import Dataset
    from commerce_core.storage import get_storage

    class _FakeClient:               # fetch_page 가 키 박힌 URL 예외를 던지는 가짜 클라이언트
        def __init__(self, *a, **k): pass
        def fetch_page(self, *a, **k):
            raise RuntimeError(f"connect fail url: /{_KEY}/json/SVC/1/1/")

    monkeypatch.setattr(bronze_tasks, "SeoulOpenApiClient", _FakeClient)
    ds = Dataset(oa_id="OA-1", name_ko="t", short="t", category="c",
                 schedule="daily", service_name="SVC")

    summary = bronze_tasks.fetch_dataset_to_bronze(
        ds, "2026-06-30", "run1", "2026-06-30_120000_000")

    assert summary["status"] == "failed"
    assert _KEY not in json.dumps(summary)
    marker = get_storage().read_json(summary["marker_key"])
    assert _KEY not in json.dumps(marker, ensure_ascii=False)
    assert PLACEHOLDER in marker.get("error", "")


# ════════════════════════════════════════════════════════════════════════════════
# 통합 보안 플러그인 확장(feat/96) — bootstrap · stdio · netio · fileio · api_guard · events
# ════════════════════════════════════════════════════════════════════════════════
from security import (  # noqa: E402
    InsecureRequestBlocked, api_receipt, event_record, http_request,
    install_security, is_security_installed, log_event, log_exception,
    response_summary, safe_join, safe_key, scrub_exception, scrub_headers,
    scrub_params, scrub_url, security_status, write_json_redacted,
)
from security.stdio_guard import (  # noqa: E402
    install_excepthook_redaction, install_stdout_redaction,
    is_excepthook_redaction_installed, is_stdout_redaction_installed,
    uninstall_excepthook_redaction, uninstall_stdout_redaction,
)


@pytest.fixture
def _registered_key():
    """가짜 키를 기본 redactor 에 등록(모듈 전역이라 테스트 간 공유 — 등록만 하면 됨)."""
    from security import register_secret
    register_secret(_KEY)
    return _KEY


# ── bootstrap: 원샷 설치/상태/원복 ───────────────────────────────────────────────
def test_install_security_one_shot_and_idempotent():
    try:
        st1 = install_security()
        assert is_security_installed(), st1
        st2 = install_security()          # 재호출해도 중복 부착/오류 없음
        assert st2["log_redaction"] and st2["stdout_redaction"] and st2["excepthook_redaction"]
        assert set(security_status()) == {
            "log_redaction", "stdout_redaction", "excepthook_redaction"}
    finally:
        uninstall_stdout_redaction()
        uninstall_excepthook_redaction()
    assert not is_stdout_redaction_installed()
    assert not is_excepthook_redaction_installed()


# ── stdio_guard: print/stderr 경로 마스킹 ───────────────────────────────────────
def test_stdout_redaction_masks_print(capsys, _registered_key):
    install_stdout_redaction()
    try:
        print(f"leak? key={_KEY} done")
        err_line = f"stderr leak {_KEY}"
        print(err_line, file=__import__("sys").stderr)
    finally:
        uninstall_stdout_redaction()
    out, err = capsys.readouterr()
    assert _KEY not in out and PLACEHOLDER in out
    assert _KEY not in err


def test_excepthook_redaction_masks_uncaught_traceback(capsys, _registered_key):
    import sys
    install_excepthook_redaction()
    try:
        try:
            raise RuntimeError(f"boom {_SEOUL_URL}")
        except RuntimeError:
            sys.excepthook(*sys.exc_info())            # 미처리 예외 경로 재현
    finally:
        uninstall_excepthook_redaction()
    err = capsys.readouterr().err
    assert "RuntimeError" in err and _KEY not in err


def test_threading_excepthook_masks(capsys, _registered_key):
    import threading
    install_excepthook_redaction()
    try:
        t = threading.Thread(
            target=lambda: (_ for _ in ()).throw(ValueError(f"thread boom {_KEY}")),
            name="sec-test")
        t.start()
        t.join()
    finally:
        uninstall_excepthook_redaction()
    err = capsys.readouterr().err
    assert "ValueError" in err and _KEY not in err


# ── netio: HTTP 정책(timeout 주입 · TLS 강제 · 예외 마스킹) ──────────────────────
class _FakeSess:
    """requests.Session 흉내 — 마지막 호출 kwargs 기록."""
    def __init__(self, exc=None):
        self.exc = exc
        self.kwargs = {}

    def request(self, method, url, **kwargs):
        self.kwargs = {"method": method, "url": url, **kwargs}
        if self.exc:
            raise self.exc
        return "RESP"


def test_netio_injects_default_timeout():
    sess = _FakeSess()
    assert http_request("GET", "http://x/", session=sess) == "RESP"
    assert sess.kwargs["timeout"] == 30.0             # 기본 timeout 주입(미지정 시)


def test_netio_respects_explicit_timeout():
    sess = _FakeSess()
    http_request("GET", "http://x/", session=sess, timeout=7)
    assert sess.kwargs["timeout"] == 7


def test_netio_blocks_tls_verify_disable():
    kw = {"verify": False}                            # 리터럴 회피(자기 감사)
    with pytest.raises(InsecureRequestBlocked):
        http_request("GET", "http://x/", session=_FakeSess(), **kw)


def test_netio_rethrows_same_type_with_scrubbed_message(_registered_key):
    exc = ConnectionError(f"Max retries exceeded with url: /{_KEY}/json/SVC/1/1/")
    with pytest.raises(ConnectionError) as ei:
        http_request("GET", "http://x/", session=_FakeSess(exc=exc), timeout=1)
    assert _KEY not in str(ei.value) and PLACEHOLDER in str(ei.value)


def test_scrub_exception_walks_cause_chain(_registered_key):
    inner = ValueError(f"inner {_KEY}")
    outer = RuntimeError(f"outer token={_KEY}")
    outer.__cause__ = inner
    scrub_exception(outer)
    assert _KEY not in str(outer) and _KEY not in str(inner)


# ── fileio: 경로 주입 차단 + at-rest 마스킹 저장 ────────────────────────────────
def test_safe_key_joins_and_validates():
    key = safe_key("raw/commerce", "2026/07/03", "run_id=x", "short.jsonl")
    assert key == "raw/commerce/2026/07/03/run_id=x/short.jsonl"


@pytest.mark.parametrize("parts", [
    ("a", "../b"), ("a/../b",), ("/etc/passwd",), ("a", "b\\c"), ("",),
])
def test_safe_key_rejects_traversal(parts):
    with pytest.raises(ValueError):
        safe_key(*parts)


def test_safe_join_confines_to_root(tmp_path):
    p = safe_join(tmp_path, "x", "y.json")
    assert str(p).startswith(str(tmp_path.resolve()))
    with pytest.raises(ValueError):
        safe_join(tmp_path, "..", "z")


def test_write_json_redacted_masks_at_rest(tmp_path, _registered_key):
    out = write_json_redacted(tmp_path / "m.json", {"error": f"url /{_KEY}/json/S/1/1/"})
    text = out.read_text(encoding="utf-8")
    assert _KEY not in text and PLACEHOLDER in text


# ── api_guard: 요청 영수증/응답 요약 안전화 ─────────────────────────────────────
def test_scrub_url_masks_path_and_query_secrets():
    url = f"http://openapi.seoul.go.kr:8088/{_KEY}/json/SVC/1/1/?apikey={_KEY}&page=2"
    out = scrub_url(url)
    assert _KEY not in out and "page=2" in out


def test_scrub_headers_and_params():
    h = scrub_headers({"Authorization": "Bearer " + _KEY, "Accept": "application/json"})
    assert h["Authorization"] == PLACEHOLDER and h["Accept"] == "application/json"
    p = scrub_params({"api_key": _KEY, "page": 3})
    assert p["api_key"] == PLACEHOLDER and p["page"] == 3


def test_api_receipt_is_storable_without_secrets(_registered_key):
    receipt = api_receipt(
        method="get", url=f"http://openapi.seoul.go.kr:8088/{_KEY}/json/SVC/1/1/",
        service="SVC", params={"api_key": _KEY, "page": 1},
        headers={"Authorization": "Bearer " + _KEY},
        status=200, ok=True, elapsed_ms=12.3)
    dumped = json.dumps(receipt, ensure_ascii=False)     # 그대로 저장 가능해야 한다
    assert _KEY not in dumped
    assert receipt["method"] == "GET" and receipt["status"] == 200
    assert receipt["kind"] == "api_receipt" and receipt["requested_at"]


def test_response_summary_hash_and_masked_sample(_registered_key):
    body = ('{"RESULT":"ok","echo_key":"' + _KEY + '"}').encode()
    summary = response_summary(status=200, body=body, max_body=128)
    assert summary["content_length"] == len(body)
    assert len(summary["content_hash"]) == 64            # sha256 hex
    assert _KEY not in json.dumps(summary)


# ── events: 분석 가능(JSON 파싱)하면서 시크릿 없는 처리/에러 로그 ────────────────
def _event_logger(name):
    buf = io.StringIO()
    h = logging.StreamHandler(buf)
    lg = logging.getLogger(name)
    lg.handlers = [h]
    lg.propagate = False
    lg.setLevel(logging.INFO)
    return lg, buf


def test_log_event_emits_parseable_masked_json(_registered_key):
    lg, buf = _event_logger("t.sec.events")
    record = log_event("bronze.page_fetched", logger=lg, where="ingest_one:t",
                       page=3, rows=1000, url=f"/{_KEY}/json/S/1/1/")
    line = buf.getvalue().strip()
    parsed = json.loads(line)                            # 단일 라인 JSON = 분석 가능
    assert parsed["event"] == "bronze.page_fetched" and parsed["page"] == 3
    assert parsed["ts"] and parsed["level"] == "info" and parsed["where"] == "ingest_one:t"
    assert _KEY not in line and record == parsed


def test_log_exception_record_masked_and_transmittable(_registered_key):
    lg, buf = _event_logger("t.sec.events.exc")
    try:
        raise RuntimeError(f"collect fail url: /{_KEY}/json/SVC/1/1/")
    except RuntimeError as exc:
        record = log_exception(exc, logger=lg, where="ingest_one:t", short="t")
    dumped = json.dumps(record, ensure_ascii=False)      # 알림 채널로 그대로 전송 가능
    assert _KEY not in dumped and _KEY not in buf.getvalue()
    assert record["error_type"] == "RuntimeError" and record["short"] == "t"
    assert any("RuntimeError" in ln for ln in record["traceback"])


def test_event_record_reserved_keys_not_overridable():
    # event/level/where 는 명명 파라미터라 kwargs 로 덮을 수 없다(TypeError).
    # 자유 필드로 들어올 수 있는 예약 키(ts)만 검증한다.
    record = event_record("x", level="info", where="w", ts="EVIL")
    assert record["event"] == "x" and record["ts"] != "EVIL"


# ════════════════════════════════════════════════════════════════════════════════
# 가이드라인 확장(feat/96 2차) — SSRF/응답상한 · crypto · archive · sanitize · 신규 audit
# 위반 샘플은 전부 "조각 결합" 문자열로 만들어 tmp 파일에 기록한다(번들 자기감사 회피).
# ════════════════════════════════════════════════════════════════════════════════
from pathlib import Path  # noqa: E402

from security import (  # noqa: E402
    ResponseTooLarge, UnsafeArchiveError, UnsafeURLBlocked, assert_url_allowed,
    constant_time_equals, generate_hex_token, generate_token, hash_password,
    is_url_allowed, needs_rehash, safe_extract_tar, safe_extract_zip,
    sanitize_log_value, verify_password,
)


# ── netio: SSRF 가드(assert_url_allowed) ────────────────────────────────────────
@pytest.mark.parametrize("url", [
    "file:///etc/passwd", "ftp://host/x", "gopher://host/x",       # 스킴
    "https://" + "user:" + "pw12345@host/x",                       # userinfo
    "http://127.0.0.1/x", "htt" + "p://10.0.0.5/x", "htt" + "p://192.168.1.1/x",
    "htt" + "p://169.254.169.254/latest/meta-data/",               # AWS IMDS
    "htt" + "p://100.100.100.200/latest/",                         # Alibaba IMDS
    "http://[::1]/x", "http://[fe80::1]/x",
    "http://[::ffff:169.254.169.254]/x",                           # v4-mapped 우회
    "http://[fd00:ec2::254]/x",                                    # IMDSv6(fc00::/7)
])
def test_ssrf_guard_blocks(url):
    with pytest.raises(UnsafeURLBlocked):
        assert_url_allowed(url)


@pytest.mark.parametrize("url", [
    "https://api.example.com/v1/x",          # 호스트명(DNS 미해석 모드)
    "htt" + "p://93.184.216.34/x",           # 공인 IP 리터럴
    "https://[2606:2800:220:1:248:1893:25c8:1946]/x",
])
def test_ssrf_guard_allows_public(url):
    assert assert_url_allowed(url) == url
    assert is_url_allowed(url)


def test_ssrf_guard_allowed_hosts_restriction():
    assert is_url_allowed("https://api.good.com/x", allowed_hosts={"api.good.com"})
    assert not is_url_allowed("https://api.evil.com/x", allowed_hosts={"api.good.com"})


def test_http_request_url_check_blocks_before_session_call():
    sess = _FakeSess()
    with pytest.raises(UnsafeURLBlocked):
        http_request("GET", "htt" + "p://169.254.169.254/x", session=sess,
                     timeout=1, url_check=True)
    assert sess.kwargs == {}                       # 차단 시 요청 자체가 안 나감


def test_http_request_url_check_defaults_redirects_off():
    sess = _FakeSess()
    http_request("GET", "htt" + "p://93.184.216.34/x", session=sess, timeout=1,
                 url_check=True)
    assert sess.kwargs.get("allow_redirects") is False


# ── netio: 응답 크기 상한(max_response_bytes) ───────────────────────────────────
class _FakeResp:
    def __init__(self, chunks, headers=None):
        self._chunks = list(chunks)
        self.headers = headers or {}
        self.closed = False

    def iter_content(self, n):
        return iter(self._chunks)

    def close(self):
        self.closed = True


class _CapSess:
    def __init__(self, resp):
        self.resp = resp
        self.kwargs = {}

    def request(self, method, url, **kwargs):
        self.kwargs = kwargs
        return self.resp


def test_response_cap_under_limit_assembles_content():
    resp = _FakeResp([b"ab", b"cd"])
    out = http_request("GET", "http://x/", session=_CapSess(resp),
                       timeout=1, max_response_bytes=10)
    assert out._content == b"abcd"


def test_response_cap_streamed_body_exceeds():
    resp = _FakeResp([b"x" * 100, b"y" * 100])
    with pytest.raises(ResponseTooLarge):
        http_request("GET", "http://x/", session=_CapSess(resp),
                     timeout=1, max_response_bytes=150)
    assert resp.closed


def test_response_cap_declared_header_exceeds():
    resp = _FakeResp([b""], headers={"Content-Length": "999999"})
    with pytest.raises(ResponseTooLarge):
        http_request("GET", "http://x/", session=_CapSess(resp),
                     timeout=1, max_response_bytes=100)


# ── crypto: CSPRNG 토큰 · 상수시간 비교 · PBKDF2 ────────────────────────────────
def test_generate_tokens_unique_and_long():
    a, b = generate_token(), generate_token()
    assert a != b and len(a) >= 40                 # 32바이트 urlsafe ≈ 43자
    assert len(generate_hex_token(16)) == 32


def test_constant_time_equals():
    assert constant_time_equals("abc", "abc") and constant_time_equals(b"x", b"x")
    assert not constant_time_equals("abc", "abd")


def test_password_hash_roundtrip_and_rehash():
    encoded = hash_password("correct horse battery", iterations=210_000)
    assert encoded.startswith("pbkdf2_sha256$210000$")
    assert verify_password("correct horse battery", encoded)
    assert not verify_password("wrong password", encoded)
    assert not verify_password("correct horse battery", "garbage$string")
    assert needs_rehash(encoded)                   # 210k < 기본 600k → 재해시 필요
    assert not needs_rehash(encoded, min_iterations=210_000)
    with pytest.raises(ValueError):
        hash_password("x" * 8, iterations=1000)    # 조용한 약화 방지
    with pytest.raises(ValueError):
        hash_password("")


# ── archive: zip-slip/zip-bomb/특수엔트리 차단 ──────────────────────────────────
def _make_zip(path, entries):
    import zipfile
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries:
            zf.writestr(name, data)
    return path


def test_safe_extract_zip_ok(tmp_path):
    src = _make_zip(tmp_path / "ok.zip", [("a.txt", b"hello"), ("d/b.txt", b"world")])
    written = safe_extract_zip(src, tmp_path / "out")
    assert sorted(p.name for p in written) == ["a.txt", "b.txt"]
    assert (tmp_path / "out" / "d" / "b.txt").read_bytes() == b"world"


@pytest.mark.parametrize("name", ["../evil.txt", "/abs.txt", "d/../../evil.txt"])
def test_safe_extract_zip_blocks_traversal(tmp_path, name):
    src = _make_zip(tmp_path / "bad.zip", [(name, b"x")])
    with pytest.raises(UnsafeArchiveError):
        safe_extract_zip(src, tmp_path / "out")
    assert not (tmp_path.parent / "evil.txt").exists()


def test_safe_extract_zip_caps(tmp_path):
    src = _make_zip(tmp_path / "many.zip", [(f"f{i}.txt", b"x") for i in range(5)])
    with pytest.raises(UnsafeArchiveError):
        safe_extract_zip(src, tmp_path / "out", max_entries=3)
    bomb = _make_zip(tmp_path / "bomb.zip", [("z.bin", b"\x00" * 200_000)])  # 고압축
    with pytest.raises(UnsafeArchiveError):
        safe_extract_zip(bomb, tmp_path / "out2", max_ratio=10.0)
    big = _make_zip(tmp_path / "big.zip", [("b.bin", bytes(range(256)) * 100)])
    with pytest.raises(UnsafeArchiveError):
        safe_extract_zip(big, tmp_path / "out3", max_total_bytes=100)


def _make_tar(path, entries, links=()):
    import io as _io
    import tarfile as _tar
    with _tar.open(path, "w") as tf:
        for name, data in entries:
            info = _tar.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, _io.BytesIO(data))
        for name, target in links:
            info = _tar.TarInfo(name)
            info.type = _tar.SYMTYPE
            info.linkname = target
            tf.addfile(info)
    return path


def test_safe_extract_tar_ok(tmp_path):
    src = _make_tar(tmp_path / "ok.tar", [("a.txt", b"hi"), ("d/b.txt", b"yo")])
    written = safe_extract_tar(src, tmp_path / "out")
    assert (tmp_path / "out" / "a.txt").read_bytes() == b"hi"
    assert len(written) == 2


def test_safe_extract_tar_blocks_traversal_and_links(tmp_path):
    bad = _make_tar(tmp_path / "bad.tar", [("../evil.txt", b"x")])
    with pytest.raises(UnsafeArchiveError):
        safe_extract_tar(bad, tmp_path / "out")
    linky = _make_tar(tmp_path / "link.tar", [], links=[("l", "/etc/passwd")])
    with pytest.raises(UnsafeArchiveError):
        safe_extract_tar(linky, tmp_path / "out2")


# ── sanitize_log_value + 로그 필터 opt-in(CWE-117) ──────────────────────────────
def test_sanitize_log_value_neutralizes_controls():
    forged = "user1\n2026-07-03 INFO fake-line \x1b[31mred\x07"
    out = sanitize_log_value(forged)
    assert "\n" not in out and "\x1b" not in out and "\x07" not in out
    assert "\\n" in out and "\\u001b" in out
    assert sanitize_log_value("a" * 3000, max_len=100).startswith("a" * 100)


def test_log_filter_neutralize_controls_opt_in():
    red = Redactor([_KEY])
    buf = io.StringIO()
    h = logging.StreamHandler(buf)
    h.addFilter(SecretRedactingFilter(red, neutralize_controls=True))
    lg = logging.getLogger("t.sec.ctl")
    lg.handlers = [h]
    lg.propagate = False
    lg.setLevel(logging.INFO)
    lg.warning("input=%s", f"x\nFAKE INFO line key={_KEY}")
    line = buf.getvalue()
    assert "\nFAKE" not in line and "\\nFAKE" in line and _KEY not in line


def test_userinfo_password_masked_structurally():
    red = Redactor()          # literal 등록 없이 structural 만으로
    dsn = "postgres" + "://" + "writer:" + "supersecretdbpw@db.internal:5432/app"
    out = red.redact_text(f"connect failed: {dsn}")
    assert "supersecretdbpw" not in out and PLACEHOLDER in out
    from security import scrub_url
    assert "supersecretdbpw" not in scrub_url(dsn)


# ── audit 신규/강화 점검 — tmp 루트에 위반 파일을 만들어 검증 ────────────────────
def _mk_root(tmp_path, files):
    root = tmp_path / "proj"
    for rel, content in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    return root


def test_check_credential_material_hits(tmp_path):
    from security.audit import check_credential_material
    pem = "-----BEGIN RSA " + "PRIVATE KEY-----"
    vendor = "gh" + "p_" + "a" * 36
    dsn = "https" + "://u:" + "supers3cretpw" + "@db.internal/x"
    root = _mk_root(tmp_path, {
        "bad1.py": f'k = "{pem}"\n',
        "bad2.md": f"token {vendor}\n",
        "bad3.py": f'dsn = "{dsn}"\n',
        "ok.py": 'url = "https' + '://user:pw@example.com/x"  # example\n',
    })
    f = check_credential_material(root)
    assert not f.ok and f.severity == "CRITICAL"
    assert "bad1.py" in f.detail and "bad2.md" in f.detail and "bad3.py" in f.detail
    assert "ok.py" not in f.detail                 # example 라인은 허용


def test_check_trojan_source(tmp_path):
    from security.audit import check_trojan_source
    root = _mk_root(tmp_path, {
        "bad.py": "x = 1  " + chr(0x202E) + "trick\n",
        "zw.py": "y = 1" + chr(0x200B) + "\n",
        "ok.py": "z = 1\n",
        "allowed.yaml": "# rtl " + chr(0x202E) + " security: allow-bidi\n",
    })
    f = check_trojan_source(root)
    assert not f.ok
    assert "bad.py" in f.detail and "zw.py" in f.detail
    assert "allowed.yaml" not in f.detail
    root_ok = _mk_root(tmp_path / "ok2", {"fine.py": "a = 1\n"})
    assert check_trojan_source(root_ok).ok


def test_check_sql_injection(tmp_path):
    from security.audit import check_sql_injection
    fstr = 'cur.exe' + 'cute(f"SELECT * FROM {t}")'
    fmt = 'cur.exe' + 'cute("SELECT {}".format(t))'
    concat = 'cur.exe' + 'cute("SELECT " + t)'
    param = 'cur.exe' + 'cute("SELECT * FROM t WHERE id=%s", (x,))'
    allowed = 'cur.exe' + 'cute(f"SELECT * FROM {T}")  # security: allow-sql'
    root = _mk_root(tmp_path, {"bad.py": f"{fstr}\n{fmt}\n{concat}\n",
                               "ok.py": f"{param}\n{allowed}\n"})
    f = check_sql_injection(root)
    assert not f.ok and f.detail.count("bad.py") == 3 and "ok.py" not in f.detail


def test_check_unsafe_extract(tmp_path):
    from security.audit import check_unsafe_extract
    bad = "import zipfile\nzf.extract" + "all(dest)\n"
    good = "import tarfile\ntf.extract" + "all(dest, filter='data')\n"
    nogate = "obj.extract" + "all()\n"             # 아카이브 모듈 미임포트 → 통과
    root = _mk_root(tmp_path, {"bad.py": bad, "good.py": good, "other.py": nogate})
    f = check_unsafe_extract(root)
    assert not f.ok and "bad.py" in f.detail
    assert "good.py" not in f.detail and "other.py" not in f.detail


def test_check_insecure_file_ops(tmp_path):
    from security.audit import check_insecure_file_ops
    root = _mk_root(tmp_path, {
        "bad.py": "p = tempfile.mk" + "temp()\nos.ch" + "mod(p, 0o7" + "77)\n",
        "ok.py": "os.ch" + "mod(p, 0o600)\nd = tempfile.mkdtemp()\n",
    })
    f = check_insecure_file_ops(root)
    assert not f.ok and f.detail.count("bad.py") == 2 and "ok.py" not in f.detail


def test_check_weak_hash(tmp_path):
    from security.audit import check_weak_hash
    root = _mk_root(tmp_path, {
        "bad.py": "h = hashlib.m" + "d5(data)\n",
        "ok.py": ("h = hashlib.sha256(data)\n"
                  "e = hashlib.m" + "d5(data, usedforsecurity=False)\n"),
    })
    f = check_weak_hash(root)
    assert not f.ok and "bad.py" in f.detail and "ok.py" not in f.detail
    assert f.severity == "MEDIUM"                  # advisory(비차단)


def test_check_insecure_random(tmp_path):
    from security.audit import check_insecure_random
    bad = "token = random.cho" + "ice(alphabet)\n"
    ok = "delay = random.unif" + "orm(0, 1)\n"     # 지터 — 시크릿 문맥 아님
    root = _mk_root(tmp_path, {"bad.py": bad, "ok.py": ok})
    f = check_insecure_random(root)
    assert not f.ok and "bad.py" in f.detail and "ok.py" not in f.detail


def test_check_web_misconfig(tmp_path):
    from security.audit import check_web_misconfig
    root = _mk_root(tmp_path, {
        "bad.py": ("app.run(host='0.0." + "0.0', debug=" + "True)\n"
                   "allow_orig" + "ins=['*']\n"),
        "bad2.py": "allow_orig" + 'ins=["*"]\n',
    })
    f = check_web_misconfig(root)
    assert not f.ok and "bad.py" in f.detail and "bad2.py" in f.detail


def test_check_xml_and_cleartext_http(tmp_path, monkeypatch):
    from security.audit import check_cleartext_http, check_xml_parsing
    root = _mk_root(tmp_path, {
        "x.py": "t = xml.etree.ElementTree.par" + "se(f)\n",
        "h.py": 'u = "htt' + 'p://evil.internal/x"\n',
        "ok.py": 'v = "htt' + 'p://openapi.seoul.go.kr:8088/x"\n',
    })
    fx = check_xml_parsing(root)
    assert not fx.ok and fx.severity == "LOW" and "x.py" in fx.detail
    fh = check_cleartext_http(root)
    assert not fh.ok and "evil.internal" in fh.detail and "seoul" not in fh.detail
    monkeypatch.setenv("SECURITY_HTTP_ALLOW_HOSTS", "evil.internal")
    assert check_cleartext_http(root).ok           # 환경변수 허용 목록 확장


def test_check_requirements_hygiene(tmp_path):
    from security.audit import check_requirements_hygiene
    root = _mk_root(tmp_path, {
        "requirements.txt": "requests\n",                       # 버전 무제한
        "requirements-dev.txt": "--index-url htt" + "p://mirror/simple\npkg>=1.0\n",
        "sub/requirements-ok.txt": "requests>=2.0\n# comment\n",
    })
    f = check_requirements_hygiene(root)
    assert not f.ok
    assert "unpinned" in f.detail and "plain-http index" in f.detail
    assert "requirements-ok" not in f.detail


def test_check_tls_and_yaml_and_dangerous_extended(tmp_path):
    from security.audit import check_dangerous_calls, check_tls_verify, check_unsafe_yaml
    root = _mk_root(tmp_path, {
        "tls.py": ("ctx = ssl._create_unverified" + "_context()\n"
                   "ctx.check_hostname = " + "False\nssl.CERT_" + "NONE\n"),
        "y.py": ("a = yaml.unsafe" + "_load(x)\n"
                 "b = yaml.lo" + "ad(x, Loader=yaml.Load" + "er)\n"
                 "c = yaml.lo" + "ad(x, Loader=yaml.SafeLoader)\n"),
        "d.py": "os.po" + "pen('ls')\nmarshal.lo" + "ads(b)\n",
    })
    ft = check_tls_verify(root)
    assert not ft.ok and ft.detail.count("tls.py") == 3
    fy = check_unsafe_yaml(root)
    assert not fy.ok and fy.detail.count("y.py") == 2      # SafeLoader 는 미검출
    fd = check_dangerous_calls(root)
    assert not fd.ok and fd.detail.count("d.py") == 2


# ════════════════════════════════════════════════════════════════════════════════
# 적대적 검증 회귀(feat/96 검증 워크플로 확인 16건) — 각 수정이 유지되는지 잠근다.
# ════════════════════════════════════════════════════════════════════════════════
import security.netio as _netio  # noqa: E402
from security import needs_rehash as _needs_rehash  # noqa: E402


# [#1] SSRF: url_check 는 호스트명도 DNS 해석해 검사(IP 리터럴만 막던 우회 차단)
def test_ssrf_hostname_resolved_and_blocked(monkeypatch):
    def fake_getaddrinfo(host, *a, **k):
        return [(2, 1, 6, "", ("169.254.169.254", 0))]      # 메타데이터로 해석되는 호스트
    monkeypatch.setattr(_netio.socket, "getaddrinfo", fake_getaddrinfo)
    with pytest.raises(UnsafeURLBlocked):
        assert_url_allowed("htt" + "p://metadata.evil.example/latest/", resolve_dns=True)
    sess = _FakeSess()
    with pytest.raises(UnsafeURLBlocked):
        http_request("GET", "htt" + "p://metadata.evil.example/x", session=sess, timeout=1,
                     url_check=True)          # 기본 resolve_dns=True → 세션 호출 전 차단
    assert sess.kwargs == {}


def test_ssrf_hostname_resolved_public_allowed(monkeypatch):
    monkeypatch.setattr(_netio.socket, "getaddrinfo",
                        lambda *a, **k: [(2, 1, 6, "", ("93.184.216.34", 0))])
    assert assert_url_allowed("https://good.example/x", resolve_dns=True).endswith("/x")


# [#6] 응답 상한: 호출자가 stream=False 를 줘도 강제로 True 로 덮어 상한 유효
def test_response_cap_forces_stream_true():
    resp = _FakeResp([b"x" * 200])
    sess = _CapSess(resp)
    with pytest.raises(ResponseTooLarge):
        http_request("GET", "http://x/", session=sess, timeout=1,
                     max_response_bytes=50, stream=False)
    assert sess.kwargs.get("stream") is True


# [#7] crypto: 손상된 저장값(iterations<=0)은 예외가 아니라 깨끗한 거부
@pytest.mark.parametrize("encoded", [
    "pbkdf2_sha256$0$c2FsdA==$aGFzaA==", "pbkdf2_sha256$-5$c2FsdA==$aGFzaA==",
    "pbkdf2_sha256$notint$x$y", "garbage", "", "a$b$c",
])
def test_verify_password_never_crashes(encoded):
    assert verify_password("whatever", encoded) is False


# [#13] crypto: NFKC 정규화로 호환 유니코드 비밀번호 동치 처리
def test_password_unicode_nfkc_equivalence():
    import unicodedata
    raw = "ﬁreﬂy"                                    # U+FB01(ﬁ), U+FB02(ﬂ) 합자
    nfkc = unicodedata.normalize("NFKC", raw)        # → "firefly"
    assert raw != nfkc
    encoded = hash_password(raw, iterations=210_000)
    assert verify_password(nfkc, encoded)            # 정규화 동치 → 검증 성공


# [#8] archive: tar 스트리밍 상한(선언 크기 신뢰 안 함) + 링크 거부(재확인)
def test_safe_extract_tar_streaming_total_cap(tmp_path):
    big = _make_tar(tmp_path / "big.tar", [("a.bin", b"x" * 500), ("b.bin", b"y" * 500)])
    with pytest.raises(UnsafeArchiveError):
        safe_extract_tar(big, tmp_path / "out", max_total_bytes=600)


def test_safe_extract_tar_compressed_input_cap(tmp_path):
    import tarfile as _tar, io as _io
    p = tmp_path / "z.tar.gz"
    with _tar.open(p, "w:gz") as tf:
        data = b"A" * 200_000
        info = _tar.TarInfo("big.bin"); info.size = len(data)
        tf.addfile(info, _io.BytesIO(data))
    with pytest.raises(UnsafeArchiveError):           # 압축 입력 상한으로 폭탄 차단
        safe_extract_tar(p, tmp_path / "out", max_total_bytes=1000, max_compressed_bytes=500)


# [#5] log_filter: 포맷 문자열의 %-지정자를 훼손하지 않는다(secret=%s 로그가 유실되지 않음)
def test_log_filter_preserves_percent_format_with_secret_name(_registered_key):
    red = Redactor([_KEY])
    lg, buf = _logger_with_filter("t.sec.pct", red)
    lg.warning("secret=%s done", _KEY)               # 포맷문자열에 'secret=' + 인자에 키
    out = buf.getvalue()
    assert _KEY not in out and "done" in out          # 라인 유실 없이 정상 렌더 + 마스킹


# [#12] log_filter: 로거+핸들러 이중 부착 시에도 sanitize 백슬래시 중복 이스케이프 없음
def test_log_filter_idempotent_no_double_escape():
    red = Redactor()
    flt = SecretRedactingFilter(red, neutralize_controls=True)
    rec = logging.LogRecord("n", logging.INFO, __file__, 1, r"path C:\a\b", None, None)
    flt.filter(rec); flt.filter(rec)                  # 두 번 통과(로거+핸들러 흉내)
    assert rec.msg.count("\\\\") == 2                 # 백슬래시 2개 → 각 1회만 이스케이프


# [#16] redaction: userinfo 토큰 단독(비밀번호 없는 https://TOKEN@host)도 마스킹
def test_userinfo_token_only_masked():
    red = Redactor()
    url = "https" + "://" + "ghp_tokenonlyabc123" + "@github.com/x"
    out = red.redact_text(url)
    assert "ghp_tokenonlyabc123" not in out and PLACEHOLDER in out


# [#2] audit: shell 옵션 활성이 중첩 괄호/멀티라인에 있어도 탐지
def test_check_dangerous_calls_shell_true_variants(tmp_path):
    from security.audit import check_dangerous_calls
    nested = "subprocess.Popen(shlex.split(cmd), shell" + "=True)\n"
    multiline = "subprocess.run(\n    cmd,\n    shell" + "=True,\n)\n"
    root = _mk_root(tmp_path, {"a.py": nested, "b.py": multiline, "ok.py": "x = 1\n"})
    f = check_dangerous_calls(root)
    assert not f.ok and "a.py" in f.detail and "b.py" in f.detail


# [#3] audit: 내부 따옴표/삼중따옴표 f-string SQL 도 탐지
def test_check_sql_injection_inner_quote_and_triple(tmp_path):
    from security.audit import check_sql_injection
    inner = 'cur.exe' + 'cute(f"UPDATE t SET a=\'x\' WHERE id={v}")\n'
    triple = 'cur.exe' + 'cute(f"""SELECT * FROM {t}""")\n'
    root = _mk_root(tmp_path, {"a.py": inner, "b.py": triple})
    f = check_sql_injection(root)
    assert not f.ok and "a.py" in f.detail and "b.py" in f.detail


# [#4] audit: 진짜 시크릿이 '<'/'example' 과 한 줄을 공유해도 탐지
def test_check_credential_material_line_sharing(tmp_path):
    from security.audit import check_credential_material
    vendor = "gh" + "p_" + "b" * 36
    root = _mk_root(tmp_path, {
        "a.py": f'tok = "{vendor}"  # <production>\n',       # '<' 있어도 잡아야
        "b.py": f'tok = "{vendor}"  # see example above\n',  # 'example' 있어도 잡아야
    })
    f = check_credential_material(root)
    assert not f.ok and "a.py" in f.detail and "b.py" in f.detail


# [#9] audit: 'filter' 단어가 주석에 있어도 filter= 인자가 아니면 탐지
def test_check_unsafe_extract_requires_filter_kwarg(tmp_path):
    from security.audit import check_unsafe_extract
    commented = "import tarfile\ntf.extract" + "all(dest)  # TODO add filter later\n"
    real = "import tarfile\ntf.extract" + "all(dest, filter='data')\n"
    root = _mk_root(tmp_path, {"a.py": commented, "b.py": real})
    f = check_unsafe_extract(root)
    assert not f.ok and "a.py" in f.detail and "b.py" not in f.detail


# [#10] audit: 위치 인자 로더(yaml.lo ad 에 yaml.Loader 를 위치로) 도 탐지
def test_check_unsafe_yaml_positional_loader(tmp_path):
    from security.audit import check_unsafe_yaml
    pos = "a = yaml.lo" + "ad(x, yaml.Loader)\n"
    kw = "b = yaml.lo" + "ad(x, Loader=yaml.UnsafeLoader)\n"
    safe = "c = yaml.lo" + "ad(x, Loader=yaml.SafeLoader)\n"
    root = _mk_root(tmp_path, {"a.py": pos, "b.py": kw, "safe.py": safe})
    f = check_unsafe_yaml(root)
    assert not f.ok and "a.py" in f.detail and "b.py" in f.detail and "safe.py" not in f.detail


# [#11] audit: 환경 마커의 연산자를 버전 고정으로 오인하지 않음
def test_check_requirements_env_marker(tmp_path):
    from security.audit import check_requirements_hygiene
    root = _mk_root(tmp_path, {
        "requirements.txt": "requests; python_version>='3.8'\n",   # 마커만 → 여전히 unpinned
        "requirements-ok.txt": "requests>=2.0; python_version>='3.8'\n",
    })
    f = check_requirements_hygiene(root)
    assert not f.ok and "unpinned" in f.detail and "requirements-ok" not in f.detail


# [#14] audit: hashlib.new 의 대문자 다이제스트명도 탐지
def test_check_weak_hash_uppercase_new(tmp_path):
    from security.audit import check_weak_hash
    root = _mk_root(tmp_path, {"a.py": 'h = hashlib.ne' + 'w("MD5", data)\n'})
    f = check_weak_hash(root)
    assert not f.ok and "a.py" in f.detail


# ════════════════════════════════════════════════════════════════════════════════
# commerce 밖 조사 반영(feat/96 3차) — DB IO 가드 + SQLAlchemy text()/오픈 리다이렉트 점검
# ════════════════════════════════════════════════════════════════════════════════
from security import assert_identifier, is_identifier, mask_dsn  # noqa: E402


@pytest.mark.parametrize("name,ok", [
    ("users", True), ("public.users", True), ("col_1", True), ("_x", True),
    ("users; DROP TABLE t", False), ("u--", False), ("1col", False), ("a.b.c", False),
    ("", False), ("a b", False), ("tbl'", False), ("x" * 200, False),
])
def test_dbio_is_identifier(name, ok):
    assert is_identifier(name) is ok


def test_dbio_assert_identifier_raises():
    assert assert_identifier("schema.table") == "schema.table"
    with pytest.raises(ValueError):
        assert_identifier("t; DELETE FROM u")


def test_dbio_mask_dsn_kv_and_url():
    kv = mask_dsn("host=db " + "password=supers3cret" + " dbname=app")
    assert "supers3cret" not in kv and PLACEHOLDER in kv and "dbname=app" in kv
    url = mask_dsn("postgres" + "://u:" + "mypw12345" + "@h:5432/db")
    assert "mypw12345" not in url and PLACEHOLDER in url
    assert mask_dsn("") == ""


def test_check_sql_text_injection(tmp_path):
    from security.audit import check_sql_text_injection
    fstr = 'session.execute(te' + 'xt(f"SELECT * FROM {t}"))\n'
    concat = 'conn.execute(te' + 'xt("SELECT " + name))\n'
    safe = 'session.execute(te' + 'xt("SELECT * FROM t WHERE id = :id"), {"id": x})\n'
    root = _mk_root(tmp_path, {"a.py": fstr, "b.py": concat, "ok.py": safe})
    f = check_sql_text_injection(root)
    assert not f.ok and "a.py" in f.detail and "b.py" in f.detail and "ok.py" not in f.detail


def test_check_open_redirect(tmp_path):
    from security.audit import check_open_redirect
    dyn = "return Redirect" + "Response(request.query_params['next'])\n"
    literal = 'return Redirect' + 'Response("/dashboard", status_code=303)\n'
    ternary = "return Redirect" + 'Response("/a" if ok else "/b")\n'
    root = _mk_root(tmp_path, {"a.py": dyn, "ok.py": literal, "tern.py": ternary})
    f = check_open_redirect(root)
    assert not f.ok and f.severity == "MEDIUM"
    assert "a.py" in f.detail and "ok.py" not in f.detail and "tern.py" not in f.detail


# ════════════════════════════════════════════════════════════════════════════════
# 정밀 리뷰 회귀(fable 세션 마감) — opus 구현부의 6개 결함 수정을 잠근다(P1~P6).
# ════════════════════════════════════════════════════════════════════════════════

# [P1] netio: verify 를 falsy(False/0/"") 로 넘기면 TLS 검증 비활성 → 전부 차단.
#      None(기본 위임)·truthy(True·CA 경로)만 허용(이전엔 `is False` 만 봐서 0/"" 가 우회).
@pytest.mark.parametrize("bad", [False, 0, "", 0.0])
def test_netio_blocks_all_falsy_verify(bad):
    from security import InsecureRequestBlocked
    with pytest.raises(InsecureRequestBlocked):
        http_request("GET", "http://x/", session=_FakeSess(), **{"verify": bad})


@pytest.mark.parametrize("ok_verify", [None, True, "/etc/ssl/ca.pem"])
def test_netio_allows_none_and_truthy_verify(ok_verify):
    sess = _FakeSess()
    http_request("GET", "http://x/", session=sess, timeout=1, **{"verify": ok_verify})
    assert sess.kwargs["verify"] == ok_verify        # 가드 통과 → 그대로 전달


# [P2] netio SSRF: IPv4-compatible(::/96, 예 ::7f00:1=127.0.0.1)·6to4/teredo 내장 IPv4 우회 차단.
@pytest.mark.parametrize("url", [
    "http://[::7f00:1]/x",                            # IPv4-compatible → 127.0.0.1
    "http://[::a9fe:a9fe]/x",                         # ::169.254.169.254 (IMDS)
    "http://[2002:7f00:1::]/x",                       # 6to4 로 127.0.0.1 임베드
    "http://[2002:a9fe:a9fe::]/x",                    # 6to4 로 IMDS 임베드
])
def test_ssrf_guard_blocks_ipv6_embedded_v4(url):
    assert not is_url_allowed(url)


def test_ssrf_guard_still_allows_public_ipv6():
    assert is_url_allowed("https://[2606:2800:220:1:248:1893:25c8:1946]/x")


# [P3] redaction/events: 시크릿이 dict '키' 로 들어와도 마스킹(값 경로와 동일 정책).
def test_redactor_masks_dict_keys():
    red = Redactor()
    red.add_secret("SECRETKEY_ABC123")
    out = red.redact({"SECRETKEY_ABC123": "v", "normal_field": "x"})
    assert "SECRETKEY_ABC123" not in out and PLACEHOLDER in out
    assert "normal_field" in out                      # 일반 필드명은 그대로


def test_event_record_masks_secret_in_dict_key(_registered_key):
    from security.events import event_record
    rec = event_record("t", where="w", ctx={_KEY: "val"})
    assert _KEY not in json.dumps(rec, ensure_ascii=False)


# [P4] crypto: 비문자열 저장값(None/bytes, 예 DB NULL)은 크래시 대신 깨끗한 거부.
@pytest.mark.parametrize("bad", [None, b"pbkdf2_sha256$600000$x$y", 12345, ["x"]])
def test_verify_password_non_str_encoded_rejects(bad):
    assert verify_password("pw", bad) is False


@pytest.mark.parametrize("bad", [None, b"x", 12345])
def test_needs_rehash_non_str_encoded_true(bad):
    assert needs_rehash(bad) is True


# [P5] audit: weak-hash 정규식이 (?i:...) 스코프 플래그(3.11+) 없이 대소문자 인자를 모두 잡는다.
def test_check_weak_hash_new_case_insensitive_portable(tmp_path):
    from security.audit import check_weak_hash
    root = _mk_root(tmp_path, {
        "u.py": 'a = hashlib.ne' + 'w("MD5")\n',       # 대문자
        "l.py": 'b = hashlib.ne' + 'w("sha1")\n',      # 소문자
        "m.py": 'c = hashlib.ne' + 'w("Sha1")\n',      # 혼합
        "ok.py": 'd = hashlib.ne' + 'w("sha256")\n',   # 안전
    })
    f = check_weak_hash(root)
    assert not f.ok
    assert all(n in f.detail for n in ("u.py", "l.py", "m.py")) and "ok.py" not in f.detail


# [P6] CLI: 비UTF-8 콘솔(cp949 등)에서 '—' 포함 리포트 출력이 크래시하지 않는다.
def test_cli_emit_survives_non_utf8_console():
    from security.__main__ import _emit

    class _Cp949Stdout:
        """print() 가 cp949 로 인코딩 실패하는 콘솔 흉내(buffer 폴백 확인)."""
        def __init__(self):
            self.buffer = io.BytesIO()

        def write(self, s):
            s.encode("cp949")                          # '—' 있으면 UnicodeEncodeError
            return len(s)

        def flush(self):
            pass

    import sys as _sys
    saved = _sys.stdout
    _sys.stdout = _Cp949Stdout()
    try:
        _emit("결과: PASS — 차단 이슈 없음")             # em-dash 포함 → print 는 실패, buffer 로 폴백
        written = _sys.stdout.buffer.getvalue()
    finally:
        _sys.stdout = saved
    assert "PASS".encode("utf-8") in written and written.endswith(b"\n")
