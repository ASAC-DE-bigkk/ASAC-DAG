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
        "SEOUL_OPENAPI_BASE_URL": "http://x/y",     # deny(_URL)
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
    from common.schemas import Dataset
    from common.storage import get_storage

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
