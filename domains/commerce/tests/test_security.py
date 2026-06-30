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
        "SEOUL_OPENAPI_KEY": _KEY,                  # 수집
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
    install_log_redaction()
    report = run_security_verification(runtime_checks=True)
    assert report.ok, report.render()


# ── end-to-end: bronze 마커(at-rest)로 키가 새지 않음 ───────────────────────────
def test_bronze_marker_error_is_redacted(tmp_path, monkeypatch):
    """네트워크 예외 메시지에 키가 박혀도 bronze 마커 JSON 에 평문 키가 남지 않아야 한다."""
    monkeypatch.setenv("SEOUL_OPENAPI_KEY", _KEY)
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
