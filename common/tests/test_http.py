"""공통 HTTP 클라이언트 단위 테스트 (#78) — 보안 강제·재시도·rate limit·auth·typed 예외."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.http import (  # noqa: E402
    HeaderKey,
    HttpCore,
    HttpProblemError,
    NoAuth,
    PathKey,
    QueryKey,
    SeoulOpenApiClient,
    TransportResponse,
)
from common.http.limits import resolve_rate_limit  # noqa: E402
from common.security import PLACEHOLDER  # noqa: E402

_KEY = "abcdEFGH1234567890abcdEFGH1234567890zzzz"   # 가짜 서울 인증키(40자)


class FakeTransport:
    """스크립트된 응답/예외를 차례로 돌려주는 가짜 Transport."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    def send(self, method, url, *, params, headers, timeout):
        self.calls.append({"method": method, "url": url, "params": dict(params or {}),
                           "headers": dict(headers or {}), "timeout": timeout})
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _core(script, **kwargs):
    sleeps = []
    transport = FakeTransport(script)
    kwargs.setdefault("rate_limit", None)
    core = HttpCore(transport=transport, sleep=sleeps.append, **kwargs)
    return core, transport, sleeps


def _ok(body=b"ok", **headers):
    return TransportResponse(status=200, content=body, headers=headers)


# ── 보안 강제 ───────────────────────────────────────────────────────────────────
def test_timeout_none_is_rejected():
    with pytest.raises(ValueError):
        HttpCore(timeout=None)


def test_timeout_always_passed_to_transport():
    core, transport, _ = _core([_ok()], timeout=7.5)
    core.get("http://x/")
    assert transport.calls[0]["timeout"] == 7.5


def test_no_verify_parameter_exposed():
    core, _, _ = _core([_ok()])
    with pytest.raises(TypeError):
        core.get("http://x/", verify=False)


def test_exhausted_error_redacts_key_in_url_and_detail(monkeypatch):
    monkeypatch.setenv("SEOUL_API_KEY_TEST", _KEY)
    from common.security import refresh_env_secrets
    refresh_env_secrets()

    core, _, _ = _core([ConnectionError(f"boom url http://openapi.seoul.go.kr:8088/{_KEY}/json/S/1/5/")],
                       max_attempts=1)
    with pytest.raises(HttpProblemError) as ei:
        core.get(f"http://openapi.seoul.go.kr:8088/{_KEY}/json/S/1/5/")
    problem = ei.value.problem
    assert _KEY not in str(problem.request)
    assert PLACEHOLDER in problem.request["url"]
    assert problem.detail is None or _KEY not in problem.detail


# ── 재시도 ──────────────────────────────────────────────────────────────────────
def test_retries_on_500_then_succeeds():
    core, transport, sleeps = _core(
        [TransportResponse(status=500), TransportResponse(status=503), _ok()])
    response = core.get("http://x/")
    assert response.status == 200 and len(transport.calls) == 3
    assert len(sleeps) == 2 and all(s > 0 for s in sleeps)


def test_retry_after_header_is_respected():
    core, _, sleeps = _core(
        [TransportResponse(status=429, headers={"Retry-After": "3"}), _ok()])
    core.get("http://x/")
    assert sleeps == [3.0]


def test_no_retry_on_4xx_non_retryable():
    core, transport, _ = _core([TransportResponse(status=404)])
    with pytest.raises(HttpProblemError) as ei:
        core.get("http://x/")
    assert len(transport.calls) == 1
    assert ei.value.problem.status == 404
    assert ei.value.problem.type == "urn:asac:error:http-error"


def test_post_not_retried_by_default():
    core, transport, _ = _core([TransportResponse(status=500)])
    with pytest.raises(HttpProblemError):
        core.request("POST", "http://x/")
    assert len(transport.calls) == 1


def test_timeout_exception_maps_to_api_timeout_type():
    core, _, _ = _core([TimeoutError("t"), TimeoutError("t"), TimeoutError("t")])
    with pytest.raises(HttpProblemError) as ei:
        core.get("http://x/")
    assert ei.value.problem.type == "urn:asac:error:api-timeout"
    assert ei.value.problem.request["attempts"] == 3


def test_connection_error_maps_to_connection_type():
    core, _, _ = _core([ConnectionError("c")], max_attempts=1)
    with pytest.raises(HttpProblemError) as ei:
        core.get("http://x/")
    assert ei.value.problem.type == "urn:asac:error:connection-error"


# ── rate limit (3단 계층) ───────────────────────────────────────────────────────
def test_rate_limit_enforces_min_interval():
    clock = {"now": 0.0}
    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        clock["now"] += seconds

    transport = FakeTransport([_ok(), _ok()])
    core = HttpCore(transport=transport, rate_limit=2.0,   # 초당 2회 → 최소 0.5s 간격
                    sleep=fake_sleep, monotonic=lambda: clock["now"])
    core.get("http://x/")
    core.get("http://x/")
    assert sleeps and abs(sleeps[0] - 0.5) < 1e-6


def test_rate_limit_layering(monkeypatch, tmp_path):
    assert resolve_rate_limit("seoul_openapi") == 5.0          # 코드 기본값
    assert resolve_rate_limit("unknown-source") is None
    config = tmp_path / "limits.yaml"
    config.write_text("seoul_openapi: 2\nkma: 1.5\n", encoding="utf-8")
    monkeypatch.setenv("ASAC_HTTP_LIMITS_FILE", str(config))
    assert resolve_rate_limit("seoul_openapi") == 2.0          # config 오버라이드
    assert resolve_rate_limit("kma") == 1.5
    assert resolve_rate_limit("seoul_openapi", 9.0) == 9.0     # 호출측 최우선
    assert resolve_rate_limit("seoul_openapi", None) is None   # 명시적 무제한도 존중


# ── auth 전략 ───────────────────────────────────────────────────────────────────
def test_query_key_injects_param():
    core, transport, _ = _core([_ok()])
    core.get("http://api/", params={"page": "1"}, auth=QueryKey("serviceKey", "k1"))
    assert transport.calls[0]["params"] == {"page": "1", "serviceKey": "k1"}


def test_path_key_replaces_placeholder_and_encodes():
    core, transport, _ = _core([_ok()])
    core.get("http://api/{api_key}/json/S/1/5/", auth=PathKey("a/b==", encode=True))
    assert transport.calls[0]["url"] == "http://api/a%2Fb%3D%3D/json/S/1/5/"


def test_path_key_requires_placeholder():
    core, _, _ = _core([_ok()])
    with pytest.raises(ValueError):
        core.get("http://api/no-placeholder/", auth=PathKey("k"))


def test_no_auth_rejects_leftover_placeholder():
    core, _, _ = _core([_ok()])
    with pytest.raises(ValueError):
        core.get("http://api/{api_key}/x/", auth=NoAuth())


def test_header_key_with_scheme():
    core, transport, _ = _core([_ok()])
    core.get("http://api/", auth=HeaderKey("Authorization", "tok", scheme="Bearer"))
    assert transport.calls[0]["headers"]["Authorization"] == "Bearer tok"


# ── 서울 어댑터 ─────────────────────────────────────────────────────────────────
def test_seoul_client_builds_url_and_injects_key():
    core, transport, _ = _core([_ok(b'{"S": {"row": []}}')])
    client = SeoulOpenApiClient(core, _KEY, base_url="http://openapi.seoul.go.kr:8088")
    document = client.fetch_json("LOCALDATA_072404", 1, 1000)
    assert document == {"S": {"row": []}}
    assert transport.calls[0]["url"] == \
        f"http://openapi.seoul.go.kr:8088/{_KEY}/json/LOCALDATA_072404/1/1000/"


def test_seoul_client_per_call_key_override():
    core, transport, _ = _core([_ok()])
    client = SeoulOpenApiClient(core, "default-key-000000", base_url="http://b")
    client.fetch_bytes("SVC", 1, 5, key="override-key-11111")
    assert "override-key-11111" in transport.calls[0]["url"]


def test_seoul_client_base_url_from_env_both_names(monkeypatch):
    # 루트 .env 이름(SEOUL_OPEN_API_BASE_URL)이 1순위, commerce 이름은 폴백
    core, transport, _ = _core([_ok(), _ok(), _ok()])
    monkeypatch.setenv("SEOUL_OPEN_API_BASE_URL", "http://from-root-env")
    monkeypatch.setenv("SEOUL_OPENAPI_BASE_URL", "http://from-commerce-env")
    SeoulOpenApiClient(core, _KEY).fetch_bytes("SVC", 1, 5)
    assert transport.calls[0]["url"].startswith("http://from-root-env/")

    monkeypatch.delenv("SEOUL_OPEN_API_BASE_URL")
    SeoulOpenApiClient(core, _KEY).fetch_bytes("SVC", 1, 5)
    assert transport.calls[1]["url"].startswith("http://from-commerce-env/")

    monkeypatch.delenv("SEOUL_OPENAPI_BASE_URL")
    SeoulOpenApiClient(core, _KEY).fetch_bytes("SVC", 1, 5)
    assert transport.calls[2]["url"].startswith("http://openapi.seoul.go.kr:8088/")
