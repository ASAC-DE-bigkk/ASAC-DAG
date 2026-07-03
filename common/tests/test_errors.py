"""공통 에러 모듈 단위 테스트 — 직렬화·유형 분류·경로 규약·at-rest redaction·콜백 (#77).

redaction 테스트는 commerce `tests/test_security.py::test_bronze_marker_error_is_redacted`
를 본떴다: 키가 박힌 예외를 만들고, 저장되는 JSON 에 평문 키가 없어야 한다.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.errors import types as error_types  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.errors.problem import Problem, ProblemError  # noqa: E402
from common.errors.sink import R2ErrorSink, build_object_key  # noqa: E402
from common.security import PLACEHOLDER  # noqa: E402

_KEY = "abcdEFGH1234567890abcdEFGH1234567890zzzz"   # 가짜 서울 인증키(40자)
_OCCURRED = datetime(2026, 7, 3, 3, 12, 45, 123456, tzinfo=timezone.utc)


def _make_problem(**overrides) -> Problem:
    base = dict(
        type="urn:asac:error:api-timeout",
        title="External API request timed out",
        detail="request timeout",
        domain="traffic",
        dag_id="traffic_incident_bronze",
        task_id="land_seoul_traffic_raw",
        run_id="scheduled__2026-07-03T03:10:00+00:00",
        try_number=3,
        occurred_at=_OCCURRED,
    )
    base.update(overrides)
    return Problem(**base)


# ── 직렬화 (RFC 9457) ───────────────────────────────────────────────────────────
def test_to_dict_has_standard_and_extension_members():
    doc = _make_problem(source_system="seoul_topis", status=504).to_dict()
    assert doc["type"] == "urn:asac:error:api-timeout"
    assert doc["title"] == "External API request timed out"
    assert doc["status"] == 504
    assert doc["domain"] == "traffic" and doc["dag_id"] == "traffic_incident_bronze"
    assert doc["try_number"] == 3 and doc["source_system"] == "seoul_topis"
    assert doc["occurred_at"] == "2026-07-03T03:12:45.123456+00:00"
    assert doc["schema_version"] == "v1"


def test_to_dict_omits_none_members():
    doc = _make_problem(status=None, request=None, docs_url=None).to_dict()
    assert "status" not in doc and "request" not in doc and "docs_url" not in doc


def test_from_exception_builds_instance_urn_and_detail():
    problem = Problem.from_exception(
        RuntimeError("boom"), domain="traffic", dag_id="d", task_id="t",
        run_id="manual__2026-07-03", try_number=2)
    assert problem.instance == "urn:asac:run:d:manual__2026-07-03/t:2"
    assert problem.detail == "RuntimeError: boom"
    assert problem.type == "urn:asac:error:unhandled"


def test_from_exception_uses_problem_error_payload():
    exc = ProblemError(error_types.API_TIMEOUT, "request timeout", status=504,
                       source_system="seoul_topis")
    problem = Problem.from_exception(exc, domain="traffic", dag_id="d", task_id="t",
                                     run_id="r", try_number=1)
    assert problem.type == "urn:asac:error:api-timeout"
    assert problem.status == 504 and problem.source_system == "seoul_topis"
    assert problem.domain == "traffic" and problem.dag_id == "d"


# ── 유형 분류 ───────────────────────────────────────────────────────────────────
def test_classify_timeout_and_connection_and_unknown():
    assert error_types.classify_exception(TimeoutError("t")).slug == "api-timeout"
    assert error_types.classify_exception(ConnectionResetError("c")).slug == "connection-error"
    assert error_types.classify_exception(ValueError("v")).slug == "unhandled"


def test_classify_urllib_http_error_before_connection():
    import urllib.error
    exc = urllib.error.HTTPError("http://x", 500, "boom", None, None)
    assert error_types.classify_exception(exc).slug == "http-error"


def test_registry_unknown_slug_falls_back_to_unhandled():
    assert error_types.get("no-such-slug").slug == "unhandled"
    assert error_types.get("api-timeout").slug == "api-timeout"


# ── 경로 규약 ───────────────────────────────────────────────────────────────────
def test_object_key_follows_date_first_convention():
    key = build_object_key(_make_problem())
    assert key == (
        "errors/observed_date=2026-07-03/domain=traffic/dag_id=traffic_incident_bronze/"
        "scheduled__2026-07-03T03-10-00-00-00__031245123456_api-timeout.json"
    )


def test_object_key_sanitizes_reserved_characters():
    key = build_object_key(_make_problem(run_id="manual__2026-07-03T01:02:03+09:00 (retry)"))
    segment = key.rsplit("/", 1)[1]
    assert ":" not in segment and "+" not in segment and " " not in segment
    assert "(" not in segment and ")" not in segment


def test_object_key_missing_fields_become_unknown():
    key = build_object_key(_make_problem(domain=None, dag_id=None, run_id=None))
    assert "/domain=unknown/" in key and "/dag_id=unknown/" in key
    assert key.rsplit("/", 1)[1].startswith("unknown__")


# ── at-rest redaction (test_bronze_marker_error_is_redacted 본뜸) ────────────────
def test_stored_document_is_redacted(monkeypatch):
    """예외 메시지에 키 박힌 URL 이 들어가도 저장 JSON 에 평문 키가 남지 않아야 한다."""
    monkeypatch.setenv("SEOUL_API_KEY_TRAFFIC", _KEY)
    stored = {}
    sink = R2ErrorSink(put_object=lambda key, body: stored.update(key=key, body=body))

    exc = RuntimeError(f"connect fail url: http://openapi.seoul.go.kr:8088/{_KEY}/json/SVC/1/1/")
    problem = Problem.from_exception(exc, domain="traffic", dag_id="d", task_id="t",
                                     run_id="r", try_number=1)
    object_key = sink.write(problem)

    text = stored["body"].decode("utf-8")
    assert _KEY not in text and PLACEHOLDER in text
    assert _KEY not in object_key
    document = json.loads(text)                       # 저장물은 유효한 JSON
    assert document["type"] == "urn:asac:error:unhandled"


def test_request_member_is_redacted(monkeypatch):
    monkeypatch.setenv("SEOUL_API_KEY_TRAFFIC", _KEY)
    stored = {}
    sink = R2ErrorSink(put_object=lambda key, body: stored.update(body=body))
    sink.write(_make_problem(request={"method": "GET", "url": f"http://x/{_KEY}/json/S/1/1/"}))
    assert _KEY not in stored["body"].decode("utf-8")


# ── Airflow 콜백 ────────────────────────────────────────────────────────────────
def _airflow_context(exc: BaseException | None) -> dict:
    return {
        "task_instance": SimpleNamespace(task_id="land_seoul_traffic_raw",
                                         dag_id="traffic_incident_bronze", try_number=3),
        "dag": SimpleNamespace(dag_id="traffic_incident_bronze"),
        "run_id": "scheduled__2026-07-03T03:10:00+00:00",
        "exception": exc,
    }


def test_callback_writes_problem_from_context():
    written = []
    sink = R2ErrorSink(put_object=lambda key, body: written.append((key, body)))
    callback = problem_failure_callback("traffic", source_system="seoul_topis", sink=sink)

    callback(_airflow_context(TimeoutError("request timeout")))

    assert len(written) == 1
    key, body = written[0]
    assert "/domain=traffic/dag_id=traffic_incident_bronze/" in key
    document = json.loads(body)
    assert document["type"] == "urn:asac:error:api-timeout"
    assert document["task_id"] == "land_seoul_traffic_raw"
    assert document["try_number"] == 3
    assert document["source_system"] == "seoul_topis"
    assert document["instance"].startswith("urn:asac:run:traffic_incident_bronze:")


def test_callback_never_raises_when_sink_fails():
    def broken_put(key, body):
        raise RuntimeError(f"R2 down, credential={_KEY}")

    callback = problem_failure_callback("traffic", sink=R2ErrorSink(put_object=broken_put))
    callback(_airflow_context(ValueError("boom")))    # 예외가 밖으로 새면 테스트 실패


def test_callback_handles_missing_exception_and_ti():
    written = []
    sink = R2ErrorSink(put_object=lambda key, body: written.append(body))
    callback = problem_failure_callback("traffic", sink=sink)
    callback({"run_id": "manual__x"})                 # ti/dag/exception 없음
    document = json.loads(written[0])
    assert document["type"] == "urn:asac:error:unhandled"
    assert document["run_id"] == "manual__x"
