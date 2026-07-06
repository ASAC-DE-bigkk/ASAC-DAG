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
