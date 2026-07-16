"""실시간 수집 결과코드 게이트 + 0행 Discord 경보 단위 테스트 (#229).

- api.result_code / raise_for_result: 서울 두 엔벨로프(지하철 errorMessage/flatten,
  주차 서비스하위 RESULT/최상위 RESULT) 모두에서 INFO-000/200 통과, 그 외 raise.
- alerts.warn_if_empty: 0행이면 Discord send_embed 호출, >0 이면 no-op.
"""
import sys
from pathlib import Path

import pytest

_TRANSIT = Path(__file__).resolve().parents[1]        # domains/transit
_DAGS = Path(__file__).resolve().parents[3]           # dags 루트
for p in (str(_DAGS), str(_TRANSIT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from seoul_transit import alerts  # noqa: E402
from seoul_transit.api import SeoulApiError, raise_for_result, result_code  # noqa: E402


# ── 지하철 엔벨로프 (swopenapi) ──────────────────────────────────────────────
def test_subway_success_errormessage_ok():
    payload = {"errorMessage": {"code": "INFO-000", "message": "정상"},
               "realtimeArrivalList": [{"x": 1}]}
    assert result_code(payload, "realtimeStationArrival") == "INFO-000"
    raise_for_result(payload, "realtimeStationArrival")  # no raise


def test_subway_no_data_info200_ok():
    payload = {"errorMessage": {"code": "INFO-200", "message": "데이터 없음"}}
    raise_for_result(payload, "realtimeStationArrival")  # no raise


def test_subway_auth_error_flattened_raises():
    # 인증오류 시 errorMessage 필드가 top-level 로 flatten (실측 구조).
    payload = {"status": 500, "code": "ERROR-300", "message": "인증키가 유효하지 않습니다",
               "link": "", "developerMessage": "", "total": 0}
    assert result_code(payload, "realtimeStationArrival") == "ERROR-300"
    with pytest.raises(SeoulApiError) as e:
        raise_for_result(payload, "realtimeStationArrival", target="강남")
    assert "ERROR-300" in str(e.value)


# ── 주차 엔벨로프 (openapi.seoul) ────────────────────────────────────────────
def test_parking_success_service_result_ok():
    payload = {"GetParkingInfo": {"list_total_count": 3,
                                   "RESULT": {"CODE": "INFO-000", "MESSAGE": "정상"},
                                   "row": [{"a": 1}]}}
    assert result_code(payload, "GetParkingInfo") == "INFO-000"
    raise_for_result(payload, "GetParkingInfo")  # no raise


def test_parking_top_level_result_error_raises():
    payload = {"RESULT": {"CODE": "INFO-100", "MESSAGE": "인증키가 유효하지 않습니다"}}
    assert result_code(payload, "GetParkingInfo") == "INFO-100"
    with pytest.raises(SeoulApiError):
        raise_for_result(payload, "GetParkingInfo")


def test_parking_no_data_info200_ok():
    payload = {"GetParkingInfo": {"RESULT": {"CODE": "INFO-200"}, "row": []}}
    raise_for_result(payload, "GetParkingInfo")  # no raise


# ── 코드 없음(구형/예외 응답)은 raise 하지 않음 — 0행 경보에 맡긴다 ──────────
def test_formless_no_code_does_not_raise():
    assert result_code({"realtimeArrivalList": []}, "realtimeStationArrival") is None
    raise_for_result({"realtimeArrivalList": []}, "realtimeStationArrival")  # no raise
    assert result_code("not a dict", "x") is None


# ── 0행 Discord 경보 ─────────────────────────────────────────────────────────
def test_warn_if_empty_sends_on_zero(monkeypatch):
    sent = {}
    def fake_send(title, description, *, color, domain, **kw):
        sent.update(title=title, color=color, domain=domain)
        return True
    monkeypatch.setattr(alerts, "send_embed", fake_send)
    assert alerts.warn_if_empty("subway_arrival", 0, "run-1") is True
    assert "subway_arrival" in sent["title"] and sent["domain"] == "transit"
    assert sent["color"] == alerts.COLOR_WARN


def test_warn_if_empty_noop_when_rows(monkeypatch):
    called = {"n": 0}
    monkeypatch.setattr(alerts, "send_embed", lambda *a, **k: called.__setitem__("n", called["n"] + 1))
    assert alerts.warn_if_empty("parking", 42, "run-1") is False
    assert called["n"] == 0


# ── 무경보 창 (심야 미운행 0행 억제) ─────────────────────────────────────────────
def test_in_quiet_hours_window_and_midnight_crossing(monkeypatch):
    from datetime import datetime
    from seoul_transit.config import KST

    monkeypatch.setattr(alerts, "TRANSIT_QUIET_HOURS", "01:00-05:00")
    assert alerts.in_quiet_hours(datetime(2026, 7, 16, 3, 0, tzinfo=KST)) is True
    assert alerts.in_quiet_hours(datetime(2026, 7, 16, 5, 0, tzinfo=KST)) is False   # 종료 경계 미포함
    assert alerts.in_quiet_hours(datetime(2026, 7, 16, 12, 0, tzinfo=KST)) is False
    # 자정 걸침 창
    monkeypatch.setattr(alerts, "TRANSIT_QUIET_HOURS", "23:00-05:00")
    assert alerts.in_quiet_hours(datetime(2026, 7, 16, 23, 30, tzinfo=KST)) is True
    assert alerts.in_quiet_hours(datetime(2026, 7, 16, 4, 0, tzinfo=KST)) is True
    assert alerts.in_quiet_hours(datetime(2026, 7, 16, 12, 0, tzinfo=KST)) is False
    # 형식 오류 → 억제 비활성(항상 경보)
    monkeypatch.setattr(alerts, "TRANSIT_QUIET_HOURS", "nonsense")
    assert alerts.in_quiet_hours(datetime(2026, 7, 16, 3, 0, tzinfo=KST)) is False


def test_warn_if_empty_suppressed_in_quiet_hours(monkeypatch):
    called = {"n": 0}
    monkeypatch.setattr(alerts, "send_embed", lambda *a, **k: called.__setitem__("n", called["n"] + 1))
    monkeypatch.setattr(alerts, "in_quiet_hours", lambda now=None: True)
    # 심야 미운행 소스(quiet_ok=True) → 0행이어도 억제
    assert alerts.warn_if_empty("subway_arrival", 0, "run-1", quiet_ok=True) is False
    assert called["n"] == 0
    # 24시간 소스(주차, 기본 quiet_ok=False) → 무경보 창이어도 경보
    alerts.warn_if_empty("parking", 0, "run-1")
    assert called["n"] == 1
