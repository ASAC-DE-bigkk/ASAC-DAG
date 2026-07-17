"""Weather watchdog DAG import tests without a live Airflow installation."""

from __future__ import annotations

import copy
from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sys
import types

import pytest


_FAKE_MODULE_NAMES = (
    "airflow",
    "airflow.models",
    "airflow.sdk",
    "airflow.providers",
    "airflow.providers.standard",
    "airflow.providers.standard.operators",
    "airflow.providers.standard.operators.python",
    "common.errors.airflow",
    "common.runmetrics",
)


@pytest.fixture(autouse=True)
def restore_fake_modules_after_dag_import():
    originals = {name: sys.modules.get(name) for name in _FAKE_MODULE_NAMES}
    yield
    for name, module in originals.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


class FakeDAG:
    _stack = []

    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.task_dict = {}

    def __enter__(self):
        self._stack.append(self)
        return self

    def __exit__(self, *_exc_info):
        self._stack.pop()

    def add_task(self, task):
        self.task_dict[task.task_id] = task


class FakePythonOperator:
    def __init__(self, task_id, python_callable, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.kwargs = kwargs
        FakeDAG._stack[-1].add_task(self)


class FakeVariable:
    @staticmethod
    def get(_key, default_var=None):
        return default_var

    @staticmethod
    def set(_key, _value):
        return None


def _track(**_kwargs):
    return lambda function: function


def load_module():
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG
    airflow_models = types.ModuleType("airflow.models")
    airflow_sdk = types.ModuleType("airflow.sdk")
    airflow_sdk.Variable = FakeVariable
    airflow_providers = types.ModuleType("airflow.providers")
    airflow_standard = types.ModuleType("airflow.providers.standard")
    airflow_operators = types.ModuleType("airflow.providers.standard.operators")
    airflow_python = types.ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = FakePythonOperator
    errors_airflow = types.ModuleType("common.errors.airflow")
    errors_airflow.problem_failure_callback = lambda **_kwargs: (
        lambda *_args, **_kwargs: None
    )
    runmetrics = types.ModuleType("common.runmetrics")
    runmetrics.track = _track
    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.models": airflow_models,
            "airflow.sdk": airflow_sdk,
            "airflow.providers": airflow_providers,
            "airflow.providers.standard": airflow_standard,
            "airflow.providers.standard.operators": airflow_operators,
            "airflow.providers.standard.operators.python": airflow_python,
            "common.errors.airflow": errors_airflow,
            "common.runmetrics": runmetrics,
        }
    )
    module_path = Path(__file__).resolve().parents[1] / "weather_reliability_report.py"
    spec = importlib.util.spec_from_file_location(
        "weather_reliability_report_under_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _weather_failure_report(run_id="scheduled__weather-a"):
    return {
        "status": "FAIL",
        "detected_at": "2026-07-14T09:00:00+09:00",
        "weather": {
            "status": "FAIL",
            "reason": "no_weather_rows",
            "coverage_ok": False,
            "freshness_status": "FAIL",
            "freshness_minutes": 400,
            "last_collected_at": "2026-07-14 00:00:00+00:00",
        },
        "dag_runs": {
            "latest_dag_run_id": run_id,
            "latest_status": "FAILED",
            "latest_is_publishable": False,
            "latest_event_at": "2026-07-14 00:05:00+00:00",
        },
        "publishability_ok": False,
    }


def test_weather_fingerprint_ignores_timestamps_and_distinguishes_failure_identity():
    module = load_module()
    first = _weather_failure_report()
    timestamp_only_change = copy.deepcopy(first)
    timestamp_only_change["detected_at"] = "2026-07-14T10:00:00+09:00"
    timestamp_only_change["weather"]["freshness_minutes"] = 460
    timestamp_only_change["weather"]["last_collected_at"] = "2026-07-14 00:10:00+00:00"
    timestamp_only_change["dag_runs"]["latest_event_at"] = "2026-07-14 00:15:00+00:00"
    different_failure = _weather_failure_report("scheduled__weather-b")

    first_fingerprint = module.notification_fingerprint(first)

    assert module.notification_fingerprint(timestamp_only_change) == first_fingerprint
    assert module.notification_fingerprint(different_failure) != first_fingerprint
    assert (
        module.should_notify_fingerprint(
            first_fingerprint,
            get=lambda *_args, **_kwargs: first_fingerprint,
        )
        is False
    )
    assert (
        module.should_notify_fingerprint(
            module.notification_fingerprint(different_failure),
            get=lambda *_args, **_kwargs: first_fingerprint,
        )
        is True
    )


def test_weather_variable_tracking_is_fail_open():
    module = load_module()

    assert (
        module.should_notify_fingerprint(
            "fingerprint",
            get=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("state unavailable")
            ),
        )
        is True
    )
    assert (
        module.record_delivered_fingerprint(
            "fingerprint",
            set=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("state unavailable")
            ),
        )
        is False
    )


def test_weather_delivery_history_keeps_prior_daily_fingerprints():
    module = load_module()
    state = {"value": "[]"}

    def get(*_args, **_kwargs):
        return state["value"]

    def set_value(_key, value):
        state["value"] = value

    first = module.daily_delivery_fingerprint(
        _weather_failure_report(),
        logical_date=datetime(2026, 7, 17, tzinfo=timezone.utc),
        run_id="scheduled__day-1",
    )
    second = module.daily_delivery_fingerprint(
        _weather_failure_report(),
        logical_date=datetime(2026, 7, 18, tzinfo=timezone.utc),
        run_id="scheduled__day-2",
    )

    assert module.should_notify_fingerprint(first, get=get) is True
    assert module.record_delivered_fingerprint(first, get=get, set=set_value) is True
    assert module.should_notify_fingerprint(second, get=get) is True
    assert module.record_delivered_fingerprint(second, get=get, set=set_value) is True
    assert module.should_notify_fingerprint(first, get=get) is False


def test_weather_records_fingerprint_only_after_successful_send(monkeypatch):
    module = load_module()
    source_report = _weather_failure_report()
    logical_date = datetime(2026, 7, 17, tzinfo=timezone.utc)
    expected_fingerprint = module.daily_delivery_fingerprint(
        source_report,
        logical_date=logical_date,
        run_id="report-run",
    )
    events = []
    monkeypatch.setattr(
        module, "build_weather_reliability_report", lambda: copy.deepcopy(source_report)
    )
    monkeypatch.setattr(
        module, "format_weather_discord_message", lambda _report: "message"
    )
    monkeypatch.setattr(
        module,
        "should_notify_fingerprint",
        lambda fingerprint: events.append(("decision", fingerprint)) or True,
    )
    monkeypatch.setattr(
        module,
        "send_discord_message",
        lambda message: events.append(("send", message)) or True,
    )
    monkeypatch.setattr(
        module,
        "record_delivered_fingerprint",
        lambda fingerprint: events.append(("record", fingerprint)) or True,
    )

    result = module.collect_and_notify(
        run_id="report-run",
        logical_date=logical_date,
    )

    assert events == [
        ("decision", expected_fingerprint),
        ("send", "message"),
        ("record", expected_fingerprint),
    ]
    assert result["discord_sent"] is True
    assert result["notification_state_recorded"] is True
    assert result["notification_fingerprint"] == module.notification_fingerprint(source_report)
    assert result["delivery_fingerprint"] == expected_fingerprint


def test_weather_daily_report_sends_again_on_next_kst_day_when_pass_is_unchanged(
    monkeypatch,
):
    module = load_module()
    source_report = _weather_failure_report()
    source_report["status"] = "PASS"
    source_report["weather"].update(
        status="PASS",
        reason=None,
        coverage_ok=True,
        freshness_status="PASS",
    )
    source_report["dag_runs"].update(
        latest_status="SUCCESS",
        latest_is_publishable=True,
    )
    source_report["publishability_ok"] = True
    state = {"value": "UNKNOWN"}
    sent = []
    recorded = []
    first_day = datetime(2026, 7, 17, tzinfo=timezone.utc)
    second_day = datetime(2026, 7, 18, tzinfo=timezone.utc)
    monkeypatch.setattr(
        module, "build_weather_reliability_report", lambda: copy.deepcopy(source_report)
    )
    monkeypatch.setattr(
        module, "format_weather_discord_message", lambda _report: "daily-message"
    )
    monkeypatch.setattr(
        module, "should_notify_fingerprint", lambda fingerprint: state["value"] != fingerprint
    )
    monkeypatch.setattr(
        module,
        "send_discord_message",
        lambda message: sent.append(message) or True,
    )
    monkeypatch.setattr(
        module,
        "record_delivered_fingerprint",
        lambda fingerprint: recorded.append(fingerprint) or state.update(value=fingerprint) or True,
    )

    first = module.collect_and_notify(run_id="scheduled__day-1", logical_date=first_day)
    same_day_retry = module.collect_and_notify(
        run_id="manual__same-day",
        logical_date=first_day,
    )
    next_day = module.collect_and_notify(run_id="scheduled__day-2", logical_date=second_day)

    assert sent == ["daily-message", "daily-message"]
    assert same_day_retry["discord_sent"] is False
    assert first["notification_fingerprint"] == next_day["notification_fingerprint"]
    assert first["delivery_fingerprint"] != next_day["delivery_fingerprint"]
    assert recorded == [first["delivery_fingerprint"], next_day["delivery_fingerprint"]]


def test_weather_failed_send_is_retried_without_recording_state(monkeypatch):
    module = load_module()
    source_report = _weather_failure_report()
    state = {"value": "UNKNOWN"}
    attempts = []
    records = []
    monkeypatch.setattr(
        module, "build_weather_reliability_report", lambda: copy.deepcopy(source_report)
    )
    monkeypatch.setattr(
        module, "format_weather_discord_message", lambda _report: "message"
    )
    monkeypatch.setattr(
        module,
        "should_notify_fingerprint",
        lambda fingerprint: state["value"] != fingerprint,
    )
    monkeypatch.setattr(
        module,
        "send_discord_message",
        lambda _message: attempts.append("send") or False,
    )
    monkeypatch.setattr(
        module,
        "record_delivered_fingerprint",
        lambda fingerprint: (
            records.append(fingerprint) or state.update(value=fingerprint) or True
        ),
    )

    first = module.collect_and_notify(run_id="report-run-1")
    second = module.collect_and_notify(run_id="report-run-2")

    assert first["discord_sent"] is False
    assert second["discord_sent"] is False
    assert attempts == ["send", "send"]
    assert records == []
    assert state["value"] == "UNKNOWN"


def test_weather_sender_exception_leaves_state_retryable(monkeypatch):
    module = load_module()
    source_report = _weather_failure_report()
    state = {"value": "UNKNOWN"}
    outcomes = [RuntimeError("network unavailable"), True]
    attempts = []
    monkeypatch.setattr(
        module, "build_weather_reliability_report", lambda: copy.deepcopy(source_report)
    )
    monkeypatch.setattr(
        module, "format_weather_discord_message", lambda _report: "message"
    )
    monkeypatch.setattr(
        module,
        "should_notify_fingerprint",
        lambda fingerprint: state["value"] != fingerprint,
    )

    def send(_message):
        attempts.append("send")
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(module, "send_discord_message", send)
    monkeypatch.setattr(
        module,
        "record_delivered_fingerprint",
        lambda fingerprint: state.update(value=fingerprint) or True,
    )

    first = module.collect_and_notify(run_id="report-run-1")
    second = module.collect_and_notify(run_id="report-run-2")

    assert first["discord_sent"] is False
    assert second["discord_sent"] is True
    assert attempts == ["send", "send"]
    assert state["value"] == module.daily_delivery_fingerprint(
        source_report,
        logical_date=None,
        run_id="report-run-2",
    )


def test_weather_formatter_error_remains_task_failure(monkeypatch):
    module = load_module()
    monkeypatch.setattr(
        module,
        "build_weather_reliability_report",
        lambda: _weather_failure_report(),
    )
    monkeypatch.setattr(module, "should_notify_fingerprint", lambda _fingerprint: True)
    monkeypatch.setattr(
        module,
        "format_weather_discord_message",
        lambda _report: (_ for _ in ()).throw(ValueError("invalid report shape")),
    )

    with pytest.raises(ValueError, match="invalid report shape"):
        module.collect_and_notify(run_id="report-run")
