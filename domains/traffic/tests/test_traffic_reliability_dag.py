"""Traffic watchdog DAG import tests without a live Airflow installation."""

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
    module_path = Path(__file__).resolve().parents[1] / "traffic_reliability_report.py"
    spec = importlib.util.spec_from_file_location(
        "traffic_reliability_report_under_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _traffic_failure_report(task_id="collect-a"):
    return {
        "status": "FAIL",
        "detected_at": "2026-07-14T09:00:00+09:00",
        "traffic": {
            "status": "PASS",
            "coverage_ok": True,
            "freshness_status": "PASS",
            "freshness_minutes": 5,
            "last_collected_at": "2026-07-14 00:00:00+00:00",
        },
        "dag_runs": {
            "latest_dag_run_id": "scheduled__traffic-latest",
            "latest_status": "SUCCESS",
            "latest_is_publishable": True,
            "latest_event_at": "2026-07-14 00:05:00+00:00",
        },
        "scheduled_runs": {
            "failed": 1,
            "failures": [
                {
                    "logical_date": "2026-07-14T00:00:00+00:00",
                    "run_id": "scheduled__traffic-failure",
                    "task_id": task_id,
                    "reason": "source timeout",
                },
            ],
        },
        "publishability_ok": True,
    }


def test_traffic_fingerprint_ignores_observation_timestamps_and_distinguishes_failure_identity():
    module = load_module()
    first = _traffic_failure_report()
    timestamp_only_change = copy.deepcopy(first)
    timestamp_only_change["detected_at"] = "2026-07-14T10:00:00+09:00"
    timestamp_only_change["traffic"]["freshness_minutes"] = 10
    timestamp_only_change["traffic"]["last_collected_at"] = "2026-07-14 00:10:00+00:00"
    timestamp_only_change["dag_runs"]["latest_event_at"] = "2026-07-14 00:15:00+00:00"
    different_failure = _traffic_failure_report("collect-b")

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


def test_traffic_fingerprint_is_stable_when_same_contiguous_gap_expands():
    module = load_module()
    first = _traffic_failure_report()
    expanded = copy.deepcopy(first)
    expanded["scheduled_runs"]["failed"] = 2
    expanded["scheduled_runs"]["failures"].append(
        {
            "logical_date": "2026-07-14T00:05:00+00:00",
            "run_id": "scheduled__2026-07-14T00:05:00+00:00",
            "task_id": "collect-a",
            "reason": "source timeout",
        }
    )
    discontiguous = copy.deepcopy(expanded)
    discontiguous["scheduled_runs"]["failed"] = 3
    discontiguous["scheduled_runs"]["failures"].append(
        {
            "logical_date": "2026-07-14T00:15:00+00:00",
            "run_id": "scheduled__2026-07-14T00:15:00+00:00",
            "task_id": "collect-a",
            "reason": "source timeout",
        }
    )

    assert module.notification_fingerprint(expanded) == module.notification_fingerprint(first)
    assert module.notification_fingerprint(discontiguous) != module.notification_fingerprint(first)


def test_traffic_fingerprint_ignores_new_started_run_when_terminal_failure_is_unchanged():
    module = load_module()
    report = _traffic_failure_report()
    report["publishability_ok"] = False
    report["dag_runs"].update(
        latest_dag_run_id="scheduled__started-1",
        latest_status="STARTED",
        latest_is_publishable=False,
        latest_terminal_dag_run_id="scheduled__terminal-failure",
        latest_terminal_status="FAILED",
        latest_terminal_is_publishable=False,
    )
    newer_started_run = copy.deepcopy(report)
    newer_started_run["dag_runs"].update(
        latest_dag_run_id="scheduled__started-2",
        latest_event_at="2026-07-14T00:20:00+00:00",
    )

    assert module.notification_fingerprint(newer_started_run) == module.notification_fingerprint(report)


def test_traffic_variable_tracking_is_fail_open():
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


def test_traffic_delivery_history_keeps_prior_daily_fingerprints():
    module = load_module()
    state = {"value": "[]"}

    def get(*_args, **_kwargs):
        return state["value"]

    def set_value(_key, value):
        state["value"] = value

    first = module.daily_delivery_fingerprint(
        _traffic_failure_report(),
        logical_date=datetime(2026, 7, 17, tzinfo=timezone.utc),
        run_id="scheduled__day-1",
    )
    second = module.daily_delivery_fingerprint(
        _traffic_failure_report(),
        logical_date=datetime(2026, 7, 18, tzinfo=timezone.utc),
        run_id="scheduled__day-2",
    )

    assert module.should_notify_fingerprint(first, get=get) is True
    assert module.record_delivered_fingerprint(first, get=get, set=set_value) is True
    assert module.should_notify_fingerprint(second, get=get) is True
    assert module.record_delivered_fingerprint(second, get=get, set=set_value) is True
    assert module.should_notify_fingerprint(first, get=get) is False


def test_traffic_records_fingerprint_only_after_successful_send(monkeypatch):
    module = load_module()
    source_report = _traffic_failure_report()
    logical_date = datetime(2026, 7, 17, tzinfo=timezone.utc)
    expected_fingerprint = module.daily_delivery_fingerprint(
        source_report,
        logical_date=logical_date,
        run_id="report-run",
    )
    events = []
    monkeypatch.setattr(
        module, "build_traffic_reliability_report", lambda: copy.deepcopy(source_report)
    )
    monkeypatch.setattr(
        module, "format_traffic_discord_message", lambda _report: "message"
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


def test_traffic_daily_report_sends_again_on_next_kst_day_when_pass_is_unchanged(
    monkeypatch,
):
    module = load_module()
    source_report = _traffic_failure_report()
    source_report["status"] = "PASS"
    source_report["scheduled_runs"] = {"failed": 0, "failures": []}
    state = {"value": "UNKNOWN"}
    sent = []
    recorded = []
    first_day = datetime(2026, 7, 17, tzinfo=timezone.utc)
    second_day = datetime(2026, 7, 18, tzinfo=timezone.utc)
    monkeypatch.setattr(
        module, "build_traffic_reliability_report", lambda: copy.deepcopy(source_report)
    )
    monkeypatch.setattr(
        module, "format_traffic_discord_message", lambda _report: "daily-message"
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


def test_traffic_collect_and_notify_does_not_depend_on_airflow_task_log_metadata(
    monkeypatch,
):
    module = load_module()
    source_report = _traffic_failure_report()
    captured = {}
    monkeypatch.setattr(
        module,
        "build_traffic_reliability_report",
        lambda **kwargs: captured.update(kwargs) or copy.deepcopy(source_report),
    )
    monkeypatch.setattr(module, "should_notify_fingerprint", lambda _fingerprint: False)

    module.collect_and_notify(
        run_id="report-run",
        ti=types.SimpleNamespace(
            log_url="http://localhost:30585/dags/traffic_bronze_reliability_report/log"
        ),
    )

    assert captured == {}


def test_traffic_failed_send_is_retried_without_recording_state(monkeypatch):
    module = load_module()
    source_report = _traffic_failure_report()
    state = {"value": "UNKNOWN"}
    attempts = []
    records = []
    monkeypatch.setattr(
        module, "build_traffic_reliability_report", lambda: copy.deepcopy(source_report)
    )
    monkeypatch.setattr(
        module, "format_traffic_discord_message", lambda _report: "message"
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


def test_traffic_sender_exception_leaves_state_retryable(monkeypatch):
    module = load_module()
    source_report = _traffic_failure_report()
    state = {"value": "UNKNOWN"}
    outcomes = [RuntimeError("network unavailable"), True]
    attempts = []
    monkeypatch.setattr(
        module, "build_traffic_reliability_report", lambda: copy.deepcopy(source_report)
    )
    monkeypatch.setattr(
        module, "format_traffic_discord_message", lambda _report: "message"
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


def test_traffic_formatter_error_remains_task_failure(monkeypatch):
    module = load_module()
    monkeypatch.setattr(
        module,
        "build_traffic_reliability_report",
        lambda: _traffic_failure_report(),
    )
    monkeypatch.setattr(module, "should_notify_fingerprint", lambda _fingerprint: True)
    monkeypatch.setattr(
        module,
        "format_traffic_discord_message",
        lambda _report: (_ for _ in ()).throw(ValueError("invalid report shape")),
    )

    with pytest.raises(ValueError, match="invalid report shape"):
        module.collect_and_notify(run_id="report-run")
