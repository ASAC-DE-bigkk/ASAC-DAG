import hashlib
import json
import types

import pytest

from traffic_transform_test_support import (
    FakeAirflowFailException,
    FakeAirflowSkipException,
    FakeVariable,
    load_transform_module,
)
from traffic_transform_test_support import restore_airflow_modules_after_dag_import  # noqa: F401


def test_silver_dag_is_incident_only_and_admits_before_any_dbt_phase(monkeypatch):
    monkeypatch.delenv("ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE", raising=False)
    module = load_transform_module()
    dag = module.dag

    assert dag.kwargs["schedule"].uri == module.TRAFFIC_INCIDENT_BRONZE_ASSET
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.task_dict["validate_dev_runtime"].downstream_task_ids == {
        "resolve_traffic_snapshot_run"
    }
    assert dag.task_dict["resolve_traffic_snapshot_run"].downstream_task_ids == {
        "admit_traffic_silver_snapshot"
    }
    assert dag.task_dict["admit_traffic_silver_snapshot"].downstream_task_ids == {
        "dbt_deps"
    }
    assert "dbt_run_gold" not in dag.task_ids
    assert "select_traffic_test_tier" not in dag.task_ids


def test_silver_dag_keeps_fence_then_publishes_asset_before_metrics():
    module = load_transform_module()
    dag = module.dag

    assert dag.task_dict["dbt_run_silver"].kwargs["op_kwargs"]["silver_fence_mode"] == "write"
    assert dag.task_dict["dbt_test_silver"].kwargs["op_kwargs"]["silver_fence_mode"] == "verify"
    assert dag.task_dict["dbt_test_silver"].downstream_task_ids == {
        "publish_traffic_incident_silver_asset"
    }
    assert dag.task_dict["publish_traffic_incident_silver_asset"].downstream_task_ids == {
        "publish_dbt_run_metrics"
    }


def test_silver_module_has_no_flow_or_citydata_import_or_resolver():
    module = load_transform_module()
    source = open(module.__file__, encoding="utf-8").read()

    assert "TRAFFIC_FLOW_BRONZE_ASSET" not in source
    assert "resolve_citydata_crowding_snapshot_id" not in source
    assert "resolve_citydata_crowding_snapshot_id" not in source


def test_silver_admission_skips_only_exact_marker_and_evidence(monkeypatch):
    module = load_transform_module()
    fingerprint = hashlib.sha256(b'["a","b"]').hexdigest()
    monkeypatch.setattr(
        module,
        "current_silver_output_evidence",
        lambda: module.SilverOutputEvidence(42, fingerprint),
    )
    FakeVariable.values[module.SILVER_SUCCESS_MARKER_KEY] = json.dumps(
        {
            "version": 1,
            "pipeline": "silver",
            "identity": {"pipeline": "silver", "incident_run_id": "incident-1"},
            "output_snapshot_id": 42,
            "compacted_files_fingerprint": fingerprint,
        }
    )
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            "incident-1"
            if task_ids == module.SNAPSHOT_TASK_ID
            else None
        )
    )

    with pytest.raises(FakeAirflowSkipException):
        module.admit_traffic_silver_snapshot(ti=ti)

    FakeVariable.values[module.SILVER_SUCCESS_MARKER_KEY] = "{malformed"
    with pytest.raises(FakeAirflowFailException):
        module.admit_traffic_silver_snapshot(ti=ti)


def test_silver_asset_publication_writes_marker_only_after_outlet_publish(monkeypatch):
    module = load_transform_module()
    calls = []
    fingerprint = hashlib.sha256(b'["a"]').hexdigest()
    monkeypatch.setattr(module, "publish_through_alias", lambda *_args, **_kwargs: calls.append("publish"))

    class OrderedVariable(FakeVariable):
        @classmethod
        def set(cls, key, value, **kwargs):
            calls.append("marker")
            super().set(key, value, **kwargs)

    monkeypatch.setattr(module, "Variable", OrderedVariable)
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            "incident-1"
            if task_ids == module.SNAPSHOT_TASK_ID
            else {"silver_snapshot_evidence": {"snapshot_id": 42, "committed_at": "2026-07-19T12:00:00+09:00", "operation": "overwrite", "compacted_files": ["a"]}}
        )
    )

    metadata = module.publish_traffic_incident_silver_asset(ti=ti, outlet_events={})

    assert calls == ["publish", "marker"]
    assert metadata["incident_run_id"] == "incident-1"
    assert metadata["silver_snapshot_id"] == 42
    assert metadata["compacted_files_fingerprint"] == fingerprint
