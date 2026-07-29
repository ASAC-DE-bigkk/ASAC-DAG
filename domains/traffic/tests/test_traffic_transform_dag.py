import sys
import types

import pytest

from traffic_transform_test_support import (
    FakeAirflowFailException,
    FakeAirflowSkipException,
    FakeVariable,
    load_gold_transform_module,
    load_transform_module,
)
from traffic_transform_test_support import restore_airflow_modules_after_dag_import  # noqa: F401


def _marker_type():
    return sys.modules["traffic_ingest.transform_admission"].TransformSuccessMarker


def _marker(module, identity, *, snapshot_id=42, fingerprint="a" * 64):
    return _marker_type()(
        version=1,
        pipeline=identity.pipeline,
        identity=identity,
        output_snapshot_id=snapshot_id,
        compacted_files_fingerprint=fingerprint,
    ).to_json()


def _silver_ti(
    module,
    incident_run_id="incident-1",
    run_result=None,
    stale_run_ids=None,
):
    return types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            list(stale_run_ids or ())
            if task_ids == module.SNAPSHOT_TASK_ID
            and key == getattr(module, "STALE_INCIDENT_RUN_IDS_XCOM_KEY", "__missing__")
            else incident_run_id
            if task_ids == module.SNAPSHOT_TASK_ID and key is None
            else run_result
            if task_ids == "dbt_run_silver"
            else None
        )
    )


def test_silver_dag_schedule_and_guard_order():
    module = load_transform_module()
    dag = module.dag

    assert dag.kwargs["schedule"].uri == module.TRAFFIC_INCIDENT_BRONZE_ASSET
    assert dag.kwargs["max_active_runs"] == 1
    # #510: Bronze source gates run first (no pin needed), then the snapshot is
    # pinned immediately before dbt_run_silver so the pin cannot be superseded by
    # a newer Bronze run during the multi-minute gate phase (livelock fix).
    assert dag.task_dict["validate_dev_runtime"].downstream_task_ids == {
        "dbt_deps"
    }
    assert dag.task_dict["dbt_deps"].downstream_task_ids == {
        "dbt_source_freshness"
    }
    assert dag.task_dict["dbt_source_freshness"].downstream_task_ids == {
        "dbt_test_traffic_incident_availability"
    }
    assert dag.task_dict[
        "dbt_test_traffic_incident_availability"
    ].downstream_task_ids == {"dbt_test_traffic_bronze_source_contract"}
    assert dag.task_dict[
        "dbt_test_traffic_bronze_source_contract"
    ].downstream_task_ids == {"resolve_traffic_snapshot_run"}
    assert dag.task_dict["resolve_traffic_snapshot_run"].downstream_task_ids == {
        "admit_traffic_silver_snapshot"
    }
    assert dag.task_dict["admit_traffic_silver_snapshot"].downstream_task_ids == {
        "assert_traffic_silver_snapshot_not_superseded"
    }
    assert dag.task_dict[
        "assert_traffic_silver_snapshot_not_superseded"
    ].downstream_task_ids == {"dbt_run_silver"}
    assert "dbt_run_gold" not in dag.task_ids
    assert "select_traffic_test_tier" not in dag.task_ids


def test_gold_dag_schedule_and_guard_order():
    module = load_gold_transform_module()
    dag = module.dag

    assert {asset.uri for asset in dag.kwargs["schedule"].assets} == {
        module.TRAFFIC_INCIDENT_SILVER_ASSET,
        module.TRAFFIC_FLOW_SILVER_ASSET,
    }
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.task_dict["validate_dev_runtime"].downstream_task_ids == {
        "select_traffic_test_tier"
    }
    assert dag.task_dict["select_traffic_test_tier"].downstream_task_ids == {
        "resolve_traffic_gold_snapshot_run"
    }
    assert dag.task_dict["resolve_traffic_gold_snapshot_run"].downstream_task_ids == {
        "admit_traffic_gold_snapshot"
    }


@pytest.mark.parametrize("loader", [load_transform_module, load_gold_transform_module])
def test_split_dags_allow_dev_or_prod_target(loader):
    module = loader()
    target = module.DEFAULT_PARAMS["target"]

    assert target.schema["enum"] == ["dev", "prod"]
    assert target.value in {"dev", "prod"}


@pytest.mark.parametrize("loader", [load_transform_module, load_gold_transform_module])
def test_split_dbt_tasks_keep_pool_priority_threads_and_absolute_weight(loader):
    module = loader()
    critical_task_ids = {"dbt_run_silver", "dbt_test_silver", "dbt_test_gold"}
    local_workload_task_ids = {"dbt_deps", "dbt_deps_gold"}
    # Pre-write checks read Trino but do not need to serialize behind the
    # heavy pool with actual writes: keeping them off it shortens the window
    # between pinning a snapshot and dbt_run_silver, so the pin is less
    # likely to be superseded by a newer Bronze run under pool contention.
    light_trino_task_ids = {
        "dbt_source_freshness",
        "dbt_test_traffic_incident_availability",
        "dbt_test_traffic_bronze_source_contract",
    }

    assert module.TRINO_TRANSFORM_POOL == "trino_traffic_transform"
    for task_id, task in module.dbt_phase_tasks.items():
        if task_id in local_workload_task_ids:
            assert "pool" not in task.kwargs
            assert task.kwargs["op_kwargs"]["threads"] is None
        elif task_id in light_trino_task_ids:
            assert "pool" not in task.kwargs
            assert task.kwargs["op_kwargs"]["threads"] == 2
        else:
            assert task.kwargs["pool"] == module.TRINO_TRANSFORM_POOL
            assert task.kwargs["op_kwargs"]["threads"] == 2
        assert task.kwargs["weight_rule"] == "absolute"
        assert task.kwargs["priority_weight"] == (
            module.PIN_CRITICAL_PRIORITY if task_id in critical_task_ids else 1
        )

    for task_id in (
        module.SNAPSHOT_TASK_ID,
        "admit_traffic_silver_snapshot"
        if module.dag.dag_id == "traffic_incident_transform"
        else "admit_traffic_gold_snapshot",
    ):
        task = module.dag.task_dict[task_id]
        assert task.kwargs["pool"] == module.TRINO_TRANSFORM_POOL
        assert task.kwargs["priority_weight"] == module.PIN_CRITICAL_PRIORITY
        assert task.kwargs["weight_rule"] == "absolute"


def test_silver_marker_is_a_separate_task_after_asset_publication():
    module = load_transform_module()
    dag = module.dag

    assert dag.task_dict["dbt_test_silver"].downstream_task_ids == {
        "publish_traffic_incident_silver_asset"
    }
    assert dag.task_dict[
        "publish_traffic_incident_silver_asset"
    ].downstream_task_ids == {"mark_traffic_silver_success"}
    assert dag.task_dict["mark_traffic_silver_success"].downstream_task_ids == {
        "publish_dbt_run_metrics"
    }
    publish_task = dag.task_dict["publish_traffic_incident_silver_asset"]
    assert publish_task.kwargs["pool"] == module.TRINO_TRANSFORM_POOL
    assert publish_task.kwargs["priority_weight"] == module.PIN_CRITICAL_PRIORITY
    assert publish_task.kwargs["weight_rule"] == "absolute"


def test_silver_publication_never_writes_marker(monkeypatch):
    module = load_transform_module()
    evidence = {
        "silver_snapshot_evidence": {
            "snapshot_id": 42,
            "committed_at": "2026-07-19T12:00:00+09:00",
            "operation": "overwrite",
            "compacted_files": ["a"],
        }
    }
    monkeypatch.setattr(module, "publish_through_alias", lambda *_args, **_kwargs: None)

    metadata = module.publish_traffic_incident_silver_asset(
        ti=_silver_ti(module, run_result=evidence), outlet_events={}
    )

    assert metadata["silver_snapshot_id"] == 42
    assert FakeVariable.set_calls == []


def test_silver_publication_coalesces_deferred_runs_before_emitting_asset(monkeypatch):
    module = load_transform_module()
    calls = []
    evidence = {
        "silver_snapshot_evidence": {
            "snapshot_id": 42,
            "committed_at": "2026-07-19T12:00:00+09:00",
            "operation": "overwrite",
            "compacted_files": ["a"],
        }
    }

    class Manifest:
        def coalesce_many(self, run_ids, *, replacement_run_id):
            calls.append(("coalesce", list(run_ids), replacement_run_id))

    monkeypatch.setattr(module, "build_traffic_manifest", Manifest)
    monkeypatch.setattr(
        module,
        "publish_through_alias",
        lambda *_args, **_kwargs: calls.append(("publish",)),
    )

    module.publish_traffic_incident_silver_asset(
        ti=_silver_ti(
            module,
            run_result=evidence,
            stale_run_ids=["incident-old-a", "incident-old-b"],
        ),
        outlet_events={},
    )

    assert calls == [
        ("coalesce", ["incident-old-a", "incident-old-b"], "incident-1"),
        ("publish",),
    ]


def test_silver_admission_coalesces_deferred_runs_when_exact_output_skips(monkeypatch):
    module = load_transform_module()
    calls = []
    FakeVariable.values[module.SILVER_SUCCESS_MARKER_KEY] = _marker(
        module, module.TransformIdentity.silver("incident-1")
    )
    monkeypatch.setattr(
        module,
        "current_silver_output_evidence",
        lambda: module.SilverOutputEvidence(42, "a" * 64),
    )

    class Manifest:
        def coalesce_many(self, run_ids, *, replacement_run_id):
            calls.append((list(run_ids), replacement_run_id))

    monkeypatch.setattr(module, "build_traffic_manifest", Manifest)

    with pytest.raises(FakeAirflowSkipException):
        module.admit_traffic_silver_snapshot(
            ti=_silver_ti(module, stale_run_ids=["incident-old"])
        )

    assert calls == [(["incident-old"], "incident-1")]


def test_assert_not_superseded_passes_when_pin_is_still_latest(monkeypatch):
    module = load_transform_module()

    class Manifest:
        def latest_publishable_run_id(self):
            return "incident-1"

    monkeypatch.setattr(module, "build_traffic_manifest", Manifest)

    result = module.assert_traffic_silver_snapshot_not_superseded(
        ti=_silver_ti(module, incident_run_id="incident-1")
    )

    assert result == "incident-1"


def test_assert_not_superseded_skips_early_before_any_dbt_phase_runs(monkeypatch):
    module = load_transform_module()

    class Manifest:
        def latest_publishable_run_id(self):
            return "incident-2"

    monkeypatch.setattr(module, "build_traffic_manifest", Manifest)

    with pytest.raises(FakeAirflowSkipException):
        module.assert_traffic_silver_snapshot_not_superseded(
            ti=_silver_ti(module, incident_run_id="incident-1")
        )


def test_silver_publication_failure_cannot_write_marker(monkeypatch):
    module = load_transform_module()
    evidence = {
        "silver_snapshot_evidence": {
            "snapshot_id": 42,
            "committed_at": "2026-07-19T12:00:00+09:00",
            "operation": "overwrite",
            "compacted_files": [],
        }
    }
    monkeypatch.setattr(
        module,
        "publish_through_alias",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("publish failed")),
    )

    with pytest.raises(RuntimeError, match="publish failed"):
        module.publish_traffic_incident_silver_asset(
            ti=_silver_ti(module, run_result=evidence), outlet_events={}
        )
    assert FakeVariable.set_calls == []


def test_mark_silver_success_uses_strict_dbt_run_evidence():
    module = load_transform_module()
    evidence = {
        "silver_snapshot_evidence": {
            "snapshot_id": 42,
            "committed_at": "2026-07-19T12:00:00+09:00",
            "operation": "overwrite",
            "compacted_files": ["b", "a"],
        }
    }

    module.mark_traffic_silver_success(ti=_silver_ti(module, run_result=evidence))

    marker = _marker_type().from_json(
        FakeVariable.values[module.SILVER_SUCCESS_MARKER_KEY]
    )
    assert marker.identity == module.TransformIdentity.silver("incident-1")
    assert marker.output_snapshot_id == 42


@pytest.mark.parametrize(
    ("raw_marker", "incident_run_id"),
    [(None, "incident-1"), ("mismatch", "incident-1")],
)
def test_silver_admission_does_not_read_telemetry_without_matching_identity(
    monkeypatch, raw_marker, incident_run_id
):
    module = load_transform_module()
    calls = []
    if raw_marker == "mismatch":
        FakeVariable.values[module.SILVER_SUCCESS_MARKER_KEY] = _marker(
            module, module.TransformIdentity.silver("incident-other")
        )
    monkeypatch.setattr(
        module,
        "current_silver_output_evidence",
        lambda: calls.append("telemetry") or (_ for _ in ()).throw(RuntimeError()),
    )

    assert (
        module.admit_traffic_silver_snapshot(
            ti=_silver_ti(module, incident_run_id=incident_run_id)
        )["action"]
        == "RUN"
    )
    assert calls == []


def test_silver_telemetry_failure_blocks_only_potential_skip(monkeypatch):
    module = load_transform_module()
    FakeVariable.values[module.SILVER_SUCCESS_MARKER_KEY] = _marker(
        module, module.TransformIdentity.silver("incident-1")
    )
    monkeypatch.setattr(
        module,
        "current_silver_output_evidence",
        lambda: (_ for _ in ()).throw(
            FakeAirflowFailException("telemetry unavailable")
        ),
    )

    with pytest.raises(FakeAirflowFailException, match="telemetry unavailable"):
        module.admit_traffic_silver_snapshot(ti=_silver_ti(module))


@pytest.mark.parametrize("loader", [load_transform_module, load_gold_transform_module])
def test_metrics_teardown_is_terminal_without_failure_fanout(loader):
    module = loader()
    metrics = module.dag.task_dict["publish_dbt_run_metrics"]
    success_task_id = (
        "mark_traffic_silver_success"
        if module.dag.dag_id == "traffic_incident_transform"
        else "mark_traffic_gold_success"
    )

    assert metrics.kwargs["trigger_rule"] == "all_done_setup_success"
    assert metrics.is_teardown is True
    assert metrics.on_failure_fail_dagrun is False
    assert module.dag.task_dict[success_task_id].downstream_task_ids == {
        "publish_dbt_run_metrics"
    }
    assert metrics.downstream_task_ids == set()
    assert "fail_transform_if_upstream_failed" not in module.dag.task_ids


def test_metrics_use_latest_current_run_dbt_artifact_path():
    module = load_transform_module()
    terminal_path = "/tmp/traffic-terminal/run_results.json"
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            {"run_results_path": terminal_path}
            if task_ids == "dbt_test_gold" and key is None
            else None
        )
    )

    assert module._current_run_results_path(ti=ti) == terminal_path


def test_metrics_use_earlier_success_artifact_when_later_phases_have_none():
    module = load_transform_module()
    earlier_path = "/tmp/traffic-silver/run_results.json"
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            {"run_results_path": earlier_path}
            if task_ids == "dbt_run_silver" and key is None
            else None
        )
    )

    assert module._current_run_results_path(ti=ti) == earlier_path


def test_metrics_use_earlier_failure_artifact():
    module = load_transform_module()
    failure_path = "/tmp/traffic-freshness-failure/run_results.json"
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            {"dbt_run_results_path": failure_path}
            if task_ids == "dbt_source_freshness" and key == module.DBT_FAILURE_XCOM_KEY
            else None
        )
    )

    assert module._current_run_results_path(ti=ti) == failure_path


def test_metrics_prefer_most_advanced_current_run_artifact():
    module = load_transform_module()
    artifacts = {
        ("dbt_deps", None): {"run_results_path": None},
        ("dbt_run_silver", module.DBT_FAILURE_XCOM_KEY): {
            "dbt_run_results_path": "/tmp/silver-failure/run_results.json"
        },
        ("dbt_run_gold", None): {"run_results_path": "/tmp/gold/run_results.json"},
    }
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: artifacts.get((task_ids, key))
    )

    assert module._current_run_results_path(ti=ti) == "/tmp/gold/run_results.json"


def test_metrics_resolver_never_falls_back_to_shared_target(tmp_path):
    module = load_transform_module()
    stale = tmp_path / "target" / "run_results.json"
    stale.parent.mkdir()
    stale.write_text("{}", encoding="utf-8")
    ti = types.SimpleNamespace(xcom_pull=lambda **_kwargs: None)

    assert module._current_run_results_path(ti=ti) is None


def test_metrics_skip_stale_shared_target_without_current_run_artifact(
    tmp_path, monkeypatch
):
    module = load_transform_module()
    stale = tmp_path / "target" / "run_results.json"
    stale.parent.mkdir()
    stale.write_text("{}", encoding="utf-8")
    published = []
    monkeypatch.setattr(
        module,
        "dump_dbt_run_results",
        lambda path, **_kwargs: published.append(path) or [{}],
    )
    ti = types.SimpleNamespace(xcom_pull=lambda **_kwargs: None)

    assert module.publish_dbt_run_metrics(ti=ti, params={"target": "dev"}) == {
        "rows": 0,
        "skipped": True,
    }
    assert published == []


def test_metrics_forward_domain_and_target(tmp_path, monkeypatch):
    module = load_transform_module()
    result_path = tmp_path / "run_results.json"
    result_path.write_text("{}", encoding="utf-8")
    captured = {}
    monkeypatch.setattr(
        module,
        "dump_dbt_run_results",
        lambda path, *, domain, target: (
            captured.update(path=path, domain=domain, target=target) or [{}]
        ),
    )

    assert module.publish_dbt_run_metrics(
        run_results_path=str(result_path), params={"target": "dev"}
    ) == {"rows": 1, "skipped": False}
    assert captured == {
        "path": str(result_path),
        "domain": "traffic",
        "target": "dev",
    }


def test_silver_module_has_no_flow_or_citydata_resolver_or_constant():
    module = load_transform_module()
    source = open(module.__file__, encoding="utf-8").read()

    assert "TRAFFIC_FLOW_BRONZE_ASSET" not in source
    assert "resolve_citydata_crowding_snapshot_id" not in source
    assert "CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY" not in source
