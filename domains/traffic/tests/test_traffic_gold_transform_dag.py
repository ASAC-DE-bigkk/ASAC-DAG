import types
from collections import UserDict, UserList

import pytest

from traffic_transform_test_support import (
    FakeAirflowFailException,
    FakeAirflowSkipException,
    FakeVariable,
    load_gold_transform_module,
)
from traffic_transform_test_support import restore_airflow_modules_after_dag_import  # noqa: F401


def _use_current_silver_evidence(
    monkeypatch,
    module,
    *,
    snapshot_id=42,
    fingerprint="a" * 64,
):
    monkeypatch.setattr(
        module,
        "current_silver_output_evidence",
        lambda: module.SilverOutputEvidence(snapshot_id, fingerprint),
    )
    monkeypatch.setattr(module, "traffic_gold_anchor_exists", lambda: True)

    class Manifest:
        def require_publishable(self, run_id):
            return run_id

    monkeypatch.setattr(module, "build_traffic_manifest", Manifest)


def test_gold_dag_is_independent_and_owns_hot_publication_marker():
    module = load_gold_transform_module()
    dag = module.dag

    assert dag.dag_id == "traffic_gold_transform"
    assert {asset.uri for asset in dag.kwargs["schedule"].assets} == {
        module.TRAFFIC_INCIDENT_SILVER_ASSET,
        module.TRAFFIC_FLOW_SILVER_ASSET,
    }
    assert "iceberg://traffic/flow/bronze" not in {
        asset.uri for asset in dag.kwargs["schedule"].assets
    }
    assert dag.kwargs["max_active_runs"] == 1
    assert "dbt_run_silver" not in dag.task_ids
    assert dag.task_dict["validate_dev_runtime"].downstream_task_ids == {
        "resolve_traffic_gold_snapshot_run"
    }
    assert dag.task_dict["resolve_traffic_gold_snapshot_run"].downstream_task_ids == {
        "admit_traffic_gold_snapshot"
    }
    assert dag.task_dict["admit_traffic_gold_snapshot"].downstream_task_ids == {
        "dbt_deps_gold"
    }
    assert dag.task_dict["dbt_run_gold"].downstream_task_ids == {
        "mark_traffic_gold_success"
    }
    assert "select_traffic_test_tier" not in dag.task_ids
    assert "dbt_test_gold" not in dag.task_ids


def test_gold_hot_build_receipt_outranks_silver_writer():
    gold = load_gold_transform_module()

    assert gold.PIN_CRITICAL_PRIORITY > 10
    assert (
        gold.dag.task_dict["dbt_run_gold"].kwargs["priority_weight"]
        == gold.PIN_CRITICAL_PRIORITY
    )


def test_gold_resolver_uses_silver_marker_for_flow_only_trigger_and_never_raw_bronze(
    monkeypatch,
):
    module = load_gold_transform_module()
    _use_current_silver_evidence(monkeypatch, module)
    FakeVariable.values[module.SILVER_SUCCESS_MARKER_KEY] = (
        module.TransformSuccessMarker(
            version=1,
            pipeline="silver",
            identity=module.TransformIdentity.silver("incident-1"),
            output_snapshot_id=42,
            compacted_files_fingerprint="a" * 64,
        ).to_json()
    )
    monkeypatch.setattr(module, "resolve_citydata_crowding_snapshot_id", lambda: 7)
    monkeypatch.setattr(
        module, "resolve_admin_dong_crosswalk_snapshot_id", lambda: 99
    )
    pushed = {}
    ti = types.SimpleNamespace(
        xcom_push=lambda *, key, value: pushed.update({key: value})
    )

    incident = module.resolve_traffic_gold_snapshot_run(
        ti=ti,
        triggering_asset_events={module.TRAFFIC_FLOW_SILVER_ASSET: []},
    )

    assert incident == "incident-1"
    assert pushed[module.FLOW_SNAPSHOT_XCOM_KEY] is None
    assert pushed[module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY] == 7
    assert pushed[module.ADMIN_DONG_CROSSWALK_PIN_XCOM_KEY] == 99
    assert pushed[module.GOLD_BOOTSTRAP_REQUIRED_XCOM_KEY] is False


def test_gold_resolver_pins_bootstrap_requirement_when_anchor_is_missing(
    monkeypatch,
):
    module = load_gold_transform_module()
    _use_current_silver_evidence(monkeypatch, module)
    monkeypatch.setattr(module, "traffic_gold_anchor_exists", lambda: False)
    FakeVariable.values[module.SILVER_SUCCESS_MARKER_KEY] = (
        module.TransformSuccessMarker(
            version=1,
            pipeline="silver",
            identity=module.TransformIdentity.silver("incident-1"),
            output_snapshot_id=42,
            compacted_files_fingerprint="a" * 64,
        ).to_json()
    )
    monkeypatch.setattr(module, "resolve_citydata_crowding_snapshot_id", lambda: 7)
    monkeypatch.setattr(
        module, "resolve_admin_dong_crosswalk_snapshot_id", lambda: 99
    )
    pushed = {}
    ti = types.SimpleNamespace(
        xcom_push=lambda *, key, value: pushed.update({key: value})
    )

    module.resolve_traffic_gold_snapshot_run(
        ti=ti,
        triggering_asset_events={module.TRAFFIC_FLOW_SILVER_ASSET: []},
    )

    assert pushed[module.GOLD_BOOTSTRAP_REQUIRED_XCOM_KEY] is True


@pytest.mark.parametrize(
    ("bootstrap_required", "expected_selector", "expected_incident_selector"),
    [
        (
            True,
            "ask_seoul_traffic_transform_gold_bootstrap_hot_build",
            "ask_seoul_traffic_transform_gold_incident_bootstrap_hot_build",
        ),
        (
            False,
            "ask_seoul_traffic_transform_gold_hot_build",
            "ask_seoul_traffic_transform_gold_incident_hot_build",
        ),
    ],
)
def test_gold_build_selects_bootstrap_only_for_a_missing_anchor(
    monkeypatch,
    bootstrap_required,
    expected_selector,
    expected_incident_selector,
):
    module = load_gold_transform_module()
    captured = {}
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            bootstrap_required
            if key == module.GOLD_BOOTSTRAP_REQUIRED_XCOM_KEY
            else "incident-1"
        )
    )
    monkeypatch.setattr(
        module.transform_runtime,
        "run_dbt_phase",
        lambda **kwargs: captured.update(kwargs) or {"status": "success"},
    )

    module.run_dbt_phase(
        dbt_command="build",
        selector="ask_seoul_traffic_transform_gold_hot_build",
        selector_when_flow_missing=(
            "ask_seoul_traffic_transform_gold_incident_hot_build"
        ),
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=True,
        snapshot_required=True,
        ti=ti,
    )

    assert captured["selector"] == expected_selector
    assert captured["selector_when_flow_missing"] == expected_incident_selector


def test_gold_resolver_accepts_airflow_lazy_asset_event_collections(monkeypatch):
    module = load_gold_transform_module()
    _use_current_silver_evidence(monkeypatch, module)
    event = types.SimpleNamespace(
        extra={
            "source_id": "seoul_traffic_incident",
            "incident_run_id": "incident-1",
            "silver_snapshot_id": 42,
            "compacted_files_fingerprint": "a" * 64,
            "event_at": "2026-07-19T08:24:20+00:00",
            "is_publishable": True,
            "contract": "traffic_incident_silver.v1",
        }
    )
    triggering_asset_events = UserDict(
        {module.TRAFFIC_INCIDENT_SILVER_ASSET: UserList([event])}
    )
    monkeypatch.setattr(module, "resolve_citydata_crowding_snapshot_id", lambda: 7)
    monkeypatch.setattr(
        module, "resolve_admin_dong_crosswalk_snapshot_id", lambda: 99
    )
    pushed = {}
    ti = types.SimpleNamespace(
        xcom_push=lambda *, key, value: pushed.update({key: value})
    )

    assert (
        module.resolve_traffic_gold_snapshot_run(
            ti=ti,
            triggering_asset_events=triggering_asset_events,
        )
        == "incident-1"
    )
    assert pushed[module.FLOW_SNAPSHOT_XCOM_KEY] is None
    assert pushed[module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY] == 7
    assert pushed[module.ADMIN_DONG_CROSSWALK_PIN_XCOM_KEY] == 99
    assert pushed[module.SILVER_OUTPUT_EVIDENCE_XCOM_KEY] == {
        "snapshot_id": 42,
        "compacted_files_fingerprint": "a" * 64,
    }


def test_gold_resolver_skips_superseded_silver_marker_before_external_reads(
    monkeypatch,
):
    module = load_gold_transform_module()
    FakeVariable.values[module.SILVER_SUCCESS_MARKER_KEY] = (
        module.TransformSuccessMarker(
            version=1,
            pipeline="silver",
            identity=module.TransformIdentity.silver("incident-old"),
            output_snapshot_id=42,
            compacted_files_fingerprint="a" * 64,
        ).to_json()
    )
    _use_current_silver_evidence(
        monkeypatch,
        module,
        snapshot_id=43,
        fingerprint="b" * 64,
    )
    monkeypatch.setattr(
        module,
        "resolve_citydata_crowding_snapshot_id",
        lambda: pytest.fail("stale Gold input must skip before Citydata"),
    )

    with pytest.raises(FakeAirflowSkipException, match="superseded"):
        module.resolve_traffic_gold_snapshot_run(
            triggering_asset_events={module.TRAFFIC_FLOW_SILVER_ASSET: []}
        )


def test_gold_resolver_skips_coalesced_manifest_before_external_reads(monkeypatch):
    module = load_gold_transform_module()
    from traffic_ingest.run_manifest import RunNotPublishableError

    FakeVariable.values[module.SILVER_SUCCESS_MARKER_KEY] = (
        module.TransformSuccessMarker(
            version=1,
            pipeline="silver",
            identity=module.TransformIdentity.silver("incident-old"),
            output_snapshot_id=42,
            compacted_files_fingerprint="a" * 64,
        ).to_json()
    )
    _use_current_silver_evidence(monkeypatch, module)

    class Manifest:
        def require_publishable(self, run_id):
            raise RunNotPublishableError(f"coalesced: {run_id}")

    monkeypatch.setattr(module, "build_traffic_manifest", Manifest)
    monkeypatch.setattr(
        module,
        "resolve_citydata_crowding_snapshot_id",
        lambda: pytest.fail("coalesced Gold input must skip before Citydata"),
    )

    with pytest.raises(FakeAirflowSkipException, match="superseded"):
        module.resolve_traffic_gold_snapshot_run(
            triggering_asset_events={module.TRAFFIC_FLOW_SILVER_ASSET: []}
        )


def test_gold_resolver_fails_closed_on_manifest_operational_error(monkeypatch):
    module = load_gold_transform_module()
    FakeVariable.values[module.SILVER_SUCCESS_MARKER_KEY] = (
        module.TransformSuccessMarker(
            version=1,
            pipeline="silver",
            identity=module.TransformIdentity.silver("incident-old"),
            output_snapshot_id=42,
            compacted_files_fingerprint="a" * 64,
        ).to_json()
    )
    _use_current_silver_evidence(monkeypatch, module)

    class Manifest:
        def require_publishable(self, run_id):
            raise RuntimeError(f"Trino unavailable: {run_id}")

    monkeypatch.setattr(module, "build_traffic_manifest", Manifest)
    monkeypatch.setattr(
        module,
        "resolve_citydata_crowding_snapshot_id",
        lambda: pytest.fail("manifest failure must stop before Citydata"),
    )

    with pytest.raises(FakeAirflowFailException, match="verification failed"):
        module.resolve_traffic_gold_snapshot_run(
            triggering_asset_events={module.TRAFFIC_FLOW_SILVER_ASSET: []}
        )


@pytest.mark.parametrize(
    ("dbt_command", "expected_exception"),
    [
        ("run", FakeAirflowSkipException),
        ("test", FakeAirflowFailException),
    ],
)
def test_gold_snapshot_required_phases_recheck_current_silver_inside_pool_task(
    monkeypatch,
    dbt_command,
    expected_exception,
):
    module = load_gold_transform_module()
    monkeypatch.setattr(
        module,
        "silver_output_evidence_from_resolver",
        lambda *_args, **_kwargs: module.SilverOutputEvidence(42, "a" * 64),
    )
    _use_current_silver_evidence(
        monkeypatch,
        module,
        snapshot_id=43,
        fingerprint="b" * 64,
    )
    monkeypatch.setattr(
        module.transform_runtime,
        "run_dbt_phase",
        lambda **kwargs: (
            kwargs["pre_execution_guard"]()
            or pytest.fail("stale Gold input must not invoke dbt")
        ),
    )

    with pytest.raises(expected_exception, match="Silver output changed"):
        module.run_dbt_phase(
            dbt_command=dbt_command,
            selector="selector",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            snapshot_required=True,
            ti=types.SimpleNamespace(),
        )


@pytest.mark.parametrize(
    ("dbt_command", "expected_exception"),
    [
        ("run", FakeAirflowSkipException),
        ("test", FakeAirflowFailException),
    ],
)
def test_gold_snapshot_required_phases_recheck_manifest_inside_pool_task(
    monkeypatch,
    dbt_command,
    expected_exception,
):
    module = load_gold_transform_module()
    from traffic_ingest.run_manifest import RunNotPublishableError

    evidence = module.SilverOutputEvidence(42, "a" * 64)
    monkeypatch.setattr(
        module,
        "silver_output_evidence_from_resolver",
        lambda *_args, **_kwargs: evidence,
    )
    monkeypatch.setattr(module, "current_silver_output_evidence", lambda: evidence)

    class Manifest:
        def require_publishable(self, run_id):
            raise RunNotPublishableError(f"coalesced: {run_id}")

    monkeypatch.setattr(module, "build_traffic_manifest", Manifest)
    monkeypatch.setattr(
        module.transform_runtime,
        "run_dbt_phase",
        lambda **kwargs: (
            kwargs["pre_execution_guard"]()
            or pytest.fail("coalesced Gold input must not invoke dbt")
        ),
    )

    ti = types.SimpleNamespace(xcom_pull=lambda *, task_ids, key=None: "incident-old")
    with pytest.raises(expected_exception, match="superseded"):
        module.run_dbt_phase(
            dbt_command=dbt_command,
            selector="selector",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            snapshot_required=True,
            ti=ti,
        )


@pytest.mark.parametrize("dbt_command", ["run", "test"])
def test_gold_snapshot_required_phases_fail_closed_on_manifest_operational_error(
    monkeypatch,
    dbt_command,
):
    module = load_gold_transform_module()
    evidence = module.SilverOutputEvidence(42, "a" * 64)
    monkeypatch.setattr(
        module,
        "silver_output_evidence_from_resolver",
        lambda *_args, **_kwargs: evidence,
    )
    monkeypatch.setattr(module, "current_silver_output_evidence", lambda: evidence)

    class Manifest:
        def require_publishable(self, run_id):
            raise RuntimeError(f"Trino unavailable: {run_id}")

    monkeypatch.setattr(module, "build_traffic_manifest", Manifest)
    monkeypatch.setattr(
        module.transform_runtime,
        "run_dbt_phase",
        lambda **kwargs: (
            kwargs["pre_execution_guard"]()
            or pytest.fail("manifest failure must stop before dbt")
        ),
    )

    ti = types.SimpleNamespace(xcom_pull=lambda *, task_ids, key=None: "incident-old")
    with pytest.raises(FakeAirflowFailException, match="verification failed"):
        module.run_dbt_phase(
            dbt_command=dbt_command,
            selector="selector",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            snapshot_required=True,
            ti=ti,
        )


def test_gold_admission_fails_closed_for_malformed_marker_and_skips_exact_tuple(
    monkeypatch,
):
    module = load_gold_transform_module()
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            None
            if key == module.FLOW_SNAPSHOT_XCOM_KEY
            else "incident-1"
            if task_ids == module.SNAPSHOT_TASK_ID
            else None
        )
    )
    monkeypatch.setattr(
        module,
        "current_silver_output_evidence",
        lambda **_kwargs: module.SilverOutputEvidence(42, "a" * 64),
    )
    monkeypatch.setattr(
        module, "resolve_gold_citydata_snapshot_id", lambda **_kwargs: 7
    )
    FakeVariable.values[module.GOLD_SUCCESS_MARKER_KEY] = "{malformed"
    with pytest.raises(FakeAirflowFailException):
        module.admit_traffic_gold_snapshot(ti=ti)

    FakeVariable.values[module.GOLD_SUCCESS_MARKER_KEY] = module.TransformSuccessMarker(
        version=1,
        pipeline="gold",
        identity=module.TransformIdentity.gold(
            "incident-1", flow_run_id=None, citydata_snapshot_id=7
        ),
        output_snapshot_id=42,
        compacted_files_fingerprint="a" * 64,
    ).to_json()
    with pytest.raises(FakeAirflowSkipException):
        module.admit_traffic_gold_snapshot(ti=ti)


def test_gold_admission_uses_fresh_current_evidence_not_stale_resolver_xcom(
    monkeypatch,
):
    module = load_gold_transform_module()
    FakeVariable.values[module.GOLD_SUCCESS_MARKER_KEY] = module.TransformSuccessMarker(
        version=1,
        pipeline="gold",
        identity=module.TransformIdentity.gold(
            "incident-1", flow_run_id=None, citydata_snapshot_id=7
        ),
        output_snapshot_id=42,
        compacted_files_fingerprint="a" * 64,
    ).to_json()
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            {"snapshot_id": 42, "compacted_files_fingerprint": "a" * 64}
            if key == module.SILVER_OUTPUT_EVIDENCE_XCOM_KEY
            else None
            if key == module.FLOW_SNAPSHOT_XCOM_KEY
            else 7
            if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY
            else "incident-1"
        )
    )
    monkeypatch.setattr(
        module,
        "current_silver_output_evidence",
        lambda: module.SilverOutputEvidence(43, "b" * 64),
    )

    assert module.admit_traffic_gold_snapshot(ti=ti)["action"] == "RUN"


def test_gold_success_marker_uses_fresh_evidence_and_heavy_pool(monkeypatch):
    module = load_gold_transform_module()
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            {"tier": "gate", "hour_bucket": "2026-07-19T15", "day_bucket": "2026-07-19"}
            if task_ids == module.SELECT_TEST_TIER_TASK_ID
            else None
            if key == module.FLOW_SNAPSHOT_XCOM_KEY
            else 7
            if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY
            else "incident-1"
        )
    )
    monkeypatch.setattr(
        module,
        "current_silver_output_evidence",
        lambda: module.SilverOutputEvidence(43, "b" * 64),
    )

    outlet_event = types.SimpleNamespace(extra=None)
    module.mark_traffic_gold_success(
        ti=ti,
        run_id="gold-run-1",
        outlet_events={module.TRAFFIC_GOLD_PUBLICATION_READY_ASSET_REF: outlet_event},
    )

    marker = module.TransformSuccessMarker.from_json(
        FakeVariable.values[module.GOLD_SUCCESS_MARKER_KEY]
    )
    assert marker.output_snapshot_id == 43
    assert outlet_event.extra[module.TRAFFIC_GOLD_PUBLICATION_SCOPE_KEY] == list(
        module.TRAFFIC_INCIDENT_PUBLICATION_PRODUCT_IDS
    )
    task = module.dag.task_dict["mark_traffic_gold_success"]
    assert task.kwargs["pool"] == module.TRINO_TRANSFORM_POOL
    assert task.kwargs["priority_weight"] == module.PIN_CRITICAL_PRIORITY
    assert task.kwargs["weight_rule"] == "absolute"


def test_gold_publication_scope_includes_flow_products_only_when_flow_is_pinned():
    module = load_gold_transform_module()
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids, key=None: (
            "flow-1" if key == module.FLOW_SNAPSHOT_XCOM_KEY else "incident-1"
        )
    )

    assert module._publication_product_ids(ti=ti) == (
        module.TRAFFIC_GOLD_PUBLICATION_PRODUCT_IDS
    )
