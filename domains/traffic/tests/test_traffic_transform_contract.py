import json
import types
from pathlib import Path

import pytest

from traffic_transform_test_support import (
    FakeAirflowFailException,
    FakeAirflowSkipException,
    FakeDAG,
    FakeVariable,
    load_flow_transform_module,
    load_gold_transform_module,
    load_transform_module,
    write_materialization_artifacts,
)
from traffic_transform_test_support import restore_airflow_modules_after_dag_import  # noqa: F401


def _silver_evidence(snapshot_id: int):
    from traffic_ingest.silver_snapshot_fence import SilverSnapshotEvidence

    return SilverSnapshotEvidence(
        snapshot_id=snapshot_id,
        committed_at="2026-07-19T00:00:00+00:00",
        operation="overwrite",
        compacted_files=(),
    )


def _successful_runtime_execution():
    completed = types.SimpleNamespace(returncode=0, stdout="", stderr="")
    return types.SimpleNamespace(
        attempts=(completed,),
        completed=completed,
        missing_expected_artifacts=(),
        existing_run_results_path="/tmp/run_results.json",
        existing_sources_path=None,
        existing_manifest_path="/tmp/manifest.json",
        selected_unique_ids=(),
    )


def _runtime_ti(*, task_id="dbt_run_silver", run_result=None, citydata=None):
    def xcom_pull(*, task_ids, key=None):
        if task_ids == "resolve_traffic_snapshot_run":
            if key == "traffic_citydata_crowding_snapshot_id":
                return citydata
            return "incident-1"
        if task_ids == "dbt_run_silver":
            return run_result
        return None

    return types.SimpleNamespace(
        task_id=task_id,
        try_number=1,
        dag_id="traffic_incident_transform",
        xcom_pull=xcom_pull,
        xcom_push=lambda **_kwargs: None,
    )


def _load_transform_runtime():
    load_transform_module()
    from traffic_ingest import transform_runtime

    return transform_runtime


class _LatestPublishableManifest:
    def __init__(self, latest_run_id):
        self.latest_run_id = latest_run_id

    def latest_publishable_run_id(self):
        return self.latest_run_id


def test_latest_publishable_guard_accepts_current_pin():
    module = load_transform_module()

    assert (
        module.require_latest_publishable_incident_snapshot(
            _LatestPublishableManifest("incident-new"),
            "incident-new",
        )
        == "incident-new"
    )


def test_latest_publishable_guard_skips_superseded_pin():
    module = load_transform_module()

    with pytest.raises(FakeAirflowSkipException, match="superseded"):
        module.require_latest_publishable_incident_snapshot(
            _LatestPublishableManifest("incident-new"),
            "incident-old",
        )


def test_latest_publishable_guard_fails_closed_on_manifest_error():
    module = load_transform_module()

    class BrokenManifest:
        def latest_publishable_run_id(self):
            raise OSError("Trino unavailable")

    with pytest.raises(FakeAirflowFailException, match="verification failed"):
        module.require_latest_publishable_incident_snapshot(
            BrokenManifest(),
            "incident-old",
        )


@pytest.mark.parametrize("invalid_latest", [None, "", 42])
def test_latest_publishable_guard_fails_closed_on_invalid_identity(invalid_latest):
    module = load_transform_module()

    with pytest.raises(FakeAirflowFailException, match="identity is invalid"):
        module.require_latest_publishable_incident_snapshot(
            _LatestPublishableManifest(invalid_latest),
            "incident-old",
        )


def _dag_support_module():
    """Load transform_dag_support with airflow fakes installed (flow resolve path)."""
    load_flow_transform_module()
    from traffic_ingest import transform_dag_support

    return transform_dag_support


class _RaisingPublishableManifest:
    def __init__(self, exc):
        self._exc = exc

    def require_publishable(self, run_id):
        raise self._exc


class TrinoExternalError(Exception):
    """Stands in for trino.exceptions.TrinoExternalError (name-matched classifier)."""


@pytest.mark.parametrize(
    "transient",
    [
        OSError("Trino unavailable"),
        ConnectionError("connection reset by peer"),
        TimeoutError("timed out"),
        TrinoExternalError(
            "TrinoExternalError(type=EXTERNAL, name=ICEBERG_CATALOG_ERROR, "
            'message="Failed to load view \'bronze_collection_run_manifest\'")'
        ),
    ],
)
def test_publishable_guard_retries_on_transient_manifest_lookup_error(transient):
    # The manifest lookup runs against the R2 Iceberg REST catalog, which
    # intermittently drops the connection or returns ICEBERG_CATALOG_ERROR.
    # These must stay retryable (propagated as-is) so the task's configured
    # retry can recover, instead of being fail-closed with AirflowFailException.
    module = _dag_support_module()

    with pytest.raises(type(transient)) as raised:
        module.require_publishable_incident_snapshot(
            _RaisingPublishableManifest(transient),
            "incident-1",
            mismatch_action="skip",
        )
    assert raised.value is transient


def test_publishable_guard_fails_closed_on_non_transient_manifest_error():
    module = _dag_support_module()

    with pytest.raises(FakeAirflowFailException, match="verification failed"):
        module.require_publishable_incident_snapshot(
            _RaisingPublishableManifest(ValueError("unexpected bug")),
            "incident-1",
            mismatch_action="skip",
        )


def test_publishable_guard_still_skips_superseded_snapshot():
    module = _dag_support_module()
    from traffic_ingest.run_manifest import RunNotPublishableError

    with pytest.raises(FakeAirflowSkipException, match="superseded"):
        module.require_publishable_incident_snapshot(
            _RaisingPublishableManifest(RunNotPublishableError("gone")),
            "incident-1",
            mismatch_action="skip",
        )


def test_snapshot_required_phase_skips_before_dbt_when_pin_is_superseded(
    monkeypatch,
):
    module = load_transform_module()
    called = False

    def execute_dbt_phase(**_kwargs):
        nonlocal called
        called = True
        return _successful_runtime_execution()

    monkeypatch.setattr(
        module,
        "build_traffic_manifest",
        lambda: _LatestPublishableManifest("incident-new"),
    )
    monkeypatch.setattr(
        module.transform_runtime,
        "collect_silver_snapshot_baseline",
        lambda: _silver_evidence(10),
    )
    monkeypatch.setattr(
        module.transform_runtime.traffic_dbt,
        "execute_dbt_phase",
        execute_dbt_phase,
    )

    with pytest.raises(FakeAirflowSkipException, match="superseded"):
        module.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_traffic_transform_incident_silver",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            snapshot_required=True,
            silver_persisted=False,
            silver_fence_mode="write",
            ti=_runtime_ti(),
            run_id="asset_triggered__stale",
            params={"target": "dev"},
        )

    assert called is False


def test_preflight_phase_does_not_query_latest_publishable_manifest(monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(
        module,
        "build_traffic_manifest",
        lambda: pytest.fail("preflight must not query the manifest"),
    )
    monkeypatch.setattr(
        module.transform_runtime.traffic_dbt,
        "execute_dbt_phase",
        lambda **_kwargs: _successful_runtime_execution(),
    )

    result = module.run_dbt_phase(
        dbt_command="source freshness",
        selector="ask_seoul_traffic_transform_source",
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        snapshot_required=False,
        silver_persisted=False,
        ti=_runtime_ti(task_id="dbt_source_freshness"),
        run_id="asset_triggered__preflight",
        params={"target": "dev"},
    )

    assert result["status"] == "success"


def test_silver_write_captures_baseline_and_returns_post_write_evidence(monkeypatch):
    runtime = _load_transform_runtime()
    monkeypatch.setattr(
        runtime, "collect_silver_snapshot_baseline", lambda: _silver_evidence(10)
    )
    monkeypatch.setattr(
        runtime, "collect_silver_snapshot_evidence", lambda: _silver_evidence(11)
    )
    monkeypatch.setattr(
        runtime.traffic_dbt,
        "execute_dbt_phase",
        lambda **_kwargs: _successful_runtime_execution(),
    )

    result = runtime.run_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        snapshot_task_id="resolve_traffic_snapshot_run",
        silver_persisted=False,
        snapshot_required=True,
        citydata_snapshot_required=False,
        silver_fence_mode="write",
        threads=2,
        ti=_runtime_ti(),
        run_id="manual__silver_fence",
        params={"target": "dev"},
    )

    assert result["silver_snapshot_evidence"] == _silver_evidence(11).as_dict()


def test_silver_write_allows_empty_baseline_but_requires_post_write_evidence(
    monkeypatch,
):
    runtime = _load_transform_runtime()
    monkeypatch.setattr(runtime, "collect_silver_snapshot_baseline", lambda: None)
    monkeypatch.setattr(
        runtime, "collect_silver_snapshot_evidence", lambda: _silver_evidence(11)
    )
    monkeypatch.setattr(
        runtime.traffic_dbt,
        "execute_dbt_phase",
        lambda **_kwargs: _successful_runtime_execution(),
    )

    result = runtime.run_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        snapshot_task_id="resolve_traffic_snapshot_run",
        silver_persisted=False,
        snapshot_required=True,
        citydata_snapshot_required=False,
        silver_fence_mode="write",
        threads=2,
        ti=_runtime_ti(),
        run_id="manual__silver_bootstrap",
        params={"target": "prod"},
    )

    assert result["silver_snapshot_evidence"] == _silver_evidence(11).as_dict()


def test_silver_verify_rejects_snapshot_change_before_dbt(monkeypatch):
    runtime = _load_transform_runtime()
    called = False
    monkeypatch.setattr(
        runtime, "collect_silver_snapshot_evidence", lambda: _silver_evidence(12)
    )

    def execute_dbt_phase(**_kwargs):
        nonlocal called
        called = True
        return _successful_runtime_execution()

    monkeypatch.setattr(runtime.traffic_dbt, "execute_dbt_phase", execute_dbt_phase)
    with pytest.raises(FakeAirflowFailException, match="EXTERNAL_COMPACTION_RACE:"):
        runtime.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_silver",
            snapshot_task_id="resolve_traffic_snapshot_run",
            silver_persisted=True,
            snapshot_required=True,
            citydata_snapshot_required=False,
            silver_fence_mode="verify",
            ti=_runtime_ti(
                task_id="dbt_test_silver",
                run_result={"silver_snapshot_evidence": _silver_evidence(11).as_dict()},
            ),
            run_id="manual__silver_test_fence",
            params={"target": "dev"},
        )

    assert called is False


def test_silver_verify_rejects_snapshot_change_after_successful_dbt(monkeypatch):
    runtime = _load_transform_runtime()
    evidence = iter((_silver_evidence(11), _silver_evidence(12)))
    monkeypatch.setattr(
        runtime, "collect_silver_snapshot_evidence", lambda: next(evidence)
    )
    monkeypatch.setattr(
        runtime.traffic_dbt,
        "execute_dbt_phase",
        lambda **_kwargs: _successful_runtime_execution(),
    )

    with pytest.raises(FakeAirflowFailException, match="EXTERNAL_COMPACTION_RACE:"):
        runtime.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_silver",
            snapshot_task_id="resolve_traffic_snapshot_run",
            silver_persisted=True,
            snapshot_required=True,
            citydata_snapshot_required=False,
            silver_fence_mode="verify",
            ti=_runtime_ti(
                task_id="dbt_test_silver",
                run_result={"silver_snapshot_evidence": _silver_evidence(11).as_dict()},
            ),
            run_id="manual__silver_test_post_fence",
            params={"target": "dev"},
        )


@pytest.mark.parametrize(
    ("run_result", "expected_message"),
    [
        (
            None,
            "invalid silver snapshot evidence: dbt_run_silver result is missing",
        ),
        (
            {},
            "invalid silver snapshot evidence: silver_snapshot_evidence is missing",
        ),
        (
            {"silver_snapshot_evidence": {}},
            "invalid silver snapshot evidence: snapshot evidence fields are invalid",
        ),
    ],
)
def test_silver_verify_rejects_missing_or_malformed_expected_evidence_before_dbt(
    monkeypatch, run_result, expected_message
):
    runtime = _load_transform_runtime()
    called = False

    def execute_dbt_phase(**_kwargs):
        nonlocal called
        called = True
        return _successful_runtime_execution()

    monkeypatch.setattr(runtime.traffic_dbt, "execute_dbt_phase", execute_dbt_phase)
    with pytest.raises(FakeAirflowFailException) as exc_info:
        runtime.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_silver",
            snapshot_task_id="resolve_traffic_snapshot_run",
            silver_persisted=True,
            snapshot_required=True,
            citydata_snapshot_required=False,
            silver_fence_mode="verify",
            ti=_runtime_ti(task_id="dbt_test_silver", run_result=run_result),
            run_id="manual__missing_evidence",
            params={"target": "dev"},
        )

    assert str(exc_info.value) == expected_message
    assert str(exc_info.value).count("invalid silver snapshot evidence:") == 1
    assert called is False


def test_citydata_snapshot_requirement_is_owned_by_gold_not_incident_pin(monkeypatch):
    runtime = _load_transform_runtime()
    monkeypatch.setattr(
        runtime.traffic_dbt,
        "execute_dbt_phase",
        lambda **_kwargs: _successful_runtime_execution(),
    )

    silver = runtime.run_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        snapshot_task_id="resolve_traffic_snapshot_run",
        silver_persisted=False,
        snapshot_required=True,
        citydata_snapshot_required=False,
        ti=_runtime_ti(),
        run_id="manual__silver_without_citydata",
        params={"target": "dev"},
    )
    assert silver["status"] == "success"

    with pytest.raises(FakeAirflowFailException, match="Citydata crowding snapshot"):
        runtime.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_traffic_transform_gold",
            snapshot_task_id="resolve_traffic_snapshot_run",
            silver_persisted=True,
            snapshot_required=True,
            citydata_snapshot_required=True,
            ti=_runtime_ti(task_id="dbt_run_gold", citydata=None),
            run_id="manual__gold_without_citydata",
            params={"target": "dev"},
        )


def test_admin_dong_crosswalk_pin_requirement_is_owned_by_gold_test_phase(monkeypatch):
    runtime = _load_transform_runtime()
    monkeypatch.setattr(
        runtime.traffic_dbt,
        "execute_dbt_phase",
        lambda **_kwargs: _successful_runtime_execution(),
    )

    silver = runtime.run_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_silver",
        snapshot_task_id="resolve_traffic_snapshot_run",
        silver_persisted=False,
        snapshot_required=True,
        admin_dong_crosswalk_pin_required=False,
        ti=_runtime_ti(),
        run_id="manual__silver_without_admin_dong_pin",
        params={"target": "dev"},
    )
    assert silver["status"] == "success"

    def xcom_pull(*, task_ids, key=None):
        if task_ids == "resolve_traffic_snapshot_run" and key is None:
            return "incident-1"
        return None

    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=1,
        dag_id="traffic_gold_transform",
        xcom_pull=xcom_pull,
        xcom_push=lambda **_kwargs: None,
    )

    with pytest.raises(FakeAirflowFailException, match="admin_dong crosswalk pin"):
        runtime.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_gold_full_tests",
            snapshot_task_id="resolve_traffic_snapshot_run",
            silver_persisted=True,
            snapshot_required=True,
            admin_dong_crosswalk_pin_required=True,
            admin_dong_crosswalk_xcom_key="admin_dong_crosswalk_pin_snapshot_id",
            ti=ti,
            run_id="manual__gold_without_admin_dong_pin",
            params={"target": "dev"},
        )


def test_admin_dong_crosswalk_pin_passes_through_to_dbt_variables(monkeypatch):
    runtime = _load_transform_runtime()
    captured = {}
    monkeypatch.setattr(
        runtime.traffic_dbt,
        "execute_dbt_phase",
        lambda **kwargs: captured.update(kwargs) or _successful_runtime_execution(),
    )

    def xcom_pull(*, task_ids, key=None):
        assert task_ids == "resolve_traffic_snapshot_run"
        if key is None:
            return "incident-1"
        if key == "admin_dong_crosswalk_pin_snapshot_id":
            return 99
        return None

    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=1,
        dag_id="traffic_gold_transform",
        xcom_pull=xcom_pull,
        xcom_push=lambda **_kwargs: None,
    )

    result = runtime.run_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_traffic_transform_gold_full_tests",
        snapshot_task_id="resolve_traffic_snapshot_run",
        silver_persisted=True,
        snapshot_required=True,
        admin_dong_crosswalk_pin_required=True,
        admin_dong_crosswalk_xcom_key="admin_dong_crosswalk_pin_snapshot_id",
        ti=ti,
        run_id="manual__gold_with_admin_dong_pin",
        params={"target": "dev"},
    )

    assert result["status"] == "success"
    assert (
        json.loads(captured["variables"])["admin_dong_crosswalk_pin_snapshot_id"] == 99
    )


def test_no_silver_fence_mode_preserves_existing_dbt_success_contract(monkeypatch):
    runtime = _load_transform_runtime()
    monkeypatch.setattr(
        runtime.traffic_dbt,
        "execute_dbt_phase",
        lambda **_kwargs: _successful_runtime_execution(),
    )
    monkeypatch.setattr(
        runtime,
        "collect_silver_snapshot_evidence",
        lambda: pytest.fail("unfenced phase must not read Silver snapshot metadata"),
    )

    result = runtime.run_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_gold",
        snapshot_task_id="resolve_traffic_snapshot_run",
        silver_persisted=True,
        snapshot_required=True,
        citydata_snapshot_required=False,
        silver_fence_mode=None,
        ti=_runtime_ti(task_id="dbt_run_gold"),
        run_id="manual__compat",
        params={"target": "dev"},
    )

    assert "silver_snapshot_evidence" not in result


def test_invalid_silver_fence_mode_fails_before_dbt_and_evidence_collection(
    monkeypatch,
):
    runtime = _load_transform_runtime()
    dbt_called = False
    evidence_called = False

    def execute_dbt_phase(**_kwargs):
        nonlocal dbt_called
        dbt_called = True
        return _successful_runtime_execution()

    def collect_evidence():
        nonlocal evidence_called
        evidence_called = True
        return _silver_evidence(11)

    monkeypatch.setattr(runtime.traffic_dbt, "execute_dbt_phase", execute_dbt_phase)
    monkeypatch.setattr(runtime, "collect_silver_snapshot_evidence", collect_evidence)

    with pytest.raises(
        FakeAirflowFailException,
        match="^invalid silver_fence_mode: typo$",
    ):
        runtime.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_traffic_transform_silver",
            snapshot_task_id="resolve_traffic_snapshot_run",
            silver_persisted=False,
            snapshot_required=True,
            citydata_snapshot_required=False,
            silver_fence_mode="typo",
            ti=_runtime_ti(),
            run_id="manual__invalid_fence_mode",
            params={"target": "dev"},
        )

    assert dbt_called is False
    assert evidence_called is False


def test_snapshot_resolver_delegates_to_the_traffic_manifest(monkeypatch):
    module = load_transform_module()
    calls = []
    monkeypatch.setattr(
        module,
        "resolve_citydata_crowding_snapshot_id",
        lambda: 8738321387624398062,
        raising=False,
    )

    class Manifest:
        def latest_publishable_run_id(self):
            return "traffic-run-42"

        def require_publishable(self, run_id):
            calls.append(run_id)
            return run_id

        def coalesce_many(self, *_args, **_kwargs):
            pass

    monkeypatch.setattr(module, "build_traffic_manifest", lambda: Manifest())

    event = types.SimpleNamespace(
        extra={
            "source_id": "seoul_traffic_incident",
            "bronze_run_id": "traffic-run-42",
            "bronze_dag_run_id": "traffic-run-42",
            "event_at": "2026-07-15T12:00:00+09:00",
            "load_date": "2026-07-15",
            "row_count": 7,
            "payload_hash": "a" * 64,
            "is_publishable": True,
        }
    )

    assert (
        module.resolve_traffic_snapshot_run(
            triggering_asset_events={module.TRAFFIC_BRONZE_ASSET: [event]}
        )
        == "traffic-run-42"
    )
    assert calls == ["traffic-run-42"]
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "bronze_collection_run_manifest" not in source


def test_traffic_snapshot_resolver_rejects_missing_asset_events():
    module = load_transform_module()

    with pytest.raises(FakeAirflowFailException, match="Incident Bronze asset"):
        module.resolve_traffic_snapshot_run(triggering_asset_events={})


def test_traffic_snapshot_resolver_defers_older_asset_coalescing_until_publish(
    monkeypatch,
):
    module = load_transform_module()
    coalesced_batches = []
    pushed = {}
    monkeypatch.setattr(
        module,
        "resolve_citydata_crowding_snapshot_id",
        lambda: 8738321387624398062,
        raising=False,
    )

    class Manifest:
        def latest_publishable_run_id(self):
            return "traffic-new"

        def require_publishable(self, run_id):
            return run_id

        def coalesce_many(self, run_ids, *, replacement_run_id):
            coalesced_batches.append((list(run_ids), replacement_run_id))

    monkeypatch.setattr(module, "build_traffic_manifest", lambda: Manifest())
    ti = types.SimpleNamespace(
        xcom_push=lambda *, key, value: pushed.update({key: value})
    )

    def event(run_id, event_at):
        return types.SimpleNamespace(
            extra={
                "source_id": "seoul_traffic_incident",
                "bronze_run_id": run_id,
                "bronze_dag_run_id": run_id,
                "event_at": event_at,
                "load_date": "2026-07-15",
                "row_count": 7,
                "payload_hash": "a" * 64,
                "is_publishable": True,
            }
        )

    assert (
        module.resolve_traffic_snapshot_run(
            ti=ti,
            triggering_asset_events={
                module.TRAFFIC_BRONZE_ASSET: [
                    event("traffic-old-a", "2026-07-15T12:00:00+09:00"),
                    event("traffic-old-b", "2026-07-15T12:00:30+09:00"),
                    event("traffic-new", "2026-07-15T12:01:00+09:00"),
                ]
            },
        )
        == "traffic-new"
    )
    assert coalesced_batches == []
    assert pushed[module.STALE_INCIDENT_RUN_IDS_XCOM_KEY] == [
        "traffic-old-a",
        "traffic-old-b",
    ]


def _pin_gold_phase_evidence(monkeypatch, module):
    evidence = module.SilverOutputEvidence(42, "a" * 64)
    monkeypatch.setattr(
        module,
        "silver_output_evidence_from_resolver",
        lambda *_args, **_kwargs: evidence,
    )
    monkeypatch.setattr(
        module,
        "current_silver_output_evidence",
        lambda: evidence,
    )

    class Manifest:
        def require_publishable(self, run_id):
            return run_id

    monkeypatch.setattr(module, "build_traffic_manifest", Manifest)


def test_traffic_snapshot_resolver_batches_large_stale_backlog(monkeypatch):
    module = load_transform_module()
    coalesced_batches = []
    pushed = {}
    monkeypatch.setattr(
        module,
        "resolve_citydata_crowding_snapshot_id",
        lambda: 8738321387624398062,
        raising=False,
    )

    class Manifest:
        def latest_publishable_run_id(self):
            return "traffic-latest"

        def require_publishable(self, run_id):
            return run_id

        def coalesce_many(self, run_ids, *, replacement_run_id):
            coalesced_batches.append((list(run_ids), replacement_run_id))

    monkeypatch.setattr(module, "build_traffic_manifest", lambda: Manifest())

    events = [
        types.SimpleNamespace(
            extra={
                "source_id": "seoul_traffic_incident",
                "bronze_run_id": f"traffic-old-{index}",
                "bronze_dag_run_id": f"traffic-old-{index}",
                "event_at": "2026-07-15T12:00:00+09:00",
                "load_date": "2026-07-15",
                "row_count": 7,
                "payload_hash": "a" * 64,
                "is_publishable": True,
            }
        )
        for index in range(80)
    ]
    events.append(
        types.SimpleNamespace(
            extra={
                "source_id": "seoul_traffic_incident",
                "bronze_run_id": "traffic-latest",
                "bronze_dag_run_id": "traffic-latest",
                "event_at": "2026-07-15T12:01:00+09:00",
                "load_date": "2026-07-15",
                "row_count": 7,
                "payload_hash": "b" * 64,
                "is_publishable": True,
            }
        )
    )

    assert (
        module.resolve_traffic_snapshot_run(
            ti=types.SimpleNamespace(
                xcom_push=lambda *, key, value: pushed.update({key: value})
            ),
            triggering_asset_events={module.TRAFFIC_BRONZE_ASSET: events},
        )
        == "traffic-latest"
    )
    assert coalesced_batches == []
    assert pushed[module.STALE_INCIDENT_RUN_IDS_XCOM_KEY] == sorted(
        f"traffic-old-{index}" for index in range(80)
    )


def test_traffic_snapshot_resolver_rejects_manifest_mismatch(monkeypatch):
    module = load_transform_module()

    class Manifest:
        def latest_publishable_run_id(self):
            return "traffic-run-42"

        def require_publishable(self, run_id):
            raise RuntimeError(f"not publishable: {run_id}")

    monkeypatch.setattr(module, "build_traffic_manifest", lambda: Manifest())
    event = types.SimpleNamespace(
        extra={
            "source_id": "seoul_traffic_incident",
            "bronze_run_id": "traffic-run-42",
            "bronze_dag_run_id": "traffic-run-42",
            "event_at": "2026-07-15T12:00:00+09:00",
            "load_date": "2026-07-15",
            "row_count": 7,
            "payload_hash": "a" * 64,
            "is_publishable": True,
        }
    )

    with pytest.raises(FakeAirflowFailException, match="publishable"):
        module.resolve_traffic_snapshot_run(
            triggering_asset_events={module.TRAFFIC_BRONZE_ASSET: [event]}
        )


def _set_silver_marker(module, incident_run_id="incident-42"):
    FakeVariable.values[module.SILVER_SUCCESS_MARKER_KEY] = (
        module.TransformSuccessMarker(
            version=1,
            pipeline="silver",
            identity=module.TransformIdentity.silver(incident_run_id),
            output_snapshot_id=42,
            compacted_files_fingerprint="a" * 64,
        ).to_json()
    )


def _flow_event(
    *,
    flow_run_id="flow-42",
    parent_incident_run_id="incident-42",
    event_at="2026-07-16T00:06:00+00:00",
):
    return types.SimpleNamespace(
        extra={
            "source_id": "seoul_traffic_flow",
            "flow_run_id": flow_run_id,
            "flow_dag_run_id": flow_run_id,
            "parent_incident_run_id": parent_incident_run_id,
            "event_at": event_at,
            "load_date": "2026-07-16",
            "row_count": 1,
            "payload_hash": "b" * 64,
            "is_publishable": True,
        }
    )


def _incident_silver_event(
    *,
    incident_run_id="incident-42",
    event_at="2026-07-16T00:05:00+00:00",
):
    return types.SimpleNamespace(
        extra={
            "source_id": "seoul_traffic_incident",
            "incident_run_id": incident_run_id,
            "silver_snapshot_id": 42,
            "compacted_files_fingerprint": "a" * 64,
            "event_at": event_at,
            "is_publishable": True,
            "contract": "traffic_incident_silver.v1",
        }
    )


def _set_current_silver_evidence(monkeypatch, module):
    monkeypatch.setattr(
        module,
        "current_silver_output_evidence",
        lambda: module.SilverOutputEvidence(42, "a" * 64),
    )
    monkeypatch.setattr(module, "traffic_gold_anchor_exists", lambda: True)

    class Manifest:
        def require_publishable(self, run_id):
            return run_id

    monkeypatch.setattr(module, "build_traffic_manifest", Manifest)


def _flow_silver_event(*, flow_run_id="flow-42", parent_incident_run_id="incident-42"):
    return types.SimpleNamespace(
        extra={
            "source_id": "seoul_traffic_flow",
            "flow_run_id": flow_run_id,
            "flow_dag_run_id": flow_run_id,
            "parent_incident_run_id": parent_incident_run_id,
            "event_at": "2026-07-16T00:07:00+00:00",
            "is_publishable": True,
            "contract": "traffic_flow_silver.v1",
        }
    )


def test_flow_silver_resolver_pins_only_matching_publishable_pair(monkeypatch):
    load_transform_module()
    from traffic_ingest import transform_dag_support
    from traffic_ingest.assets import (
        TRAFFIC_FLOW_BRONZE_ASSET,
        TRAFFIC_INCIDENT_SILVER_ASSET,
    )

    flow_calls = []
    pushed = {}

    class FlowManifest:
        def require_publishable(self, run_id):
            flow_calls.append(run_id)
            return run_id

    ti = types.SimpleNamespace(
        xcom_push=lambda *, key, value: pushed.update({key: value})
    )
    incident_run_id = transform_dag_support.resolve_traffic_flow_silver_snapshot_run(
        context={
            "ti": ti,
            "triggering_asset_events": {
                TRAFFIC_INCIDENT_SILVER_ASSET: [_incident_silver_event()],
                TRAFFIC_FLOW_BRONZE_ASSET: [_flow_event()],
            },
        },
        flow_manifest_factory=FlowManifest,
        flow_xcom_key="traffic_flow_snapshot_dag_run_id",
    )

    assert incident_run_id == "incident-42"
    assert flow_calls == ["flow-42"]
    assert pushed["traffic_flow_snapshot_dag_run_id"] == "flow-42"


def test_flow_silver_resolver_fails_closed_for_mismatched_parent():
    load_transform_module()
    from traffic_ingest import transform_dag_support
    from traffic_ingest.assets import (
        TRAFFIC_FLOW_BRONZE_ASSET,
        TRAFFIC_INCIDENT_SILVER_ASSET,
    )

    with pytest.raises(FakeAirflowFailException, match="matching"):
        transform_dag_support.resolve_traffic_flow_silver_snapshot_run(
            context={
                "triggering_asset_events": {
                    TRAFFIC_INCIDENT_SILVER_ASSET: [
                        _incident_silver_event(incident_run_id="incident-new")
                    ],
                    TRAFFIC_FLOW_BRONZE_ASSET: [
                        _flow_event(parent_incident_run_id="incident-old")
                    ],
                }
            },
            flow_manifest_factory=lambda: pytest.fail(
                "mismatched Flow must not read the manifest"
            ),
            flow_xcom_key="traffic_flow_snapshot_dag_run_id",
        )


def test_flow_silver_resolver_skips_stale_flow_event_for_newer_incident_parent():
    load_transform_module()
    from traffic_ingest import transform_dag_support
    from traffic_ingest.assets import (
        TRAFFIC_FLOW_BRONZE_ASSET,
        TRAFFIC_INCIDENT_SILVER_ASSET,
    )

    with pytest.raises(FakeAirflowSkipException, match="awaiting Flow Bronze"):
        transform_dag_support.resolve_traffic_flow_silver_snapshot_run(
            context={
                "triggering_asset_events": {
                    TRAFFIC_INCIDENT_SILVER_ASSET: [
                        _incident_silver_event(
                            incident_run_id="incident-new",
                            event_at="2026-07-16T00:05:00+00:00",
                        )
                    ],
                    TRAFFIC_FLOW_BRONZE_ASSET: [
                        _flow_event(
                            parent_incident_run_id="incident-old",
                            event_at="2026-07-16T00:04:00+00:00",
                        )
                    ],
                }
            },
            flow_manifest_factory=lambda: pytest.fail(
                "stale Flow must not read the manifest"
            ),
            flow_xcom_key="traffic_flow_snapshot_dag_run_id",
        )


def test_gold_flow_asset_pins_compatible_publishable_flow(monkeypatch):
    module = load_gold_transform_module()
    _set_silver_marker(module)
    _set_current_silver_evidence(monkeypatch, module)
    flow_calls = []
    pushed = {}

    class FlowManifest:
        def require_publishable(self, run_id):
            flow_calls.append(run_id)
            return run_id

    monkeypatch.setattr(module, "build_traffic_flow_manifest", lambda: FlowManifest())
    monkeypatch.setattr(module, "resolve_citydata_crowding_snapshot_id", lambda: 7)
    monkeypatch.setattr(
        module, "resolve_admin_dong_crosswalk_snapshot_id", lambda: 99
    )
    ti = types.SimpleNamespace(
        xcom_push=lambda *, key, value: pushed.update({key: value})
    )

    assert (
        module.resolve_traffic_gold_snapshot_run(
            ti=ti,
            triggering_asset_events={
                module.TRAFFIC_FLOW_SILVER_ASSET: [_flow_silver_event()]
            },
        )
        == "incident-42"
    )
    assert flow_calls == ["flow-42"]
    assert pushed[module.FLOW_SNAPSHOT_XCOM_KEY] == "flow-42"
    assert pushed[module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY] == 7
    assert pushed[module.ADMIN_DONG_CROSSWALK_PIN_XCOM_KEY] == 99


def test_gold_stale_incompatible_flow_becomes_none(monkeypatch):
    module = load_gold_transform_module()
    _set_silver_marker(module, "incident-new")
    _set_current_silver_evidence(monkeypatch, module)
    pushed = {}
    monkeypatch.setattr(
        module,
        "build_traffic_flow_manifest",
        lambda: pytest.fail("incompatible Flow must not be read"),
    )
    monkeypatch.setattr(module, "resolve_citydata_crowding_snapshot_id", lambda: 7)
    monkeypatch.setattr(
        module, "resolve_admin_dong_crosswalk_snapshot_id", lambda: 99
    )
    ti = types.SimpleNamespace(
        xcom_push=lambda *, key, value: pushed.update({key: value})
    )

    assert (
        module.resolve_traffic_gold_snapshot_run(
            ti=ti,
            triggering_asset_events={
                module.TRAFFIC_FLOW_SILVER_ASSET: [
                    _flow_silver_event(
                        flow_run_id="flow-old",
                        parent_incident_run_id="incident-old",
                    )
                ]
            },
        )
        == "incident-new"
    )
    assert pushed[module.FLOW_SNAPSHOT_XCOM_KEY] is None


def test_gold_resolver_fails_closed_when_citydata_unavailable(monkeypatch):
    module = load_gold_transform_module()
    _set_silver_marker(module)
    _set_current_silver_evidence(monkeypatch, module)
    from traffic_ingest.external_snapshot import ExternalSnapshotUnavailableError

    monkeypatch.setattr(
        module,
        "resolve_citydata_crowding_snapshot_id",
        lambda: (_ for _ in ()).throw(
            ExternalSnapshotUnavailableError("no Citydata snapshot")
        ),
    )

    with pytest.raises(FakeAirflowFailException, match="no Citydata snapshot"):
        module.resolve_traffic_gold_snapshot_run(triggering_asset_events={})


def test_gold_resolver_propagates_citydata_query_errors_for_retry(monkeypatch):
    module = load_gold_transform_module()
    _set_silver_marker(module)
    _set_current_silver_evidence(monkeypatch, module)
    monkeypatch.setattr(
        module,
        "resolve_citydata_crowding_snapshot_id",
        lambda: (_ for _ in ()).throw(RuntimeError("Trino connection reset")),
    )

    with pytest.raises(RuntimeError, match="Trino connection reset"):
        module.resolve_traffic_gold_snapshot_run(triggering_asset_events={})


def test_gold_low_level_resolver_pins_admin_dong_crosswalk_snapshot_and_pushes_xcom():
    load_transform_module()
    from traffic_ingest import transform_dag_support

    _set_silver_marker(transform_dag_support)

    class Manifest:
        def require_publishable(self, run_id):
            return run_id

    pushed = {}
    ti = types.SimpleNamespace(
        xcom_push=lambda *, key, value: pushed.update({key: value})
    )

    incident_run_id = transform_dag_support.resolve_traffic_gold_snapshot_run(
        context={"ti": ti, "triggering_asset_events": {}},
        variable=FakeVariable,
        incident_manifest_factory=Manifest,
        flow_manifest_factory=Manifest,
        citydata_snapshot_resolver=lambda: 7,
        admin_dong_crosswalk_snapshot_resolver=lambda: 99,
        current_silver_evidence_loader=lambda: transform_dag_support.SilverOutputEvidence(
            42, "a" * 64
        ),
        flow_xcom_key="traffic_flow_snapshot_dag_run_id",
        citydata_xcom_key="traffic_citydata_crowding_snapshot_id",
        admin_dong_crosswalk_xcom_key="admin_dong_crosswalk_pin_snapshot_id",
    )

    assert incident_run_id == "incident-42"
    assert pushed["traffic_citydata_crowding_snapshot_id"] == 7
    assert pushed["admin_dong_crosswalk_pin_snapshot_id"] == 99


@pytest.mark.parametrize("invalid_snapshot_id", [0, -1, None, "not-an-int"])
def test_gold_low_level_resolver_fails_closed_on_invalid_admin_dong_crosswalk_snapshot(
    invalid_snapshot_id,
):
    load_transform_module()
    from traffic_ingest import transform_dag_support

    _set_silver_marker(transform_dag_support)

    class Manifest:
        def require_publishable(self, run_id):
            return run_id

    ti = types.SimpleNamespace(xcom_push=lambda *, key, value: None)

    with pytest.raises(FakeAirflowFailException, match="admin_dong crosswalk"):
        transform_dag_support.resolve_traffic_gold_snapshot_run(
            context={"ti": ti, "triggering_asset_events": {}},
            variable=FakeVariable,
            incident_manifest_factory=Manifest,
            flow_manifest_factory=Manifest,
            citydata_snapshot_resolver=lambda: 7,
            admin_dong_crosswalk_snapshot_resolver=lambda: invalid_snapshot_id,
            current_silver_evidence_loader=lambda: transform_dag_support.SilverOutputEvidence(
                42, "a" * 64
            ),
            flow_xcom_key="traffic_flow_snapshot_dag_run_id",
            citydata_xcom_key="traffic_citydata_crowding_snapshot_id",
            admin_dong_crosswalk_xcom_key="admin_dong_crosswalk_pin_snapshot_id",
        )


def test_dbt_snapshot_variables_includes_admin_dong_crosswalk_pin_when_present():
    load_transform_module()
    from traffic_ingest import transform_dag_support

    def xcom_pull(*, task_ids, key=None):
        assert task_ids == "resolve_traffic_gold_snapshot_run"
        if key == "traffic_citydata_crowding_snapshot_id":
            return 7
        if key == "admin_dong_crosswalk_pin_snapshot_id":
            return 99
        if key is None:
            return "incident-42"
        return None

    ti = types.SimpleNamespace(xcom_pull=xcom_pull)

    variables = transform_dag_support.dbt_snapshot_variables(
        ti,
        "resolve_traffic_gold_snapshot_run",
        "incident-42",
        "traffic_flow_snapshot_dag_run_id",
        "traffic_citydata_crowding_snapshot_id",
        admin_dong_crosswalk_xcom_key="admin_dong_crosswalk_pin_snapshot_id",
    )

    assert variables["admin_dong_crosswalk_pin_snapshot_id"] == 99


def test_dbt_snapshot_variables_omits_admin_dong_crosswalk_pin_when_key_not_passed():
    load_transform_module()
    from traffic_ingest import transform_dag_support

    def xcom_pull(*, task_ids, key=None):
        if key == "traffic_citydata_crowding_snapshot_id":
            return 7
        return None

    ti = types.SimpleNamespace(xcom_pull=xcom_pull)

    variables = transform_dag_support.dbt_snapshot_variables(
        ti,
        "resolve_traffic_gold_snapshot_run",
        "incident-42",
        "traffic_flow_snapshot_dag_run_id",
        "traffic_citydata_crowding_snapshot_id",
    )

    assert "admin_dong_crosswalk_pin_snapshot_id" not in variables


def test_traffic_hot_build_delegates_membership_to_one_dbt_selector():
    module = load_transform_module()
    build_task = module.dag.task_dict["dbt_run_silver"]
    assert build_task.kwargs["op_kwargs"]["dbt_command"] == "build"
    assert build_task.kwargs["op_kwargs"]["selector"] == (
        "ask_seoul_traffic_transform_incident_hot_build"
    )
    assert not hasattr(module, "TRAFFIC_BRONZE_SOURCE_CONTRACT_TESTS")
    assert not hasattr(module, "normalize_dbt_test_tuples")
    assert not hasattr(module, "assert_exact_dbt_test_set")


def test_contract_gate_runs_non_empty_dbt_ls_before_dbt_test(tmp_path, monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "dbt"))
    commands = []
    ti = types.SimpleNamespace(
        task_id="dbt_test_traffic_bronze_source_contract",
        try_number=1,
        xcom_pull=lambda task_ids: 'snapshot-"\\a',
    )

    def fake_run(command, **_kwargs):
        commands.append(command)
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"test.traffic.contract","resource_type":"test"}\n',
                stderr="",
            )
        write_materialization_artifacts(command)
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    result = module.run_dbt_phase(
        dbt_command="test",
        selector="traffic_transform_contract_gate",
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=False,
        ti=ti,
        run_id="manual__a",
        params={"target": "dev"},
    )

    assert result["status"] == "success"
    assert result["manifest_path"].endswith("/manifest.json")
    assert commands[0][1:4] == ["ls", "--resource-type", "test"]
    assert "--output" in commands[0]
    assert (
        commands[0][commands[0].index("--selector") + 1]
        == "traffic_transform_contract_gate"
    )
    assert commands[1][1:3] == ["test", "--selector"]
    assert commands[1][3] == "traffic_transform_contract_gate"
    for command in commands:
        assert "--indirect-selection=buildable" not in command
        assert "--target-path" in command
        assert "--log-path" in command
        variables = command[command.index("--vars") + 1]
        assert json.loads(variables) == {"traffic_snapshot_dag_run_id": 'snapshot-"\\a'}


def test_contract_gate_does_not_run_dbt_test_after_empty_selection(monkeypatch):
    module = load_transform_module()
    commands = []
    pushed = {}
    ti = types.SimpleNamespace(
        dag_id="traffic_incident_transform",
        task_id="dbt_test_traffic_bronze_source_contract",
        try_number=1,
        xcom_pull=lambda task_ids: "snapshot-a",
        xcom_push=lambda key, value: pushed.update(key=key, value=value),
    )

    def fake_run(command, **_kwargs):
        commands.append(command)
        return types.SimpleNamespace(
            returncode=0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    with pytest.raises(FakeAirflowFailException, match="model-execution-failed"):
        module.run_dbt_phase(
            dbt_command="test",
            selector="traffic_transform_contract_gate",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=False,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    assert len(commands) == 1
    assert commands[0][1] == "ls"
    assert pushed["value"]["failure_classification"] == "model-execution-failed"


def test_preflight_phase_uses_non_materializing_snapshot_sentinel(monkeypatch):
    module = load_transform_module()
    captured = {}
    completed = types.SimpleNamespace(returncode=0, stdout="", stderr="")
    execution = types.SimpleNamespace(
        attempts=(completed,),
        completed=completed,
        missing_expected_artifacts=(),
        existing_run_results_path="/tmp/preflight/run_results.json",
        existing_sources_path=None,
        existing_manifest_path="/tmp/preflight/manifest.json",
        selected_unique_ids=(),
        primary_artifact_path="/tmp/preflight/run_results.json",
    )
    monkeypatch.setattr(
        module.traffic_dbt,
        "execute_dbt_phase",
        lambda **kwargs: captured.update(kwargs) or execution,
    )
    ti = types.SimpleNamespace(
        task_id="dbt_source_freshness",
        try_number=1,
        xcom_pull=lambda **_kwargs: None,
    )

    result = module.run_dbt_phase(
        dbt_command="source freshness",
        selector="ask_seoul_traffic_transform_source",
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        snapshot_required=False,
        silver_persisted=False,
        ti=ti,
        run_id="asset_triggered__preflight",
        params={"target": "dev"},
    )

    assert result["status"] == "success"
    assert json.loads(captured["variables"]) == {
        "traffic_snapshot_dag_run_id": module.PREFLIGHT_SNAPSHOT_DAG_RUN_ID
    }


def test_snapshot_required_phase_passes_citydata_snapshot_id_to_dbt(monkeypatch):
    module = load_gold_transform_module()
    _pin_gold_phase_evidence(monkeypatch, module)
    captured = {}
    completed = types.SimpleNamespace(returncode=0, stdout="", stderr="")
    execution = types.SimpleNamespace(
        attempts=(completed,),
        completed=completed,
        missing_expected_artifacts=(),
        existing_run_results_path="/tmp/pinned/run_results.json",
        existing_sources_path="/tmp/pinned/sources.json",
        existing_manifest_path="/tmp/pinned/manifest.json",
        selected_unique_ids=(),
        primary_artifact_path="/tmp/pinned/run_results.json",
    )
    monkeypatch.setattr(
        module.traffic_dbt,
        "execute_dbt_phase",
        lambda **kwargs: captured.update(kwargs) or execution,
    )

    def xcom_pull(*, task_ids, key=None):
        values = {
            (module.SNAPSHOT_TASK_ID, None): "snapshot-a",
            (module.SNAPSHOT_TASK_ID, module.FLOW_SNAPSHOT_XCOM_KEY): None,
            (
                module.SNAPSHOT_TASK_ID,
                module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY,
            ): 8738321387624398062,
            (
                module.SNAPSHOT_TASK_ID,
                module.ADMIN_DONG_CROSSWALK_PIN_XCOM_KEY,
            ): 8738321387624398063,
        }
        return values[(task_ids, key)]

    ti = types.SimpleNamespace(
        task_id="dbt_run_gold",
        try_number=1,
        xcom_pull=xcom_pull,
    )

    result = module.run_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_gold",
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        snapshot_required=True,
        citydata_snapshot_required=True,
        admin_dong_crosswalk_pin_required=True,
        silver_persisted=True,
        ti=ti,
        run_id="asset_triggered__pinned",
        params={"target": "dev"},
    )

    assert result["status"] == "success"
    assert json.loads(captured["variables"]) == {
        "traffic_snapshot_dag_run_id": "snapshot-a",
        "traffic_citydata_crowding_snapshot_id": 8738321387624398062,
        "admin_dong_crosswalk_pin_snapshot_id": 8738321387624398063,
    }


@pytest.mark.parametrize(
    "external_snapshot_id",
    [None, 0, -1, True, "8738321387624398062"],
)
def test_citydata_required_phase_rejects_missing_or_invalid_citydata_snapshot(
    monkeypatch,
    external_snapshot_id,
):
    module = load_gold_transform_module()
    called = False

    def fail_if_called(**_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(module.traffic_dbt, "execute_dbt_phase", fail_if_called)

    def xcom_pull(*, task_ids, key=None):
        if key is None:
            return "snapshot-a"
        if key == module.FLOW_SNAPSHOT_XCOM_KEY:
            return None
        if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY:
            return external_snapshot_id
        return None

    ti = types.SimpleNamespace(
        task_id="dbt_run_gold",
        try_number=1,
        xcom_pull=xcom_pull,
    )

    with pytest.raises(
        FakeAirflowFailException,
        match="traffic dbt phase requires Citydata crowding snapshot",
    ):
        module.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_traffic_transform_gold",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            snapshot_required=True,
            citydata_snapshot_required=True,
            silver_persisted=True,
            ti=ti,
            run_id="asset_triggered__missing-citydata-pin",
            params={"target": "dev"},
        )

    assert called is False


def test_snapshot_required_phase_rejects_missing_late_pin(monkeypatch):
    module = load_transform_module()
    called = False

    def fail_if_called(**_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(module.traffic_dbt, "execute_dbt_phase", fail_if_called)
    ti = types.SimpleNamespace(
        task_id="dbt_run_silver",
        try_number=1,
        xcom_pull=lambda **_kwargs: None,
    )

    with pytest.raises(FakeAirflowFailException, match="resolved snapshot"):
        module.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_traffic_transform_silver",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            snapshot_required=True,
            silver_persisted=False,
            ti=ti,
            run_id="asset_triggered__missing-pin",
            params={"target": "dev"},
        )

    assert called is False


def test_gold_test_selector_is_chosen_from_current_run_tier(monkeypatch):
    module = load_gold_transform_module()
    _pin_gold_phase_evidence(monkeypatch, module)
    captured = {}

    def execute_dbt_phase(**kwargs):
        captured.update(kwargs)
        return types.SimpleNamespace(
            attempts=[types.SimpleNamespace(returncode=0, stdout="", stderr="")],
            completed=types.SimpleNamespace(returncode=0, stdout="", stderr=""),
            existing_run_results_path=None,
            existing_sources_path=None,
            existing_manifest_path=None,
            missing_expected_artifacts=(),
            selected_unique_ids=tuple(
                f"test.asac.gold_gate_{index}" for index in range(123)
            ),
        )

    monkeypatch.setattr(module.traffic_dbt, "execute_dbt_phase", execute_dbt_phase)
    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=1,
        xcom_pull=lambda *, task_ids, key=None: (
            {
                "tier": module.TrafficTestTier.GATE.value,
                "hour_bucket": "2026-07-18T10",
                "day_bucket": "2026-07-18",
            }
            if task_ids == module.SELECT_TEST_TIER_TASK_ID
            else (
                8738321387624398062
                if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY
                else "snapshot-a"
            )
        ),
    )

    result = module.run_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_traffic_transform_gold_full_tests",
        selector_by_test_tier={
            module.TrafficTestTier.GATE: "ask_seoul_traffic_transform_gold_gate_tests",
            module.TrafficTestTier.HOURLY: "ask_seoul_traffic_transform_gold_hourly_tests",
            module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_gold_full_tests",
        },
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=True,
        fresh_parse=True,
        snapshot_required=True,
        threads=2,
        ti=ti,
        run_id="manual__tier",
        params={"target": "dev"},
    )

    assert result["status"] == "success"
    assert captured["selector"] == "ask_seoul_traffic_transform_gold_gate_tests"
    assert captured["threads"] == 2
    assert len(result["selected_unique_ids"]) == 123


def _execute_gold_phase_with_flow(
    monkeypatch,
    *,
    dbt_command="run",
    selector="ask_seoul_traffic_transform_gold",
    selector_by_test_tier=None,
    flow_run_id=None,
    tier=None,
    citydata=8738321387624398062,
):
    module = load_gold_transform_module()
    _pin_gold_phase_evidence(monkeypatch, module)
    captured = {}

    def execute_dbt_phase(**kwargs):
        captured.update(kwargs)
        return types.SimpleNamespace(
            attempts=[types.SimpleNamespace(returncode=0, stdout="", stderr="")],
            completed=types.SimpleNamespace(returncode=0, stdout="", stderr=""),
            existing_run_results_path=None,
            existing_sources_path=None,
            existing_manifest_path=None,
            missing_expected_artifacts=(),
            selected_unique_ids=(),
        )

    def xcom_pull(*, task_ids, key=None):
        if task_ids == module.SELECT_TEST_TIER_TASK_ID:
            return {
                "tier": tier.value,
                "hour_bucket": "2026-07-18T10",
                "day_bucket": "2026-07-18",
            }
        if task_ids == module.SNAPSHOT_TASK_ID:
            if key == module.FLOW_SNAPSHOT_XCOM_KEY:
                return flow_run_id
            if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY:
                return citydata
            return "incident-1"
        return None

    monkeypatch.setattr(module.traffic_dbt, "execute_dbt_phase", execute_dbt_phase)
    ti = types.SimpleNamespace(
        task_id="dbt_test_gold" if dbt_command == "test" else "dbt_run_gold",
        try_number=1,
        xcom_pull=xcom_pull,
    )

    module.run_dbt_phase(
        dbt_command=dbt_command,
        selector=selector,
        selector_by_test_tier=selector_by_test_tier,
        selector_when_flow_missing="ask_seoul_traffic_transform_gold_incident_models",
        selector_by_test_tier_when_flow_missing=(
            {
                module.TrafficTestTier.GATE: (
                    "ask_seoul_traffic_transform_gold_incident_gate_tests"
                ),
                module.TrafficTestTier.HOURLY: (
                    "ask_seoul_traffic_transform_gold_incident_hourly_tests"
                ),
                module.TrafficTestTier.FULL: (
                    "ask_seoul_traffic_transform_gold_incident_full_tests"
                ),
            }
            if selector_by_test_tier is not None
            else None
        ),
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=True,
        fresh_parse=dbt_command == "test",
        snapshot_required=True,
        citydata_snapshot_required=True,
        threads=2,
        ti=ti,
        run_id="manual__flow-aware-selector",
        params={"target": "dev"},
    )

    return captured


def test_gold_run_uses_incident_selector_when_flow_snapshot_is_missing(monkeypatch):
    captured = _execute_gold_phase_with_flow(monkeypatch, flow_run_id=None)

    assert captured["selector"] == "ask_seoul_traffic_transform_gold_incident_models"


def test_gold_run_uses_incident_selector_when_flow_snapshot_is_empty(monkeypatch):
    captured = _execute_gold_phase_with_flow(monkeypatch, flow_run_id="")

    assert captured["selector"] == "ask_seoul_traffic_transform_gold_incident_models"


def test_gold_run_keeps_full_selector_when_flow_snapshot_is_present(monkeypatch):
    captured = _execute_gold_phase_with_flow(monkeypatch, flow_run_id="flow-42")

    assert captured["selector"] == "ask_seoul_traffic_transform_gold"


@pytest.mark.parametrize(
    ("tier_name", "expected_selector"),
    [
        ("GATE", "ask_seoul_traffic_transform_gold_incident_gate_tests"),
        ("HOURLY", "ask_seoul_traffic_transform_gold_incident_hourly_tests"),
        ("FULL", "ask_seoul_traffic_transform_gold_incident_full_tests"),
    ],
)
def test_gold_test_uses_incident_selector_by_tier_when_flow_snapshot_is_missing(
    monkeypatch, tier_name, expected_selector
):
    module = load_gold_transform_module()
    captured = _execute_gold_phase_with_flow(
        monkeypatch,
        dbt_command="test",
        selector="ask_seoul_traffic_transform_gold_full_tests",
        selector_by_test_tier={
            module.TrafficTestTier.GATE: "ask_seoul_traffic_transform_gold_gate_tests",
            module.TrafficTestTier.HOURLY: "ask_seoul_traffic_transform_gold_hourly_tests",
            module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_gold_full_tests",
        },
        flow_run_id=None,
        tier=getattr(module.TrafficTestTier, tier_name),
    )

    assert captured["selector"] == expected_selector


@pytest.mark.parametrize(
    ("tier_name", "expected_selector"),
    [
        ("GATE", "ask_seoul_traffic_transform_gold_gate_tests"),
        ("HOURLY", "ask_seoul_traffic_transform_gold_hourly_tests"),
        ("FULL", "ask_seoul_traffic_transform_gold_full_tests"),
    ],
)
def test_gold_test_keeps_full_selector_by_tier_when_flow_snapshot_is_present(
    monkeypatch, tier_name, expected_selector
):
    module = load_gold_transform_module()
    captured = _execute_gold_phase_with_flow(
        monkeypatch,
        dbt_command="test",
        selector="ask_seoul_traffic_transform_gold_full_tests",
        selector_by_test_tier={
            module.TrafficTestTier.GATE: "ask_seoul_traffic_transform_gold_gate_tests",
            module.TrafficTestTier.HOURLY: "ask_seoul_traffic_transform_gold_hourly_tests",
            module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_gold_full_tests",
        },
        flow_run_id="flow-42",
        tier=getattr(module.TrafficTestTier, tier_name),
    )

    assert captured["selector"] == expected_selector


def test_select_traffic_test_tier_freezes_current_run_decision():
    module = load_gold_transform_module()

    result = module.select_traffic_test_tier()

    assert result["tier"] == module.TrafficTestTier.FULL.value
    assert set(result) == {"tier", "hour_bucket", "day_bucket"}
    assert FakeVariable.values == {}


def test_mark_traffic_test_tier_writes_frozen_decision_in_conservative_order():
    module = load_gold_transform_module()
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids: (
            {
                "tier": module.TrafficTestTier.FULL.value,
                "hour_bucket": "2026-07-18T10",
                "day_bucket": "2026-07-18",
            }
            if task_ids == module.SELECT_TEST_TIER_TASK_ID
            else None
        )
    )

    result = module.mark_traffic_test_tier(ti=ti)

    assert FakeVariable.set_calls == [
        ("ask_seoul_traffic_gold_test_last_success_hour_kst", "2026-07-18T10"),
        ("ask_seoul_traffic_gold_test_last_success_day_kst", "2026-07-18"),
    ]
    assert result == {
        "ask_seoul_traffic_gold_test_last_success_hour_kst": "2026-07-18T10",
        "ask_seoul_traffic_gold_test_last_success_day_kst": "2026-07-18",
    }


INVALID_TRAFFIC_TEST_DECISIONS = (
    pytest.param(
        {"tier": "full", "hour_bucket": None, "day_bucket": "2026-07-18"},
        id="non-string-hour",
    ),
    pytest.param(
        {"tier": "full", "hour_bucket": "2026-07-18T1", "day_bucket": "2026-07-18"},
        id="malformed-hour",
    ),
    pytest.param(
        {"tier": "full", "hour_bucket": "2026-07-18T24", "day_bucket": "2026-07-18"},
        id="invalid-hour",
    ),
    pytest.param(
        {"tier": "full", "hour_bucket": "2026-07-18T10", "day_bucket": "2026-7-18"},
        id="malformed-day",
    ),
    pytest.param(
        {"tier": "full", "hour_bucket": "2026-02-29T10", "day_bucket": "2026-02-29"},
        id="invalid-calendar-date",
    ),
    pytest.param(
        {"tier": "full", "hour_bucket": "2026-07-17T23", "day_bucket": "2026-07-18"},
        id="inconsistent-buckets",
    ),
)


@pytest.mark.parametrize("raw_decision", INVALID_TRAFFIC_TEST_DECISIONS)
def test_gold_selector_rejects_invalid_buckets_before_executor(
    monkeypatch, raw_decision
):
    module = load_gold_transform_module()
    calls = []
    monkeypatch.setattr(
        module.traffic_dbt,
        "execute_dbt_phase",
        lambda **kwargs: calls.append(kwargs),
    )
    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=1,
        xcom_pull=lambda *, task_ids, key=None: (
            raw_decision
            if task_ids == module.SELECT_TEST_TIER_TASK_ID
            else (
                8738321387624398062
                if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY
                else "snapshot-a"
            )
        ),
    )

    with pytest.raises(FakeAirflowFailException, match="invalid traffic test decision"):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_gold_full_tests",
            selector_by_test_tier={
                module.TrafficTestTier.GATE: "ask_seoul_traffic_transform_gold_gate_tests",
                module.TrafficTestTier.HOURLY: "ask_seoul_traffic_transform_gold_hourly_tests",
                module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_gold_full_tests",
            },
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            fresh_parse=True,
            snapshot_required=True,
            threads=2,
            ti=ti,
            run_id="manual__invalid-buckets",
            params={"target": "dev"},
        )

    assert calls == []


@pytest.mark.parametrize("raw_decision", INVALID_TRAFFIC_TEST_DECISIONS)
def test_marker_rejects_invalid_buckets_before_variable_write(raw_decision):
    module = load_gold_transform_module()
    ti = types.SimpleNamespace(
        xcom_pull=lambda *, task_ids: (
            raw_decision if task_ids == module.SELECT_TEST_TIER_TASK_ID else None
        )
    )

    with pytest.raises(FakeAirflowFailException, match="invalid traffic test decision"):
        module.mark_traffic_test_tier(ti=ti)

    assert FakeVariable.set_calls == []
    assert FakeVariable.values == {}


def test_gold_test_selector_fails_closed_for_invalid_tier():
    module = load_gold_transform_module()
    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=1,
        xcom_pull=lambda *, task_ids, key=None: (
            {
                "tier": "bad-tier",
                "hour_bucket": "2026-07-18T10",
                "day_bucket": "2026-07-18",
            }
            if task_ids == module.SELECT_TEST_TIER_TASK_ID
            else (
                8738321387624398062
                if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY
                else "snapshot-a"
            )
        ),
    )

    with pytest.raises(FakeAirflowFailException, match="invalid traffic test decision"):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_gold_full_tests",
            selector_by_test_tier={
                module.TrafficTestTier.GATE: "ask_seoul_traffic_transform_gold_gate_tests",
                module.TrafficTestTier.HOURLY: "ask_seoul_traffic_transform_gold_hourly_tests",
                module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_gold_full_tests",
            },
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            fresh_parse=True,
            snapshot_required=True,
            threads=2,
            ti=ti,
            run_id="manual__tier",
            params={"target": "dev"},
        )


def test_gold_test_selector_fails_closed_for_malformed_decision():
    module = load_gold_transform_module()
    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=1,
        xcom_pull=lambda *, task_ids, key=None: (
            {"hour_bucket": "2026-07-18T10", "day_bucket": "2026-07-18"}
            if task_ids == module.SELECT_TEST_TIER_TASK_ID
            else (
                8738321387624398062
                if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY
                else "snapshot-a"
            )
        ),
    )

    with pytest.raises(FakeAirflowFailException, match="invalid traffic test decision"):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_gold_full_tests",
            selector_by_test_tier={
                module.TrafficTestTier.GATE: "ask_seoul_traffic_transform_gold_gate_tests",
                module.TrafficTestTier.HOURLY: "ask_seoul_traffic_transform_gold_hourly_tests",
                module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_gold_full_tests",
            },
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            fresh_parse=True,
            snapshot_required=True,
            threads=2,
            ti=ti,
            run_id="manual__tier",
            params={"target": "dev"},
        )


def test_axes_and_admin_tier_noop_returns_success_without_executor(monkeypatch):
    module = load_gold_transform_module()
    calls = []
    monkeypatch.setattr(
        module.traffic_dbt,
        "execute_dbt_phase",
        lambda **kwargs: calls.append(kwargs),
    )

    def xcom_pull(*, task_ids, key=None):
        if task_ids == module.SELECT_TEST_TIER_TASK_ID:
            return {
                "tier": module.TrafficTestTier.GATE.value,
                "hour_bucket": "2026-07-18T10",
                "day_bucket": "2026-07-18",
            }
        if task_ids == module.SNAPSHOT_TASK_ID and key is None:
            return None
        pytest.fail(f"tier noop should not read {task_ids} XCom key={key}")

    ti = types.SimpleNamespace(
        task_id="dbt_test_asac_axes_seed_contract",
        try_number=1,
        xcom_pull=xcom_pull,
    )

    result = module.run_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_traffic_transform_asac_axes_contract",
        selector_by_test_tier={
            module.TrafficTestTier.GATE: None,
            module.TrafficTestTier.HOURLY: None,
            module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_asac_axes_contract",
        },
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=False,
        threads=2,
        ti=ti,
        run_id="manual__tier",
        params={"target": "dev"},
    )

    assert result == {
        "status": "success",
        "skipped": True,
        "skip_reason": "traffic_test_tier_noop",
        "run_results_path": None,
        "sources_path": None,
        "manifest_path": None,
        "selected_unique_ids": [],
    }
    assert calls == []


def test_axes_and_admin_missing_tier_key_fails_instead_of_noop():
    module = load_gold_transform_module()
    ti = types.SimpleNamespace(
        task_id="dbt_test_common_admin_dong_dimension",
        try_number=1,
        xcom_pull=lambda *, task_ids, key=None: (
            {
                "tier": module.TrafficTestTier.HOURLY.value,
                "hour_bucket": "2026-07-18T10",
                "day_bucket": "2026-07-18",
            }
            if task_ids == module.SELECT_TEST_TIER_TASK_ID
            else "snapshot-a"
        ),
    )

    with pytest.raises(FakeAirflowFailException, match="missing dbt selector"):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_common_admin",
            selector_by_test_tier={
                module.TrafficTestTier.GATE: None,
                module.TrafficTestTier.FULL: "ask_seoul_traffic_transform_common_admin",
            },
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=False,
            threads=2,
            ti=ti,
            run_id="manual__tier",
            params={"target": "dev"},
        )


def test_transform_phase_specs_have_single_pipeline_owner():
    load_transform_module()
    from traffic_ingest.transform_specs import (
        DBT_PHASE_SPECS,
        DBT_PHASE_TASK_IDS,
        GOLD_DBT_PHASE_SPECS,
        SILVER_DBT_PHASE_SPECS,
    )

    silver_ids = tuple(spec.task_id for spec in SILVER_DBT_PHASE_SPECS)
    gold_ids = tuple(spec.task_id for spec in GOLD_DBT_PHASE_SPECS)

    assert silver_ids == (
        "dbt_deps",
        "dbt_run_silver",
    )
    assert gold_ids == (
        "dbt_deps_gold",
        "dbt_run_gold",
    )
    assert set(silver_ids).isdisjoint(gold_ids)

    compatibility_ids = (
        "dbt_deps",
        "dbt_run_silver",
        "dbt_deps_gold",
        "dbt_run_gold",
    )
    assert tuple(spec.task_id for spec in DBT_PHASE_SPECS) == compatibility_ids
    assert DBT_PHASE_TASK_IDS == compatibility_ids


def test_split_phase_specs_isolate_hot_build_pins():
    load_gold_transform_module()
    from traffic_ingest.transform_specs import (
        GOLD_DBT_PHASE_SPECS,
        SILVER_DBT_PHASE_SPECS,
    )

    all_specs = SILVER_DBT_PHASE_SPECS + GOLD_DBT_PHASE_SPECS
    assert all(not spec.citydata_snapshot_required for spec in SILVER_DBT_PHASE_SPECS)
    assert all(
        not spec.citydata_snapshot_required
        for spec in GOLD_DBT_PHASE_SPECS
        if spec.snapshot_required
    )
    assert {
        spec.task_id: spec.silver_fence_mode
        for spec in all_specs
        if spec.silver_fence_mode is not None
    } == {
        "dbt_run_silver": "write",
    }

    silver_build = next(
        spec for spec in SILVER_DBT_PHASE_SPECS if spec.task_id == "dbt_run_silver"
    )
    assert silver_build.dbt_command == "build"
    assert silver_build.selector == "ask_seoul_traffic_transform_incident_hot_build"
    assert silver_build.silver_persisted is True
    assert silver_build.selector_by_test_tier is None

    gold_specs = {spec.task_id: spec for spec in GOLD_DBT_PHASE_SPECS}
    assert gold_specs["dbt_deps_gold"].workload.value == "local"
    assert gold_specs["dbt_deps_gold"].threads is None
    assert gold_specs["dbt_run_gold"].dbt_command == "build"
    assert gold_specs["dbt_run_gold"].selector == (
        "ask_seoul_traffic_transform_gold_hot_build"
    )
    assert gold_specs["dbt_run_gold"].selector_when_flow_missing == (
        "ask_seoul_traffic_transform_gold_incident_hot_build"
    )
    assert gold_specs["dbt_run_gold"].selector_by_test_tier is None
    assert gold_specs["dbt_run_gold"].selector_by_test_tier_when_flow_missing is None
    assert all(
        spec.selector_when_flow_missing is None
        and spec.selector_by_test_tier_when_flow_missing is None
        for spec in all_specs
        if spec.task_id not in {"dbt_run_gold", "dbt_test_gold"}
    )


def test_dbt_phase_task_adapter_forwards_hot_runtime_contract():
    module = load_transform_module()
    from traffic_ingest.transform_specs import (
        GOLD_DBT_PHASE_SPECS,
        SILVER_DBT_PHASE_SPECS,
    )

    specs = {
        spec.task_id: spec
        for spec in SILVER_DBT_PHASE_SPECS + GOLD_DBT_PHASE_SPECS
        if spec.task_id in {"dbt_run_silver", "dbt_run_gold"}
    }

    with FakeDAG("adapter_contract"):
        tasks = {
            task_id: module.build_dbt_phase_task(
                spec,
                python_callable=object(),
                snapshot_task_id=module.SNAPSHOT_TASK_ID,
                retry_delay=object(),
                pin_critical_priority=module.PIN_CRITICAL_PRIORITY,
                failure_callback=object(),
            )
            for task_id, spec in specs.items()
        }

    assert {
        task_id: (
            task.kwargs["op_kwargs"]["citydata_snapshot_required"],
            task.kwargs["op_kwargs"]["silver_fence_mode"],
            task.kwargs["op_kwargs"]["selector_when_flow_missing"],
            task.kwargs["op_kwargs"]["selector_by_test_tier_when_flow_missing"],
        )
        for task_id, task in tasks.items()
    } == {
        "dbt_run_silver": (False, "write", None, None),
        "dbt_run_gold": (
            False,
            None,
            "ask_seoul_traffic_transform_gold_incident_hot_build",
            None,
        ),
    }


def test_dbt_phase_task_adapter_forwards_admin_dong_crosswalk_pin_required():
    module = load_transform_module()
    from traffic_ingest.transform_specs import DbtPhaseSpec

    spec = DbtPhaseSpec(
        "dbt_test_gold_admin_dong_pin_probe",
        "test",
        "ask_seoul_traffic_transform_gold_full_tests_without_commerce",
        admin_dong_crosswalk_pin_required=True,
    )

    with FakeDAG("adapter_admin_dong_pin_contract"):
        task = module.build_dbt_phase_task(
            spec,
            python_callable=object(),
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            retry_delay=object(),
            pin_critical_priority=module.PIN_CRITICAL_PRIORITY,
            failure_callback=object(),
        )

    assert task.kwargs["op_kwargs"]["admin_dong_crosswalk_pin_required"] is True


def test_traffic_transform_uses_only_the_silver_phase_contract():
    module = load_transform_module()
    dag = module.dag

    expected_phase_contracts = {
        "dbt_deps": ("deps", None),
        "dbt_run_silver": (
            "build",
            "ask_seoul_traffic_transform_incident_hot_build",
        ),
    }
    assert set(dag.task_ids) >= set(expected_phase_contracts)
    assert list(module.dbt_phase_tasks) == list(expected_phase_contracts)
    assert "dbt_run_gold" not in dag.task_ids
    for task_id, (dbt_command, selector) in expected_phase_contracts.items():
        op_kwargs = dag.task_dict[task_id].kwargs["op_kwargs"]
        assert (op_kwargs["dbt_command"], op_kwargs["selector"]) == (
            dbt_command,
            selector,
        )
    assert (
        dag.task_dict["dbt_run_silver"].kwargs["op_kwargs"]["silver_fence_mode"]
        == "write"
    )


def test_pin_and_compound_receipt_build_are_the_only_path_into_persisted_silver():
    module = load_transform_module()
    dag = module.dag

    # The resolver keeps the #510 latest-pin fence immediately before the
    # compound model+receipt build.
    assert dag.task_dict["dbt_deps"].downstream_task_ids == {
        "resolve_traffic_snapshot_run"
    }
    assert dag.task_dict["dbt_run_silver"].upstream_task_ids == {
        "resolve_traffic_snapshot_run",
    }
    silver_ancestors = {
        task.task_id
        for task in dag.task_dict["dbt_run_silver"].get_flat_relatives(upstream=True)
    }
    assert {
        "dbt_deps",
        "resolve_traffic_snapshot_run",
    } <= silver_ancestors


def test_traffic_dag_contains_no_model_or_test_membership_literals():
    module = load_transform_module()
    source = Path(module.__file__).read_text(encoding="utf-8")

    assert "silver_seoul_traffic_incident" not in source
    assert "gold_traffic_incident_summary" not in source
    assert "assert_silver_traffic_" not in source
    assert "assert_gold_traffic_" not in source


def test_gold_contract_test_fresh_parses_in_same_task_artifact(tmp_path, monkeypatch):
    module = load_gold_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "dbt"))
    commands = []
    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=2,
        xcom_pull=lambda task_ids: "snapshot-a",
    )

    def run(command, **_kwargs):
        commands.append(command)
        write_materialization_artifacts(command)
        return types.SimpleNamespace(
            returncode=0,
            stdout=(
                '{"unique_id":"test.traffic.gold","resource_type":"test"}\n'
                if command[1] == "ls"
                else ""
            ),
            stderr="",
        )

    monkeypatch.setattr(module.subprocess, "run", run)

    result = module.run_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_traffic_transform_gold",
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=True,
        fresh_parse=True,
        ti=ti,
        run_id="manual__a",
        params={"target": "dev"},
    )

    assert [command[1] for command in commands] == ["parse", "ls", "test"]
    assert "--no-partial-parse" in commands[0]
    assert commands[0][commands[0].index("--target") + 1] == "dev"
    assert commands[2][commands[2].index("--target") + 1] == "dev"
    assert commands[0][commands[0].index("--vars") + 1] == (
        '{"traffic_snapshot_dag_run_id": "snapshot-a"}'
    )
    assert commands[2][commands[2].index("--vars") + 1] == (
        '{"traffic_snapshot_dag_run_id": "snapshot-a"}'
    )
    parse_target = Path(commands[0][commands[0].index("--target-path") + 1])
    test_target = Path(commands[2][commands[2].index("--target-path") + 1])
    assert parse_target.name == "preflight"
    assert test_target.name == "execution"
    assert parse_target.parent == test_target.parent
    assert test_target == Path(result["run_results_path"]).parent
    assert commands[2][commands[2].index("--log-path") + 1].endswith(
        "traffic-transform/manual__a/dbt_test_gold/try2/dbt_test_gold/execution"
    )
    assert Path(result["run_results_path"]).parts[-6:] == (
        "manual__a",
        "dbt_test_gold",
        "try2",
        "dbt_test_gold",
        "execution",
        "run_results.json",
    )


def test_gold_contract_parse_failure_stops_before_test_and_records_task_artifact(
    monkeypatch,
):
    module = load_gold_transform_module()
    commands = []
    loaded_paths = []
    pushed = {}
    ti = types.SimpleNamespace(
        dag_id="traffic_incident_transform",
        task_id="dbt_test_gold",
        try_number=2,
        xcom_pull=lambda task_ids: "snapshot-a",
        xcom_push=lambda key, value: pushed.update(key=key, value=value),
    )

    def run(command, **_kwargs):
        commands.append(command)
        if command[1] == "parse":
            return types.SimpleNamespace(
                returncode=1,
                stdout="",
                stderr="Compilation Error",
            )
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", run)
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda path: loaded_paths.append(Path(path)) or [],
    )

    with pytest.raises(FakeAirflowFailException, match="model-execution-failed"):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_gold",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            fresh_parse=True,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    assert [command[1] for command in commands] == ["parse"]
    assert pushed["key"] == module.DBT_FAILURE_XCOM_KEY
    record = pushed["value"]
    assert record["dbt_artifact_path"] is None
    assert record["dbt_run_results_path"] is None
    assert record["dbt_manifest_path"] is None
    assert loaded_paths == []
    assert record["failure_classification"] == "model-execution-failed"
    assert record["traffic_snapshot_dag_run_id"] == "snapshot-a"
    assert record["run_id"] == "manual__a"
    assert record["task_id"] == "dbt_test_gold"
    assert record["try_number"] == 2
