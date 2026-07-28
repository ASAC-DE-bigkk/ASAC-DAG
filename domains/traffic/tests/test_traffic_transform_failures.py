import types
from pathlib import Path

import pytest

from traffic_transform_test_support import (
    FakeAirflowException,
    FakeAirflowFailException,
    FakePythonOperator,
    load_flow_transform_module,
    load_gold_transform_module,
    load_transform_module,
    write_materialization_artifacts,
)
from traffic_transform_test_support import restore_airflow_modules_after_dag_import  # noqa: F401


def test_external_compaction_race_is_not_classified_as_a_generic_dbt_failure(
    monkeypatch,
):
    module = load_transform_module()
    from traffic_ingest import transform_runtime
    from traffic_ingest.silver_snapshot_fence import SilverSnapshotEvidence

    baseline = SilverSnapshotEvidence(
        snapshot_id=10,
        committed_at="2026-07-19T00:00:00+00:00",
        operation="overwrite",
        compacted_files=(),
    )
    raced = SilverSnapshotEvidence(
        snapshot_id=11,
        committed_at="2026-07-19T00:01:00+00:00",
        operation="replace",
        compacted_files=(),
    )
    evidence = iter((baseline, raced))

    class CurrentManifest:
        def latest_publishable_run_id(self):
            return "snapshot-a"

    monkeypatch.setattr(module, "build_traffic_manifest", CurrentManifest)
    monkeypatch.setattr(
        transform_runtime,
        "collect_silver_snapshot_evidence",
        lambda: next(evidence),
    )
    monkeypatch.setattr(
        transform_runtime.traffic_dbt,
        "execute_dbt_phase",
        lambda **_kwargs: types.SimpleNamespace(
            attempts=(types.SimpleNamespace(returncode=0, stdout="", stderr=""),),
            completed=types.SimpleNamespace(returncode=0, stdout="", stderr=""),
            missing_expected_artifacts=(),
            existing_run_results_path="/tmp/run_results.json",
            existing_sources_path=None,
            existing_manifest_path="/tmp/manifest.json",
            selected_unique_ids=(),
        ),
    )
    ti = types.SimpleNamespace(
        task_id="dbt_run_silver",
        try_number=1,
        dag_id="traffic_incident_transform",
        xcom_pull=lambda *, task_ids, key=None: "snapshot-a",
        xcom_push=lambda **_kwargs: pytest.fail(
            "race must not produce dbt failure XCom"
        ),
    )

    with pytest.raises(FakeAirflowFailException, match="^EXTERNAL_COMPACTION_RACE: "):
        module.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_traffic_transform_silver",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=False,
            snapshot_required=True,
            citydata_snapshot_required=False,
            silver_fence_mode="write",
            ti=ti,
            run_id="manual__race",
            params={"target": "dev"},
        )


def test_traffic_dbt_tasks_classify_failures_before_airflow_retries():
    silver = load_transform_module()
    gold = load_gold_transform_module()
    classified_task_ids = {
        silver: [
            "dbt_deps",
            "dbt_source_freshness",
            "dbt_test_traffic_incident_availability",
            "dbt_test_traffic_bronze_source_contract",
            "dbt_run_silver",
            "dbt_test_silver",
        ],
        gold: [
            "dbt_deps_gold",
            "dbt_seed_asac_axes",
            "dbt_run_common_admin_dong_dimension",
            "dbt_test_common_admin_dong_dimension",
            "dbt_test_asac_axes_seed_contract",
            "dbt_run_gold",
            "dbt_test_gold",
        ],
    }

    for module, task_ids in classified_task_ids.items():
        for task_id in task_ids:
            task = module.dag.task_dict[task_id]
            assert isinstance(task, FakePythonOperator)
            assert task.python_callable is module.run_dbt_phase
            if task_id in {
                "dbt_deps",
                "dbt_deps_gold",
                "dbt_source_freshness",
                "dbt_test_traffic_incident_availability",
                "dbt_test_traffic_bronze_source_contract",
            }:
                assert "pool" not in task.kwargs or task.kwargs["pool"] in (
                    None,
                    "default_pool",
                )
            else:
                assert task.kwargs["pool"] == module.TRINO_TRANSFORM_POOL
            assert task.kwargs["retries"] == 1
            assert task.kwargs["retry_delay"] == module.DBT_RETRY_DELAY
            assert (
                task.kwargs["on_failure_callback"] is module.record_traffic_dbt_problem
            )
            assert "dbt_command" in task.kwargs["op_kwargs"]
            assert "selector" in task.kwargs["op_kwargs"]


def test_dbt_deps_and_selected_phases_use_only_supported_isolated_paths(
    tmp_path, monkeypatch
):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "dbt"))
    commands = []
    ti = types.SimpleNamespace(
        task_id="dbt_run_silver",
        try_number=1,
        xcom_pull=lambda task_ids: (
            "snapshot-a" if task_ids == module.SNAPSHOT_TASK_ID else None
        ),
    )

    def run(command, **_kwargs):
        commands.append(command)
        write_materialization_artifacts(command)
        return types.SimpleNamespace(
            returncode=0,
            stdout=(
                '{"unique_id":"model.traffic.silver","resource_type":"model"}\n'
                if command[1] == "ls"
                else ""
            ),
            stderr="",
        )

    monkeypatch.setattr(module.subprocess, "run", run)

    for dbt_command, selector in (
        ("deps", None),
        ("run", "ask_seoul_traffic_transform_silver"),
    ):
        module.run_dbt_phase(
            dbt_command=dbt_command,
            selector=selector,
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=False,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    deps_command, ls_command, run_command = commands
    assert "--target-path" not in deps_command
    assert "--log-path" in deps_command
    assert "--target-path" in ls_command
    assert "--log-path" in ls_command
    assert "--target-path" in run_command
    assert "--log-path" in run_command
    ls_target = Path(ls_command[ls_command.index("--target-path") + 1])
    run_target = Path(run_command[run_command.index("--target-path") + 1])
    assert ls_target.name == "preflight"
    assert run_target.name == "execution"
    assert ls_target.parent == run_target.parent
    assert (
        run_command[run_command.index("--vars") + 1]
        == '{"traffic_snapshot_dag_run_id": "snapshot-a"}'
    )


def test_successful_pinned_dbt_phase_returns_citydata_snapshot_lineage(
    tmp_path, monkeypatch
):
    module = load_gold_transform_module()
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
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "dbt"))
    snapshot_id = 8738321387624398062

    def xcom_pull(*, task_ids, key=None):
        if task_ids != module.SNAPSHOT_TASK_ID:
            return None
        if key == module.FLOW_SNAPSHOT_XCOM_KEY:
            return None
        if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY:
            return snapshot_id
        return "snapshot-a"

    ti = types.SimpleNamespace(
        task_id="dbt_run_gold",
        try_number=1,
        xcom_pull=xcom_pull,
    )

    def run(command, **_kwargs):
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"model.traffic.gold","resource_type":"model"}\n',
                stderr="",
            )
        write_materialization_artifacts(command)
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", run)

    result = module.run_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_traffic_transform_gold",
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        silver_persisted=True,
        snapshot_required=True,
        citydata_snapshot_required=True,
        ti=ti,
        run_id="manual__a",
        params={"target": "dev"},
    )

    assert result["traffic_citydata_crowding_snapshot_id"] == snapshot_id


def test_dbt_contract_failure_skips_airflow_retry_and_records_pinned_snapshot(
    tmp_path, monkeypatch
):
    module = load_gold_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "dbt"))
    pushed = {}

    def xcom_pull(*, task_ids, key=None):
        if task_ids != module.SNAPSHOT_TASK_ID:
            return None
        if key == module.FLOW_SNAPSHOT_XCOM_KEY:
            return None
        if key == module.CITYDATA_CROWDING_SNAPSHOT_XCOM_KEY:
            return 8738321387624398062
        return "snapshot-a"

    ti = types.SimpleNamespace(
        task_id="dbt_test_gold",
        try_number=1,
        xcom_pull=xcom_pull,
        xcom_push=lambda key, value: pushed.update(key=key, value=value),
    )

    def fail_contract_after_preflight(command, **_kwargs):
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"test.traffic.gold","resource_type":"test"}\n',
                stderr="",
            )
        write_materialization_artifacts(command)
        return types.SimpleNamespace(returncode=1, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fail_contract_after_preflight)
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda _path: [
            {
                "unique_id": "test.asac_seoul.assert_silver_traffic_location_contract",
                "status": "fail",
                "failures": 2,
            }
        ],
    )

    with pytest.raises(FakeAirflowFailException):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_gold_full_tests",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            citydata_snapshot_required=True,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    assert pushed["key"] == module.DBT_FAILURE_XCOM_KEY
    assert pushed["value"]["traffic_snapshot_dag_run_id"] == "snapshot-a"
    assert (
        pushed["value"]["traffic_citydata_crowding_snapshot_id"] == 8738321387624398062
    )
    assert pushed["value"]["failure_classification"] == "data-contract-violation"
    assert pushed["value"]["silver_persisted"] is True
    assert pushed["value"]["dbt_run_results_path"].endswith("/run_results.json")
    assert pushed["value"]["dbt_manifest_path"].endswith("/manifest.json")


def test_flow_silver_dbt_failure_records_the_exact_flow_parent(tmp_path, monkeypatch):
    module = load_flow_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "dbt"))
    pushed = {}

    def xcom_pull(*, task_ids, key=None):
        if task_ids != module.SNAPSHOT_TASK_ID:
            return None
        if key == module.FLOW_SNAPSHOT_XCOM_KEY:
            return "flow-snapshot-a"
        return "incident-snapshot-a"

    ti = types.SimpleNamespace(
        task_id="dbt_run_flow_silver",
        try_number=1,
        xcom_pull=xcom_pull,
        xcom_push=lambda key, value: pushed.update(key=key, value=value),
    )

    def fail_after_preflight(command, **_kwargs):
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"model.asac_seoul.silver_seoul_traffic_flow","resource_type":"model"}\n',
                stderr="",
            )
        write_materialization_artifacts(command)
        return types.SimpleNamespace(returncode=1, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fail_after_preflight)
    monkeypatch.setattr(module, "load_dbt_results", lambda _path: [])

    with pytest.raises(FakeAirflowFailException):
        module.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_traffic_transform_flow_silver_model",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            snapshot_required=True,
            silver_persisted=False,
            ti=ti,
            run_id="manual__flow_failure",
            params={"target": "dev"},
        )

    assert pushed["key"] == module.DBT_FAILURE_XCOM_KEY
    assert pushed["value"]["traffic_snapshot_dag_run_id"] == "incident-snapshot-a"
    assert pushed["value"]["traffic_flow_snapshot_dag_run_id"] == "flow-snapshot-a"


def test_failed_silver_run_reports_the_model_that_already_persisted(
    tmp_path, monkeypatch
):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "dbt"))
    pushed = {}
    ti = types.SimpleNamespace(
        task_id="dbt_run_silver",
        try_number=1,
        xcom_pull=lambda task_ids: (
            "snapshot-a" if task_ids == module.SNAPSHOT_TASK_ID else None
        ),
        xcom_push=lambda key, value: pushed.update(key=key, value=value),
    )

    def fail_model_after_preflight(command, **_kwargs):
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout=(
                    '{"unique_id":"model.asac_seoul.silver_seoul_traffic_incident",'
                    '"resource_type":"model"}\n'
                    '{"unique_id":"model.asac_seoul.silver_seoul_traffic_incident_current",'
                    '"resource_type":"model"}\n'
                ),
                stderr="",
            )
        write_materialization_artifacts(command)
        return types.SimpleNamespace(returncode=1, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fail_model_after_preflight)
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda _path: [
            {
                "unique_id": "model.asac_seoul.silver_seoul_traffic_incident",
                "status": "success",
            },
            {
                "unique_id": "model.asac_seoul.silver_seoul_traffic_incident_current",
                "status": "error",
                "message": "Compilation Error",
            },
        ],
    )

    with pytest.raises(FakeAirflowFailException):
        module.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_traffic_transform_silver",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=False,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    assert pushed["value"]["silver_persisted"] is True


def test_trino_dns_failure_retries_with_the_same_pinned_snapshot(tmp_path, monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "dbt"))
    pushed = {}
    ti = types.SimpleNamespace(
        task_id="dbt_test_silver",
        try_number=1,
        xcom_pull=lambda task_ids: (
            "snapshot-a" if task_ids == module.SNAPSHOT_TASK_ID else None
        ),
        xcom_push=lambda key, value: pushed.update(key=key, value=value),
    )
    commands = []

    def fail_infrastructure_after_preflight(command, **_kwargs):
        commands.append(command)
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"test.traffic.silver","resource_type":"test"}\n',
                stderr="",
            )
        write_materialization_artifacts(command)
        return types.SimpleNamespace(returncode=2, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fail_infrastructure_after_preflight)
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda _path: [
            {
                "unique_id": "test.asac_seoul.assert_silver_traffic_location_contract",
                "status": "error",
                "message": "TrinoConnectionError: getaddrinfo temporary failure in name resolution",
            }
        ],
    )

    with pytest.raises(FakeAirflowException):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_traffic_transform_silver",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=True,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )

    assert pushed["value"]["failure_classification"] == "retryable-infrastructure-error"
    assert all(
        any("snapshot-a" in argument for argument in command) for command in commands
    )


def test_model_execution_failure_skips_airflow_retry(monkeypatch):
    module = load_transform_module()
    ti = types.SimpleNamespace(
        task_id="dbt_run_silver",
        try_number=1,
        xcom_pull=lambda task_ids: (
            "snapshot-a" if task_ids == module.SNAPSHOT_TASK_ID else None
        ),
        xcom_push=lambda **_kwargs: None,
    )

    def fail_execution_after_preflight(command, **_kwargs):
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"model.traffic.silver","resource_type":"model"}\n',
                stderr="",
            )
        return types.SimpleNamespace(returncode=1, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fail_execution_after_preflight)
    monkeypatch.setattr(
        module,
        "load_dbt_results",
        lambda _path: [
            {
                "unique_id": "model.asac_seoul.silver_seoul_traffic_incident",
                "status": "error",
                "message": "Compilation Error",
            }
        ],
    )

    with pytest.raises(FakeAirflowFailException):
        module.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_traffic_transform_silver",
            snapshot_task_id=module.SNAPSHOT_TASK_ID,
            silver_persisted=False,
            ti=ti,
            run_id="manual__a",
            params={"target": "dev"},
        )


def test_final_dbt_failure_callback_persists_recovery_record_and_notifies(monkeypatch):
    module = load_transform_module()
    record = {
        "dag_id": "traffic_incident_transform",
        "task_id": "dbt_test_silver",
        "run_id": "manual__a",
        "failure_classification": "data-contract-violation",
        "traffic_snapshot_dag_run_id": "snapshot-a",
        "traffic_flow_snapshot_dag_run_id": "flow-snapshot-a",
        "traffic_citydata_crowding_snapshot_id": 8738321387624398062,
        "dbt_artifact_path": "/tmp/run_results.json",
        "dbt_test_names": ["assert_silver_traffic_location_contract"],
        "dbt_failed_row_count": 2,
        "silver_persisted": True,
        "recovery_action": "manual-approval-required",
    }
    ti = types.SimpleNamespace(
        task_id="dbt_test_silver",
        dag_id="traffic_incident_transform",
        try_number=1,
        xcom_pull=lambda **_kwargs: record,
    )
    written = {}
    notified = {}
    monkeypatch.setattr(
        module,
        "R2ErrorSink",
        lambda: types.SimpleNamespace(
            write=lambda problem: written.update(problem=problem)
        ),
    )
    monkeypatch.setattr(
        module,
        "R2RecoveryRecordSink",
        lambda: types.SimpleNamespace(
            write=lambda value: written.update(recovery=value)
        ),
    )
    monkeypatch.setattr(module, "first_notice_for_run", lambda *_args: True)
    monkeypatch.setattr(
        module,
        "send_embed",
        lambda title, description, **kwargs: notified.update(
            title=title, description=description, kwargs=kwargs
        ),
    )

    module.record_traffic_dbt_problem(
        {
            "ti": ti,
            "dag": types.SimpleNamespace(dag_id="traffic_incident_transform"),
            "run_id": "manual__a",
            "exception": FakeAirflowFailException("contract"),
        }
    )

    assert written["recovery"] == record
    problem_document = written["problem"].to_dict()
    assert problem_document["traffic_snapshot_dag_run_id"] == "snapshot-a"
    assert problem_document["traffic_flow_snapshot_dag_run_id"] == "flow-snapshot-a"
    assert (
        problem_document["traffic_citydata_crowding_snapshot_id"] == 8738321387624398062
    )
    assert problem_document["dbt_test_names"] == [
        "assert_silver_traffic_location_contract"
    ]
    assert problem_document["dbt_failed_row_count"] == 2
    assert problem_document["dbt_artifact_path"] == "/tmp/run_results.json"
    assert problem_document["silver_persisted"] is True
    assert problem_document["failure_classification"] == "data-contract-violation"
    assert "assert_silver_traffic_location_contract" in notified["description"]
    assert "/tmp/run_results.json" in notified["description"]


def test_final_dbt_failure_callback_logs_best_effort_failures(monkeypatch):
    module = load_transform_module()
    record = {
        "dag_id": "traffic_incident_transform",
        "task_id": "dbt_test_silver",
        "run_id": "manual__a",
        "failure_classification": "data-contract-violation",
    }
    ti = types.SimpleNamespace(
        task_id="dbt_test_silver",
        xcom_pull=lambda **_kwargs: record,
    )
    warnings = []

    def fail(error):
        def raise_error(*_args, **_kwargs):
            raise error

        return raise_error

    monkeypatch.setattr(
        module,
        "problem_from_airflow_context",
        fail(ValueError("sensitive detail")),
    )
    monkeypatch.setattr(
        module,
        "R2RecoveryRecordSink",
        lambda: types.SimpleNamespace(write=fail(OSError("sensitive detail"))),
    )
    monkeypatch.setattr(module, "first_notice_for_run", lambda *_args: True)
    monkeypatch.setattr(
        module,
        "send_embed",
        fail(RuntimeError("sensitive detail")),
    )
    monkeypatch.setattr(
        module.LOGGER,
        "warning",
        lambda message, *args: warnings.append(message % args),
    )

    module.record_traffic_dbt_problem({"ti": ti})

    assert warnings == [
        "traffic Problem record write failed: ValueError",
        "traffic recovery record write failed: OSError",
        "traffic dbt failure notification failed: RuntimeError",
    ]
    assert all("sensitive detail" not in warning for warning in warnings)
