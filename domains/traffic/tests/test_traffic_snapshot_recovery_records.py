import types
from pathlib import Path

from traffic_snapshot_recovery_test_support import load_recovery_module
from traffic_snapshot_recovery_test_support import (
    restore_airflow_modules_after_dag_import,  # noqa: F401
)


def test_recovery_completion_writes_and_notifies_snapshot_evidence(monkeypatch):
    module = load_recovery_module()

    selected_by_task = {
        "dbt_run_recovery_silver": ["model.asac_seoul.recovery_silver"],
        "dbt_run_recovery_metadata": ["model.asac_seoul.recovery_metadata"],
        "dbt_run_recovery_gold": ["model.asac_seoul.recovery_gold"],
    }
    artifacts = {
        task_id: {
            "status": "success",
            "run_results_path": f"/artifacts/{task_id}/run_results.json",
            "selected_unique_ids": selected_by_task.get(task_id, []),
        }
        for task_id in module.RECOVERY_DBT_TASK_IDS
    }

    class TaskInstance:
        dag_id = "traffic_snapshot_recovery"
        task_id = "record_recovery_completion"
        try_number = 1

        def xcom_pull(self, *, task_ids, key=None):
            assert key is None
            if task_ids == "validate_publishable_snapshot":
                return "historical-run"
            return artifacts[task_ids]

    written = {}
    notified = {}
    monkeypatch.setattr(
        module,
        "R2RecoveryRecordSink",
        lambda: types.SimpleNamespace(
            write=lambda record: written.update(record=record) or "recovery/key.json"
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

    record = module.record_recovery_completion(
        ti=TaskInstance(),
        run_id="manual__recovery",
        dag=types.SimpleNamespace(dag_id="traffic_snapshot_recovery"),
    )

    assert written["record"] == record
    assert record["recovery_purpose"] == "historical-snapshot-validation"
    assert record["recovery_status"] == "success"
    assert record["traffic_snapshot_dag_run_id"] == "historical-run"
    assert record["recovery_relations"] == sorted(
        relation.rsplit(".", 1)[-1]
        for relations in selected_by_task.values()
        for relation in relations
    )
    assert record["dbt_task_statuses"] == {
        task_id: "success" for task_id in module.RECOVERY_DBT_TASK_IDS
    }
    assert record["dbt_artifacts"] == {
        task_id: artifacts[task_id]["run_results_path"]
        for task_id in module.RECOVERY_ARTIFACT_TASK_IDS
    }
    assert "historical-run" in notified["description"]
    assert "recovery_gold" in notified["description"]


def test_recovery_dbt_failure_callback_writes_and_notifies_the_classified_record(
    monkeypatch,
):
    module = load_recovery_module()
    failure_record = {
        "traffic_snapshot_dag_run_id": "historical-run",
        "failure_classification": "data-contract-violation",
        "dbt_test_names": ["assert_recovery_silver_traffic_snapshot_matches_bronze"],
        "dbt_failed_row_count": 1,
        "dbt_artifact_path": "/artifacts/test/run_results.json",
        "silver_persisted": True,
        "recovery_action": "manual-approval-required",
        "dag_id": "traffic_snapshot_recovery",
        "run_id": "manual__recovery",
        "task_id": "dbt_test_recovery_silver",
    }

    class TaskInstance:
        task_id = "dbt_test_recovery_silver"

        def xcom_pull(self, *, task_ids, key):
            assert task_ids == self.task_id
            assert key == module.DBT_FAILURE_XCOM_KEY
            return failure_record

    written = {}
    notified = {}
    monkeypatch.setattr(
        module,
        "R2RecoveryRecordSink",
        lambda: types.SimpleNamespace(
            write=lambda record: written.update(record=record)
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

    module.record_recovery_dbt_problem({"ti": TaskInstance()})

    assert written["record"] == failure_record
    assert "historical-run" in notified["description"]
    assert notified["kwargs"]["domain"] == "traffic"


def test_recovery_dbt_failure_callback_logs_best_effort_failures(monkeypatch):
    module = load_recovery_module()
    failure_record = {
        "dag_id": "traffic_snapshot_recovery",
        "run_id": "manual__recovery",
        "task_id": "dbt_test_recovery_silver",
    }
    ti = types.SimpleNamespace(
        task_id="dbt_test_recovery_silver",
        xcom_pull=lambda **_kwargs: failure_record,
    )
    warnings = []

    def fail(error):
        def raise_error(*_args, **_kwargs):
            raise error

        return raise_error

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

    module.record_recovery_dbt_problem({"ti": ti})

    assert warnings == [
        "traffic recovery record write failed: OSError",
        "traffic recovery failure notification failed: RuntimeError",
    ]
    assert all("sensitive detail" not in warning for warning in warnings)


def test_readme_documents_the_manual_recovery_boundary():
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text(
        encoding="utf-8"
    )

    assert "traffic_snapshot_recovery" in readme
    assert "snapshot_dag_run_id" in readme
    assert "recovery_silver_seoul_traffic_incident" in readme
    assert "canonical" in readme


def test_recovery_completion_keeps_deps_status_without_a_run_results_artifact(
    monkeypatch,
):
    module = load_recovery_module()
    results = {
        task_id: {
            "status": "success",
            "run_results_path": f"/artifacts/{task_id}/run_results.json",
        }
        for task_id in module.RECOVERY_DBT_TASK_IDS
    }
    results["dbt_deps"] = {"status": "success", "run_results_path": None}

    class TaskInstance:
        dag_id = "traffic_snapshot_recovery"
        task_id = "record_recovery_completion"
        try_number = 1

        def xcom_pull(self, *, task_ids, key=None):
            if task_ids == module.SNAPSHOT_TASK_ID:
                return "historical-run"
            return results[task_ids]

    monkeypatch.setattr(
        module,
        "R2RecoveryRecordSink",
        lambda: types.SimpleNamespace(write=lambda _record: None),
    )
    monkeypatch.setattr(module, "first_notice_for_run", lambda *_args: False)

    record = module.record_recovery_completion(
        ti=TaskInstance(), run_id="manual__recovery"
    )

    assert record["dbt_task_statuses"]["dbt_deps"] == "success"
    assert "dbt_deps" not in record["dbt_artifacts"]
