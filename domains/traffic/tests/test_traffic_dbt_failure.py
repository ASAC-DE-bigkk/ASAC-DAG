from datetime import datetime, timezone

from domains.traffic.traffic_dbt_failure import (
    R2RecoveryRecordSink,
    build_failure_notification,
    build_recovery_record,
    classify_dbt_failure,
    load_dbt_results,
    silver_persisted_from_results,
)


def test_classifies_failed_dbt_test_as_non_retryable_contract_violation():
    failure = classify_dbt_failure(
        returncode=1,
        results=[
            {
                "unique_id": "test.ask_seoul.assert_silver_traffic_location_contract",
                "status": "fail",
                "failures": 3,
            }
        ],
        artifact_path="/artifacts/run_results.json",
    )

    assert failure.classification == "data-contract-violation"
    assert failure.retryable is False
    assert failure.failed_test_names == ["assert_silver_traffic_location_contract"]
    assert failure.failed_row_count == 3
    assert failure.artifact_path == "/artifacts/run_results.json"


def test_classifies_trino_dns_error_as_retryable_infrastructure_failure():
    failure = classify_dbt_failure(
        returncode=2,
        results=[
            {
                "unique_id": "test.ask_seoul.assert_silver_traffic_location_contract",
                "status": "error",
                "message": "TrinoConnectionError: [Errno -3] Temporary failure in name resolution",
            }
        ],
        artifact_path="/artifacts/run_results.json",
    )

    assert failure.classification == "retryable-infrastructure-error"
    assert failure.retryable is True
    assert failure.failed_test_names == []
    assert failure.failed_row_count == 0


def test_classifies_dbt_model_execution_error_as_non_retryable_failure():
    failure = classify_dbt_failure(
        returncode=1,
        results=[
            {
                "unique_id": "model.ask_seoul.silver_seoul_traffic_incident",
                "status": "error",
                "message": "Compilation Error in model silver_seoul_traffic_incident",
            }
        ],
        artifact_path="/artifacts/run_results.json",
    )

    assert failure.classification == "model-execution-failed"
    assert failure.retryable is False
    assert failure.failed_test_names == []
    assert failure.failed_row_count == 0


def test_missing_current_attempt_artifact_is_recorded_as_unknown_not_a_stale_path():
    failure = classify_dbt_failure(
        returncode=1,
        results=[],
        artifact_path=None,
        command_output="Compilation Error before dbt execution",
    )
    record = build_recovery_record(
        failure,
        traffic_snapshot_dag_run_id="snapshot-a",
        dag_id="traffic_incident_transform",
        task_id="dbt_test_gold",
        run_id="manual__1",
        try_number=1,
        silver_persisted=False,
        occurred_at=datetime(2026, 7, 14, tzinfo=timezone.utc),
    )

    assert failure.artifact_path is None
    assert record["dbt_artifact_path"] is None


def test_does_not_retry_an_unscoped_connection_error_in_model_output():
    failure = classify_dbt_failure(
        returncode=1,
        results=[
            {
                "unique_id": "model.ask_seoul.silver_seoul_traffic_incident",
                "status": "error",
                "message": "Compilation Error: connection refused while resolving a model config",
            }
        ],
        artifact_path="/artifacts/run_results.json",
    )

    assert failure.classification == "model-execution-failed"
    assert failure.retryable is False


def test_marks_silver_persisted_when_first_silver_model_completed_before_failure():
    results = [
        {
            "unique_id": "model.ask_seoul.silver_seoul_traffic_incident",
            "status": "success",
        },
        {
            "unique_id": "model.ask_seoul.silver_seoul_traffic_incident_current",
            "status": "error",
        },
    ]

    assert (
        silver_persisted_from_results(
            results,
            selected_unique_ids=(
                "model.ask_seoul.silver_seoul_traffic_incident",
                "model.ask_seoul.silver_seoul_traffic_incident_current",
            ),
            default=False,
        )
        is True
    )


def test_marks_silver_persisted_independently_of_dbt_project_namespace():
    results = [
        {
            "unique_id": "model.asac_seoul.silver_seoul_traffic_incident",
            "status": "success",
        },
        {
            "unique_id": "model.asac_seoul.silver_seoul_traffic_incident_current",
            "status": "error",
        },
    ]

    assert (
        silver_persisted_from_results(
            results,
            selected_unique_ids=(
                "model.asac_seoul.silver_seoul_traffic_incident",
                "model.asac_seoul.silver_seoul_traffic_incident_current",
            ),
            default=False,
        )
        is True
    )
    assert (
        silver_persisted_from_results(
            [
                {
                    "unique_id": "model.asac_seoul.unrelated_silver_model",
                    "status": "success",
                }
            ],
            selected_unique_ids=("model.asac_seoul.silver_seoul_traffic_incident",),
            default=False,
        )
        is False
    )


def test_marks_any_successful_selected_model_without_a_hardcoded_model_name():
    assert (
        silver_persisted_from_results(
            [
                {
                    "unique_id": "model.asac_seoul.renamed_traffic_silver",
                    "status": "success",
                }
            ],
            selected_unique_ids=("model.asac_seoul.renamed_traffic_silver",),
            default=False,
        )
        is True
    )


def test_builds_recovery_record_with_snapshot_contract_context():
    failure = classify_dbt_failure(
        returncode=1,
        results=[
            {
                "unique_id": "test.ask_seoul.assert_silver_traffic_location_contract",
                "status": "fail",
                "failures": 3,
            }
        ],
        artifact_path="/artifacts/run_results.json",
    )

    record = build_recovery_record(
        failure,
        traffic_snapshot_dag_run_id="scheduled__2026-07-13T01:35:00+00:00",
        dag_id="traffic_incident_transform",
        task_id="dbt_test_silver",
        run_id="scheduled__2026-07-13T02:12:00+00:00",
        try_number=1,
        silver_persisted=True,
        occurred_at=datetime(2026, 7, 13, tzinfo=timezone.utc),
    )

    assert (
        record["traffic_snapshot_dag_run_id"] == "scheduled__2026-07-13T01:35:00+00:00"
    )
    assert record["dbt_test_names"] == ["assert_silver_traffic_location_contract"]
    assert record["dbt_failed_row_count"] == 3
    assert record["dbt_artifact_path"] == "/artifacts/run_results.json"
    assert record["silver_persisted"] is True
    assert record["failure_classification"] == "data-contract-violation"
    assert record["recovery_action"] == "manual-approval-required"


def test_recovery_record_sink_writes_a_run_scoped_r2_document():
    failure = classify_dbt_failure(
        returncode=1,
        results=[
            {
                "unique_id": "test.ask_seoul.assert_silver_traffic_location_contract",
                "status": "fail",
                "failures": 3,
            }
        ],
        artifact_path="/artifacts/run_results.json",
    )
    record = build_recovery_record(
        failure,
        traffic_snapshot_dag_run_id="snapshot-a",
        dag_id="traffic_incident_transform",
        task_id="dbt_test_silver",
        run_id="manual__2026-07-13T02:12:00+00:00",
        try_number=1,
        silver_persisted=True,
        occurred_at=datetime(2026, 7, 13, tzinfo=timezone.utc),
    )
    written = {}

    key = R2RecoveryRecordSink(
        put_object=lambda object_key, body: written.update(key=object_key, body=body)
    ).write(record)

    assert key == written["key"]
    assert key.startswith("recovery/observed_date=2026-07-13/domain=traffic/")
    assert "manual__2026-07-13T02-12-00-00-00__dbt_test_silver__try1.json" in key
    assert b'"failure_classification": "data-contract-violation"' in written["body"]


def test_load_dbt_results_reads_only_the_results_array(tmp_path):
    artifact = tmp_path / "run_results.json"
    artifact.write_text(
        '{"metadata": {"invocation_id": "ignored"}, "results": [{"status": "fail"}]}',
        encoding="utf-8",
    )

    assert load_dbt_results(artifact) == [{"status": "fail"}]


def test_failure_notification_contains_the_operator_recovery_context():
    failure = classify_dbt_failure(
        returncode=1,
        results=[
            {
                "unique_id": "test.ask_seoul.assert_silver_traffic_location_contract",
                "status": "fail",
                "failures": 3,
            }
        ],
        artifact_path="/artifacts/run_results.json",
    )
    record = build_recovery_record(
        failure,
        traffic_snapshot_dag_run_id="snapshot-a",
        dag_id="traffic_incident_transform",
        task_id="dbt_test_silver",
        run_id="run-a",
        try_number=1,
        silver_persisted=True,
        occurred_at=datetime(2026, 7, 13, tzinfo=timezone.utc),
    )

    title, description, footer = build_failure_notification(record)

    assert "data-contract-violation" in title
    assert "snapshot-a" in description
    assert "assert_silver_traffic_location_contract" in description
    assert "3" in description
    assert "/artifacts/run_results.json" in description
    assert "yes" in description
    assert "run-a" in footer
