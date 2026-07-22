from domains.weather.weather_dbt_failure import classify_weather_dbt_failure


def test_classifies_shared_axis_rebuild_race_as_retryable():
    failure = classify_weather_dbt_failure(
        dbt_command="test",
        returncode=1,
        artifact_path=None,
        missing_expected_artifacts=(),
        command_output=(
            "Database Error in test "
            "assert_gold_weather_forecast_by_admin_dong_latest_grid_record\n"
            "TrinoUserError(type=USER_ERROR, name=INVALID_VIEW, "
            "message=\"line 27:10: Failed analyzing stored view "
            "'iceberg_dev.common.dim_admin_dong': line 28:6: Table "
            "'iceberg_dev.common.seoul_admin_dong_crosswalk' does not exist\", "
            "query_id=20260722_045215_06725_32p5n)"
        ),
    )

    assert failure.classification == "retryable-shared-axis-rebuild-race"
    assert failure.retryable is True


def test_classifies_genuine_data_contract_violation_as_non_retryable():
    failure = classify_weather_dbt_failure(
        dbt_command="test",
        returncode=1,
        artifact_path=None,
        missing_expected_artifacts=(),
        command_output=(
            "Failure in test "
            "assert_gold_weather_forecast_by_admin_dong_grain_unique"
        ),
    )

    assert failure.classification == "data-contract-violation"
    assert failure.retryable is False


def test_classifies_trino_dns_error_as_retryable_infrastructure_error():
    failure = classify_weather_dbt_failure(
        dbt_command="run",
        returncode=2,
        artifact_path=None,
        missing_expected_artifacts=(),
        command_output=(
            "TrinoConnectionError: [Errno -3] Temporary failure in name resolution"
        ),
    )

    assert failure.classification == "retryable-infrastructure-error"
    assert failure.retryable is True


def test_classifies_missing_artifacts_as_non_retryable_artifact_violation():
    failure = classify_weather_dbt_failure(
        dbt_command="test",
        returncode=1,
        artifact_path=None,
        missing_expected_artifacts=("run_results.json",),
        command_output="",
    )

    assert failure.classification == "artifact-contract-violation"
    assert failure.retryable is False
