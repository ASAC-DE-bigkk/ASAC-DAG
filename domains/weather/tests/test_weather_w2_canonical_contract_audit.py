import json
import types

from weather_transform_test_support import (
    FakePythonOperator,
    FakeTaskInstance,
    load_transform_module,
)
from weather_transform_test_support import (
    restore_airflow_modules_after_transform_import,  # noqa: F401
)


def load_audit_module():
    return load_transform_module(
        "weather_w2_canonical_contract_audit.py",
        "weather_w2_canonical_contract_audit_under_test",
    )


def test_full_contract_audit_is_daily_read_only_and_fail_closed():
    module = load_audit_module()

    assert module.DAG_ID == "weather_w2_canonical_contract_audit"
    assert module.DBT_PIPELINE == "weather-w2-canonical-contract-audit"
    assert module.dag.kwargs["schedule"] == "15 9 * * *"
    assert module.dag.kwargs["max_active_runs"] == 1
    assert module.dag.kwargs["is_paused_upon_creation"] is True
    assert module.dag.kwargs["catchup"] is False
    assert module.DEFAULT_PARAMS["target"].schema["enum"] == ["dev"]

    expected_order = [
        "validate_dev_runtime",
        module.CROSSWALK_SNAPSHOT_TASK_ID,
        "dbt_deps",
        "dbt_test_w2_canonical_full_contracts",
        "publish_dbt_run_metrics",
    ]
    assert list(module.dag.task_dict) == expected_order
    assert "dbt_run_w2_canonical_models" not in module.dag.task_dict

    for upstream, downstream in zip(expected_order, expected_order[1:]):
        assert module.dag.task_dict[upstream].downstream_task_ids == {downstream}

    deps = module.dag.task_dict["dbt_deps"]
    assert isinstance(deps, FakePythonOperator)
    assert "pool" not in deps.kwargs

    audit = module.dag.task_dict["dbt_test_w2_canonical_full_contracts"]
    assert isinstance(audit, FakePythonOperator)
    assert audit.kwargs["pool"] == "trino_weather_heavy"
    assert audit.kwargs["op_kwargs"]["selector"] == (
        "ask_seoul_weather_w2_canonical_full_contracts"
    )
    assert audit.kwargs["op_kwargs"]["threads"] == 1


def test_full_contract_audit_passes_revision_and_crosswalk_pin(monkeypatch):
    module = load_audit_module()
    captured = {}
    run_results_path = "/tmp/weather-w2-audit/run_results.json"

    def fake_execute_dbt_phase(**kwargs):
        captured.update(kwargs)
        return types.SimpleNamespace(
            existing_run_results_path=run_results_path,
            existing_sources_path=None,
            existing_manifest_path="/tmp/weather-w2-audit/manifest.json",
            selected_unique_ids=("test.asac_seoul.full_contract",),
            missing_expected_artifacts=False,
            attempts=(),
            completed=types.SimpleNamespace(returncode=0),
        )

    monkeypatch.setattr(module.weather_dbt, "execute_dbt_phase", fake_execute_dbt_phase)
    task_instance = FakeTaskInstance(
        task_id="dbt_test_w2_canonical_full_contracts",
        pulls={
            (
                module.CROSSWALK_SNAPSHOT_TASK_ID,
                None,
            ): 8738321387624398062
        },
    )

    result = module.run_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_weather_w2_canonical_full_contracts",
        include_project_vars=True,
        crosswalk_snapshot_task_id=module.CROSSWALK_SNAPSHOT_TASK_ID,
        threads=1,
        ti=task_instance,
        run_id="scheduled__2026-07-28T00:15:00+00:00",
        params={"target": "dev"},
    )

    assert captured["pipeline"] == "weather-w2-canonical-contract-audit"
    assert captured["selector"] == "ask_seoul_weather_w2_canonical_full_contracts"
    assert captured["target"] == "dev"
    assert captured["threads"] == 1
    assert json.loads(captured["variables"]) == {
        "weather_w2_canonical_revision_date": "2025-04-01",
        "admin_dong_crosswalk_pin_snapshot_id": 8738321387624398062,
    }
    assert result["run_results_path"] == run_results_path


def test_full_contract_audit_rejects_invalid_crosswalk_pin():
    module = load_audit_module()
    task_instance = FakeTaskInstance(
        task_id="dbt_test_w2_canonical_full_contracts",
        pulls={(module.CROSSWALK_SNAPSHOT_TASK_ID, None): None},
    )

    try:
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_weather_w2_canonical_full_contracts",
            include_project_vars=True,
            crosswalk_snapshot_task_id=module.CROSSWALK_SNAPSHOT_TASK_ID,
            threads=1,
            ti=task_instance,
            run_id="scheduled__2026-07-28T00:15:00+00:00",
            params={"target": "dev"},
        )
    except module.AirflowFailException as exc:
        assert "crosswalk snapshot" in str(exc)
    else:
        raise AssertionError("invalid crosswalk pin must fail closed")
