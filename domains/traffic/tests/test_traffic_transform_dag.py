import types

import pytest

from traffic_transform_test_support import (
    FakeAirflowFailException,
    FakeTriggerRule,
    load_transform_module,
)
from traffic_transform_test_support import restore_airflow_modules_after_dag_import  # noqa: F401


def test_traffic_transform_defaults_to_hourly_cron_after_bronze_completion_window(
    monkeypatch,
):
    monkeypatch.delenv("ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE", raising=False)
    module = load_transform_module()

    assert module.TRAFFIC_TRANSFORM_CRON_KST == "12 * * * *"
    assert module.dag.kwargs["schedule"] == module.TRAFFIC_TRANSFORM_CRON_KST


def test_traffic_transform_validates_dev_runtime_before_dbt():
    module = load_transform_module()
    guard = module.dag.task_dict["validate_dev_runtime"]

    assert guard.kwargs["op_kwargs"] == {
        "domain": "traffic",
        "requested_target": "{{ params.target }}",
    }
    assert guard.downstream_task_ids == {
        "resolve_traffic_snapshot_run",
        "fail_transform_if_upstream_failed",
    }


def test_traffic_transform_limits_target_param_to_dev_or_prod():
    module = load_transform_module()

    target_param = module.DEFAULT_PARAMS["target"]

    assert target_param.value == "dev"
    assert target_param.schema["enum"] == ["dev"]


def test_traffic_transform_publishes_dbt_run_metrics_after_terminal_test():
    module = load_transform_module()

    task = module.dag.task_dict["publish_dbt_run_metrics"]

    assert task.kwargs["trigger_rule"] == "all_done"
    assert module.dag.task_dict["dbt_test_gold"].downstream_task_ids == {
        "publish_dbt_run_metrics",
        "fail_transform_if_upstream_failed",
    }


def test_traffic_transform_has_independent_failure_propagating_leaf():
    module = load_transform_module()
    dag = module.dag
    transform_task_ids = {
        "validate_dev_runtime",
        "resolve_traffic_snapshot_run",
        "dbt_deps",
        "dbt_source_freshness",
        "dbt_test_traffic_incident_availability",
        "dbt_test_traffic_bronze_source_contract",
        "dbt_seed_asac_axes",
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
        "dbt_test_asac_axes_seed_contract",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
    }

    metrics = dag.task_dict["publish_dbt_run_metrics"]
    watcher = dag.task_dict["fail_transform_if_upstream_failed"]

    assert metrics.kwargs["trigger_rule"] == FakeTriggerRule.ALL_DONE
    assert watcher.kwargs["trigger_rule"] == FakeTriggerRule.ONE_FAILED
    assert watcher.kwargs["retries"] == 0
    assert metrics.downstream_task_ids == set()
    assert watcher.downstream_task_ids == set()
    assert watcher.upstream_task_ids == transform_task_ids


def test_traffic_failure_propagation_callable_always_fails():
    module = load_transform_module()

    with pytest.raises(
        FakeAirflowFailException, match="traffic transform upstream task failed"
    ):
        module.fail_transform_if_upstream_failed()


def test_traffic_metrics_use_latest_current_run_dbt_artifact_path():
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


def test_traffic_metrics_use_earlier_success_artifact_when_later_phases_have_none():
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


def test_traffic_metrics_use_earlier_failure_artifact():
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


def test_traffic_metrics_prefer_most_advanced_current_run_artifact():
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


def test_traffic_metrics_resolver_never_falls_back_to_shared_target(tmp_path):
    module = load_transform_module()
    stale_shared_result = tmp_path / "target" / "run_results.json"
    stale_shared_result.parent.mkdir()
    stale_shared_result.write_text("{}", encoding="utf-8")
    ti = types.SimpleNamespace(xcom_pull=lambda **_kwargs: None)

    assert module._current_run_results_path(ti=ti) is None


def test_traffic_metrics_skip_stale_shared_target_without_current_run_artifact(
    tmp_path, monkeypatch
):
    module = load_transform_module()
    stale_shared_result = tmp_path / "target" / "run_results.json"
    stale_shared_result.parent.mkdir()
    stale_shared_result.write_text("{}", encoding="utf-8")
    published_paths = []
    monkeypatch.setattr(
        module,
        "dump_dbt_run_results",
        lambda path, **_kwargs: published_paths.append(path) or [{}],
    )
    ti = types.SimpleNamespace(xcom_pull=lambda **_kwargs: None)

    assert module.publish_dbt_run_metrics(ti=ti, params={"target": "dev"}) == {
        "rows": 0,
        "skipped": True,
    }
    assert published_paths == []


def test_traffic_publish_dbt_run_metrics_forwards_domain_and_target(
    tmp_path, monkeypatch
):
    module = load_transform_module()
    run_results = tmp_path / "run_results.json"
    run_results.write_text("{}", encoding="utf-8")
    captured = {}

    def fake_dump(path, *, domain, target):
        captured.update(path=path, domain=domain, target=target)
        return [{}]

    monkeypatch.setattr(module, "dump_dbt_run_results", fake_dump)

    assert module.publish_dbt_run_metrics(
        run_results_path=str(run_results), params={"target": "dev"}
    ) == {"rows": 1, "skipped": False}
    assert captured == {"path": str(run_results), "domain": "traffic", "target": "dev"}
