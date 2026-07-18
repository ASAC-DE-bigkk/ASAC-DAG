import types

from traffic_transform_test_support import (
    load_transform_module,
)
from traffic_transform_test_support import restore_airflow_modules_after_dag_import  # noqa: F401


def test_traffic_transform_subscribes_to_incident_or_flow_bronze_asset_by_default(
    monkeypatch,
):
    monkeypatch.delenv("ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE", raising=False)
    module = load_transform_module()

    assert module.TRAFFIC_TRANSFORM_CRON_KST == "12 * * * *"
    schedule = module.dag.kwargs["schedule"]
    assert {asset.uri for asset in schedule.assets} == {
        module.TRAFFIC_BRONZE_ASSET,
        module.TRAFFIC_FLOW_BRONZE_ASSET,
    }


def test_traffic_transform_trino_tasks_do_not_inflate_pool_priority_from_chain():
    module = load_transform_module()
    critical_task_ids = {
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_test_gold",
    }

    assert module.TRINO_HEAVY_POOL == "trino_traffic_heavy"
    for task_id in module.DBT_PHASE_TASK_IDS:
        task = module.dag.task_dict[task_id]
        if task_id == "dbt_deps":
            assert "pool" not in task.kwargs or task.kwargs["pool"] in (
                None,
                "default_pool",
            )
            assert task.kwargs["op_kwargs"]["threads"] is None
        else:
            assert task.kwargs["pool"] == module.TRINO_HEAVY_POOL
            assert task.kwargs["op_kwargs"]["threads"] == 2
        assert task.kwargs["weight_rule"] == "absolute"
        expected_priority = (
            module.PIN_CRITICAL_PRIORITY
            if task_id in critical_task_ids
            else 1
        )
        assert task.kwargs["priority_weight"] == expected_priority

    resolver = module.dag.task_dict[module.SNAPSHOT_TASK_ID]
    assert resolver.kwargs["pool"] == module.TRINO_HEAVY_POOL
    assert resolver.kwargs["weight_rule"] == "absolute"
    assert resolver.kwargs["priority_weight"] == module.PIN_CRITICAL_PRIORITY


def test_traffic_transform_validates_dev_runtime_before_dbt():
    module = load_transform_module()
    guard = module.dag.task_dict["validate_dev_runtime"]

    assert guard.kwargs["op_kwargs"] == {
        "domain": "traffic",
        "requested_target": "{{ params.target }}",
    }
    assert guard.downstream_task_ids == {"select_traffic_test_tier"}
    select_tier = module.dag.task_dict["select_traffic_test_tier"]
    assert select_tier.python_callable is module.select_traffic_test_tier
    assert select_tier.downstream_task_ids == {"dbt_deps"}


def test_traffic_transform_limits_target_param_to_dev_or_prod():
    module = load_transform_module()

    target_param = module.DEFAULT_PARAMS["target"]

    assert target_param.value == "dev"
    assert target_param.schema["enum"] == ["dev"]


def test_traffic_transform_publishes_dbt_run_metrics_after_terminal_test():
    module = load_transform_module()

    task = module.dag.task_dict["publish_dbt_run_metrics"]

    assert task.kwargs["trigger_rule"] == "all_done_setup_success"
    assert task.is_teardown is True
    assert task.on_failure_fail_dagrun is False
    assert module.dag.task_dict["dbt_test_gold"].downstream_task_ids == {
        "mark_traffic_test_tier"
    }
    assert module.dag.task_dict["mark_traffic_test_tier"].downstream_task_ids == {
        "publish_dbt_run_metrics"
    }


def test_traffic_transform_has_no_failure_propagating_fanout_leaf():
    module = load_transform_module()
    dag = module.dag
    metrics = dag.task_dict["publish_dbt_run_metrics"]
    assert metrics.downstream_task_ids == set()
    assert "fail_transform_if_upstream_failed" not in dag.task_ids


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
