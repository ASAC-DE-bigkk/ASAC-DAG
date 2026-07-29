from traffic_transform_test_support import (
    load_flow_transform_module,
    load_transform_module,
)
from traffic_transform_test_support import restore_airflow_modules_after_dag_import  # noqa: F401


def test_incident_hot_path_keeps_pin_order_with_one_dbt_build():
    module = load_transform_module()
    specs = {spec.task_id: spec for spec in module.SILVER_DBT_PHASE_SPECS}

    assert list(specs) == ["dbt_deps", "dbt_run_silver"]
    build = specs["dbt_run_silver"]
    assert build.dbt_command == "build"
    assert build.selector == "ask_seoul_traffic_transform_incident_hot_build"
    assert build.snapshot_required is True
    assert build.pin_critical is True
    assert build.silver_fence_mode == "write"

    dag = module.dag
    assert dag.task_dict["dbt_deps"].downstream_task_ids == {
        module.SNAPSHOT_TASK_ID
    }
    assert dag.task_dict[module.SNAPSHOT_TASK_ID].downstream_task_ids == {
        "dbt_run_silver"
    }


def test_flow_hot_path_combines_model_and_contract_in_one_pinned_build():
    module = load_flow_transform_module()
    specs = {spec.task_id: spec for spec in module.FLOW_SILVER_DBT_PHASE_SPECS}

    assert list(specs) == ["dbt_deps_flow_silver", "dbt_run_flow_silver"]
    build = specs["dbt_run_flow_silver"]
    assert build.dbt_command == "build"
    assert build.selector == "ask_seoul_traffic_transform_flow_hot_build"
    assert build.snapshot_required is True
    assert build.pin_critical is True
    assert build.silver_persisted is True

    dag = module.dag
    assert dag.task_dict["dbt_run_flow_silver"].downstream_task_ids == {
        "publish_traffic_flow_silver_asset"
    }
    assert "dbt_test_flow_silver" not in dag.task_ids
