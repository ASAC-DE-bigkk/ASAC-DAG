from __future__ import annotations

from datetime import datetime

from traffic_transform_test_support import load_flow_transform_module
from traffic_transform_test_support import restore_airflow_modules_after_dag_import  # noqa: F401


def test_flow_transform_owns_only_flow_silver_materialization():
    module = load_flow_transform_module()
    dag = module.dag

    assert dag.dag_id == "traffic_flow_transform"
    assert {asset.uri for asset in dag.kwargs["schedule"].assets} == {
        module.TRAFFIC_FLOW_BRONZE_ASSET,
        module.TRAFFIC_INCIDENT_SILVER_ASSET,
    }
    assert dag.kwargs["max_active_runs"] == 1
    assert "dbt_run_gold" not in dag.task_ids
    assert dag.task_dict["validate_dev_runtime"].downstream_task_ids == {
        "resolve_traffic_flow_silver_snapshot_run"
    }
    assert dag.task_dict[
        "resolve_traffic_flow_silver_snapshot_run"
    ].downstream_task_ids == {"dbt_deps_flow_silver"}
    assert dag.task_dict["dbt_deps_flow_silver"].downstream_task_ids == {
        "dbt_run_flow_silver"
    }
    assert dag.task_dict["dbt_run_flow_silver"].downstream_task_ids == {
        "publish_traffic_flow_silver_asset"
    }


def test_flow_transform_phase_specs_are_narrow_and_pinned():
    module = load_flow_transform_module()
    specs = {spec.task_id: spec for spec in module.FLOW_SILVER_DBT_PHASE_SPECS}

    assert list(specs) == [
        "dbt_deps_flow_silver",
        "dbt_run_flow_silver",
    ]
    assert specs["dbt_run_flow_silver"].selector == (
        "ask_seoul_traffic_transform_flow_hot_build"
    )
    assert specs["dbt_run_flow_silver"].dbt_command == "build"
    assert specs["dbt_run_flow_silver"].snapshot_required is True
    assert specs["dbt_run_flow_silver"].silver_persisted is True


def test_flow_silver_asset_is_published_only_with_exact_pair_metadata():
    module = load_flow_transform_module()

    class Accessor:
        def __init__(self):
            self.events = []

        def add(self, asset, extra):
            self.events.append((asset, extra))

    class TaskInstance:
        def xcom_pull(self, *, task_ids, key=None):
            assert task_ids == module.SNAPSHOT_TASK_ID
            if key == module.FLOW_SNAPSHOT_XCOM_KEY:
                return "flow-42"
            return "incident-42"

    accessor = Accessor()
    metadata = module.publish_traffic_flow_silver_asset(
        ti=TaskInstance(),
        outlet_events={module.TRAFFIC_FLOW_SILVER_MATERIALIZED_ALIAS: accessor},
    )

    assert set(metadata) == {
        "source_id",
        "flow_run_id",
        "flow_dag_run_id",
        "parent_incident_run_id",
        "event_at",
        "is_publishable",
        "contract",
    }
    assert metadata["source_id"] == "seoul_traffic_flow"
    assert metadata["flow_run_id"] == metadata["flow_dag_run_id"] == "flow-42"
    assert metadata["parent_incident_run_id"] == "incident-42"
    assert metadata["is_publishable"] is True
    assert metadata["contract"] == "traffic_flow_silver.v1"
    assert datetime.fromisoformat(metadata["event_at"]).tzinfo is not None
    assert accessor.events == [(module.TRAFFIC_FLOW_SILVER_ASSET_REF, metadata)]
