import importlib.util
from pathlib import Path


def load_transform_module():
    module_path = Path(__file__).resolve().parents[1] / "weather_vilage_fcst_transform.py"
    spec = importlib.util.spec_from_file_location("weather_vilage_fcst_transform_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_weather_transform_runs_place_mapping_seed_and_mart():
    module = load_transform_module()
    dag = module.dag

    expected_task_order = [
        "dbt_seed_place_mapping",
        "dbt_test_place_mapping_seed",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
        "dbt_run_place_mart",
        "dbt_test_place_mart",
    ]

    assert set(expected_task_order) <= set(dag.task_ids)
    for upstream_task_id, downstream_task_id in zip(expected_task_order, expected_task_order[1:]):
        assert dag.task_dict[upstream_task_id].downstream_task_ids == {downstream_task_id}

    task_commands = {
        task_id: dag.task_dict[task_id].bash_command
        for task_id in expected_task_order
    }

    assert "seed --select weather_place_grid_mapping" in task_commands["dbt_seed_place_mapping"]
    assert "weather_place_grid_mapping" in task_commands["dbt_test_place_mapping_seed"]
    assert "assert_weather_place_grid_mapping_major_aliases" in task_commands["dbt_test_place_mapping_seed"]
    assert (
        "assert_weather_place_grid_mapping_within_collected_grid_scope"
        in task_commands["dbt_test_place_mapping_seed"]
    )
    assert "run --select dim_weather_place gold_weather_forecast_by_place" in task_commands["dbt_run_place_mart"]
    assert "dim_weather_place" in task_commands["dbt_test_place_mart"]
    assert "gold_weather_forecast_by_place" in task_commands["dbt_test_place_mart"]
    assert "assert_gold_weather_forecast_by_place_grain_unique" in task_commands["dbt_test_place_mart"]
    assert "assert_gold_weather_forecast_by_place_major_coverage" in task_commands["dbt_test_place_mart"]
