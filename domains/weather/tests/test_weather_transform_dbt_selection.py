import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest


_AIRFLOW_MODULE_NAMES = (
    "airflow",
    "airflow.exceptions",
    "airflow.models",
    "airflow.models.param",
    "airflow.providers",
    "airflow.providers.standard",
    "airflow.providers.standard.operators",
    "airflow.providers.standard.operators.python",
    "airflow.sdk",
)


@pytest.fixture(autouse=True)
def restore_airflow_modules_after_transform_import():
    originals = {name: sys.modules.get(name) for name in _AIRFLOW_MODULE_NAMES}
    yield
    for name, module in originals.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


class FakeDAG:
    _stack = []

    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.task_dict = {}

    def __enter__(self):
        self._stack.append(self)
        return self

    def __exit__(self, *_exc_info):
        self._stack.pop()

    @property
    def task_ids(self):
        return list(self.task_dict)

    def add_task(self, task):
        self.task_dict[task.task_id] = task


class FakePythonOperator:
    def __init__(self, task_id, python_callable, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.kwargs = kwargs
        self.downstream_task_ids = set()
        self.upstream_task_ids = set()
        FakeDAG._stack[-1].add_task(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)
        return other

    def as_teardown(self, setups=None, on_failure_fail_dagrun=False):
        self.is_teardown = True
        self.on_failure_fail_dagrun = on_failure_fail_dagrun
        self.kwargs["trigger_rule"] = "all_done_setup_success"
        return self


class FakeParam:
    def __init__(self, default=None, **schema):
        self.value = default
        self.schema = schema


class FakeAsset:
    def __init__(self, uri):
        self.uri = uri

    def __eq__(self, other):
        return isinstance(other, FakeAsset) and self.uri == other.uri


class FakeAirflowException(Exception):
    pass


class FakeTaskInstance:
    def __init__(self, *, task_id="dbt_run_silver", try_number=1, pulls=None):
        self.task_id = task_id
        self.try_number = try_number
        self.pulls = pulls or {}
        self.pushes = []

    def xcom_pull(self, *, task_ids, key=None):
        return self.pulls.get((task_ids, key))

    def xcom_push(self, *, key, value):
        self.pushes.append((key, value))


def install_airflow_fakes():
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG

    airflow_exceptions = types.ModuleType("airflow.exceptions")
    airflow_exceptions.AirflowException = FakeAirflowException

    airflow_models = types.ModuleType("airflow.models")
    airflow_models_param = types.ModuleType("airflow.models.param")
    airflow_models_param.Param = FakeParam

    airflow_providers = types.ModuleType("airflow.providers")
    airflow_standard = types.ModuleType("airflow.providers.standard")
    airflow_operators = types.ModuleType("airflow.providers.standard.operators")
    airflow_python = types.ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = FakePythonOperator
    airflow_sdk = types.ModuleType("airflow.sdk")
    airflow_sdk.Asset = FakeAsset

    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.exceptions": airflow_exceptions,
            "airflow.models": airflow_models,
            "airflow.models.param": airflow_models_param,
            "airflow.providers": airflow_providers,
            "airflow.providers.standard": airflow_standard,
            "airflow.providers.standard.operators": airflow_operators,
            "airflow.providers.standard.operators.python": airflow_python,
            "airflow.sdk": airflow_sdk,
        }
    )


def load_transform_module():
    install_airflow_fakes()
    module_path = Path(__file__).resolve().parents[1] / "weather_vilage_fcst_transform.py"
    spec = importlib.util.spec_from_file_location("weather_vilage_fcst_transform_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


EXPECTED_DBT_PHASES = (
    ("dbt_deps", "deps", False),
    ("dbt_source_freshness", "source freshness", True),
    ("dbt_seed_asac_axes", "seed --select asac_axes", True),
    (
        "dbt_run_common_admin_dong_dimension",
        "run --select asac_axes.dim_admin_dong",
        True,
    ),
    (
        "dbt_test_common_admin_dong_dimension",
        "test --select asac_axes.dim_admin_dong",
        True,
    ),
    ("dbt_seed_place_mapping", "seed --select weather_place_grid_mapping", True),
    (
        "dbt_test_place_mapping_seed",
        "test --select weather_place_grid_mapping "
        "assert_weather_place_grid_mapping_major_aliases "
        "assert_weather_place_grid_mapping_within_collected_grid_scope "
        "assert_weather_place_grid_mapping_alias_unique_except_allowed",
        True,
    ),
    ("dbt_run_silver", "run --select silver_kma_vilage_fcst", True),
    (
        "dbt_test_silver",
        "test --select silver_kma_vilage_fcst "
        "assert_silver_kma_vilage_fcst_grain_unique "
        "assert_silver_kma_vilage_fcst_grid_coverage "
        "assert_silver_kma_uses_publishable_runs "
        "assert_silver_kma_event_at_matches_forecast_at "
        "--exclude assert_gold_weather_counts_match_silver",
        True,
    ),
    ("dbt_run_gold", "run --select gold_weather_forecast_summary", True),
    (
        "dbt_test_gold",
        "test --select gold_weather_forecast_summary "
        "assert_gold_weather_counts_match_silver "
        "assert_gold_weather_row_counts_positive",
        True,
    ),
    (
        "dbt_run_place_mart",
        "run --select dim_weather_place "
        "silver_weather_forecast_by_admin_dong "
        "gold_weather_forecast_by_place",
        True,
    ),
    (
        "dbt_test_place_mart",
        "test --select dim_weather_place "
        "silver_weather_forecast_by_admin_dong "
        "gold_weather_forecast_by_place "
        "assert_silver_weather_admin_dong_grain_unique "
        "assert_silver_weather_admin_axis_consistent "
        "assert_silver_weather_admin_event_at_matches_forecast_at "
        "assert_gold_weather_forecast_by_place_grain_unique "
        "assert_gold_weather_forecast_by_place_major_coverage "
        "assert_gold_weather_forecast_by_place_admin_axis_consistent "
        "assert_dim_weather_place_admin_axis_consistent "
        "assert_gold_weather_forecast_by_place_event_at_matches_forecast_at "
        "assert_gold_weather_forecast_by_place_latest_silver_record",
        True,
    ),
)


def test_weather_artifact_path_sanitizes_run_and_task_and_keeps_try_number():
    module = load_transform_module()

    assert module._artifact_path(
        run_id="run/id:with space",
        task_id="dbt/test silver",
        try_number=3,
    ) == (
        "/opt/airflow/dbt/domains/weather/target/weather-transform/"
        "run-id-with-space/dbt-test-silver/try3/run_results.json"
    )
    assert module._artifact_path(
        run_id="run/한é",
        task_id="dbt/테",
        try_number=3,
    ).endswith(
        "weather-transform/run---/dbt--/try3/run_results.json"
    )


def test_weather_dbt_deps_omits_project_vars_and_target_path(tmp_path, monkeypatch, capsys):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    captured = {}

    def fake_run(command, **kwargs):
        captured.update(command=command, kwargs=kwargs)
        return types.SimpleNamespace(returncode=0, stdout="deps stdout\n", stderr="deps stderr\n")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    ti = FakeTaskInstance(task_id="dbt_deps", try_number=1)

    assert module.run_dbt_phase(
        dbt_args="deps",
        include_project_vars=False,
        ti=ti,
        run_id="manual__1",
        params={"target": "dev"},
    ) == {"status": "success", "artifact_path": None}

    command = captured["command"]
    assert command == [module.DBT_BIN, "deps", "--target", "dev", "--no-use-colors"]
    assert "--vars" not in command
    assert "--target-path" not in command
    assert captured["kwargs"]["cwd"] == module.DBT_PROJECT
    assert captured["kwargs"]["check"] is False
    assert captured["kwargs"]["capture_output"] is True
    assert captured["kwargs"]["text"] is True
    assert captured["kwargs"]["env"]["DBT_PROFILES_DIR"] == module.DBT_PROJECT
    assert captured["kwargs"]["env"]["DBT_PROJECT_DIR"] == module.DBT_PROJECT
    assert ti.pushes == [(module.WEATHER_DBT_ARTIFACT_XCOM_KEY, None)]
    streams = capsys.readouterr()
    assert "deps stdout" in streams.out
    assert "deps stderr" in streams.err


def test_weather_dbt_model_command_writes_isolated_artifact(tmp_path, monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    captured = {}

    def fake_run(command, **kwargs):
        captured.update(command=command, kwargs=kwargs)
        target_path = Path(command[command.index("--target-path") + 1])
        target_path.mkdir(parents=True)
        (target_path / "run_results.json").write_text("{}", encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    ti = FakeTaskInstance(task_id="dbt_run_silver", try_number=2)

    result = module.run_dbt_phase(
        dbt_args='run --select "silver model"',
        ti=ti,
        run_id="scheduled/2026:07",
        params={"target": "dev"},
    )

    artifact_path = module._artifact_path(
        run_id="scheduled/2026:07",
        task_id="dbt_run_silver",
        try_number=2,
    )
    command = captured["command"]
    assert command[:4] == [module.DBT_BIN, "run", "--select", "silver model"]
    assert command[command.index("--target") + 1] == "dev"
    assert command[command.index("--vars") + 1] == json.dumps(
        module.WEATHER_DBT_CONTRACT_VARS,
        separators=(",", ":"),
    )
    assert command[command.index("--target-path") + 1] == str(Path(artifact_path).parent)
    assert "--no-use-colors" in command
    assert Path(artifact_path).exists()
    assert result == {"status": "success", "artifact_path": artifact_path}
    assert ti.pushes == [(module.WEATHER_DBT_ARTIFACT_XCOM_KEY, artifact_path)]


def test_weather_dbt_attempt_overwrites_artifact_xcom_when_process_cannot_start(
    tmp_path, monkeypatch
):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    ti = FakeTaskInstance(task_id="dbt_run_silver", try_number=3)
    artifact_path = module._artifact_path(
        run_id="manual__spawn_failure",
        task_id=ti.task_id,
        try_number=ti.try_number,
    )
    stale_artifact = Path(artifact_path)
    stale_artifact.parent.mkdir(parents=True)
    stale_artifact.write_text('{"metadata": "stale"}', encoding="utf-8")

    def fail_to_start(*_args, **_kwargs):
        raise OSError("dbt process could not start")

    monkeypatch.setattr(module.subprocess, "run", fail_to_start)

    with pytest.raises(OSError, match="could not start"):
        module.run_dbt_phase(
            dbt_args="run --select silver_kma_vilage_fcst",
            ti=ti,
            run_id="manual__spawn_failure",
            params={"target": "dev"},
        )

    assert ti.pushes == [(module.WEATHER_DBT_ARTIFACT_XCOM_KEY, None)]


def test_weather_dbt_attempt_pushes_none_when_artifact_reset_fails(tmp_path, monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    ti = FakeTaskInstance(task_id="dbt_run_silver", try_number=5)
    artifact_path = module._artifact_path(
        run_id="manual__reset_failure",
        task_id=ti.task_id,
        try_number=ti.try_number,
    )
    stale_artifact = Path(artifact_path)
    stale_artifact.parent.mkdir(parents=True)
    stale_artifact.write_text('{"metadata": "stale"}', encoding="utf-8")

    def fail_to_remove(path):
        assert path == artifact_path
        raise PermissionError("artifact reset failed")

    def fail_if_started(*_args, **_kwargs):
        raise AssertionError("dbt must not start after artifact reset failure")

    monkeypatch.setattr(module.os, "remove", fail_to_remove)
    monkeypatch.setattr(module.subprocess, "run", fail_if_started)

    with pytest.raises(PermissionError, match="artifact reset failed"):
        module.run_dbt_phase(
            dbt_args="run --select silver_kma_vilage_fcst",
            ti=ti,
            run_id="manual__reset_failure",
            params={"target": "dev"},
        )

    assert ti.pushes == [(module.WEATHER_DBT_ARTIFACT_XCOM_KEY, None)]
    assert stale_artifact.exists()


def test_weather_failed_dbt_command_pushes_existing_artifact_before_raising(
    tmp_path, monkeypatch
):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    ti = FakeTaskInstance(task_id="dbt_test_silver", try_number=4)
    artifact_path = module._artifact_path(
        run_id="manual__failed",
        task_id=ti.task_id,
        try_number=ti.try_number,
    )
    artifact = Path(artifact_path)

    def fail_with_artifact(*_args, **_kwargs):
        artifact.parent.mkdir(parents=True)
        artifact.write_text('{"metadata": "current"}', encoding="utf-8")
        return types.SimpleNamespace(
            returncode=1,
            stdout="dbt stdout\n",
            stderr="dbt failed\n",
        )

    monkeypatch.setattr(
        module.subprocess,
        "run",
        fail_with_artifact,
    )

    with pytest.raises(FakeAirflowException, match="weather dbt command failed"):
        module.run_dbt_phase(
            dbt_args="test --select silver_kma_vilage_fcst",
            ti=ti,
            run_id="manual__failed",
            params={"target": "dev"},
        )

    assert ti.pushes == [(module.WEATHER_DBT_ARTIFACT_XCOM_KEY, artifact_path)]


def test_weather_dbt_factory_preserves_phase_contracts():
    module = load_transform_module()
    expected_task_ids = tuple(task_id for task_id, _dbt_args, _include_vars in EXPECTED_DBT_PHASES)

    assert module.DBT_PHASE_TASK_IDS == expected_task_ids
    for task_id, dbt_args, include_project_vars in EXPECTED_DBT_PHASES:
        task = module.dag.task_dict[task_id]
        assert isinstance(task, FakePythonOperator)
        assert task.python_callable is module.run_dbt_phase
        assert task.kwargs["op_kwargs"] == {
            "dbt_args": dbt_args,
            "include_project_vars": include_project_vars,
        }
        assert task.kwargs["pool"] == module.TRINO_HEAVY_POOL
        assert task.kwargs["retries"] == 1
        assert task.kwargs["retry_delay"] == module.DBT_RETRY_DELAY
        assert task.kwargs["on_failure_callback"] == [
            module.notify_weather_transform_failure,
            module.record_weather_problem,
        ]


def test_weather_transform_runs_place_mapping_seed_and_mart():
    module = load_transform_module()
    dag = module.dag

    expected_task_order = [task_id for task_id, _dbt_args, _include_vars in EXPECTED_DBT_PHASES]

    assert set(expected_task_order) <= set(dag.task_ids)
    for upstream_task_id, downstream_task_id in zip(expected_task_order, expected_task_order[1:]):
        assert dag.task_dict[upstream_task_id].downstream_task_ids == {downstream_task_id}

    task_commands = {
        task_id: dag.task_dict[task_id].kwargs["op_kwargs"]["dbt_args"]
        for task_id in expected_task_order
    }

    assert "deps" in task_commands["dbt_deps"]
    assert "source freshness" in task_commands["dbt_source_freshness"]
    assert "seed --select asac_axes" in task_commands["dbt_seed_asac_axes"]
    assert (
        "run --select asac_axes.dim_admin_dong"
        in task_commands["dbt_run_common_admin_dong_dimension"]
    )
    assert (
        "test --select asac_axes.dim_admin_dong"
        in task_commands["dbt_test_common_admin_dong_dimension"]
    )
    for task_id in (
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
    ):
        assert dag.task_dict[task_id].kwargs["on_failure_callback"] == [
            module.notify_weather_transform_failure,
            module.record_weather_problem,
        ]
    assert "seed --select weather_place_grid_mapping" in task_commands["dbt_seed_place_mapping"]
    assert "weather_place_grid_mapping" in task_commands["dbt_test_place_mapping_seed"]
    assert "assert_weather_place_grid_mapping_major_aliases" in task_commands["dbt_test_place_mapping_seed"]
    assert (
        "assert_weather_place_grid_mapping_within_collected_grid_scope"
        in task_commands["dbt_test_place_mapping_seed"]
    )
    assert (
        "assert_weather_place_grid_mapping_alias_unique_except_allowed"
        in task_commands["dbt_test_place_mapping_seed"]
    )
    assert "assert_silver_kma_event_at_matches_forecast_at" in task_commands["dbt_test_silver"]
    assert "--exclude assert_gold_weather_counts_match_silver" in task_commands["dbt_test_silver"]
    assert (
        "assert_gold_weather_forecast_by_place_latest_silver_record"
        not in task_commands["dbt_test_silver"]
    )
    assert (
        "run --select dim_weather_place silver_weather_forecast_by_admin_dong gold_weather_forecast_by_place"
        in task_commands["dbt_run_place_mart"]
    )
    assert "dim_weather_place" in task_commands["dbt_test_place_mart"]
    assert "silver_weather_forecast_by_admin_dong" in task_commands["dbt_test_place_mart"]
    assert "gold_weather_forecast_by_place" in task_commands["dbt_test_place_mart"]
    assert "assert_silver_weather_admin_dong_grain_unique" in task_commands["dbt_test_place_mart"]
    assert "assert_silver_weather_admin_axis_consistent" in task_commands["dbt_test_place_mart"]
    assert (
        "assert_silver_weather_admin_event_at_matches_forecast_at"
        in task_commands["dbt_test_place_mart"]
    )
    assert (
        "assert_gold_weather_forecast_by_place_admin_axis_consistent"
        in task_commands["dbt_test_place_mart"]
    )
    assert "assert_gold_weather_forecast_by_place_grain_unique" in task_commands["dbt_test_place_mart"]
    assert "assert_gold_weather_forecast_by_place_major_coverage" in task_commands["dbt_test_place_mart"]
    assert "assert_dim_weather_place_admin_axis_consistent" in task_commands["dbt_test_place_mart"]
    assert (
        "assert_gold_weather_forecast_by_place_event_at_matches_forecast_at"
        in task_commands["dbt_test_place_mart"]
    )
    assert (
        "assert_gold_weather_forecast_by_place_latest_silver_record"
        in task_commands["dbt_test_place_mart"]
    )


def test_weather_transform_passes_w2_canonical_revision_to_model_commands():
    module = load_transform_module()
    dag = module.dag
    model_parsing_task_ids = (
        "dbt_source_freshness",
        "dbt_seed_asac_axes",
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
        "dbt_seed_place_mapping",
        "dbt_test_place_mapping_seed",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
        "dbt_run_place_mart",
        "dbt_test_place_mart",
    )

    for task_id in model_parsing_task_ids:
        assert dag.task_dict[task_id].kwargs["op_kwargs"]["include_project_vars"] is True
    assert dag.task_dict["dbt_deps"].kwargs["op_kwargs"]["include_project_vars"] is False


def test_weather_current_run_results_selects_latest_success_artifact(tmp_path):
    module = load_transform_module()
    earlier = tmp_path / "earlier" / "run_results.json"
    latest = tmp_path / "latest" / "run_results.json"
    failure = tmp_path / "failure" / "run_results.json"
    for path in (earlier, latest, failure):
        path.parent.mkdir()
        path.write_text("{}", encoding="utf-8")
    earlier_task = module.DBT_PHASE_TASK_IDS[-2]
    latest_task = module.DBT_PHASE_TASK_IDS[-1]
    ti = FakeTaskInstance(
        pulls={
            (earlier_task, None): {"artifact_path": str(earlier)},
            (latest_task, None): {"artifact_path": str(latest)},
            (latest_task, module.WEATHER_DBT_ARTIFACT_XCOM_KEY): str(failure),
        }
    )

    assert module._current_run_results_path(ti=ti) == str(latest)


def test_weather_current_run_results_falls_back_past_nonexistent_and_malformed_candidates(
    tmp_path,
):
    module = load_transform_module()
    existing = tmp_path / "existing" / "run_results.json"
    existing.parent.mkdir()
    existing.write_text("{}", encoding="utf-8")
    latest_task = module.DBT_PHASE_TASK_IDS[-1]
    malformed_task = module.DBT_PHASE_TASK_IDS[-2]
    fallback_task = module.DBT_PHASE_TASK_IDS[-3]
    ti = FakeTaskInstance(
        pulls={
            (latest_task, None): {
                "artifact_path": str(tmp_path / "missing" / "run_results.json")
            },
            (latest_task, module.WEATHER_DBT_ARTIFACT_XCOM_KEY): {"not": "a path"},
            (malformed_task, None): {"artifact_path": 7},
            (malformed_task, module.WEATHER_DBT_ARTIFACT_XCOM_KEY): ["also malformed"],
            (fallback_task, None): {"artifact_path": str(existing)},
        }
    )

    assert module._current_run_results_path(task_instance=ti) == str(existing)


def test_weather_current_run_results_accepts_failure_artifact_xcom(tmp_path):
    module = load_transform_module()
    artifact = tmp_path / "failed" / "run_results.json"
    artifact.parent.mkdir()
    artifact.write_text("{}", encoding="utf-8")
    failed_task = module.DBT_PHASE_TASK_IDS[-4]
    ti = FakeTaskInstance(
        pulls={
            (failed_task, module.WEATHER_DBT_ARTIFACT_XCOM_KEY): str(artifact),
        }
    )

    assert module._current_run_results_path(ti=ti) == str(artifact)


def test_weather_implicit_metrics_never_reads_shared_run_results(tmp_path, monkeypatch):
    module = load_transform_module()
    shared = tmp_path / "target" / "run_results.json"
    shared.parent.mkdir()
    shared.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(module, "RUN_RESULTS_PATH", str(shared))

    def fail_if_published(*_args, **_kwargs):
        raise AssertionError("shared run_results.json must not be published")

    monkeypatch.setattr(module, "dump_dbt_run_results", fail_if_published)
    ti = FakeTaskInstance()

    assert module._current_run_results_path(ti=ti) is None
    assert module.publish_dbt_run_metrics(ti=ti, params={"target": "dev"}) == {
        "rows": 0,
        "skipped": True,
    }


def test_weather_transform_subscribes_to_bronze_asset_by_default():
    module = load_transform_module()

    assert module.dag.kwargs["schedule"] == [FakeAsset(module.WEATHER_BRONZE_ASSET)]


def test_weather_transform_names_common_admin_dong_failure_stage():
    module = load_transform_module()

    for task_id in (
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
    ):
        assert module.transform_stage_name(task_id) == "공용 행정동 차원 실행/검증"


def test_weather_transform_validates_dev_runtime_before_dbt():
    module = load_transform_module()
    guard = module.dag.task_dict["validate_dev_runtime"]

    assert guard.kwargs["op_kwargs"] == {
        "domain": "weather",
        "requested_target": "{{ params.target }}",
    }
    assert guard.downstream_task_ids == {"dbt_deps"}


def test_weather_transform_limits_target_param_to_dev_or_prod():
    module = load_transform_module()

    target_param = module.DEFAULT_PARAMS["target"]

    assert target_param.value == "dev"
    assert target_param.schema["enum"] == ["dev"]


def test_weather_transform_publishes_dbt_run_metrics_as_non_gating_teardown():
    module = load_transform_module()
    dag = module.dag

    assert "publish_dbt_run_metrics" in dag.task_ids
    metrics = dag.task_dict["publish_dbt_run_metrics"]

    assert metrics.python_callable is module.publish_dbt_run_metrics
    assert metrics.is_teardown is True
    assert metrics.on_failure_fail_dagrun is False
    assert metrics.kwargs["trigger_rule"] == "all_done_setup_success"
    assert metrics.kwargs["on_failure_callback"] is module.record_weather_problem
    assert dag.task_dict["dbt_test_place_mart"].downstream_task_ids == {
        "publish_dbt_run_metrics"
    }
    assert metrics.downstream_task_ids == set()
    assert "fail_transform_if_upstream_failed" not in dag.task_ids
    assert "on_success_callback" not in dag.kwargs


def test_weather_publish_dbt_run_metrics_forwards_domain_and_target(tmp_path, monkeypatch):
    module = load_transform_module()
    run_results = tmp_path / "run_results.json"
    run_results.write_text("{}", encoding="utf-8")
    captured = {}

    def fake_dump(path, *, domain, target):
        captured.update(path=path, domain=domain, target=target)
        return [{}, {}]

    monkeypatch.setattr(module, "dump_dbt_run_results", fake_dump)

    assert module.publish_dbt_run_metrics(
        run_results_path=str(run_results), params={"target": "dev"}
    ) == {"rows": 2, "skipped": False}
    assert captured == {"path": str(run_results), "domain": "weather", "target": "dev"}
