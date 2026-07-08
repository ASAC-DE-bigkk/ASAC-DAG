"""transit_transform DAG 순수 로직 단위 테스트 (#190).

Airflow 를 가짜 모듈로 대체해 DAG 파일을 import 하고, dbt 명령 조립·태스크 배선·
스케줄 오버라이드·run_results 경로·메트릭 적재 콜러블(가짜 sink)을 실호출 없이 검증한다.
"""

import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest


class FakeDAG:
    _stack: list = []

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


class _FakeOperator:
    def __init__(self, task_id, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs
        self.downstream_task_ids = set()
        FakeDAG._stack[-1].add_task(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        return other


class FakeBashOperator(_FakeOperator):
    def __init__(self, task_id, bash_command, **kwargs):
        super().__init__(task_id, **kwargs)
        self.bash_command = bash_command


class FakePythonOperator(_FakeOperator):
    def __init__(self, task_id, python_callable, **kwargs):
        super().__init__(task_id, **kwargs)
        self.python_callable = python_callable


class FakeParam:
    def __init__(self, default=None, **schema):
        self.value = default
        self.schema = schema


def install_airflow_fakes():
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG

    airflow_models = types.ModuleType("airflow.models")
    airflow_models_param = types.ModuleType("airflow.models.param")
    airflow_models_param.Param = FakeParam

    airflow_providers = types.ModuleType("airflow.providers")
    airflow_standard = types.ModuleType("airflow.providers.standard")
    airflow_operators = types.ModuleType("airflow.providers.standard.operators")
    airflow_bash = types.ModuleType("airflow.providers.standard.operators.bash")
    airflow_bash.BashOperator = FakeBashOperator
    airflow_python = types.ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = FakePythonOperator

    airflow_utils = types.ModuleType("airflow.utils")
    airflow_trigger = types.ModuleType("airflow.utils.trigger_rule")

    class TriggerRule:
        ALL_DONE = "all_done"
        ALL_SUCCESS = "all_success"

    airflow_trigger.TriggerRule = TriggerRule

    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.models": airflow_models,
            "airflow.models.param": airflow_models_param,
            "airflow.providers": airflow_providers,
            "airflow.providers.standard": airflow_standard,
            "airflow.providers.standard.operators": airflow_operators,
            "airflow.providers.standard.operators.bash": airflow_bash,
            "airflow.providers.standard.operators.python": airflow_python,
            "airflow.utils": airflow_utils,
            "airflow.utils.trigger_rule": airflow_trigger,
        }
    )


def load_transform_module():
    install_airflow_fakes()
    module_path = Path(__file__).resolve().parents[1] / "transit_transform.py"
    spec = importlib.util.spec_from_file_location(
        "transit_transform_under_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


# ── 태스크 배선 ──────────────────────────────────────────────────────────────────
def test_task_order_deps_build_publish():
    module = load_transform_module()
    dag = module.dag

    expected = ["dbt_deps", "dbt_build", "publish_silver_metrics"]
    assert set(expected) <= set(dag.task_ids)
    for upstream, downstream in zip(expected, expected[1:]):
        assert dag.task_dict[upstream].downstream_task_ids == {downstream}


def test_dbt_build_is_single_contract_gate():
    module = load_transform_module()
    dag = module.dag

    build_cmd = dag.task_dict["dbt_build"].bash_command
    # 계약 게이트: run/test 를 쪼개지 않고 build 하나(seed+dim+silver+test).
    assert " build " in f" {build_cmd} "
    assert "run --select" not in build_cmd
    assert "test --select" not in build_cmd


def test_dbt_command_injects_project_env_and_target():
    module = load_transform_module()
    cmd = module.dbt_command("build")

    assert "set -euo pipefail" in cmd
    assert "cd /opt/airflow/dbt/domains/transit" in cmd
    assert (
        "export DBT_PROFILES_DIR=/opt/airflow/dbt/domains/transit "
        "DBT_PROJECT_DIR=/opt/airflow/dbt/domains/transit" in cmd
    )
    assert "/home/airflow/dbt-venv/bin/dbt build" in cmd
    assert "--target '{{ params.target }}'" in cmd
    assert "--no-use-colors" in cmd


def test_deps_runs_before_build():
    module = load_transform_module()
    deps_cmd = module.dag.task_dict["dbt_deps"].bash_command
    assert "/home/airflow/dbt-venv/bin/dbt deps" in deps_cmd


# ── run_results 경로 결정 ────────────────────────────────────────────────────────
def test_run_results_path_under_project_target():
    module = load_transform_module()
    assert module.RUN_RESULTS_PATH.replace("\\", "/") == (
        "/opt/airflow/dbt/domains/transit/target/run_results.json"
    )


# ── 스케줄: @hourly 기본 + env 오버라이드 ────────────────────────────────────────
def test_schedule_defaults_hourly(monkeypatch):
    monkeypatch.delenv("TRANSIT_TRANSFORM_SCHEDULE", raising=False)
    module = load_transform_module()
    assert module.transform_schedule() == "@hourly"
    assert module.dag.kwargs["schedule"] == "@hourly"


def test_schedule_env_override(monkeypatch):
    monkeypatch.setenv("TRANSIT_TRANSFORM_SCHEDULE", "*/30 * * * *")
    module = load_transform_module()
    assert module.transform_schedule() == "*/30 * * * *"


# ── target param ─────────────────────────────────────────────────────────────────
def test_target_param_limited_to_dev_or_prod():
    module = load_transform_module()
    param = module.DEFAULT_PARAMS["target"]
    assert param.value == "dev"
    assert param.schema["enum"] == ["dev", "prod"]


# ── 메트릭 적재 태스크 ───────────────────────────────────────────────────────────
def test_publish_metrics_runs_on_all_done():
    module = load_transform_module()
    task = module.dag.task_dict["publish_silver_metrics"]
    assert task.kwargs["trigger_rule"] == "all_done"


def _sample_run_results(path: Path) -> None:
    document = {
        "metadata": {"invocation_id": "inv-abc-123"},
        "results": [
            {
                "unique_id": "model.transit.slv_transit_subway_arrival",
                "status": "success",
                "execution_time": 1.5,
                "adapter_response": {"rows_affected": 42},
                "timing": [
                    {"name": "execute", "started_at": "2026-07-07T00:00:00Z",
                     "completed_at": "2026-07-07T00:00:01Z"},
                ],
            },
        ],
    }
    path.write_text(json.dumps(document), encoding="utf-8")


def test_publish_silver_metrics_dumps_records(tmp_path, monkeypatch):
    module = load_transform_module()
    run_results = tmp_path / "run_results.json"
    _sample_run_results(run_results)

    # ASAC_METRICS_DIR 로 파일 sink 강제 — R2/boto3 접근 없이 로컬 파일로 적재.
    metrics_dir = tmp_path / "metrics"
    monkeypatch.setenv("ASAC_METRICS_DIR", str(metrics_dir))

    result = module.publish_silver_metrics(
        run_results_path=str(run_results), params={"target": "dev"}
    )
    assert result == {"rows": 1, "skipped": False}

    written = list(metrics_dir.rglob("*.json"))
    assert len(written) == 1
    record = json.loads(written[0].read_text(encoding="utf-8"))
    assert record["layer"] == "silver"
    assert record["domain"] == "transit"
    assert record["task_id"] == "slv_transit_subway_arrival"
    assert record["target"] == "dev"
    assert record["rows"] == 42


def test_publish_silver_metrics_skips_when_no_run_results(tmp_path):
    module = load_transform_module()
    missing = tmp_path / "nope" / "run_results.json"
    result = module.publish_silver_metrics(run_results_path=str(missing), params={"target": "dev"})
    assert result == {"skipped": True, "rows": 0}
