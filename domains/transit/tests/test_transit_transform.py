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

    def as_teardown(self, setups=None, on_failure_fail_dagrun=False):
        # traffic/weather transform_test_support 의 fake 와 동일 모델링(#526)
        self.is_teardown = True
        self.on_failure_fail_dagrun = on_failure_fail_dagrun
        self.kwargs["trigger_rule"] = "all_done_setup_success"
        return self


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


def test_publish_teardown_restores_run_failure():
    # #526: all_done 리프(publish)가 유일 리프면 dbt_build 실패가 DagRun success 로
    # 마스킹된다(리프 판정). weather/traffic 관례대로 publish 를 teardown 으로 선언해
    # DagRun 판정에서 제외 → dbt_build 가 실질 리프 → build 실패 = run 실패 복원.
    module = load_transform_module()
    publish = module.dag.task_dict["publish_silver_metrics"]

    assert getattr(publish, "is_teardown", False) is True
    assert publish.on_failure_fail_dagrun is False  # teardown 자체 실패도 run 상태 불개입


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


# ── 스케줄: */15 기본 + env 오버라이드 ───────────────────────────────────────────
def test_schedule_defaults_every_15min(monkeypatch):
    # #443: 사용자향 '지금' 카드 신선도 때문에 @hourly → */15. 실측 build 344초.
    # 되돌릴 때는 G1(dong_now)의 최대 지연이 1시간이 된다는 점을 함께 판단할 것.
    monkeypatch.delenv("TRANSIT_TRANSFORM_SCHEDULE", raising=False)
    module = load_transform_module()
    assert module.transform_schedule() == "*/15 * * * *"
    assert module.dag.kwargs["schedule"] == "*/15 * * * *"


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
def test_publish_metrics_runs_even_when_build_fails():
    # #188 의도: build 실패에서도 모델/테스트 메트릭을 남긴다.
    # teardown(#526)의 all_done_setup_success 트리거가 그 역할을 승계한다.
    module = load_transform_module()
    task = module.dag.task_dict["publish_silver_metrics"]
    assert task.kwargs["trigger_rule"] == "all_done_setup_success"


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


# ── fresh/heavy 분리 (#443 예고 분리) ────────────────────────────────────────────
def test_dbt_command_selector_and_target_path_assembly():
    module = load_transform_module()

    cmd = module.dbt_command("build", select="tag:heavy", target_path="target_heavy")
    assert "--select tag:heavy" in cmd
    assert "--target-path target_heavy" in cmd

    cmd = module.dbt_command("build", exclude="tag:heavy")
    assert "--exclude tag:heavy" in cmd
    assert "--select" not in cmd
    assert "--target-path" not in cmd

    # 기본값(셀렉터 없음)은 현행 조립과 동일해야 한다.
    plain = module.dbt_command("build")
    assert "--select" not in plain and "--exclude" not in plain


def test_fresh_build_excludes_heavy_models():
    module = load_transform_module()
    build_cmd = module.dag.task_dict["dbt_build"].bash_command
    assert "--exclude tag:heavy" in build_cmd
    # fresh 는 기본 target/ 을 그대로 쓴다(격리는 heavy 쪽 책임).
    assert "--target-path" not in build_cmd


def test_heavy_dag_wiring_mirrors_fresh():
    module = load_transform_module()
    dag = module.heavy_dag

    assert dag.dag_id == "transit_transform_heavy"
    assert dag.kwargs["max_active_runs"] == 1
    expected = ["check_transform_gate", "dbt_deps", "dbt_build", "publish_silver_metrics"]
    assert set(expected) <= set(dag.task_ids)
    for upstream, downstream in zip(expected, expected[1:]):
        assert dag.task_dict[upstream].downstream_task_ids == {downstream}


def test_heavy_build_selects_only_heavy_without_ancestors():
    module = load_transform_module()
    build_cmd = module.heavy_dag.task_dict["dbt_build"].bash_command

    # 상류 silver/dim 은 fresh DAG 가 갱신하므로 조상 포함(+tag:heavy)을 쓰면 안 된다 —
    # heavy 런까지 무거워져 분리 목적이 무너진다.
    assert "--select tag:heavy" in build_cmd
    assert "+tag:heavy" not in build_cmd
    # artifact 격리: fresh 의 target/run_results.json 을 덮어쓰지 않는다.
    assert "--target-path target_heavy" in build_cmd


def test_heavy_publish_reads_isolated_run_results():
    module = load_transform_module()

    assert module.HEAVY_RUN_RESULTS_PATH.replace("\\", "/") == (
        "/opt/airflow/dbt/domains/transit/target_heavy/run_results.json"
    )
    publish = module.heavy_dag.task_dict["publish_silver_metrics"]
    assert publish.kwargs["op_kwargs"] == {
        "run_results_path": module.HEAVY_RUN_RESULTS_PATH
    }
    # teardown 관례(#526)도 fresh 와 동일 — 리프 마스킹 방지 + build 실패에서도 적재.
    assert getattr(publish, "is_teardown", False) is True
    assert publish.on_failure_fail_dagrun is False


def test_heavy_schedule_defaults_offset_not_top_of_hour(monkeypatch):
    # */30 이면 매 :00/:30 에 fresh(*/15)와 반드시 동시 트리거 — Trino 순간부하 완화라는
    # 분리 목적에 역행하므로 5,35 오프셋이 기본이다.
    monkeypatch.delenv("TRANSIT_TRANSFORM_HEAVY_SCHEDULE", raising=False)
    module = load_transform_module()
    assert module.transform_heavy_schedule() == "5,35 * * * *"
    assert module.heavy_dag.kwargs["schedule"] == "5,35 * * * *"


def test_heavy_schedule_env_override(monkeypatch):
    monkeypatch.setenv("TRANSIT_TRANSFORM_HEAVY_SCHEDULE", "*/30 * * * *")
    module = load_transform_module()
    assert module.transform_heavy_schedule() == "*/30 * * * *"


def test_both_builds_serialize_on_transit_trino_pool():
    # 단일노드 Trino 보호: fresh/heavy 의 dbt_deps·dbt_build 가 같은 도메인 전용
    # pool(slot 1)을 써서 빌드끼리도, 공유 dbt_packages/ 를 다시 쓰는 deps 와
    # 상대 DAG 의 deps·parse 도 절대 겹치지 않는다 (weather/traffic 관례).
    module = load_transform_module()
    assert module.TRINO_TRANSIT_HEAVY_POOL == "trino_transit_heavy"
    for dag in (module.dag, module.heavy_dag):
        for task_id in ("dbt_deps", "dbt_build"):
            assert dag.task_dict[task_id].kwargs["pool"] == "trino_transit_heavy"


def test_fresh_and_heavy_do_not_share_param_instances():
    # 두 DAG 가 같은 Param 객체를 공유하면 직렬화/변형이 서로 간섭할 수 있다.
    module = load_transform_module()
    fresh_param = module.dag.kwargs["params"]["target"]
    heavy_param = module.heavy_dag.kwargs["params"]["target"]
    assert fresh_param is not heavy_param
    assert heavy_param.value == fresh_param.value
    assert heavy_param.schema["enum"] == ["dev", "prod"]
