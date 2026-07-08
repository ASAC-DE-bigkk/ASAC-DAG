"""수집 스코프 계약(#212) — bronze DAG 의 SOURCES 를 못박는 회귀 테스트.

silver 가 소비하지 않는 실시간 dataset(subway_position·bus_arrival)은 수집에서
제외돼 있다(SOURCES 에서 주석 처리). 누가 실수로 주석을 해제하거나 스코프를
바꾸면 이 테스트가 알려준다. 재개는 의도된 결정 + 이 테스트 갱신과 함께 PR 로.

Airflow 를 가짜 모듈로 대체해 DAG 파일을 import 만 한다(실호출 없음).
"""
import importlib.util
import sys
import types
from pathlib import Path

_TRANSIT = Path(__file__).resolve().parents[1]        # domains/transit
_DAGS = Path(__file__).resolve().parents[3]           # dags 루트
for p in (str(_DAGS), str(_TRANSIT)):
    if p not in sys.path:
        sys.path.insert(0, p)


# ── Airflow 가짜 모듈 + DAG 로더 ─────────────────────────────────────────────────
class _FakeDAG:
    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        pass


class _FakePythonOperator:
    def __init__(self, task_id, python_callable=None, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.kwargs = kwargs


def _install_airflow_fakes():
    airflow = types.ModuleType("airflow")
    airflow.DAG = _FakeDAG
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    operators = types.ModuleType("airflow.providers.standard.operators")
    python_mod = types.ModuleType("airflow.providers.standard.operators.python")
    python_mod.PythonOperator = _FakePythonOperator
    sys.modules.update({
        "airflow": airflow,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": operators,
        "airflow.providers.standard.operators.python": python_mod,
    })


def _load_dag(filename: str):
    _install_airflow_fakes()
    path = _TRANSIT / filename
    spec = importlib.util.spec_from_file_location(f"{Path(filename).stem}_under_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


# ── 수집 스코프 계약 ─────────────────────────────────────────────────────────────
def test_subway_sources_collect_arrival_only():
    # #212: subway_position 은 silver 미소비 → 수집 제외(주석 처리). 되살리려면 PR 로.
    module = _load_dag("transit_subway_bronze.py")
    assert module.SOURCES == {"bronze_subway_arrival": "subway_arrival"}


def test_bus_sources_collect_position_only():
    # #212: bus_arrival 은 silver 미소비 → 수집 제외(주석 처리). 되살리려면 PR 로.
    module = _load_dag("transit_bus_bronze.py")
    assert module.SOURCES == {"bronze_bus_position": "bus_position"}
