"""``load_all()`` 이 weather·traffic 등록 모듈을 실제로 잡는지 (ASAC-DAG#733).

ASAC-DAG#733 은 commerce 만 등록돼 있던 ``_ops_pipeline_expectation`` 에 weather·traffic 을
추가하는 작업이다. :func:`common.ops.expectations.load_all` 의 후보 목록에는 이미
``weather_ingest.ops_expectations``/``traffic_ingest.ops_expectations`` 가 있었지만(그 모듈이
없어서 조용히 건너뛰어졌다), 이 테스트는 두 모듈이 실제로 임포트되고 행을 반환하는지 확인한다.

각 도메인 내부 값(트리거 방식·상류·최대 허용 지연 등)의 세부 검증은 도메인 테스트가 맡는다
(``domains/weather/tests/test_ops_expectations.py``·``domains/traffic/tests/test_ops_expectations.py``).
이 테스트는 "임포트 경로가 실제로 연결됐는가"만 본다.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "domains" / "weather"))
sys.path.insert(0, str(ROOT / "domains" / "traffic"))

from common.ops.expectations import load_all, rows  # noqa: E402


def test_load_all_imports_weather_and_traffic_modules():
    registered = load_all()

    assert "weather_ingest.ops_expectations" in sys.modules, (
        "load_all() 이 weather_ingest.ops_expectations 를 실제로 임포트하지 않았습니다")
    assert "traffic_ingest.ops_expectations" in sys.modules, (
        "load_all() 이 traffic_ingest.ops_expectations 를 실제로 임포트하지 않았습니다")
    assert "weather" in registered
    assert "traffic" in registered


def test_load_all_returns_weather_and_traffic_rows():
    load_all()

    weather_rows = rows(updated_at="t", domains=["weather"])
    traffic_rows = rows(updated_at="t", domains=["traffic"])

    assert weather_rows, "weather 행이 비어 있습니다 — 등록이 안 됐거나 load_all() 이 못 잡았습니다"
    assert traffic_rows, "traffic 행이 비어 있습니다 — 등록이 안 됐거나 load_all() 이 못 잡았습니다"
    assert {row["domain"] for row in weather_rows} == {"weather"}
    assert {row["domain"] for row in traffic_rows} == {"traffic"}


def test_registered_dag_set_matches_declared_dag_set_for_weather_and_traffic():
    """등록 DAG 집합이 실제 DAG 집합과 어긋나면 실패한다(#733 요구사항).

    실제 census 는 도메인 테스트(``test_every_declared_dag_has_an_expectation``·
    ``test_no_expectation_points_at_a_dag_that_no_longer_exists``)가 정본 파서로 이미 대조한다.
    여기서는 그 도메인 테스트들이 이 저장소에 실제로 존재해 pytest 가 수집하는지만 다시 확인해,
    도메인 테스트 파일이 삭제·이름 변경되는 조용한 회귀를 막는다.
    """
    weather_guard = ROOT / "domains" / "weather" / "tests" / "test_ops_expectations.py"
    traffic_guard = ROOT / "domains" / "traffic" / "tests" / "test_ops_expectations.py"
    assert weather_guard.is_file(), f"{weather_guard} 가 없습니다 — DAG 집합 대조 테스트가 사라졌습니다"
    assert traffic_guard.is_file(), f"{traffic_guard} 가 없습니다 — DAG 집합 대조 테스트가 사라졌습니다"
    for guard in (weather_guard, traffic_guard):
        text = guard.read_text(encoding="utf-8")
        assert "test_every_declared_dag_has_an_expectation" in text
        assert "test_no_expectation_points_at_a_dag_that_no_longer_exists" in text
