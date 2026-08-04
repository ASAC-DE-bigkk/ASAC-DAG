"""공용 기대 주기 등록이 도메인 표와 어긋나지 않는다 (ASK-Seoul#78 §9).

도메인 표 ↔ 실제 DAG 대조는 ``test_schedule_registry.py`` 가 이미 한다. 여기서 잠그는
것은 **어댑터가 값을 옮기다 흘리지 않는지** 하나다 — 값을 여기 다시 적으면 정본이
하나 더 늘어난다(`S-1`).
"""
import pytest

from common.ops.expectations import Expectation, rows
# 모듈 경로는 공용 로더가 정한다 — `common/ops/expectations.py` 가 도메인별로
# ``<pkg>.ops_expectations`` 를 import 해 등록을 발동시킨다. 경로가 어긋나면 등록이
# 조용히 안 되고, 그건 "감시 제외"가 아니라 "아직 등록 안 됨"으로 남는다.
from culture_ingest import ops_expectations
from culture_ingest.ops.schedule_registry import BY_DAG_ID, TIMEZONE, TRIGGER_ASSET


def test_every_registered_dag_is_carried_over():
    assert {e.dag_id for e in ops_expectations.EXPECTATIONS} == set(BY_DAG_ID)


def test_asset_triggered_dag_declares_upstream_not_interval():
    """상류 이벤트형에 고정 주기를 적으면 상류가 늦을 때마다 오탐(`S-3`)."""
    transform = next(e for e in ops_expectations.EXPECTATIONS if e.dag_id == "culture_transform")
    assert transform.trigger_type == TRIGGER_ASSET
    assert transform.expected_interval is None
    assert transform.upstream == "culture_bronze"


def test_scheduled_dags_carry_human_interval_and_timezone():
    for exp in ops_expectations.EXPECTATIONS:
        if exp.trigger_type == TRIGGER_ASSET:
            continue
        assert exp.expected_interval, f"{exp.dag_id}: 화면에 뜰 사람 말 표기가 비었다"
        assert exp.schedule_timezone == TIMEZONE


def test_max_delay_matches_the_domain_table():
    for exp in ops_expectations.EXPECTATIONS:
        assert exp.max_delay_minutes == BY_DAG_ID[exp.dag_id].max_delay_min


def test_rows_are_emitted_for_culture():
    emitted = rows(updated_at="2026-08-04T00:00:00Z", domains=["culture"])
    assert {r["dag_id"] for r in emitted} == set(BY_DAG_ID)
    assert all(r["owner"] == ops_expectations.OWNER for r in emitted)


def test_monitored_dag_without_max_delay_is_rejected():
    """등록 시점에 거부되는지 — 없으면 늦은 것과 죽은 것을 가를 수 없다(`S-2`)."""
    with pytest.raises(ValueError, match="max_delay_minutes"):
        Expectation("culture_x", "schedule", "일 1회")
