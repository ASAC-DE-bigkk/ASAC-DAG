"""commerce 기대 주기 등록 — 코드 선언과 조회 DB 사본이 어긋나지 않는지 (ASK-Seoul#78 §9).

값의 정본은 DAG 파일의 ``schedule=`` 이다(S-1). 이 테스트는 그 정본을 실제로 읽어 사본과
대조한다 — 사람이 표를 고치는 것을 잊으면 여기서 막힌다.
"""
from __future__ import annotations

import re
from pathlib import Path

from commerce_core import ops_expectations
from common.ops import d1_ops

BUNDLE = Path(__file__).resolve().parents[1]


def _declared_schedules() -> dict[str, str]:
    """DAG 파일에서 ``dag_id=`` 와 그 뒤 ``schedule=`` 을 그대로 읽는다(정본)."""
    found: dict[str, str] = {}
    for path in sorted(BUNDLE.glob("commerce_*.py")):
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(
                r'dag_id="(?P<dag_id>commerce_[a-z_]+)",?\s*\n?\s*schedule=(?P<schedule>[^\n,]+)',
                text):
            found[match.group("dag_id")] = match.group("schedule").strip()
    return found


def test_every_declared_dag_has_an_expectation():
    declared = set(_declared_schedules())
    registered = {dag_id for dag_id, *_ in ops_expectations.EXPECTATIONS}
    assert declared, "DAG 선언을 하나도 못 읽었습니다 — 파서를 확인하세요"
    assert declared <= registered, (
        "기대 주기가 등록되지 않은 DAG 가 있습니다. 등록되지 않으면 그 파이프라인이 죽어도 "
        f"'원래 안 도는 시간'으로 처리됩니다(S-5): {sorted(declared - registered)}")


def test_no_expectation_points_at_a_dag_that_no_longer_exists():
    declared = set(_declared_schedules())
    registered = {dag_id for dag_id, *_ in ops_expectations.EXPECTATIONS}
    assert registered <= declared, f"사라진 DAG 의 기대치가 남아 있습니다: {sorted(registered - declared)}"


def test_manual_only_dag_is_excluded_from_monitoring():
    """수동 실행 전용은 감시 대상에서 뺀다(S-4) — 안 도는 것이 정상이라 알림이 소음이 된다."""
    rows = {row["dag_id"]: row for row in ops_expectations.expectation_rows(updated_at="t")}
    assert rows["commerce_load_gold_refresh"]["monitored"] == 0
    assert rows["commerce_load_gold_refresh"]["trigger_type"] == "manual"
    assert _declared_schedules()["commerce_load_gold_refresh"] == "None"


def test_asset_triggered_dag_declares_upstream_and_max_delay():
    """상류 완료로 도는 DAG 는 고정 주기 대신 트리거 방식·상류·최대 허용 지연으로 등록한다(S-3)."""
    rows = {row["dag_id"]: row for row in ops_expectations.expectation_rows(updated_at="t")}
    export = rows["commerce_serving_export"]
    assert export["trigger_type"] == "asset"
    assert export["expected_interval"] is None
    assert export["upstream"] and export["max_delay_minutes"] > 0


def test_every_monitored_dag_has_a_max_delay():
    for row in ops_expectations.expectation_rows(updated_at="t"):
        if row["monitored"]:
            assert row["max_delay_minutes"], f"{row['dag_id']}: 최대 허용 지연이 없습니다(S-2)"


def test_rows_match_the_shared_table_schema():
    rows = ops_expectations.expectation_rows(updated_at="2026-08-01T00:00:00+00:00")
    assert set(rows[0]) == set(d1_ops.columns_of(d1_ops.PIPELINE_EXPECTATION_TABLE))
    statements = d1_ops.pipeline_expectation_upsert_statements(rows)
    # D-4: 공유 테이블은 통째 교체가 아니라 자연키 범위로만 갱신한다.
    assert 'ON CONFLICT("dag_id") DO UPDATE' in statements[0]
    assert "DELETE" not in statements[0].upper() and "DROP" not in statements[0].upper()
