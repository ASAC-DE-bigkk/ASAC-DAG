"""commerce 기대 주기 등록 — 코드 선언과 조회 DB 사본이 어긋나지 않는지 (ASK-Seoul#78 §9).

값의 정본은 DAG 파일의 ``schedule=`` 이다(`S-1`). 이 테스트는 그 정본을 실제로 읽어 사본과
양방향 대조한다 — 스케줄만 바꾸고 표를 안 고치면 여기서 막힌다.

스키마·등록 규칙은 공용(`common.ops.expectations`)이라 그쪽 회귀는 `common/tests` 가 맡는다.
"""
from __future__ import annotations

import re
from pathlib import Path

from commerce_core import ops_expectations
from common.ops import d1_ops
from common.ops.expectations import rows as shared_rows

BUNDLE = Path(__file__).resolve().parents[1]
DOMAIN = "commerce"


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


def _registered() -> dict[str, dict]:
    return {row["dag_id"]: row for row in shared_rows(updated_at="t", domains=[DOMAIN])}


def test_every_declared_dag_has_an_expectation():
    declared = set(_declared_schedules())
    assert declared, "DAG 선언을 하나도 못 읽었습니다 — 파서를 확인하세요"
    missing = sorted(declared - set(_registered()))
    assert not missing, (
        "기대 주기가 등록되지 않은 DAG 가 있습니다. 등록되지 않으면 그 파이프라인이 죽어도 "
        f"'원래 안 도는 시간'으로 처리됩니다(S-5): {missing}")


def test_no_expectation_points_at_a_dag_that_no_longer_exists():
    stale = sorted(set(_registered()) - set(_declared_schedules()))
    assert not stale, f"사라진 DAG 의 기대치가 남아 있습니다: {stale}"


def test_manual_only_dag_is_excluded_from_monitoring():
    """수동 실행 전용은 감시 대상에서 뺀다(S-4) — 안 도는 것이 정상이라 알림이 소음이 된다."""
    row = _registered()["commerce_load_gold_refresh"]
    assert row["monitored"] == 0 and row["trigger_type"] == "manual"
    assert _declared_schedules()["commerce_load_gold_refresh"] == "None"


def test_asset_triggered_dag_declares_upstream_and_max_delay():
    """상류 완료로 도는 DAG 는 고정 주기 대신 트리거·상류·최대 허용 지연으로 등록한다(S-3)."""
    row = _registered()["commerce_serving_export"]
    assert row["trigger_type"] == "asset"
    assert row["expected_interval"] is None
    assert row["upstream"] and row["max_delay_minutes"] > 0


def test_every_monitored_dag_has_a_max_delay():
    for dag_id, row in _registered().items():
        if row["monitored"]:
            assert row["max_delay_minutes"], f"{dag_id}: 최대 허용 지연이 없습니다(S-2)"


def test_rows_match_the_shared_table_schema():
    rows = shared_rows(updated_at="2026-08-03T00:00:00+00:00", domains=[DOMAIN])
    assert set(rows[0]) == set(d1_ops.columns_of(d1_ops.PIPELINE_EXPECTATION_TABLE))
    statements = d1_ops.pipeline_expectation_upsert_statements(rows)
    # D-4: 공유 테이블은 통째 교체가 아니라 자연키 범위로만 갱신한다.
    assert 'ON CONFLICT("dag_id") DO UPDATE' in statements[0]
    assert "DELETE" not in statements[0].upper() and "DROP" not in statements[0].upper()


def test_owner_is_recorded():
    """등록 전 도메인 오너 확인이 필수다(S-5) — 누가 언제 확인했는지가 없으면 믿을 근거가 없다."""
    row = next(iter(_registered().values()))
    assert row["owner"] == ops_expectations.OWNER
    assert row["owner_confirmed_on"] == ops_expectations.OWNER_CONFIRMED_ON
