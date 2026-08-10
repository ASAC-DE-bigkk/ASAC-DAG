"""traffic 기대 주기 등록 — 코드 선언과 조회 DB 사본이 어긋나지 않는지 (ASK-Seoul#78 §9, ASAC-DAG#733).

값의 정본은 실제 DAG 파일의 ``dag_id=``(또는 ``DAG_ID =`` 류 상수) 선언이다(`S-1`). 이 테스트는
그 정본을 실제로 읽어 :mod:`traffic_ingest.ops_expectations` 사본과 양방향 대조한다 — DAG 를
새로 추가하거나 지우고 등록을 안 고치면 여기서 막힌다.

스키마·등록 규칙은 공용(`common.ops.expectations`)이라 그쪽 회귀는 `common/tests` 가 맡는다.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "domains" / "traffic"))

from traffic_ingest import ops_expectations  # noqa: E402
from common.ops import d1_ops  # noqa: E402
from common.ops.expectations import rows as shared_rows  # noqa: E402

BUNDLE = Path(__file__).resolve().parents[1]
DOMAIN = "traffic"

# dag_id 상수(DAG_ID/RECOLLECT_DAG_ID/BACKFILL_DAG_ID)가 top-level DAG 파일이 아니라
# traffic_ingest/bronze_dag_support.py 에 선언되고, top-level 파일들이 값만 가져다 쓴다.
_EXTRA_DECLARATION_FILES = (BUNDLE / "traffic_ingest" / "bronze_dag_support.py",)

_DAG_ID_PATTERN = re.compile(r'(?i)dag_id\s*=\s*["\']([A-Za-z][A-Za-z0-9_]*)["\']')


def _declared_dag_ids() -> set[str]:
    """DAG 파일에서 ``dag_id=``/``DAG_ID =`` 류 선언을 그대로 읽는다(정본)."""
    found: set[str] = set()
    files = list(BUNDLE.glob("*.py")) + list(_EXTRA_DECLARATION_FILES)
    for path in files:
        text = path.read_text(encoding="utf-8")
        found.update(_DAG_ID_PATTERN.findall(text))
    return found


def _registered() -> dict[str, dict]:
    return {row["dag_id"]: row for row in shared_rows(updated_at="t", domains=[DOMAIN])}


def test_every_declared_dag_has_an_expectation():
    declared = _declared_dag_ids()
    assert declared, "DAG 선언을 하나도 못 읽었습니다 — 파서를 확인하세요"
    missing = sorted(declared - set(_registered()))
    assert not missing, (
        "기대 주기가 등록되지 않은 DAG 가 있습니다. 등록되지 않으면 그 파이프라인이 죽어도 "
        f"'원래 안 도는 시간'으로 처리됩니다(S-5): {missing}")


def test_no_expectation_points_at_a_dag_that_no_longer_exists():
    stale = sorted(set(_registered()) - _declared_dag_ids())
    assert not stale, f"사라진 DAG 의 기대치가 남아 있습니다: {stale}"


def test_manual_only_dags_are_excluded_from_monitoring():
    """수동 실행 전용 DAG 는 등록은 하되 감시에서 뺀다(S-4)."""
    expected_manual = {
        "traffic_incident_recollect",
        "traffic_incident_bronze_backfill",
        "traffic_link_reference_backfill",
        "traffic_snapshot_recovery",
    }
    registered = _registered()
    manual = {dag_id for dag_id, row in registered.items() if row["trigger_type"] == "manual"}
    assert manual == expected_manual
    for dag_id in manual:
        assert registered[dag_id]["monitored"] == 0, f"{dag_id}: 수동 전용은 monitored=False 여야 한다"


def test_asset_triggered_dags_declare_upstream_and_max_delay():
    """상류 완료로 도는 DAG 는 고정 주기 대신 트리거·상류·최대 허용 지연으로 등록한다(S-3)."""
    registered = _registered()
    asset_dags = {dag_id for dag_id, row in registered.items() if row["trigger_type"] == "asset"}
    assert asset_dags == {
        "traffic_incident_transform",
        "traffic_flow_bronze",
        "traffic_flow_transform",
        "traffic_gold_transform",
        "traffic_serving_export",
        "traffic_cross_domain_gold_transform",
        "traffic_cross_domain_serving_export",
    }
    for dag_id in asset_dags:
        row = registered[dag_id]
        assert row["expected_interval"] is None
        assert row["upstream"] and row["max_delay_minutes"] > 0


def test_every_monitored_dag_has_a_max_delay():
    for dag_id, row in _registered().items():
        if row["monitored"]:
            assert row["max_delay_minutes"], f"{dag_id}: 최대 허용 지연이 없습니다(S-2)"


def test_daily_link_reference_sync_is_monitored_at_its_actual_cadence():
    row = _registered()["traffic_link_reference_sync"]

    assert row["trigger_type"] == "schedule"
    assert row["expected_interval"] == "일 1회 03:37"
    assert row["max_delay_minutes"] == 26 * 60
    assert row["monitored"] == 1


def test_rows_match_the_shared_table_schema():
    rows = shared_rows(updated_at="2026-08-08T00:00:00+00:00", domains=[DOMAIN])
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
