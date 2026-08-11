"""transit 기대 주기 등록 — 코드 선언과 사본이 어긋나지 않는지 (ASK-Seoul#78 §9, ASAC-DAG#733).

값의 정본은 실제 DAG 파일의 ``dag_id=``/``DAG_ID =`` 선언이다(`S-1`). 이 테스트는 그 정본을
읽어 :mod:`seoul_transit.ops_expectations` 사본과 양방향 대조한다 — DAG 를 추가/삭제하고
등록을 안 고치면 여기서 막힌다. (weather·traffic 도메인 테스트와 같은 형태.)
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "domains" / "transit"))

from seoul_transit import ops_expectations  # noqa: E402  (등록 부작용)
from common.ops.expectations import rows as shared_rows  # noqa: E402

BUNDLE = Path(__file__).resolve().parents[1]
DOMAIN = "transit"

_DAG_ID_PATTERN = re.compile(r'(?i)dag_id\s*=\s*["\']([A-Za-z][A-Za-z0-9_]*)["\']')

# dag_id 상수가 번들 루트 밖에서 선언되는 파일(현재 없음 — 생기면 여기 추가).
_EXTRA_DECLARATION_FILES: tuple[Path, ...] = ()


def _declared_dag_ids() -> set[str]:
    found: set[str] = set()
    for path in list(BUNDLE.glob("*.py")) + list(_EXTRA_DECLARATION_FILES):
        found.update(_DAG_ID_PATTERN.findall(path.read_text(encoding="utf-8")))
    return found


def _registered() -> dict[str, dict]:
    return {row["dag_id"]: row for row in shared_rows(updated_at="t", domains=[DOMAIN])}


def test_every_declared_dag_has_an_expectation():
    declared = _declared_dag_ids()
    assert declared, "DAG 선언을 하나도 못 읽었습니다 — 파서를 확인하세요"
    missing = sorted(declared - set(_registered()))
    assert not missing, (
        "기대 주기가 등록되지 않은 DAG 가 있습니다 — 등록 없이는 죽어도 "
        f"'원래 안 도는 시간'으로 처리됩니다(S-5): {missing}")


def test_no_expectation_points_at_a_dag_that_no_longer_exists():
    declared = _declared_dag_ids()
    stale = sorted(set(_registered()) - declared)
    assert not stale, f"코드에 없는 DAG 가 등록돼 있습니다(사본 부패): {stale}"


def test_monitored_entries_have_max_delay_and_manual_are_unmonitored():
    for dag_id, row in _registered().items():
        if row["monitored"]:
            assert row["max_delay_minutes"], f"{dag_id}: 감시 대상인데 max_delay_minutes 없음(S-2)"
        if row["trigger_type"] == "manual":
            assert not row["monitored"], f"{dag_id}: 수동 전용은 감시 제외여야 한다(S-4)"


def test_utc_cron_collectors_declare_timezone():
    """수집계(naive start_date)는 UTC 크론 — timezone 명시가 빠지면 판정이 9시간 어긋난다."""
    utc_expected = {
        "transit_bronze_loader", "transit_bus_bronze", "transit_subway_bronze",
        "transit_parking_bronze", "transit_bus_route_master", "transit_master_bronze",
        "transit_subway_timetable_bronze", "transit_maintenance",
    }
    reg = _registered()
    for dag_id in sorted(utc_expected):
        assert reg[dag_id]["schedule_timezone"] == "UTC", (
            f"{dag_id}: naive start_date 크론은 UTC 로 해석된다 — schedule_timezone='UTC' 필요")
