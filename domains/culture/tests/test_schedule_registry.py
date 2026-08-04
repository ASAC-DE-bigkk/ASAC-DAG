"""기대 주기 등록값이 실제 DAG 정의와 어긋나면 깨진다 (#619 '기대 주기 등록').

확정안 표가 culture 를 '주 1회'로 적었던 건 주간 정비 DAG 만 보고 쓴 값이었다.
오너 확인이 코멘트로만 남으면 다음 개편에서 또 틀리므로, 선언을 코드에 두고
**여기서 실제 스케줄과 대조**한다. 스케줄을 바꾸면서 등록값을 안 고치면 실패한다.
"""
import pathlib
import re

from culture_ingest.ops.schedule_registry import (
    BY_DAG_ID,
    DOMAIN_CADENCE_DAG_ID,
    REGISTRY,
    TRIGGER_ASSET,
    TRIGGER_SCHEDULE,
    as_rows,
)

_CULTURE = pathlib.Path(__file__).resolve().parents[1]

# dag_id → 그 DAG 를 정의한 파일. serving_export 는 공통 팩토리라 파일명이 곧 dag_id 는 아니다.
_DAG_FILES = {
    "culture_bronze": "culture_bronze.py",
    "culture_transform": "culture_transform.py",
    "culture_serving_export": "culture_serving_export.py",
    "culture_slo": "culture_slo.py",
    "culture_maintenance": "culture_maintenance.py",
    "culture_facility_refresh": "culture_facility_refresh.py",
}


def _source(dag_id: str) -> str:
    return (_CULTURE / _DAG_FILES[dag_id]).read_text(encoding="utf-8")


def test_registry_covers_every_culture_dag():
    dag_files = {p.name for p in _CULTURE.glob("culture_*.py")}
    assert dag_files == set(_DAG_FILES.values()), "DAG 가 늘거나 줄면 등록값도 같이 고쳐야 한다"
    assert set(BY_DAG_ID) == set(_DAG_FILES)


def test_registered_cron_matches_the_dag():
    for cadence in REGISTRY:
        if cadence.trigger_type != TRIGGER_SCHEDULE:
            continue
        pattern = r'schedule\s*=\s*"' + re.escape(cadence.cron) + r'"'
        assert re.search(pattern, _source(cadence.dag_id)), (
            f"{cadence.dag_id}: 등록 cron {cadence.cron!r} 이 DAG 정의와 다르다"
        )


def test_asset_triggered_dag_is_not_registered_as_fixed_cadence():
    """상류에 이어 도는 DAG 를 고정 주기 칸에 넣으면 등록 자체가 거짓이 된다."""
    transform = BY_DAG_ID["culture_transform"]
    assert transform.trigger_type == TRIGGER_ASSET
    assert transform.cron is None
    assert transform.upstream == "culture_bronze"
    assert "schedule=[Asset(" in _source("culture_transform")


def test_domain_cadence_is_daily_not_weekly():
    """확정안 표의 'culture 주 1회'는 오류였다 — 본류는 매일 03:00 이다."""
    main = BY_DAG_ID[DOMAIN_CADENCE_DAG_ID]
    assert main.cron == "0 3 * * *"
    day_of_week = main.cron.split()[4]
    assert day_of_week == "*", "본류가 특정 요일에 묶여 있으면 도메인 주기가 주 1회다"


def test_weekly_dags_are_labelled_as_maintenance_not_domain_cadence():
    for dag_id in ("culture_maintenance", "culture_facility_refresh"):
        cadence = BY_DAG_ID[dag_id]
        assert cadence.cron.split()[4] != "*"      # 요일 고정 = 주 1회
        assert dag_id != DOMAIN_CADENCE_DAG_ID     # 도메인 주기의 근거가 아니다


def test_rows_carry_timezone_only_for_cron_entries():
    rows = {r["dag_id"]: r for r in as_rows()}
    assert rows["culture_bronze"]["timezone"] == "Asia/Seoul"
    assert rows["culture_transform"]["timezone"] is None  # 고정 시각이 없으니 타임존도 없다
    assert all(r["domain"] == "culture" for r in rows.values())


def test_every_entry_declares_a_delay_budget():
    assert all(c.max_delay_min > 0 for c in REGISTRY)


def test_human_interval_agrees_with_the_cron():
    """화면에 뜨는 사람 말 표기(`interval_ko`)가 cron 과 다른 시각을 말하면 안 된다.

    cron 이 정본이고 표기는 사본이다 — 스케줄을 옮기면서 표기만 남으면, 공유 화면이
    실제로 도는 시각과 다른 시각을 안내한다.
    """
    for cadence in REGISTRY:
        if cadence.trigger_type != TRIGGER_SCHEDULE:
            assert cadence.interval_ko, f"{cadence.dag_id}: 표기가 비었다"
            continue
        minute, hour = cadence.cron.split()[:2]
        assert f"{int(hour):02d}:{int(minute):02d}" in cadence.interval_ko, (
            f"{cadence.dag_id}: 표기 {cadence.interval_ko!r} 가 cron {cadence.cron!r} 과 다르다"
        )
