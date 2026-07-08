"""#206 — 자정/주간 크롤 분리: plan 데이터셋 선택 로직."""
from __future__ import annotations

from culture_ingest.source.datasets import (
    BY_NAME,
    WEEKLY_FACILITY_REFRESH_CONF,
    plan_dataset_names,
)


def test_scheduled_run_excludes_weekly_datasets():
    names = plan_dataset_names([], include_detail=True)
    assert "kopis_facility_detail" not in names  # weekly(정적 dim)는 자정런 제외
    assert "kopis_performance_detail" in names   # 공연 상세(fact)는 유지
    assert "kopis_facility" in names             # 시설 목록은 유지


def test_explicit_selection_includes_weekly():
    names = plan_dataset_names(
        ["kopis_facility", "kopis_facility_detail"], include_detail=True
    )
    assert names == ["kopis_facility", "kopis_facility_detail"]  # 목록이 detail 앞


def test_include_detail_false_still_excludes_details():
    names = plan_dataset_names([], include_detail=False)
    assert all(BY_NAME[n].kind != "kopis_detail" for n in names)


def test_details_sorted_last_for_id_reuse():
    names = plan_dataset_names([], include_detail=True)
    kinds = [BY_NAME[n].kind for n in names]
    tail = kinds[kinds.index("kopis_detail"):] if "kopis_detail" in kinds else []
    assert all(k == "kopis_detail" for k in tail)  # detail 은 전부 뒤(#146 id 재사용)


def test_weekly_refresh_conf_contract():
    assert all(n in BY_NAME for n in WEEKLY_FACILITY_REFRESH_CONF["datasets"])
    assert "kopis_facility_detail" in WEEKLY_FACILITY_REFRESH_CONF["datasets"]
    assert WEEKLY_FACILITY_REFRESH_CONF["max_detail"] >= 1700  # 시설 1,686 + 여유
    assert WEEKLY_FACILITY_REFRESH_CONF["include_detail"] is True
