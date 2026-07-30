"""#206 — 자정/주간 크롤 분리: plan 데이터셋 선택 로직."""
from __future__ import annotations

from culture_ingest.source.datasets import (
    BY_NAME,
    WEEKLY_FACILITY_REFRESH_CONF,
    plan_dataset_names,
)


def test_scheduled_run_includes_facility_detail():
    names = plan_dataset_names([], include_detail=True)
    assert "kopis_facility_detail" in names      # #466: 야간 top-up 편입(missing 모드)
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


def test_first_wave_is_not_all_one_source():
    """#201 — 매핑 슬롯에 먼저 들어가는 앞자리가 한 원천으로 몰리면 안 된다.

    상한 2 짜리 첫 웨이브가 KOPIS 2발이면 완화가 반쪽이 된다. 원천 라운드로빈이라
    앞 두 자리는 서로 다른 원천이어야 한다.
    """
    names = plan_dataset_names([], include_detail=True)
    head = [BY_NAME[n].source for n in names[:2]]
    assert len(set(head)) == 2, f"첫 두 자리가 같은 원천이다: {head}"


def test_lists_alternate_sources_while_details_stay_last():
    """섞기는 목록 구간에만 적용되고 detail 후순위(#146)는 그대로 유지된다."""
    names = plan_dataset_names([], include_detail=True)
    kinds = [BY_NAME[n].kind for n in names]
    list_names = [n for n in names if BY_NAME[n].kind != "kopis_detail"]
    sources = [BY_NAME[n].source for n in list_names]
    # 목록 구간에 같은 원천이 연달아 오는 자리는, 남은 원천이 동나는 꼬리에만 있어야 한다
    runs = sum(1 for a, b in zip(sources, sources[1:]) if a == b)
    assert runs < len(sources) // 2, f"원천이 여전히 묶여 있다: {sources}"
    assert "kopis_detail" not in kinds[:len(list_names)]


def test_every_selected_dataset_survives_interleaving():
    """섞기가 데이터셋을 잃거나 중복시키면 안 된다 — 순서만 바뀌어야 한다."""
    for include_detail in (True, False):
        names = plan_dataset_names([], include_detail=include_detail)
        assert len(names) == len(set(names))
        expected = {
            ds.name
            for ds in BY_NAME.values()
            if ds.enabled and ds.refresh == "daily"
            and (include_detail or ds.kind != "kopis_detail")
        }
        assert set(names) == expected


def test_weekly_refresh_conf_contract():
    assert all(n in BY_NAME for n in WEEKLY_FACILITY_REFRESH_CONF["datasets"])
    assert "kopis_facility_detail" in WEEKLY_FACILITY_REFRESH_CONF["datasets"]
    assert WEEKLY_FACILITY_REFRESH_CONF["max_detail"] >= 1700  # 시설 1,686 + 여유
    assert WEEKLY_FACILITY_REFRESH_CONF["include_detail"] is True
    assert WEEKLY_FACILITY_REFRESH_CONF["detail_mode"] == "full"  # 주간은 전수(#466)
