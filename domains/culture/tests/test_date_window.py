"""날짜창(stdate/eddate) 진실원 단일화 회귀 그물.

DATE_WINDOW_ENDPOINTS 를 Dataset.uses_date_window 에서 파생하도록 바꾼 뒤(드리프트
제거), (1) 파생 집합이 종전 리터럴과 정확히 같고 (2) 플래그와 늘 일치하며 (3) endpoint
키잉이 유지돼 kopis_detail fallback 정합이 깨지지 않음을 고정한다.
"""
from __future__ import annotations

from culture_ingest.source.datasets import ALL_DATASETS
from culture_ingest.source.ingest import DATE_WINDOW_ENDPOINTS, IngestOptions, _with_date_window


def test_derived_set_matches_legacy_literal():
    # 파생 전환이 동작을 바꾸지 않았음을 못박는다(종전 하드코딩 값).
    assert DATE_WINDOW_ENDPOINTS == {"pblprfr", "prffest", "boxoffice"}


def test_derived_set_is_single_source_of_truth():
    expected = {ds.endpoint for ds in ALL_DATASETS if ds.uses_date_window}
    assert DATE_WINDOW_ENDPOINTS == expected


def test_window_applied_for_windowed_endpoint():
    opts = IngestOptions(date_from="20260701", date_to="20260710")
    out = _with_date_window("pblprfr", {"signgucode": "11"}, opts)
    assert out["stdate"] == "20260701" and out["eddate"] == "20260710"
    assert out["signgucode"] == "11"  # base_params 보존


def test_no_window_for_non_windowed_endpoint():
    # prfplc(시설)는 창 데이터셋 아님 — detail fallback 이 endpoint 로 키잉해도 창을 안 받음.
    opts = IngestOptions(date_from="20260701", date_to="20260710")
    out = _with_date_window("prfplc", {"signgucode": "11"}, opts)
    assert "stdate" not in out and "eddate" not in out
