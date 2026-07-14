"""scripts/purge_v2_environment — 순수 분류 로직 테스트(v2 만 선택, v1/_RUN 무접촉).

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_purge_v2.py -q
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import purge_v2_environment as pv  # noqa: E402

V2 = {"water_pollution_facility", "air_pollution_facility"}


def test_classify_raw_keys_selects_only_v2():
    keys = [
        # v2 — 전부 매칭되어야 함
        "raw/commerce/2026/07/13/run_id=2026-07-13_000001_000/water_pollution_facility.jsonl",
        "raw/commerce/2026/07/13/run_id=2026-07-13_000001_000/_full/air_pollution_facility.jsonl",
        "raw/commerce/2026/07/13/run_id=2026-07-13_000001_000/_markers/water_pollution_facility.completed",
        "raw/commerce/2026/07/14/run_id=2026-07-14_000001_000/_markers/air_pollution_facility.incomplete",
        "raw/commerce/_diff_target/water_pollution_facility.2026-07-14.jsonl",
        "raw/commerce/_diff_target/water_pollution_facility.2026-07-14.key",
        "raw/commerce/_diff_target/air_pollution_facility.jsonl",       # 구형(무날짜)도 매칭
        # v1/무관 — 절대 매칭 금지
        "raw/commerce/2026/07/13/run_id=2026-07-13_000001_000/general_restaurant.jsonl",
        "raw/commerce/2026/07/13/run_id=2026-07-13_000001_000/_markers/general_restaurant.completed",
        "raw/commerce/2026/07/13/run_id=2026-07-13_000001_000/_markers/_RUN.completed",
        "raw/commerce/_diff_target/general_restaurant.2026-07-14.jsonl",
        # 유사 이름(부분 문자열) — stem 정확 일치가 아니면 금지
        "raw/commerce/_diff_target/water_pollution_facility_v2.2026-07-14.jsonl",
    ]
    hit = pv.classify_raw_keys(keys, V2)
    assert len(hit["increments"]) == 1
    assert len(hit["landings"]) == 1
    assert len(hit["markers"]) == 2
    assert len(hit["diff_targets"]) == 3
    flat = [k for group in hit.values() for k in group]
    assert not any("general_restaurant" in k or "_RUN" in k or "_v2" in k for k in flat)


def test_strip_watermark_and_pending():
    wm, n = pv.strip_shorts_from_watermark(
        {"water_pollution_facility": "2026-07-14_000001_000", "general_restaurant": "r1"}, V2)
    assert wm == {"general_restaurant": "r1"} and n == 1
    pending, n2 = pv.strip_shorts_from_pending(
        [{"date": "2026-07-13", "short": "air_pollution_facility"},
         {"date": "2026-07-13", "short": "pharmacy"}], V2)
    assert pending == [{"date": "2026-07-13", "short": "pharmacy"}] and n2 == 1


def test_classify_receipt_keys():
    keys = [
        "commerce_bronze_state/receipts/2026-07-13/2026-07-13_000001_000__water_pollution_facility.json",
        "commerce_bronze_state/receipts/2026-07-13/2026-07-13_000001_000__general_restaurant.json",
        "commerce_bronze_state/receipts/2026-07-13/malformed.json",
    ]
    hit = pv.classify_receipt_keys(keys, V2)
    assert hit == [keys[0]]


def test_v2_detail_objects_from_catalog():
    rows = [
        ("commerce_env_facility_detail", "water_pollution_facility air_pollution_facility"),
        ("commerce_restaurant_detail", "general_restaurant rest_restaurant"),
        ("commerce_mixed_detail", "pharmacy air_pollution_facility"),   # 혼재 클러스터도 대상
    ]
    assert pv.v2_detail_objects(rows, V2) == ["commerce_env_facility_detail", "commerce_mixed_detail"]
