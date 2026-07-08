"""dataset_registry.yaml 무결성 — 개수(51종)·중복 없음·필수필드.

수집 대상이 늘어날 때 **short(=bronze 파일명)·service_name(LOCALDATA 코드)·oa_id 중복**을 조기에
잡는 가드. short 가 겹치면 bronze `<short>.jsonl` 이 서로 덮어써 데이터가 오염된다.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_registry.py -q
"""
from commerce_core import registry

EXPECTED_COUNT = 107       # 39(원본) → 51(#207 보건/식품 12종) → 107(문화 상권 56종)


def _dups(values):
    seen, dups = set(), []
    for v in values:
        (dups.append(v) if v in seen else seen.add(v))
    return dups


def test_dataset_count():
    assert len(registry.all_datasets()) == EXPECTED_COUNT


def test_short_unique():
    shorts = [d.short for d in registry.all_datasets()]
    assert _dups(shorts) == [], f"중복 short: {_dups(shorts)}"


def test_service_name_unique():
    codes = [d.service_name for d in registry.all_datasets() if d.service_name]
    assert _dups(codes) == [], f"중복 service_name(LOCALDATA 코드): {_dups(codes)}"


def test_oa_id_unique():
    ids = [d.oa_id for d in registry.all_datasets()]
    assert _dups(ids) == [], f"중복 oa_id: {_dups(ids)}"


def test_required_fields_and_code_prefix():
    for d in registry.all_datasets():
        assert d.oa_id and d.name_ko and d.short and d.category, f"필수필드 누락: {d}"
        assert d.service_name and d.service_name.startswith("LOCALDATA_"), \
            f"service_name 규약 위반: {d.short}={d.service_name!r}"


def test_all_daily_collectible():
    """현재 전 종 daily + service_name 존재 → 51종 전부 수집 대상."""
    assert len(registry.enabled_for_schedule("daily")) == EXPECTED_COUNT
    assert registry.pending_for_schedule("daily") == []      # 미해석(수집 제외) 없음
