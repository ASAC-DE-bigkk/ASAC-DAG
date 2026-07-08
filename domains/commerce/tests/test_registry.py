"""dataset_registry.yaml 무결성 — 개수(51종)·중복 없음·필수필드.

수집 대상이 늘어날 때 **short(=bronze 파일명)·service_name(LOCALDATA 코드)·oa_id 중복**을 조기에
잡는 가드. short 가 겹치면 bronze `<short>.jsonl` 이 서로 덮어써 데이터가 오염된다.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_registry.py -q
"""
from commerce_core import registry

EXPECTED_COUNT = 139       # 39 → 51(#207 12종) → 107(문화 56종) → 139(산업 32종)

# service_name 은 대부분 LOCALDATA_ 접두이나 포털이 비표준명을 준 예외가 있다(라이브 확인).
_NON_LOCALDATA_OK = {"repair092801"}       # 계량기수리업(OA-21239)


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


def test_required_fields_and_service_name():
    for d in registry.all_datasets():
        assert d.oa_id and d.name_ko and d.short and d.category, f"필수필드 누락: {d}"
        assert d.service_name, f"service_name 없음: {d.short}"
        assert d.service_name.startswith("LOCALDATA_") or d.service_name in _NON_LOCALDATA_OK, \
            f"service_name 규약 위반(신규 비-LOCALDATA 는 allowlist 등록): {d.short}={d.service_name!r}"


def test_all_daily_collectible():
    """현재 전 종 daily + service_name 존재 → 전 종 수집 대상."""
    assert len(registry.enabled_for_schedule("daily")) == EXPECTED_COUNT
    assert registry.pending_for_schedule("daily") == []      # 미해석(수집 제외) 없음


def test_industry_has_sub_category():
    """대분류(category)=industry 는 명칭분류(sub_category)를 반드시 가진다.
    (sub_category 는 대분류 하위 세분류 — 다른 대분류는 아직 미지정 허용.)"""
    ind = [d for d in registry.all_datasets() if d.category == "industry"]
    assert ind, "industry 데이터셋 없음"
    missing = [d.short for d in ind if not d.sub_category]
    assert missing == [], f"sub_category 누락(industry): {missing}"
