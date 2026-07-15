"""gold 카탈로그 규칙(순수 로직) — 엄격 경계·명명·버전 결정성."""
import pytest

from gold import catalog_rules as cr

# 합성 필드셋: 공통코어(전 종) + cluster 후보 3종(공유 9필드, general_restaurant 포함) + 단독 2종
_CORE = {f"COMMON{i}" for i in range(14)}
_SHARED = {f"FOOD{i}" for i in range(9)}          # >= SHARED_MIN(8)


def _fields():
    return {
        "general_restaurant": _CORE | _SHARED | {"GR_ONLY"},
        "bakery": _CORE | _SHARED,
        "food_mfg": _CORE | _SHARED | {"MFG_ONLY"},
        "pharmacy": _CORE | {"PHARMTRDAR", "ASGNYMD"},
        "optical_shop": _CORE | {"LENSCUTNUM"},
    }


def _meta():
    return {s: {"fmt": "v1"} for s in _fields()}


def test_strict_cluster_and_singles():
    cat = cr.build_catalog(_fields(), _meta())
    by_kind = {}
    for d in cat["details"]:
        by_kind.setdefault(d["kind"], []).append(d)
    # 명확한 겹침(3종·공유9) → cluster 1개, 도메인 명명 적용
    assert len(by_kind["detail_cluster"]) == 1
    cl = by_kind["detail_cluster"][0]
    assert cl["object"] == "silver_food_sanitation_business_detail"
    assert cl["members"] == ["bakery", "food_mfg", "general_restaurant"]
    # payload = 비공통 합집합 lowercase(공통코어 제외)
    assert "food0" in cl["payload"] and "gr_only" in cl["payload"]
    assert not any(c.startswith("common") for c in cl["payload"])
    # 나머지는 전부 단독
    singles = {d["object"] for d in by_kind["detail_single"]}
    assert singles == {"silver_pharmacy_detail", "silver_optical_shop_detail"}


def test_dataset_map_branching():
    cat = cr.build_catalog(_fields(), _meta())
    m = cat["dataset_map"]
    assert m["bakery"]["detail_table"] == "silver_food_sanitation_business_detail"
    assert m["bakery"]["entity_type"] == "food_sanitation_business"
    assert m["pharmacy"]["detail_table"] == "silver_pharmacy_detail"


def test_version_deterministic_and_drift():
    v1 = cr.build_catalog(_fields(), _meta())["version"]
    v2 = cr.build_catalog(_fields(), _meta())["version"]
    assert v1 == v2                                   # 같은 입력 = 같은 버전
    changed = _fields()
    changed["pharmacy"] = changed["pharmacy"] | {"NEW_FIELD"}
    assert cr.build_catalog(changed, _meta())["version"] != v1   # 필드 드리프트 = 버전 변경


def test_unnamed_cluster_fails():
    """이름 미정 cluster 는 무단 자동명명 없이 실패해야 한다."""
    fields = {f"mystery_{i}": _CORE | _SHARED for i in range(3)}   # 3종·공유9, 명명맵에 없음
    # 필러 4종(고유 필드) — 공유필드가 공통코어(>=90%)로 흡수되지 않게 분모를 늘린다.
    fields.update({f"filler_{i}": _CORE | {f"F{i}"} for i in range(4)})
    with pytest.raises(ValueError):
        cr.build_catalog(fields, {s: {"fmt": "v1"} for s in fields})


def test_small_overlap_stays_single():
    """멤버·공유가 엄격 기준 미달이면 유사해도 단독(명확한 것만 공통화)."""
    fields = {
        "tobacco_import_sale": _CORE | {"T1", "T2", "T3"},
        "tobacco_wholesale": _CORE | {"T1", "T2", "T3"},          # 2종(멤버<3) — 단독 유지
    }
    cat = cr.build_catalog(fields, {s: {"fmt": "v1"} for s in fields})
    assert all(d["kind"] == "detail_single" for d in cat["details"])
