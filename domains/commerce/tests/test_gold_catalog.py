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


def test_unnamed_cluster_falls_back_to_singles_without_losing_data():
    """이름 미정 cluster 는 **무단 자동명명도, 파이프라인 중단도 하지 않는다.**

    예전에는 여기서 ValueError 를 던졌는데, 그 한 번이 detail 적재 전체를 막아 silver detail
    0건이 됐다(2026-08-04 운영 사고). 지금은 클러스터를 포기하고 단건으로 떨어뜨린다 —
    임계 미달일 때와 같은 처리라 **데이터를 하나도 잃지 않는다**.

    대신 조용히 넘어가면 안 되므로 `pending_cluster_names` 로 올라온다.
    """
    fields = {f"mystery_{i}": _CORE | _SHARED for i in range(3)}   # 3종·공유9, 명명맵에 없음
    # 필러 4종(고유 필드) — 공유필드가 공통코어(>=90%)로 흡수되지 않게 분모를 늘린다.
    fields.update({f"filler_{i}": _CORE | {f"F{i}"} for i in range(4)})
    cat = cr.build_catalog(fields, {s: {"fmt": "v1"} for s in fields})

    # 이름 없는 3종은 각각 단건 테이블로 — 자동으로 이름을 지어내지 않는다
    objects = {d["object"] for d in cat["details"]}
    assert {f"silver_mystery_{i}_detail" for i in range(3)} <= objects
    assert all(d["kind"] == "detail_single" for d in cat["details"]
               if d["members"][0].startswith("mystery_"))
    # 그리고 후속 과제로 올라온다
    pending = cat["pending_cluster_names"]
    assert len(pending) == 1
    assert sorted(pending[0]["members"]) == [f"mystery_{i}" for i in range(3)]


def test_named_cluster_still_merges():
    """이름이 있으면 지금까지처럼 하나로 합친다 — 폴백이 정상 경로를 바꾸지 않는다."""
    named = next(iter(cr.NAME_BY_MEMBER))
    fields = {named: _CORE | _SHARED}
    fields.update({f"peer_{i}": _CORE | _SHARED for i in range(2)})
    fields.update({f"filler_{i}": _CORE | {f"F{i}"} for i in range(4)})
    cat = cr.build_catalog(fields, {s: {"fmt": "v1"} for s in fields})
    clusters = [d for d in cat["details"] if d["kind"] == "detail_cluster"]
    assert len(clusters) == 1 and named in clusters[0]["members"]
    assert cat["pending_cluster_names"] == []


def test_small_overlap_stays_single():
    """멤버·공유가 엄격 기준 미달이면 유사해도 단독(명확한 것만 공통화)."""
    fields = {
        "tobacco_import_sale": _CORE | {"T1", "T2", "T3"},
        "tobacco_wholesale": _CORE | {"T1", "T2", "T3"},          # 2종(멤버<3) — 단독 유지
    }
    cat = cr.build_catalog(fields, {s: {"fmt": "v1"} for s in fields})
    assert all(d["kind"] == "detail_single" for d in cat["details"])
