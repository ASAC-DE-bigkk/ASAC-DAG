"""통신판매업 v2→v1 Raw 정본화 계약."""
from __future__ import annotations

import pytest

import json

from bronze import bronze_tasks, incremental
from commerce_core import registry
from commerce_core.schemas import (
    CANONICAL_MAPPING_VERSION,
    COLUMN_ALIASES_V2,
    Dataset,
    SchemaCanonicalizationError,
    canonicalize_row,
)


def _v2_row() -> dict:
    return {alias: f"value-{i}" for i, alias in enumerate(COLUMN_ALIASES_V2.values())}


def test_mapping_is_an_exact_25_to_25_bijection():
    assert len(COLUMN_ALIASES_V2) == 25
    assert len(set(COLUMN_ALIASES_V2.values())) == 25


def test_v2_to_v1_changes_only_keys_and_is_normalization_equivalent():
    v2 = _v2_row()
    v1 = canonicalize_row(v2, source_format="v2", canonical_format="v1")
    expected = {canonical: v2[alias] for canonical, alias in COLUMN_ALIASES_V2.items()}

    assert set(v1) == set(COLUMN_ALIASES_V2)
    assert list(v1.values()) == [v2[alias] for alias in COLUMN_ALIASES_V2.values()]
    assert canonicalize_row(v1, source_format="v2", canonical_format="v1") == v1
    assert incremental.normalize(v1) == incremental.normalize(expected)


@pytest.mark.parametrize("mutation", ["missing", "unknown", "duplicate"])
def test_partial_or_extended_schema_is_converted_not_rejected(mutation):
    """데이터셋마다 필드 구성이 다르다 — 25필드 정확 일치를 요구하면 아무것도 못 지나간다.

    초판은 `keys == v2_keys` 를 요구했는데, 2026-08-06 실측에서 **전환 12종 중 0종이 통과**했다.
    공통 필드 일부가 없는 업종이 있고(판매방식 `NTSL_MTH_NM` 은 판매업만 가진다), 업종 고유
    필드(`LCTN_AREA`·`CPTL`·`DEL_YMD` …)가 따로 있기 때문이다. 키 단위 치환이 맞다.
    """
    row = _v2_row()
    if mutation == "missing":
        dropped = next(iter(row))
        row.pop(dropped)
    elif mutation == "unknown":
        row["LCTN_AREA"] = "업종 고유 필드"
    else:
        row["MGTNO"] = row["MNG_NO"]        # 같은 값 중복 — 정보 손실 없음

    out = canonicalize_row(row, source_format="v2", canonical_format="v1")

    assert not (set(out) & set(COLUMN_ALIASES_V2.values())), "v2 키가 남았습니다"
    if mutation == "unknown":
        assert out["LCTN_AREA"] == "업종 고유 필드", "고유 필드는 그대로 통과해야 합니다"
    if mutation == "missing":
        assert COLUMN_ALIASES_V2[
            next(k for k, v in COLUMN_ALIASES_V2.items() if v == dropped)] not in out


def test_conflicting_v1_v2_values_fail_closed():
    """v1·v2 이름이 공존하고 **값이 다르면** 어느 쪽이 정본인지 정할 수 없다 — 멈춘다."""
    row = _v2_row()
    row["MGTNO"] = "v1 쪽 값"
    row["MNG_NO"] = "v2 쪽 값"

    with pytest.raises(SchemaCanonicalizationError):
        canonicalize_row(row, source_format="v2", canonical_format="v1")


def test_conversion_is_idempotent_on_already_canonical_rows():
    v1 = canonicalize_row(_v2_row(), source_format="v2", canonical_format="v1")
    assert canonicalize_row(v1, source_format="v2", canonical_format="v1") == v1


def test_mail_order_registry_declares_v2_source_and_v1_canonical():
    dataset = registry.by_short("mail_order_sale")
    assert dataset.fmt == "v2"
    assert dataset.canonical_fmt == "v1"


def test_full_diff_finds_tail_change_after_an_aligned_head():
    """timestamp가 안 바뀐 실제 수정도 첫 일치 뒤에서 놓치지 않는다."""
    prev = [
        {"MGTNO": "2", "OPNSFTEAMCODE": "A", "UPDATEDT": "2026-02-01", "BPLCNM": "same"},
        {"MGTNO": "1", "OPNSFTEAMCODE": "A", "UPDATEDT": "2026-01-01", "BPLCNM": "old"},
    ]
    today = [dict(prev[0]), {**prev[1], "BPLCNM": "changed-without-new-timestamp"}]

    assert list(incremental.diff_new_rows(
        today, prev, stop_on_aligned_match=True)) == []
    assert list(incremental.diff_new_rows(
        today, prev, stop_on_aligned_match=False)) == [today[1]]


class _Storage:
    def __init__(self):
        self.data = {}

    def write_json(self, key, value):
        self.data[key] = json.dumps(value, ensure_ascii=False).encode()

    def exists(self, key):
        return key in self.data

    def delete(self, key):
        self.data.pop(key, None)


def test_bronze_task_canonicalizes_before_diff_and_records_lineage(monkeypatch):
    captured = {}

    def fake_store(_storage, **kwargs):
        captured.update(kwargs)
        captured["rows"] = list(kwargs["rows"])
        return {"mode": "identical", "key": "k", "count": 1, "increment_count": 0,
                "increment_key": None, "target_key": "diff.jsonl"}

    monkeypatch.setattr(incremental, "find_diff_target", lambda *_a, **_k: (None, None))
    monkeypatch.setattr(incremental, "incremental_store", fake_store)
    dataset = Dataset(
        oa_id="OA-16100", name_ko="통신판매업", short="mail_order_sale",
        category="industry", schedule="daily", service_name="LOCALDATA_082604",
        fmt="v2", canonical_fmt="v1")
    page = json.dumps({"LOCALDATA_082604": {
        "list_total_count": 1, "RESULT": {"CODE": "INFO-000", "MESSAGE": "정상"},
        "row": [_v2_row()]}}).encode()
    storage = _Storage()

    bronze_tasks._write_bronze(
        storage, prefix="", bronze_run_id="2026-08-07_000000_001", dataset=dataset,
        raw_pages=[page], page_metas=[{"page": 1}],
        base={"observed_date": "2026-08-06", "run_id": "dag", "bronze_run_id": "run"},
        status="ok", rows_total=1, list_total_count=1, complete=True,
        schema_version="v1", base_url="https://example.invalid", started_at="t")

    assert captured["stop_on_aligned_match"] is False
    assert set(captured["rows"][0]) == set(COLUMN_ALIASES_V2)
    marker_key = next(k for k in storage.data if k.endswith("mail_order_sale.completed"))
    marker = json.loads(storage.data[marker_key])
    assert marker["source_format"] == "v2"
    assert marker["canonical_format"] == "v1"
    assert marker["canonical_mapping_version"] == CANONICAL_MAPPING_VERSION


# ── 데이터셋 스코프 도메인 별칭(2026-08-07 유도 · 54쌍) ──────────────────────────
# 전환은 업종 고유 필드까지 개명했다. 전역표 확장이 불가한 이유(CAPT→CPTL vs
# CAPTSCALE→CPTL 충돌)와, 데이터셋별 오버레이가 실제로 적용되는 경로를 못박는다.

def test_dataset_overlay_v2_names_collide_across_datasets():
    """같은 v2 이름이 데이터셋마다 다른 v1 에서 온다 — 전역 역표가 성립하지 않는 근거."""
    from commerce_core.schemas import DATASET_COLUMN_ALIASES_V2 as D

    assert D["groundwater_construction"]["CAPT"] == "CPTL"
    assert D["mutual_aid_funeral"]["CAPTSCALE"] == "CPTL"


def test_overlay_rekeys_domain_fields_per_dataset():
    from commerce_core.schemas import DATASET_COLUMN_ALIASES_V2, canonicalize_row

    row = {"MNG_NO": "m-1", "LCTN_AREA": "84.3", "RGHT_MNBD_SN": "2"}
    out = canonicalize_row(row, source_format="v2", canonical_format="v1",
                           extra_aliases=DATASET_COLUMN_ALIASES_V2["animal_sale"])
    assert out == {"MGTNO": "m-1", "SITEAREA": "84.3", "RGTMBDSNO": "2"}


def test_without_overlay_domain_fields_pass_through_unchanged():
    """오버레이 없는 데이터셋(예: 처음부터 v2 인 13종)은 고유 필드를 건드리지 않는다."""
    from commerce_core.schemas import canonicalize_row

    row = {"MNG_NO": "m-1", "LCTN_AREA": "84.3"}
    out = canonicalize_row(row, source_format="v2", canonical_format="v1")
    assert out == {"MGTNO": "m-1", "LCTN_AREA": "84.3"}


def test_canonicalize_dataset_row_applies_overlay_by_short():
    from commerce_core.schemas import Dataset, canonicalize_dataset_row

    ds = Dataset(oa_id="x", name_ko="동물판매업", short="animal_sale", category="livestock",
                 schedule="daily", service_name="SVC", fmt="v2", canonical_fmt="v1")
    out = canonicalize_dataset_row(ds, {"MNG_NO": "m-1", "LCTN_AREA": "84.3"})
    assert out == {"MGTNO": "m-1", "SITEAREA": "84.3"}


def test_overlay_conflict_same_row_both_names_fails_closed():
    from commerce_core.schemas import DATASET_COLUMN_ALIASES_V2, canonicalize_row

    row = {"MNG_NO": "m-1", "SITEAREA": "84.3", "LCTN_AREA": "99.9"}
    with pytest.raises(SchemaCanonicalizationError):
        canonicalize_row(row, source_format="v2", canonical_format="v1",
                         extra_aliases=DATASET_COLUMN_ALIASES_V2["animal_sale"])


def test_overlay_registry_shorts_and_bijection():
    """오버레이 키는 실제 등록 데이터셋이어야 하고, 짝은 데이터셋 안에서 전단사여야 한다."""
    from commerce_core.schemas import DATASET_COLUMN_ALIASES_V2 as D

    registered = {d.short for d in registry.all_datasets()}
    assert set(D) <= registered
    assert len(D) == 12                                   # 2026-08-04 전환 12종(전수 유도)
    for short, pairs in D.items():
        assert len(set(pairs.values())) == len(pairs), short
        canonical = registry.by_short(short)
        assert canonical.fmt == "v2" and canonical.canonical_fmt == "v1", short
