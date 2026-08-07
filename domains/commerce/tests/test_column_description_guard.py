"""게시되는 컬럼에 설명이 비면 반드시 드러나야 한다 — ASAC-DBT#434.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_column_description_guard.py -q

소비자(사람·MCP/AI)는 `d1_catalog_columns` 만 보고 컬럼 의미를 판단한다. 빈 설명은 "설명이 없다"가
아니라 **"이 제품은 덜 만들어졌다"** 로 읽힌다.

운영 실측(2026-08-05)에서 commerce 222 컬럼 중 1건(`commerce_uptae_rollup.share`)이 비어 있었다.
원인은 **롤업이 export 시점에 만들어 내는 파생 컬럼**이라 gold 에 없고, 그래서 dbt `columns:` 로
선언할 수 없었던 것이다(`contract.enforced` 가 gold 실출력과 대조한다). 그 자리로
`meta.serving.d1_derived_columns` 가 이미 코드에 마련돼 있었는데 **한 번도 쓰이지 않았고**,
비어도 아무 신호가 없어 드러나지 않았다.

게시 자체는 막지 않는다 — 설명 하나 때문에 직전본을 유지하는 쪽이 더 나쁘다. 대신 **무엇이 비었는지
컬럼 이름까지** 기록한다.
"""
from __future__ import annotations

import types

import pytest

from gold import serving_export as se


@pytest.fixture
def spec():
    return types.SimpleNamespace(
        d1_table="d1_uptae_rollup", tier="d1_rollup", source="gold_detail_uptae_mix")


def _events(monkeypatch) -> list[tuple[str, dict]]:
    seen: list[tuple[str, dict]] = []
    monkeypatch.setattr(se, "log_event", lambda name, **kw: seen.append((name, kw)))
    return seen


COLS = [("major", "varchar"), ("uptaenm", "varchar"), ("active_cnt", "bigint"),
        ("share", "double")]


def test_missing_description_is_reported_with_column_names(spec, monkeypatch):
    seen = _events(monkeypatch)
    meta = {"columns": {"major": "대분류", "uptaenm": "세부 업태", "active_cnt": "영업 중 업소 수"},
            "serving": {}}                       # share 미선언 → 비어서 나감

    rows, _ext, _pat, _disp = se._handoff_rows(spec, meta, COLS, "pub-1")

    assert [r["column_name"] for r in rows if not r["description_ko"]] == ["share"]
    assert len(seen) == 1
    name, kw = seen[0]
    assert name == "serve.column_description_missing"
    assert kw["level"] == "warning"
    assert kw["columns"] == ["share"]             # 이름까지 남긴다 — 개수만으론 못 고친다
    assert kw["settled_rows"] == 4 and kw["affected_rows"] == 1
    assert kw["affected_ratio_pct"] == 25.0
    assert "d1_derived_columns" in kw["hint"]     # 어디에 채우면 되는지 알려준다


def test_derived_column_declaration_fills_the_gap(spec, monkeypatch):
    """`meta.serving.d1_derived_columns` 가 gold 에 없는 파생 컬럼의 설명 자리다."""
    seen = _events(monkeypatch)
    meta = {"columns": {"major": "대분류", "uptaenm": "세부 업태", "active_cnt": "영업 중 업소 수"},
            "serving": {"d1_derived_columns": {"share": "같은 dataset 안 구성비(0~1)"}}}

    rows, _ext, _pat, _disp = se._handoff_rows(spec, meta, COLS, "pub-1")

    assert {r["column_name"]: r["description_ko"] for r in rows}["share"] == (
        "같은 dataset 안 구성비(0~1)")
    assert seen == [], "전부 채워졌는데 경보가 났습니다"


def test_publication_is_not_blocked_by_a_missing_description(spec, monkeypatch):
    """설명 하나 때문에 게시를 막으면 직전본이 그대로 남는다 — 그게 더 나쁘다."""
    _events(monkeypatch)
    rows, ext, _pat, _disp = se._handoff_rows(spec, {"columns": {}, "serving": {}}, COLS, "pub-1")

    assert len(rows) == len(COLS)                 # 행은 그대로 만들어진다
    assert all(r["publication_id"] == "pub-1" for r in rows)
    assert ext["table_name"] == "d1_uptae_rollup"


def test_dbt_columns_win_nothing_when_derived_overrides(spec, monkeypatch):
    """같은 이름이면 파생 선언이 우선 — 롤업이 값의 의미를 바꿔 놓기 때문이다."""
    _events(monkeypatch)
    meta = {"columns": {"share": "gold 쪽 설명"},
            "serving": {"d1_derived_columns": {"share": "롤업 기준 구성비"}}}

    rows, _ext, _pat, _disp = se._handoff_rows(spec, meta, [("share", "double")], "pub-1")
    assert rows[0]["description_ko"] == "롤업 기준 구성비"
