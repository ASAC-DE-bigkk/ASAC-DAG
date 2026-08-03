"""D1 카탈로그 컬럼 설명 — 롤업 파생 컬럼도 뜻을 가지는지 (#406).

API/MCP 소비자는 저장소 접근 없이 **D1 만 보고** 컬럼 뜻을 판단한다(#638 §2.1). 그런데
`uptae_rollup.share` 같은 롤업 파생 컬럼은 gold 에 없어서 dbt `columns:` 로 선언할 수 없다 —
`contract.enforced` 가 gold 실출력과 대조하므로 선언하면 빌드가 깨진다. 그래서 설명을 둘 자리
자체가 없었고, D1 전수 실측에서 이 컬럼만 `description_ko` 가 NULL 이었다.

`meta.serving.d1_derived_columns` 가 그 자리다(정본은 여전히 dbt yml). 이 테스트가 고정하는 것:
파생 컬럼 설명이 게시되는가, 그리고 **선언이 없으면 지어내지 않는가**.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

SPEC_ARGS = ("gold_detail_uptae_mix", "d1_uptae_rollup", "d1_rollup", "SELECT 1", (1, 10))
COL_DEFS = [("uptaenm", "varchar"), ("share", "double")]


def test_derived_column_description_is_published():
    from gold.serving_export import Serve, _handoff_rows

    meta = {"columns": {"uptaenm": "업태명."},
            "serving": {"d1_derived_columns": {"share": "dataset 내 영업 업소 비중(0~1)."}}}
    col_rows, _ext, _pats = _handoff_rows(Serve(*SPEC_ARGS), meta, COL_DEFS, "pub1")

    by_name = {r["column_name"]: r for r in col_rows}
    assert by_name["uptaenm"]["description_ko"] == "업태명."                      # gold 선언분
    assert by_name["share"]["description_ko"] == "dataset 내 영업 업소 비중(0~1)."  # 파생분


def test_missing_declaration_stays_empty_not_invented():
    """선언이 없으면 비운다 — 틀린 설명이 붙는 것보다 빈 설명이 낫다."""
    from gold.serving_export import Serve, _handoff_rows

    col_rows, _ext, _pats = _handoff_rows(Serve(*SPEC_ARGS), {"columns": {}}, COL_DEFS, "pub1")
    assert all(r["description_ko"] is None for r in col_rows)


def test_gold_declaration_wins_over_nothing_and_derived_never_overwrites_it():
    """이름이 겹치면 파생 선언이 gold 설명을 덮는다 — 롤업이 그 컬럼의 의미를 바꾸기 때문."""
    from gold.serving_export import Serve, _handoff_rows

    meta = {"columns": {"share": "gold 원본 의미."},
            "serving": {"d1_derived_columns": {"share": "롤업 재계산 의미."}}}
    col_rows, _ext, _pats = _handoff_rows(
        Serve(*SPEC_ARGS), meta, [("share", "double")], "pub1")
    assert col_rows[0]["description_ko"] == "롤업 재계산 의미."
