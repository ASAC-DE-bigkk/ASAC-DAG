"""detail payload 추출이 v1/v2 양쪽을 본다 — 원천 컬럼 표준 전환에 골드가 안 무너지게.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_detail_v2_alias.py -q

2026-08-04 `mail_order_sale` 이 v1 → v2 컬럼 표준으로 전환됐다(값 동일, 키 이름만 교체).
실측: 그 run 의 행에 `$.UPTAENM` 0건 · `$.BZSTAT_SE_NM` 386,800건.

브론즈·실버는 무사했다 — 식별키는 `canonical_get` 이, 실버 공통 컬럼은 `lf()` 별칭이 흡수한다.
그런데 **detail 은 실버의 정규화 컬럼이 아니라 원형 보존된 `record_json` 에서 payload 를 뽑는다.**
그래서 `lf()` 보호가 닿지 않고, v2 행의 `uptaenm` 이 통째로 빈다.

`uptaenm` 은 `gold_detail_uptae_mix` 의 GROUP BY 축이자 `where uptaenm is not null` 조건이라,
비면 행이 집계에서 탈락한다. 그 골드는 외부 공개 제품(`d1_uptae_rollup`, external=1)이고
mail_order 가 게시본의 97.5%(7,486/7,676)를 차지한다. 행수 밴드 하한이 1이라 게이트도 못 막는다.
"""
from __future__ import annotations

import pytest

from commerce_core.schemas import COLUMN_ALIASES_V2, canonical_get
from gold import loader


DETAIL = {"object": "silver_mail_order_sale_detail", "kind": "single",
          "members": ["mail_order_sale"], "payload": ["uptaenm", "sitearea"]}


# ── 매핑 자체 ────────────────────────────────────────────────────────────────
def test_uptaenm_pairs_with_the_v2_name_seen_in_prod():
    assert COLUMN_ALIASES_V2["UPTAENM"] == "BZSTAT_SE_NM"


def test_canonical_get_reads_either_standard():
    assert canonical_get({"UPTAENM": "한식"}, "UPTAENM") == "한식"
    assert canonical_get({"BZSTAT_SE_NM": "인터넷"}, "UPTAENM") == "인터넷"


# ── 추출식 ──────────────────────────────────────────────────────────────────
def test_aliased_column_reads_v1_first_then_v2():
    expr = loader._payload_expr("uptaenm")
    assert expr.startswith(", coalesce(")
    assert "'$.UPTAENM'" in expr and "'$.BZSTAT_SE_NM'" in expr
    assert expr.index("'$.UPTAENM'") < expr.index("'$.BZSTAT_SE_NM'")   # v1 정본 우선


def test_column_without_alias_is_untouched():
    """별칭 없는 컬럼은 종전 그대로 — 불필요한 SQL 변화를 만들지 않는다."""
    assert loader._payload_expr("sitearea") == (
        ", json_extract_scalar(record_json, '$.SITEAREA')")


def test_payload_column_names_and_count_are_unchanged():
    """스키마 무변경 계약: INSERT 컬럼 목록·개수가 그대로여야 한다."""
    sql = loader.detail_insert_sql("cat.sch", DETAIL)
    head = sql.split(") SELECT")[0]
    assert head.endswith(
        "(dataset, opnsfteamcode, mgtno, collected_at, content_hash, uptaenm, sitearea")
    assert sql.count("json_extract_scalar") == 3      # uptaenm 2(v1+v2) + sitearea 1
    assert "FROM cat.sch.silver_license_history" in sql


def test_bucketed_variant_keeps_the_alias():
    sql = loader.detail_insert_sql("cat.sch", DETAIL, bucket=(1, 4))
    assert "'$.BZSTAT_SE_NM'" in sql and "mod(from_base(" in sql


@pytest.mark.parametrize("bad", ["a b", "x;drop", "'", "a-b"])
def test_unsafe_payload_name_is_rejected(bad):
    with pytest.raises(Exception):
        loader._payload_expr(bad)
