"""gold Iceberg loader — DDL/증분 SQL 생성·식별자 게이트·버킷 분할(순수 로직).

서빙 레이어 개편(PROJECT.md §4): detail 은 카탈로그 구동으로 Iceberg 에 DDL ensure + 멤버별
증분 INSERT INTO SELECT. 여기서는 DB 왕복 없이 SQL 생성 계약을 고정한다.
"""
from __future__ import annotations

import pytest

from gold import loader

_Q = "iceberg_dev.commerce"
_DETAIL = {"object": "silver_food_detail", "kind": "detail_cluster",
           "members": ["bakery", "general_restaurant"], "payload": ["sitearea", "uptaenm"]}


def test_detail_ddl_natural_key_no_bigserial():
    ddl = loader.detail_ddl(_Q, _DETAIL)
    assert "CREATE TABLE IF NOT EXISTS iceberg_dev.commerce.silver_food_detail" in ddl
    for col in loader.DETAIL_KEY_COLUMNS:          # 자연키+버전 키 전부 포함
        assert col in ddl
    assert "sitearea varchar" in ddl and "uptaenm varchar" in ddl
    assert "serial" not in ddl.lower() and "entity_seq" not in ddl   # 서러게이트 없음(§4.2)
    assert "PARQUET" in ddl


def test_detail_insert_sql_snapshot_watermark_and_json_extract():
    sql = loader.detail_insert_sql(_Q, _DETAIL)
    # 워터마크는 **바인딩 파라미터**(member_watermark 스냅샷) — correlated 서브쿼리 금지.
    # 서브쿼리면 버킷0 커밋이 워터마크를 전진시켜 버킷1~k 가 유실된다(2026-07-15 실측 버그).
    assert "WHERE dataset = ?" in sql
    assert "collected_at > coalesce(CAST(? AS timestamp(6)), timestamp '1970-01-01')" in sql
    assert "SELECT coalesce(max(collected_at)" not in sql          # 재평가 서브쿼리 부재(회귀 방지)
    # payload 는 record_json json 추출(대문자 키)
    assert "json_extract_scalar(record_json, '$.SITEAREA')" in sql
    assert "json_extract_scalar(record_json, '$.UPTAENM')" in sql
    assert "FROM iceberg_dev.commerce.silver_license_history" in sql


def test_bucketed_inserts_share_one_watermark_window():
    # k개 버킷 문이 전부 같은 창(같은 바인딩 자리)을 쓰는지 — 버킷별 SQL 차이는 bucket 술어뿐.
    base = loader.detail_insert_sql(_Q, _DETAIL, bucket=(0, 3))
    for b in range(1, 3):
        s = loader.detail_insert_sql(_Q, _DETAIL, bucket=(b, 3))
        assert s.replace(f", 3) = {b}", ", 3) = 0") == base
        assert s.count("?") == 2                                    # (member, wm) 고정


def test_detail_insert_sql_bucket_pred():
    sql = loader.detail_insert_sql(_Q, _DETAIL, bucket=(2, 5))
    assert "mod(from_base(substr(content_hash, 1, 8), 16), 5) = 2" in sql
    assert "mod(" not in loader.detail_insert_sql(_Q, _DETAIL)       # 기본은 버킷 없음


def test_identifier_gate_blocks_injection():
    bad_obj = {**_DETAIL, "object": "x; drop table y"}
    bad_col = {**_DETAIL, "payload": ["good", "bad-col;--"]}
    bad_member = {**_DETAIL, "members": ["ok", "1' or '1"]}
    for bad in (bad_obj, bad_col, bad_member):
        with pytest.raises(Exception):
            loader.detail_ddl(_Q, bad)
        with pytest.raises(Exception):
            loader.detail_insert_sql(_Q, bad)
