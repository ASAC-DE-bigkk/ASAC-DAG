"""gold DDL/뷰 생성 — 컬럼 계약(위치코드·updatedt)·멱등·매핑 키."""
import pytest

from gold import ddl

_DETAIL = {"object": "commerce_pharmacy_detail", "kind": "detail_single",
           "members": ["pharmacy"], "payload": ["pharmtrdar", "asgnymd"]}
_CLUSTER = {"object": "commerce_food_sanitation_business_detail", "kind": "detail_cluster",
            "members": ["bakery", "general_restaurant"], "payload": ["uptaenm", "chaircnt"]}


def _sql(name, stmts):
    return next(s for n, s in stmts if n == name)


def test_entity_ddl_contract():
    sql = _sql("commerce_business_entity", ddl.create_core_sql())
    assert "create table if not exists" in sql            # 멱등(DDL ensure)
    # 위치 매핑 키(시군구·행정동) + 시간축(업데이트 일자) — 사용자 필수 컬럼
    for col in ("gu_code", "admin_dong_code", "legal_code", "updatedt", "updatedt_ts",
                "lastmodts_ts", "entity_type", "detail_table"):
        assert col in sql, col
    assert "primary key (entity_seq)" in sql
    assert "entity_seq bigint" in sql
    # 타입 규격화: 원천 날짜 = date(text 아님), 파싱 시각 = timestamp, 코드 = text
    assert "opened_at date" in sql and "closed_at date" in sql
    assert "updatedt_ts timestamp" in sql and "gu_code text" in sql
    assert "longitude double precision" in sql


def test_history_date_types_standardized():
    sql = _sql("commerce_business_entity_history", ddl.create_core_sql())
    assert "opened_at date" in sql and "closed_at date" in sql and "observed_date date" in sql


def test_history_and_marker_ddl():
    stmts = ddl.create_core_sql()
    hist = _sql("commerce_business_entity_history", stmts)
    assert "primary key (entity_seq, collected_at, content_hash)" in hist   # 버전 grain
    assert "updatedt_ts" in hist and "admin_dong_code" in hist
    marker = _sql("commerce_load_run_marker", stmts)
    assert "watermark_collected_at" in marker and "model_name text primary key" in marker


def test_entity_key_and_code_value_ddl():
    stmts = ddl.create_core_sql()
    key = _sql("commerce_entity_key", stmts)
    assert "entity_seq bigserial primary key" in key
    assert "unique (dataset, opnsfteamcode, mgtno)" in key
    code = _sql("commerce_code_value", stmts)
    assert "primary key (domain, value)" in code


def test_view_construction_indexes():
    stmts = ddl.create_index_sql()
    names = [n for n, _ in stmts]
    for t in ("commerce_business_entity", "commerce_business_entity_history"):
        for suffix in ("dataset_idx", "admin_dong_idx", "status_idx", "legal_code_idx",
                       "updatedt_idx", "lastmodts_idx", "opened_at_idx", "closed_at_idx"):
            assert f"{t}_{suffix}" in names
    assert "commerce_business_entity_natural_id_idx" in names
    assert "commerce_business_entity_history_natural_id_idx" not in names  # HISTORY_COLUMNS 에 없는 컬럼
    assert len(stmts) == 17                               # 2테이블×8 + entity 전용(natural_id)×1


def test_detail_index_auto_classification():
    # 날짜(ymd)·수량(cnt)만 매칭, 명칭류(uptaenm)는 정규화 대상이라 제외
    cluster_idx = dict(ddl.create_detail_index_sql(_CLUSTER))
    assert f"{_CLUSTER['object']}_chaircnt_idx" in cluster_idx
    assert f"{_CLUSTER['object']}_uptaenm_idx" not in cluster_idx
    detail_idx = dict(ddl.create_detail_index_sql(_DETAIL))
    assert f"{_DETAIL['object']}_asgnymd_idx" in detail_idx
    assert f"{_DETAIL['object']}_pharmtrdar_idx" not in detail_idx


def test_detail_ddl_key_mapping_only():
    name, sql = ddl.create_detail_sql(_DETAIL)
    assert name == "commerce_pharmacy_detail"
    assert "primary key (entity_seq, collected_at, content_hash)" in sql    # 공통과 매핑 키만
    assert "pharmtrdar text" in sql
    assert "business_name" not in sql                     # 공통 컬럼 재저장 금지


def test_views_expose_codes_and_updatedt():
    (_, current), (_, history) = ddl.view_domain_sql(_CLUSTER)
    for sql in (current, history):
        for col in ("gu_code", "admin_dong_code", "legal_code", "updatedt"):
            assert col in sql, col                        # 코드+시간축 상시 노출
        assert "commerce_dim_region" in sql               # 코드 → 이름 dim 해석
    api = ddl.view_api_sql("pharmacy", _DETAIL)
    assert "where e.dataset = 'pharmacy'" in api[0][1]
    assert "where h.dataset = 'pharmacy'" in api[1][1]


def test_identifier_gate_rejects_injection():
    # C1(§20): 외부 API 유래 payload/object 명이 무검증으로 DDL/JSONPath f-string 에 유입되면
    # SQL 주입. 악성 이름은 create_detail_sql/generate_all 진입에서 ValueError 로 막혀야 한다.
    evil_payload = {"object": "commerce_evil_detail", "kind": "detail_single",
                    "members": ["evil"], "payload": ["x'); drop table t; --"]}
    with pytest.raises(ValueError):
        ddl.create_detail_sql(evil_payload)
    with pytest.raises(ValueError):
        ddl.generate_all([evil_payload])
    with pytest.raises(ValueError):
        ddl.create_detail_index_sql(evil_payload)

    evil_object = {"object": "commerce_x; drop table y", "kind": "detail_single",
                   "members": ["x"], "payload": ["okcol"]}
    with pytest.raises(ValueError):
        ddl.create_detail_sql(evil_object)
    with pytest.raises(ValueError):
        ddl.generate_all([evil_object])

    # 정상(LOCALDATA 관행 [A-Z0-9_]) 이름은 통과 — 게이트가 정상 카탈로그를 막지 않는다.
    ddl.create_detail_sql(_DETAIL)
    ddl.generate_all([_CLUSTER, _DETAIL])


def test_generate_all_order_and_count():
    details = [_CLUSTER, _DETAIL]
    stmts = ddl.generate_all(details)
    names = [n for n, _ in stmts]
    # core(6: entity_key+entity+history+marker+catalog+code_value) + index(17) + dim(3)
    # + detail(2 테이블 × (DDL 1 + 자동 detail 인덱스 1) = 4) + 도메인뷰(cluster 1×2) + API뷰(멤버 3×2)
    assert len(stmts) == 6 + 17 + 3 + 4 + 2 + 6
    assert names.index("commerce_catalog") < names.index("commerce_pharmacy_detail")
    assert names.index("commerce_pharmacy_detail") < names.index("commerce_v_api_pharmacy")
