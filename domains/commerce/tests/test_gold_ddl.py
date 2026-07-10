"""gold DDL/뷰 생성 — 컬럼 계약(위치코드·updatedt)·멱등·매핑 키."""
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
    assert "primary key (entity_id)" in sql
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
    assert "primary key (entity_id, collected_at, content_hash)" in hist   # 버전 grain
    assert "updatedt_ts" in hist and "admin_dong_code" in hist
    marker = _sql("commerce_load_run_marker", stmts)
    assert "watermark_collected_at" in marker and "model_name text primary key" in marker


def test_detail_ddl_key_mapping_only():
    name, sql = ddl.create_detail_sql(_DETAIL)
    assert name == "commerce_pharmacy_detail"
    assert "primary key (entity_id, collected_at, content_hash)" in sql    # 공통과 매핑 키만
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


def test_generate_all_order_and_count():
    details = [_CLUSTER, _DETAIL]
    stmts = ddl.generate_all(details)
    names = [n for n, _ in stmts]
    # core(4) + dim(3) + detail(2) + 도메인뷰(cluster 1×2) + API뷰(멤버 3×2)
    assert len(stmts) == 4 + 3 + 2 + 2 + 6
    assert names.index("commerce_catalog") < names.index("commerce_pharmacy_detail")
    assert names.index("commerce_pharmacy_detail") < names.index("commerce_v_api_pharmacy")
