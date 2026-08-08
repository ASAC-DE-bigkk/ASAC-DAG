"""usage_pattern SQL 정적 보안 감사(pattern_audit) — 게시 전 테이블 스코프·문장 형태 게이트.

게이트웨이(run_pattern)는 값만 bind 하고 저장 SQL 이 어느 테이블을 읽는지 검사하지 않는다.
공유 D1 에는 게이트웨이 내부 표(_keys·_usage)와 타 도메인 d1_* 가 함께 있으므로, commerce
게시 경로가 "commerce 소유 d1_* 만 읽는 읽기 전용 단일문"을 기계로 보증해야 한다. 지켜야 할 것:
① allowlist 는 SERVING_SPEC 파생(별도 목록 금지 — 드리프트 방지)
② 내부/타도메인 테이블 참조·스택 쿼리·쓰기/DDL/PRAGMA/ATTACH 차단, CTE 이름은 오탐 아님
③ 주석 속임수(주석 제거 후 검사)와 LIMIT 파라미터 이름 규약(n/limit/top_n) 검사
④ export 게시 경로(_handoff_rows)가 위반 패턴을 게시에서 제외한다
"""
from gold import serving_export as se
from gold.pattern_audit import audit_pattern_sql, audit_patterns, commerce_allowlist


def test_allowlist_is_derived_from_serving_spec():
    allow = commerce_allowlist()
    assert allow == frozenset(s.d1_table for s in se.SERVING_SPEC)
    assert "d1_churn_yearly" in allow and "_keys" not in allow


def test_clean_patterns_pass():
    ok_sqls = [
        "-- :y='2025'\nSELECT gu_code FROM d1_churn_yearly WHERE y = :y",
        "WITH s AS (SELECT y FROM d1_flow_yearly) SELECT * FROM s",
        # 교차 commerce 테이블 JOIN 은 허용 대상이다
        "SELECT a.gu_code FROM d1_churn_yearly a JOIN d1_gu_specialization b ON a.gu_code = b.gu_code",
        "SELECT gu_code FROM d1_churn_yearly ORDER BY 1 LIMIT :n",
        # `:from`/`:to` 파라미터 이름의 from 은 절이 아니다 (오탐 회귀 보호)
        "SELECT y FROM d1_churn_yearly WHERE y BETWEEN :from AND :to",
        # 배열 IN 관용구 — json_each 는 허용 테이블값 함수 (실 D1 검증 2026-08-08)
        "SELECT gu_code FROM d1_churn_yearly WHERE gu_code IN (SELECT value FROM json_each(:gus))",
    ]
    for sql in ok_sqls:
        assert audit_pattern_sql(sql) == [], sql


def test_internal_and_foreign_tables_blocked():
    assert any("_keys" in f for f in audit_pattern_sql("SELECT key_hash FROM _keys"))
    assert any("d1_weather_daily" in f
               for f in audit_pattern_sql("SELECT * FROM d1_weather_daily"))


def test_comma_join_bypass_blocked():
    # 레드팀 확증(2026-08-08): 콤마 조인의 2번째 이후 테이블을 정규식이 놓쳐 _keys 유출이
    # 가능했다. 토크나이저는 FROM 절의 모든 테이블을 열거해 이 계열을 전부 막는다.
    payloads = [
        "SELECT k.key_hash, k.email FROM d1_gu_specialization c, _keys k",
        "SELECT u.count FROM d1_churn_yearly c, _usage u LIMIT :n",
        "SELECT * FROM d1_churn_yearly, d1_weather_daily",
        "SELECT p.sql FROM d1_churn_yearly c, d1_usage_patterns p",
        "SELECT t.name FROM d1_churn_yearly c, pragma_table_info('_keys') t",
        "SELECT * FROM (SELECT 1) x, _keys",
        "SELECT c.gu_code, (SELECT k.email FROM d1_flow_yearly x, _keys k LIMIT 1) AS leak "
        "FROM d1_churn_yearly c",
        "SELECT email FROM d1_churn_yearly c, main._keys",
    ]
    for sql in payloads:
        assert audit_pattern_sql(sql), f"콤마조인 우회가 통과됨: {sql}"


def test_stacked_write_pragma_attach_blocked():
    assert audit_pattern_sql("SELECT 1 FROM d1_churn_yearly; DROP TABLE _keys")
    assert audit_pattern_sql("DELETE FROM d1_churn_yearly")
    assert audit_pattern_sql("SELECT * FROM pragma_table_info('_keys')")
    assert audit_pattern_sql("ATTACH DATABASE 'x' AS y")


def test_comment_tricks_do_not_hide_references():
    # 게이트웨이가 주석을 벗기고 실행하므로 감사도 벗긴 본문 기준이어야 한다
    sql = "SELECT * FROM d1_churn_yearly /* */ JOIN _usage u ON 1=1"
    assert any("_usage" in f for f in audit_pattern_sql(sql))
    # 반대로, 주석 안에만 있는 이름은 실행되지 않으므로 오탐이 아니어야 한다
    assert audit_pattern_sql("-- _keys 언급은 주석뿐\nSELECT y FROM d1_flow_yearly") == []


def test_limit_param_name_convention():
    assert any("LIMIT :rows" in f
               for f in audit_pattern_sql("SELECT y FROM d1_flow_yearly LIMIT :rows"))
    assert audit_pattern_sql("SELECT y FROM d1_flow_yearly LIMIT :top_n") == []


def test_handoff_rows_excludes_violating_patterns(caplog):
    spec = next(s for s in se.SERVING_SPEC if s.d1_table == "d1_churn_yearly")
    m = {"serving": {"usage_patterns": [
        {"pattern_id": "good", "sql": "SELECT y FROM d1_churn_yearly", "requires": []},
        {"pattern_id": "evil", "sql": "SELECT key_hash FROM _keys", "requires": []},
    ]}}
    _, _, pat_rows, _ = se._handoff_rows(spec, m, [("y", "varchar")], "pub-1")
    assert [r["pattern_id"] for r in pat_rows] == ["good"]


def test_current_yml_all_clean_shape():
    # 대표 관용구(차원 스위치·센티널·기간창)가 감사를 통과하는지 — 신규 패턴 회귀 보호
    idioms = [
        "SELECT CASE :dim WHEN 'gu' THEN gu_code ELSE category END AS d, SUM(cnt) "
        "FROM d1_flow_yearly GROUP BY CASE :dim WHEN 'gu' THEN gu_code ELSE category END",
        "SELECT y FROM d1_churn_yearly WHERE (:gu_code='ALL' OR gu_code = :gu_code)",
        "SELECT y FROM d1_churn_yearly WHERE y BETWEEN :from_y AND :to_y",
    ]
    for sql in idioms:
        assert audit_pattern_sql(sql) == [], sql


def test_audit_patterns_maps_only_violations():
    out = audit_patterns([
        {"pattern_id": "a", "sql": "SELECT y FROM d1_flow_yearly"},
        {"pattern_id": "b", "sql": "SELECT * FROM _burst"},
    ])
    assert set(out) == {"b"}
