"""서빙 핸드오프 메타 게시 — 자연키 upsert 규약(#638 §3).

전량 교체(DROP+CREATE)는 금지다: 공용 테이블이 된 보조 4종에서 한 도메인의 게시가 다른
도메인의 메타를 지우는 사고의 실물이 구 구현이었다. 지켜야 하는 것:
① upsert + 제품/어휘 스코프 잔여 정리만 — 테이블 전체 DROP/DELETE 없음
② 밴드 스킵 제품은 upsert 도 정리도 하지 않는다(직전 행 자연 보존 — 구 보존 로직 대체)
③ 레거시(자연키 없음) 스키마는 1회 재생성(#638 §4 예정된 동작)
④ 식별자는 화이트리스트, 값은 공용 빌더 이스케이프(§20)
"""
from common.serving.d1_client import HANDOFF_COLUMN_TYPES, HANDOFF_PRIMARY_KEYS
from gold import serving_export as se


def _v1_pragma(table: str) -> list[dict]:
    """PRAGMA table_info 대역 — v1(자연키 포함) 스키마 응답."""
    key = HANDOFF_PRIMARY_KEYS[table]
    return [{"name": name, "pk": (key.index(name) + 1) if name in key else 0}
            for name, _ in HANDOFF_COLUMN_TYPES[table]]


class _D1Spy:
    """_d1 대역 — 실행된 SQL 기록, PRAGMA 는 준비된 스키마 응답."""

    def __init__(self, pragma: dict[str, list] | None = None):
        self.pragma = pragma if pragma is not None else {t: _v1_pragma(t) for t in HANDOFF_COLUMN_TYPES}
        self.sqls: list[str] = []

    def d1(self, sql, token):
        self.sqls.append(sql)
        if sql.startswith("PRAGMA table_info("):
            return self.pragma.get(sql.split('"')[1], [])
        return []


def _cols_row(pid="commerce_dong_summary", pub="pub-1"):
    return {"product_id": pid, "table_name": "d1_dong_summary", "ordinal": 0,
            "column_name": "gu", "type": "TEXT", "description_ko": "자치구명",
            "publication_id": pub}


def _glossary_row(vocab="commerce:major", stamp="t1"):
    return {"vocabulary_id": vocab, "code": "health", "label_ko": "보건",
            "origin": "commerce", "source_type": "warehouse", "exported_at": stamp}


def _run(monkeypatch, spy, *, columns=(), ext=(), patterns=(), display=(), glossary=(),
         published=None, stamp="t1"):
    monkeypatch.setattr(se, "_d1", spy.d1)
    se._publish_handoff("token", list(columns), list(ext), list(patterns), list(display),
                        list(glossary), published or {}, stamp)


def test_current_schema_upserts_without_any_drop(monkeypatch):
    """v1 스키마면 DROP 이 한 번도 없어야 한다 — 전량 교체 금지(#638 §3)의 핵심 단언."""
    spy = _D1Spy()
    _run(monkeypatch, spy, columns=[_cols_row()],
         published={"commerce_dong_summary": "pub-1"})
    assert not [s for s in spy.sqls if "DROP TABLE" in s]
    upserts = [s for s in spy.sqls if s.startswith('INSERT OR REPLACE INTO "d1_catalog_columns"')]
    assert upserts and "commerce_dong_summary" in upserts[0] and "pub-1" in upserts[0]


def test_legacy_schema_without_natural_key_migrates_rows_once(monkeypatch):
    """전량 교체 시절 테이블(컬럼 집합 같고 자연키 없음)은 **행 보존 이행** 1회(#638 §4).

    본 테이블을 복사 없이 날리는 경로(구 재생성)가 없어야 한다 — 이행 run 에 게시되지 않는
    밴드 스킵 제품의 직전 메타가 살아남는 근거.
    """
    legacy = {t: _v1_pragma(t) for t in HANDOFF_COLUMN_TYPES}
    legacy["d1_catalog_ext"] = [dict(r, pk=0) for r in legacy["d1_catalog_ext"]]  # 자연키 제거
    spy = _D1Spy(pragma=legacy)
    _run(monkeypatch, spy)
    migrations = [s for s in spy.sqls if 'RENAME TO "d1_catalog_ext__migrate"' in s]
    assert len(migrations) == 1
    assert 'INSERT OR REPLACE INTO "d1_catalog_ext"' in migrations[0]   # 행 보존 복사
    assert 'PRIMARY KEY ("product_id")' in migrations[0]                # 자연키 강제
    assert not [s for s in spy.sqls if s.startswith('DROP TABLE IF EXISTS "d1_catalog_ext";')]


def test_absent_table_created_without_drop(monkeypatch):
    """테이블 부재(최초 게시)면 CREATE IF NOT EXISTS 만 — DROP 경로를 타지 않는다."""
    spy = _D1Spy(pragma={t: [] for t in HANDOFF_COLUMN_TYPES})
    _run(monkeypatch, spy)
    assert not [s for s in spy.sqls if "DROP TABLE" in s]
    assert sum(s.startswith("CREATE TABLE IF NOT EXISTS") for s in spy.sqls) == len(HANDOFF_COLUMN_TYPES)


def test_stale_cleanup_scoped_to_published_products_only(monkeypatch):
    """잔여 정리는 이번 run 게시 제품 스코프만 — 밴드 스킵 제품 행은 무접촉(직전 메타 보존).

    columns/patterns 는 선언 키셋(NOT IN) 정리 — publication_id 재사용(#601) run 에도 유효.
    """
    spy = _D1Spy()
    _run(monkeypatch, spy, columns=[_cols_row(pid="commerce_dong_summary")],
         published={"commerce_dong_summary": "pub-1"})   # commerce_lifespan 은 스킵됐다
    deletes = [s for s in spy.sqls if s.startswith("DELETE FROM")]
    assert deletes and all("commerce_dong_summary" in s for s in deletes)
    columns_delete = next(s for s in deletes if '"d1_catalog_columns"' in s)
    assert "NOT IN" in columns_delete and "'gu'" in columns_delete       # 선언 키셋 기준
    ext_delete = next(s for s in deletes if '"d1_catalog_ext"' in s)
    assert "publication_id" in ext_delete and "'pub-1'" in ext_delete    # 단일 키 — 부등 판별
    patterns_delete = next(s for s in deletes if '"d1_usage_patterns"' in s)
    assert patterns_delete.rstrip(";").endswith("'commerce_dong_summary'")  # 선언 0건 → 스코프 전체
    assert not [s for s in spy.sqls if "commerce_lifespan" in s]
    assert not [s for s in spy.sqls if s.startswith("SELECT")]   # 구 보존 조회 경로 부재


def test_invalid_product_id_is_not_interpolated(monkeypatch):
    """pid 는 ^[a-z0-9_:]+$ 만 통과 — 식별자 화이트리스트(§20)."""
    spy = _D1Spy()
    _run(monkeypatch, spy, published={"bad'; DROP TABLE x; --": "p"})
    assert not [s for s in spy.sqls if "DROP TABLE x" in s]


def test_glossary_cleanup_scoped_by_vocabulary(monkeypatch):
    """용어사전 정리는 vocabulary_id 스코프 — 이번 run 에 없는 어휘(타 도메인 소유 포함) 무접촉."""
    spy = _D1Spy()
    _run(monkeypatch, spy, glossary=[_glossary_row(vocab="commerce:major", stamp="t9")], stamp="t9")
    deletes = [s for s in spy.sqls if s.startswith('DELETE FROM "d1_catalog_glossary"')]
    scope_deletes = [s for s in deletes if "exported_at" in s]
    assert len(scope_deletes) == 1 and "'commerce:major'" in scope_deletes[0] and "'t9'" in scope_deletes[0]
    # 승격 잔재(commerce:gu_code) 정리는 별도 멱등 DELETE — 다른 어휘 스코프는 건드리지 않는다
    superseded = [s for s in deletes if "commerce:gu_code" in s]
    assert len(superseded) == 1 and "exported_at" not in superseded[0]


def test_unregistered_vocabulary_is_rejected(monkeypatch):
    """레지스트리 미등록 어휘는 게시 거부(#638 §5-5) — 등록 어휘만 실리고 run 은 진행."""
    spy = _D1Spy()
    rogue = dict(_glossary_row(vocab="commerce:unknown_vocab"), code="x")
    _run(monkeypatch, spy, glossary=[_glossary_row(vocab="commerce:major"), rogue])
    assert not [s for s in spy.sqls if "commerce:unknown_vocab" in s]
    assert [s for s in spy.sqls if s.startswith('INSERT OR REPLACE INTO "d1_catalog_glossary"')
            and "commerce:major" in s]


def test_registry_origin_mismatch_is_rejected(monkeypatch):
    """레지스트리 정본과 origin/source_type 이 어긋난 행도 거부 — 출처 위조 방지."""
    spy = _D1Spy()
    forged = dict(_glossary_row(vocab="common:gu_code"), origin="commerce")  # 정본은 asac_axes
    _run(monkeypatch, spy, glossary=[forged])
    assert not [s for s in spy.sqls if s.startswith('INSERT OR REPLACE INTO "d1_catalog_glossary"')]


def test_gu_code_sources_from_common_live_master():
    """gu_code 승격(#638 §2.4) — 자체 스냅샷이 아니라 공용 축 라이브 마스터에서 뽑는다."""
    class _Cur:
        def __init__(self):
            self.sqls = []
            self._out = []

        def execute(self, sql):
            self.sqls.append(sql)
            self._out = [("11680", "강남구")] if "dim_admin_dong" in sql else []

        def fetchall(self):
            return self._out

    cur = _Cur()
    rows = se._glossary_rows(cur, "iceberg_dev", "iceberg_dev.commerce", "t1")
    gu_rows = [r for r in rows if r["vocabulary_id"] == "common:gu_code"]
    assert gu_rows == [{"vocabulary_id": "common:gu_code", "code": "11680", "label_ko": "강남구",
                        "origin": "asac_axes", "source_type": "warehouse", "exported_at": "t1"}]
    assert any("iceberg_dev.common.dim_admin_dong" in s for s in cur.sqls)
    assert not any("bronze_ref_admin_dong" in s for s in cur.sqls)   # 자체 스냅샷 파생 폐기


def test_failed_label_source_leaves_previous_vocabulary_rows(monkeypatch):
    """라벨 소스 실패로 이번 run 에 빠진 어휘는 정리 대상이 아니다 — 직전 행 유지.

    구 DROP 방식에선 그 어휘가 통째로 사라졌다(회귀 방지 단언).
    """
    spy = _D1Spy()
    _run(monkeypatch, spy, glossary=[_glossary_row(vocab="commerce:major")])  # event_type 부재 run
    assert not [s for s in spy.sqls if "commerce:event_type" in s]


# ── 표시 메타 d1_catalog_display (서빙 계약 v1.10 · ASAC-DAG#706) ──────────────────
# commerce 는 공용 publisher(`common/serving/publisher.py`)가 아니라 자체 `_publish_handoff`
# 로 게시한다. 그래서 계약에 display 가 들어와도 **이 파일이 함께 바뀌지 않으면 한 행도
# 안 나간다** — 도메인 채택이 "선언뿐"이 아니었던 이유. 아래가 그 경로를 못박는다.

_SPEC_ARGS = ("gold_license_dong_summary", "d1_dong_summary", "d1_direct", "SELECT 1", (1, 10))
_GRID_ARGS = ("gold_license_geo_grid", "d1_geo_grid_detail", "d1_rollup", "SELECT 1", (1, 10))


def _display_row(pid="commerce_dong_summary", pub="pub-1"):
    return {"product_id": pid, "title": "우리 동네 상권 요약", "summary": "행정동별 업소 현황입니다.",
            "caveat": None, "use_cases": '["상권 분석"]', "publication_id": pub}


def test_display_row_is_upserted_never_dropped(monkeypatch):
    spy = _D1Spy()
    _run(monkeypatch, spy, display=[_display_row()],
         published={"commerce_dong_summary": "pub-1"})

    assert not [s for s in spy.sqls if "DROP TABLE" in s]
    upserts = [s for s in spy.sqls if s.startswith('INSERT OR REPLACE INTO "d1_catalog_display"')]
    assert upserts and "우리 동네 상권 요약" in upserts[0] and "pub-1" in upserts[0]


def test_undeclared_product_publishes_no_display_row_and_clears_stale(monkeypatch):
    """미선언은 빈 값으로 꾸미지 않는다 — upsert 0건 + 옛 행 정리(#706 '빈 제목 금지')."""
    spy = _D1Spy()
    _run(monkeypatch, spy, published={"commerce_dong_summary": "pub-1"})

    assert not [s for s in spy.sqls if s.startswith('INSERT OR REPLACE INTO "d1_catalog_display"')]
    stale = [s for s in spy.sqls if "d1_catalog_display" in s and s.startswith("DELETE")]
    assert stale and "commerce_dong_summary" in stale[0]


def test_display_publish_does_not_touch_other_domain_tables(monkeypatch):
    """신규 표라 공유 표를 건드릴 이유가 없다 — 컬럼 추가안을 철회한 근거의 회귀 단언."""
    spy = _D1Spy()
    _run(monkeypatch, spy, display=[_display_row()],
         published={"commerce_dong_summary": "pub-1"})

    for shared in ("_catalog", "d1_meta", "_request_log", "_publication_ledger"):
        assert not [s for s in spy.sqls if f'"{shared}"' in s]


def test_contract_display_becomes_a_row():
    sv = {"display": {"title": "우리 동네 상권 요약", "summary": "행정동별 업소 현황입니다.",
                      "use_cases": ["상권 분석", "입지 검토"]}}
    row = se._display_row(se.Serve(*_SPEC_ARGS), sv, "commerce_dong_summary", "pub-1")

    assert row["title"] == "우리 동네 상권 요약"
    assert row["use_cases"] == '["상권 분석", "입지 검토"]'
    assert row["caveat"] is None                     # 미선언은 NULL — 빈 문자열이 아니다


def test_missing_declaration_yields_no_row():
    assert se._display_row(se.Serve(*_SPEC_ARGS), {}, "commerce_dong_summary", "pub-1") is None


def test_one_model_two_products_get_distinct_titles():
    """geo_grid 는 한 모델이 overview·detail 두 제품을 낳는다 — 같은 제목이 나란히 서면 안 된다."""
    sv = {"display": {"title": "상권 밀집 격자", "summary": "500m 격자 밀도입니다."},
          "d1_display": {"d1_geo_grid_detail": {"title": "상권 밀집 격자(업종별)",
                                                "summary": "격자 × 업종 상세입니다."}}}
    detail = se._display_row(se.Serve(*_GRID_ARGS), sv, "commerce_geo_grid_detail", "pub-1")
    overview = se._display_row(
        se.Serve(*_GRID_ARGS[:1], "d1_geo_grid_overview", *_GRID_ARGS[2:]),
        sv, "commerce_geo_grid_overview", "pub-1")

    assert detail["title"] == "상권 밀집 격자(업종별)"
    assert overview["title"] == "상권 밀집 격자"      # 덮어쓰기 없는 제품은 계약 필드 그대로
