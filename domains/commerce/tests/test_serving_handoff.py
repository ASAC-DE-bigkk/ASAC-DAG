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


def _run(monkeypatch, spy, *, columns=(), ext=(), patterns=(), glossary=(),
         published=None, stamp="t1"):
    monkeypatch.setattr(se, "_d1", spy.d1)
    se._publish_handoff("token", list(columns), list(ext), list(patterns),
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
    assert len(deletes) == 1 and "'commerce:major'" in deletes[0] and "'t9'" in deletes[0]
    assert "exported_at" in deletes[0]                   # 어휘 스코프 판별축은 exported_at


def test_failed_label_source_leaves_previous_vocabulary_rows(monkeypatch):
    """라벨 소스 실패로 이번 run 에 빠진 어휘는 정리 대상이 아니다 — 직전 행 유지.

    구 DROP 방식에선 그 어휘가 통째로 사라졌다(회귀 방지 단언).
    """
    spy = _D1Spy()
    _run(monkeypatch, spy, glossary=[_glossary_row(vocab="commerce:major")])  # event_type 부재 run
    assert not [s for s in spy.sqls if "commerce:event_type" in s]
