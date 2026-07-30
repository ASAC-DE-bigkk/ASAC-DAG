"""서빙 핸드오프 메타 게시 — 스왑 스킵 제품의 직전 메타 보존(#593).

보조 테이블은 DROP+CREATE 라, 성공분만 다시 넣으면 밴드 게이트로 스킵된 제품의 컬럼 설명·
질의 예시가 사라진다. 그 제품의 D1 데이터·`_catalog` 행은 남아 있으므로(retain_last_good)
메타만 없어지면 '데이터는 있는데 설명이 없는' 상태가 된다.
"""
from gold import serving_export as se


class _D1Spy:
    """_d1 / _insert_rows 대역 — 실행된 SQL 과 삽입 행을 기록."""

    def __init__(self, existing: dict[str, list[dict]] | None = None):
        self.existing = existing or {}
        self.sqls: list[str] = []
        self.inserts: dict[str, list] = {}

    def d1(self, sql, token):
        self.sqls.append(sql)
        for table, rows in self.existing.items():
            if sql.startswith("SELECT") and f'FROM "{table}"' in sql:
                return rows
        return []

    def insert(self, table, colnames, rows, token):
        self.inserts.setdefault(table, []).extend(rows)


def _patch(monkeypatch, spy):
    monkeypatch.setattr(se, "_d1", spy.d1)
    monkeypatch.setattr(se, "_insert_rows", spy.insert)


def test_skipped_product_meta_is_preserved(monkeypatch):
    prev_cols = [{"product_id": "commerce_lifespan", "table_name": "d1_lifespan", "ordinal": 0,
                  "column_name": "p50_days", "type": "INTEGER", "description_ko": "중앙 수명(일)"}]
    prev_pat = [{"product_id": "commerce_lifespan", "pattern_id": "inv_shortest_lifespan",
                 "question_ko": "가장 단명한 업종은?", "sql": "SELECT 1", "axes": "정렬 반전",
                 "verified_rows": 10, "insight_sample_ko": "즉석판매 17일"}]
    spy = _D1Spy({"d1_catalog_columns": prev_cols, "d1_usage_patterns": prev_pat})
    _patch(monkeypatch, spy)

    new_cols = [("commerce_dong_summary", "d1_dong_summary", 0, "gu", "TEXT", "자치구명")]
    se._publish_handoff(spy, new_cols, [], [], [], skipped_pids=["commerce_lifespan"])

    # 성공분 + 스킵 제품의 직전 행이 함께 실린다
    got_cols = spy.inserts["d1_catalog_columns"]
    assert len(got_cols) == 2
    assert any(r[0] == "commerce_lifespan" and r[3] == "p50_days" for r in got_cols)
    assert any(r[0] == "commerce_dong_summary" for r in got_cols)
    # 질의 예시도 보존
    assert spy.inserts["d1_usage_patterns"][0][1] == "inv_shortest_lifespan"


def test_no_skips_does_not_query_previous_rows(monkeypatch):
    spy = _D1Spy()
    _patch(monkeypatch, spy)
    se._publish_handoff(spy, [], [], [], [], skipped_pids=[])
    assert not [s for s in spy.sqls if s.startswith("SELECT")]   # 불필요한 조회 없음


def test_invalid_product_id_is_not_interpolated(monkeypatch):
    """pid 는 ^[a-z0-9_]+$ 만 통과 — 식별자 화이트리스트(§20)."""
    spy = _D1Spy()
    _patch(monkeypatch, spy)
    se._publish_handoff(spy, [], [], [], [], skipped_pids=["bad'; DROP TABLE x; --"])
    assert not [s for s in spy.sqls if "DROP TABLE x" in s]


def test_glossary_is_always_regenerated(monkeypatch):
    """용어사전은 제품 스코프가 아니라 웨어하우스 파생 — 스킵과 무관하게 전량 교체."""
    spy = _D1Spy({"d1_catalog_glossary": [{"field": "x", "code": "y", "label_ko": "z",
                                           "source": "s"}]})
    _patch(monkeypatch, spy)
    se._publish_handoff(spy, [], [], [], [("major", "health", "보건", "gold")],
                        skipped_pids=["commerce_lifespan"])
    assert spy.inserts["d1_catalog_glossary"] == [("major", "health", "보건", "gold")]
