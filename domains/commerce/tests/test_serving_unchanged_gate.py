"""무변경 스킵 게이트 — payload 지문이 같으면 행 재기록만 생략(ASAC-DAG#601).

실측 배경: flow_monthly/flow_yearly gold 는 append-only 증분이라 월/연 경계와 full-refresh
외에는 물리 변경이 0이다. 그런데 서빙 export 는 매 run DROP+CREATE+INSERT 로 동일 바이트를
재전송했다(2제품 202,994행 = commerce 일일 D1 쓰기의 77.5%).

게이트가 지켜야 하는 것: ① 메타(`_catalog`/`d1_meta`/핸드오프)는 무변경에도 매 run 갱신
(26h 미게시 감시축 유지) ② 밴드 게이트 스킵('stale', 경보)과 코드 경로·상태값 분리
③ fail-open — 지문 부재·조회 실패·D1 실측 행수 불일치는 전부 재기록.
"""
import pytest

from gold import serving_export as se

_SPEC = se.Serve("gold_x", "d1_x", "d1_direct", "SELECT * FROM {q}.gold_x", (1, 100))
_COLS = [("ym", "varchar"), ("cnt", "integer")]
_ROWS = [["2026-01", 3], ["2026-02", 5]]


class _FakeCursor:
    """Trino 커서 대역 — spec SELECT 만 값을 돌려주고 용어사전 질의는 빈 결과."""

    def __init__(self, col_defs, rows):
        self._col_defs, self._rows = col_defs, rows
        self.description = None

    def execute(self, sql):
        if "gold_x" in sql:
            self.description = [(c, t) for c, t in self._col_defs]
            self._out = list(self._rows)
        else:                                   # glossary 등
            self.description = []
            self._out = []

    def fetchall(self):
        return self._out


class _FakeConn:
    def __init__(self, cur):
        self._cur = cur

    def cursor(self):
        return self._cur

    def close(self):
        pass


class _FakeD1:
    """D1 HTTP 대역 — publish_state 조회·행수 조회만 실제처럼 응답한다."""

    def __init__(self, state=None, counts=None):
        self.state = dict(state or {})
        self.counts = dict(counts or {})
        self.sqls: list[str] = []
        self.inserted: dict[str, list] = {}

    def d1(self, sql, token):
        self.sqls.append(sql)
        if sql.startswith("SELECT") and "FROM d1_publish_state" in sql:
            return list(self.state.values())
        if sql.startswith("SELECT count(*) AS n FROM"):
            return [{"n": self.counts.get(sql.split('"')[1], 0)}]
        return []

    def insert(self, table, colnames, rows, token):
        self.inserted.setdefault(table, []).extend(rows)

    # ── 조회 헬퍼 ──
    def wrote_data_table(self) -> bool:
        return any('DROP TABLE IF EXISTS "d1_x"' in s for s in self.sqls)

    def state_upserts(self) -> list[str]:
        return [s for s in self.sqls if "INSERT OR REPLACE INTO d1_publish_state" in s]

    def meta_upserts(self) -> list[str]:
        return [s for s in self.sqls if "INSERT OR REPLACE INTO d1_meta" in s]

    def catalog_upserts(self) -> list[str]:
        return [s for s in self.sqls if "INSERT OR REPLACE INTO _catalog" in s]


def _run(monkeypatch, fake, *, rows=_ROWS, col_defs=_COLS):
    import bronze.warehouse as wh

    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "t")
    monkeypatch.setattr(se, "SERVING_SPEC", (_SPEC,))
    monkeypatch.setattr(se, "_d1", fake.d1)
    monkeypatch.setattr(se, "_insert_rows", fake.insert)
    monkeypatch.setattr(se, "_report", lambda *a, **k: None)
    monkeypatch.setattr(se, "_write_serve_state", lambda *a, **k: None)
    monkeypatch.setattr(wh, "_qualified", lambda: ("cat", "sch", "cat.sch"))
    monkeypatch.setattr(wh, "_connect", lambda c, s: _FakeConn(_FakeCursor(col_defs, rows)))
    return se.export_to_d1()


def _fingerprint(rows=_ROWS, col_defs=_COLS) -> str:
    ddl = ", ".join(f'"{c}" {se._sqlite_type(t)}' for c, t in col_defs)
    return se._payload_fingerprint(ddl, [c for c, _ in col_defs], rows)


def _prev(fp, *, n=2, pid="pid-old", written="2026-07-29T00:00:00+00:00"):
    return {"d1_x": {"table_name": "d1_x", "payload_hash": fp, "row_count": n,
                     "publication_id": pid, "written_at": written}}


# ── 지문 함수 자체 ────────────────────────────────────────────────────────────
def test_fingerprint_is_order_independent():
    """Iceberg 파일 재작성·OPTIMIZE 로 반환 순서만 바뀌어도 같은 지문이어야 한다."""
    assert _fingerprint(rows=[["a", 1], ["b", 2]]) == _fingerprint(rows=[["b", 2], ["a", 1]])


def test_fingerprint_detects_value_schema_and_count_change():
    base = _fingerprint()
    assert _fingerprint(rows=[["2026-01", 3], ["2026-02", 6]]) != base   # 값
    assert _fingerprint(rows=[["2026-01", 3]]) != base                   # 행수
    assert _fingerprint(col_defs=[("ym", "varchar"), ("cnt", "varchar")]) != base   # 타입
    assert _fingerprint(col_defs=[("ym", "varchar"), ("total", "integer")]) != base  # 컬럼명


def test_fingerprint_handles_null_and_types():
    fp = _fingerprint(rows=[[None, 1], ["2026-01", None]])
    assert fp != _fingerprint(rows=[["", 1], ["2026-01", None]])   # NULL ≠ 빈 문자열


# ── 게이트 동작 ───────────────────────────────────────────────────────────────
def test_unchanged_payload_skips_rewrite_but_refreshes_meta(monkeypatch):
    fake = _FakeD1(state=_prev(_fingerprint()), counts={"d1_x": 2})
    result = _run(monkeypatch, fake)

    assert not fake.wrote_data_table()             # 행 재기록 생략
    assert "d1_x" not in fake.inserted
    assert result["unchanged"] == 1 and result["exported"] == 0
    assert result["rows"] == 0 and result["rows_served"] == 2
    assert result["status"] == "ok"                # 정상 — 경고 상태로 만들지 않는다
    # 메타는 그대로 갱신 — 26h 미게시 감시축이 계속 유효해야 한다
    assert fake.catalog_upserts() and fake.meta_upserts()
    assert "'ready'" in fake.meta_upserts()[0]     # 'stale' 이 아니다
    # 핸드오프 메타도 정상 재생성(설명 없는 데이터 방지)
    assert any(r[1] == "d1_x" for r in fake.inserted.get("d1_catalog_columns", []))


def test_unchanged_reuses_publication_id_and_keeps_written_at(monkeypatch):
    fake = _FakeD1(state=_prev(_fingerprint(), pid="pid-old",
                               written="2026-07-29T00:00:00+00:00"), counts={"d1_x": 2})
    _run(monkeypatch, fake)
    catalog_sql = fake.catalog_upserts()[0]
    assert "pid-old" in catalog_sql                # 같은 게시가 계속 서빙 중
    state_sql = fake.state_upserts()[0]
    assert "2026-07-29T00:00:00+00:00" in state_sql   # 실제로 쓴 시각은 보존
    assert state_sql.count("'") >= 2                  # checked_at 은 현재 시각으로 갱신


def test_changed_payload_rewrites_and_issues_new_publication_id(monkeypatch):
    fake = _FakeD1(state=_prev("deadbeef", pid="pid-old"), counts={"d1_x": 2})
    result = _run(monkeypatch, fake)
    assert fake.wrote_data_table() and len(fake.inserted["d1_x"]) == 2
    assert result["exported"] == 1 and result["unchanged"] == 0
    assert "pid-old" not in fake.catalog_upserts()[0]


def test_row_count_mismatch_forces_rewrite(monkeypatch):
    """지문은 같지만 D1 실측 행수가 다르면(배치 INSERT 중도 실패로 잘린 테이블) 강제 재기록."""
    fake = _FakeD1(state=_prev(_fingerprint()), counts={"d1_x": 1})
    result = _run(monkeypatch, fake)
    assert fake.wrote_data_table() and result["exported"] == 1


def test_stale_row_count_in_state_forces_rewrite(monkeypatch):
    fake = _FakeD1(state=_prev(_fingerprint(), n=99), counts={"d1_x": 2})
    assert _run(monkeypatch, fake)["exported"] == 1


def test_missing_state_publishes(monkeypatch):
    fake = _FakeD1(state={}, counts={"d1_x": 2})
    result = _run(monkeypatch, fake)
    assert fake.wrote_data_table() and result["exported"] == 1
    assert fake.state_upserts()                    # 다음 run 이 비교할 지문을 남긴다


def test_state_query_failure_is_fail_open(monkeypatch):
    class _Broken(_FakeD1):
        def d1(self, sql, token):
            if sql.startswith("SELECT") and "FROM d1_publish_state" in sql:
                raise RuntimeError("no such table")
            return super().d1(sql, token)

    fake = _Broken(counts={"d1_x": 2})
    assert _run(monkeypatch, fake)["exported"] == 1


def test_state_is_invalidated_before_destructive_write(monkeypatch):
    """지문 커밋이 파괴적 쓰기를 감싸는지 — 불변식: 커밋된 지문 ⊆ D1 실물.

    루프 밖에서 한 번만 커밋하면, 데이터를 이미 쓴 뒤 공유 `_catalog` upsert 등에서 죽었을 때
    D1 은 새 내용·상태는 옛 지문이 된다. 이후 원천이 옛 내용으로 되돌아오면(운영자 full-refresh
    복구가 정확히 이 형태) 지문이 일치해 영구히 스킵되고, 행수가 같은 채 값만 바뀌는 정상 변경
    형태라 `_d1_row_count` 도 못 잡는다. 그래서 순서를 단언한다.
    """
    fake = _FakeD1(state=_prev("deadbeef"), counts={"d1_x": 2})
    _run(monkeypatch, fake)

    def idx(pred):
        return next(i for i, s in enumerate(fake.sqls) if pred(s))

    invalidate = idx(lambda s: "INSERT OR REPLACE INTO d1_publish_state" in s and "''" in s)
    drop = idx(lambda s: 'DROP TABLE IF EXISTS "d1_x"' in s)
    commit = idx(lambda s: "INSERT OR REPLACE INTO d1_publish_state" in s
                 and _fingerprint() in s)
    assert invalidate < drop < commit           # 무효화 → 쓰기 → 실제 지문 커밋
    assert fake.inserted["d1_x"]                 # INSERT 는 커밋 전에 끝나 있다
    assert fake.sqls.index(fake.sqls[commit]) > drop


def test_write_failure_leaves_no_matching_fingerprint(monkeypatch):
    """쓰기가 중간에 죽으면 커밋된 지문이 실물과 일치하지 않아야 한다(다음 run 이 재기록)."""
    class _Failing(_FakeD1):
        def insert(self, table, colnames, rows, token):
            if table == "d1_x":
                raise RuntimeError("D1 write limit")
            return super().insert(table, colnames, rows, token)

    fake = _Failing(state=_prev("deadbeef"), counts={"d1_x": 2})
    with pytest.raises(RuntimeError):
        _run(monkeypatch, fake)
    state_sqls = fake.state_upserts()
    assert state_sqls and "''" in state_sqls[-1]        # 마지막 커밋은 무효화된 지문
    assert not any(_fingerprint() in s for s in state_sqls)   # 실제 지문은 커밋되지 않았다


def test_band_skip_keeps_stale_path_and_no_state_row(monkeypatch):
    """밴드 밖 스킵은 기존 경로 그대로 — 'stale' + 경보, 지문 상태는 갱신하지 않는다."""
    fake = _FakeD1(state=_prev(_fingerprint()), counts={"d1_x": 2})
    rows = [["2026-01", i] for i in range(200)]     # band hi=100 초과
    result = _run(monkeypatch, fake, rows=rows)
    assert result["skipped"] == 1 and result["status"] == "stale"
    assert not fake.wrote_data_table()
    assert "'stale'" in fake.meta_upserts()[0]
    assert not fake.state_upserts()                 # D1 내용이 안 바뀌었으므로 직전 지문 유지
    assert not fake.catalog_upserts()                # 직전 published 행을 그대로 둔다
