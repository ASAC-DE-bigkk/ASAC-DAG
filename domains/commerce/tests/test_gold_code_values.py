"""gold.code_values — 정규화 Option 1(공유 코드 테이블) 채움 로직(DB 왕복 없음, Fake cursor)."""
from gold import code_values


class _FakeCursor:
    def __init__(self, script):
        self._script = list(script)   # [(예상 SQL 접두, 반환행)] 순서대로 소비
        self.calls = []
        self._last_rows = []

    def execute(self, sql, params=None):
        self.calls.append((sql.strip(), params))
        if self._script:
            _prefix, rows = self._script.pop(0)
            self._last_rows = rows
        else:
            self._last_rows = []

    def fetchall(self):
        return self._last_rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, cur):
        self._cur = cur

    def cursor(self):
        return self._cur

    def commit(self):
        pass


def test_build_code_values_filters_flags_and_constants(monkeypatch):
    monkeypatch.setattr(code_values, "CANDIDATES", [
        ("commerce_food_sanitation_business_detail", "uptaenm"),   # 진짜 통제어휘 — 채택
        ("commerce_livestock_sale_detail", "lindjobgbnnm"),        # 상수 1값 — 제외
        ("commerce_x_detail", "isream"),                           # 0/1 플래그 — 제외
        ("commerce_missing_detail", "ghost"),                      # 카탈로그 드리프트로 없음 — skip
    ])
    existing = [
        ("commerce_food_sanitation_business_detail", "uptaenm"),
        ("commerce_livestock_sale_detail", "lindjobgbnnm"),
        ("commerce_x_detail", "isream"),
    ]
    script = [
        ("information_schema.columns", existing),
        ("select uptaenm", [("한식", 100), ("중식", 50)]),
        ("delete", []),
        ("select lindjobgbnnm", [("축산물판매업", 41933)]),          # distinct=1 → 채택 안 함
        ("delete", []),
        ("select isream", [("0", 900), ("1", 100)]),                # 0/1 만 → 채택 안 함
        ("delete", []),
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    inserted = []
    monkeypatch.setattr(code_values.pg, "execute_values",
                         lambda c, sql, rows, **kw: inserted.append((sql, rows)))

    out = code_values.build_code_values(conn)

    assert out == {"domains": 1, "skipped": 3}     # uptaenm 만 채택, 나머지 3(상수/플래그/드리프트) skip
    assert len(inserted) == 1                       # commerce_code_value insert 정확히 1회
    domain, value, n = inserted[0][1][0]
    assert domain == "food_sanitation_business_detail.uptaenm"
    assert {r[1] for r in inserted[0][1]} == {"한식", "중식"}
