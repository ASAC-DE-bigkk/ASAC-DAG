"""bronze.warehouse — 적재 엔진(투영·Trino·dispatch·manifest) 단위테스트(Trino/PyIceberg 오프라인).

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_warehouse.py -q

DB/카탈로그 왕복 없이 검증: row/page-NDJSON, content_hash·record_seq·timestamp 투영,
Trino delete-then-insert 멱등·파라미터 바인딩, 엔진 dispatch, dataset 단위 manifest.
"""
import json
from datetime import datetime

import pytest

from bronze import warehouse as wh


class _FakeStorage:
    def __init__(self):
        self.data: dict[str, bytes] = {}

    def read_bytes(self, k):
        return self.data[k]

    def write_bytes(self, k, b):
        self.data[k] = b

    def write_json(self, k, obj):
        self.data[k] = json.dumps(obj, ensure_ascii=False).encode("utf-8")


class _FakeCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchall(self):
        return []

    def of(self, kind):
        return [(s, p) for (s, p) in self.calls if s.strip().upper().startswith(kind)]


class _FakeConn:
    def __init__(self, cur):
        self._cur = cur

    def cursor(self):
        return self._cur

    def close(self):
        pass


@pytest.fixture
def trino_env(monkeypatch):
    monkeypatch.setenv("COMMERCE_SCHEMA", "commerce")
    monkeypatch.setenv("DBT_TARGET", "dev")
    monkeypatch.setenv("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    monkeypatch.setenv("SCHEMA_VERSION", "v1")
    monkeypatch.setenv("STORAGE_BACKEND", "local")


def _rows_ndjson(rows):
    return ("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n").encode("utf-8")


def _unit(short, key, *, mode="changed", count=2, engine="trino"):
    return {"short": short, "run_id": "2026-07-03_143025_123", "date": "2026-07-03",
            "increment_key": key, "increment_mode": mode, "increment_count": count,
            "observed_date": "2026-07-03", "dag_run_id": "af_run",
            "collected_at": "2026-07-03T05:30:25.123456+00:00", "engine": engine}


# ── 투영/파싱 ────────────────────────────────────────────────────────────────
def test_canonical_hash_key_order_independent():
    assert wh._canonical_json({"A": 1, "B": 2}) == wh._canonical_json({"B": 2, "A": 1})


def test_to_naive_utc():
    dt = wh._to_naive_utc("2026-07-03T05:30:25.123456+00:00")
    assert dt == datetime(2026, 7, 3, 5, 30, 25, 123456) and dt.tzinfo is None


def test_iter_increment_row_ndjson():
    st = _FakeStorage()
    st.data["k"] = _rows_ndjson([{"MGTNO": "1", "BPLCNM": "a"}, {"MGTNO": "2"}])
    assert [r["MGTNO"] for r in wh.iter_increment_rows(st, "k")] == ["1", "2"]


def test_iter_increment_page_ndjson_parsed():
    st = _FakeStorage()
    # feat/58 이전 page-NDJSON: 줄 = API 페이지 응답(봉투). parse_page 로 레코드 추출.
    page = {"LOCALDATA_072404": {"list_total_count": 2, "RESULT": {"CODE": "INFO-000"},
                                 "row": [{"MGTNO": "A"}, {"MGTNO": "B"}]}}
    st.data["k"] = (json.dumps(page, ensure_ascii=False) + "\n").encode("utf-8")
    recs = list(wh.iter_increment_rows(st, "k", service_name="LOCALDATA_072404"))
    assert [r["MGTNO"] for r in recs] == ["A", "B"]


def test_iter_increment_page_ndjson_needs_service_name():
    st = _FakeStorage()
    page = {"LOCALDATA_072404": {"list_total_count": 1, "RESULT": {"CODE": "INFO-000"},
                                 "row": [{"MGTNO": "A"}]}}
    st.data["k"] = (json.dumps(page, ensure_ascii=False) + "\n").encode("utf-8")
    with pytest.raises(ValueError):
        list(wh.iter_increment_rows(st, "k"))   # service_name 없음


def test_project_records_lineage():
    recs = [{"MGTNO": "A", "UPDATEDT": "20260101000000", "BPLCNM": "가"}]
    (row,) = list(wh.project_records(
        recs, dataset="bakery", observed_date="2026-07-03", load_date="2026-07-03",
        bronze_run_id="B", dag_run_id="D", raw_object_key="rk", increment_mode="changed",
        schema_version="v1", collected_dt=datetime(2026, 7, 3, 5, 30, 25)))
    assert row["dataset"] == "bakery" and row["mgtno"] == "A" and row["record_seq"] == 0
    assert json.loads(row["record_json"])["BPLCNM"] == "가"
    assert len(row["content_hash"]) == 64


def test_content_hash_includes_source_coordinates():
    # 계약(#63, 사용자 확정): 해시 입력 = raw 원본 전체 — **원천 좌표(X/Y·XCRD/YCRD)는 포함**.
    # 좌표 채움(UPDATEDT 무갱신)도 원천 변경 = 정당한 버전. 제외 대상은 임의추가/파생컬럼뿐인데
    # (행정동/법정동/위경도) 그것들은 silver 파생이라 입력(raw)에 구조적으로 존재하지 않는다.
    base = {"MGTNO": "A", "UPDATEDT": "20260101000000", "BPLCNM": "가", "X": None, "Y": None}
    coords = {**base, "X": "192371.111", "Y": "451234.222"}
    v2 = {"MNG_NO": "A", "DATA_UPDT_YMD": "20260101", "XCRD": "1.0", "YCRD": "2.0"}
    v2_moved = {**v2, "XCRD": "9.9", "YCRD": "8.8"}
    assert wh._canonical_json(base) != wh._canonical_json(coords)          # 원천 좌표 변경 = 내용 변경
    assert wh._canonical_json(v2) != wh._canonical_json(v2_moved)          # v2 별칭 동일 계약
    assert wh._canonical_json(base) != wh._canonical_json({**base, "UPDATEDT": "20260202000000"})


def test_content_hash_differs_when_only_coordinates_change():
    # project_records 경유로도 동일 — 좌표만 다른 두 레코드는 서로 다른 버전(해시 상이),
    # record_json 은 원본 그대로 보존(§2.2).
    import json as _json
    recs = [{"MGTNO": "A", "UPDATEDT": "20260101000000", "X": "192371.1", "Y": "451234.2"},
            {"MGTNO": "A", "UPDATEDT": "20260101000000", "X": None, "Y": None}]
    rows = list(wh.project_records(
        recs, dataset="bakery", observed_date="2026-07-03", load_date="2026-07-03",
        bronze_run_id="B", dag_run_id="D", raw_object_key="rk", increment_mode="changed",
        schema_version="v1", collected_dt=datetime(2026, 7, 3, 5, 30, 25)))
    assert _json.loads(rows[0]["record_json"])["X"] == "192371.1"          # 원본 보존
    assert rows[0]["content_hash"] != rows[1]["content_hash"]              # 좌표 변경 = 버전


# ── Trino 적재(멱등·바인딩) ─────────────────────────────────────────────────
def test_load_unit_trino_delete_then_insert_bound(trino_env, monkeypatch):
    st = _FakeStorage()
    st.data["inc/bakery.jsonl"] = _rows_ndjson(
        [{"MGTNO": "1", "BPLCNM": "빵집"}, {"MGTNO": "2"}])
    cur = _FakeCursor()
    monkeypatch.setattr(wh, "_connect", lambda c, s: _FakeConn(cur))
    n = wh.load_unit_trino(st, _unit("bakery", "inc/bakery.jsonl"), load_date="2026-07-03")
    assert n == 2
    assert len(cur.of("DELETE")) == 1 and cur.of("DELETE")[0][1] == ("bakery", "2026-07-03_143025_123")
    (sql, params) = cur.of("INSERT")[0]
    assert "빵집" not in sql and "?" in sql                        # 데이터 미포함(바인딩)
    assert len(params) == 2 * len(wh._COLUMNS)
    assert any("빵집" in str(p) for p in params)


def test_load_unit_trino_batches(trino_env, monkeypatch):
    st = _FakeStorage()
    n = wh.INSERT_BATCH_ROWS + 3
    st.data["inc/big.jsonl"] = _rows_ndjson([{"MGTNO": str(i)} for i in range(n)])
    cur = _FakeCursor()
    monkeypatch.setattr(wh, "_connect", lambda c, s: _FakeConn(cur))
    assert wh.load_unit_trino(st, _unit("big", "inc/big.jsonl", count=n), load_date="d") == n
    assert len(cur.of("INSERT")) == 2


# ── dispatch / manifest ─────────────────────────────────────────────────────
def test_load_unit_dispatches_by_engine(trino_env, monkeypatch):
    st = _FakeStorage()
    monkeypatch.setattr(wh, "load_unit_trino", lambda s, u, load_date: 3)
    monkeypatch.setattr(wh, "load_unit_pyiceberg", lambda s, u, load_date: 9)
    monkeypatch.setattr(wh.load_state, "write_receipt", lambda *a, **k: None)
    r_t = wh.load_unit(st, _unit("a", "k", count=3, engine="trino"), load_date="2026-07-03")
    r_p = wh.load_unit(st, _unit("b", "k", count=9, engine="pyiceberg"), load_date="2026-07-03")
    assert r_t["engine"] == "trino" and r_t["rows_loaded"] == 3 and r_t["is_publishable"]
    assert r_p["engine"] == "pyiceberg" and r_p["rows_loaded"] == 9 and r_p["is_publishable"]


def test_load_unit_publishable_false_on_count_mismatch(trino_env, monkeypatch):
    st = _FakeStorage()
    monkeypatch.setattr(wh, "load_unit_trino", lambda s, u, load_date: 1)   # 기대 5, 실제 1
    monkeypatch.setattr(wh.load_state, "write_receipt", lambda *a, **k: None)
    r = wh.load_unit(st, _unit("a", "k", count=5, engine="trino"), load_date="d")
    assert r["is_publishable"] is False


def test_load_unit_legacy_publishable_on_rows_positive(trino_env, monkeypatch):
    st = _FakeStorage()
    monkeypatch.setattr(wh, "load_unit_pyiceberg", lambda s, u, load_date: 500)
    monkeypatch.setattr(wh.load_state, "write_receipt", lambda *a, **k: None)
    u = _unit("a", "k", engine="pyiceberg", count=500); u["legacy"] = True
    r = wh.load_unit(st, u, load_date="d")
    assert r["action"] == "loaded" and r["is_publishable"] is True and r["rows_loaded"] == 500


def test_write_manifest_batched(trino_env, monkeypatch):
    cur = _FakeCursor()
    monkeypatch.setattr(wh, "_connect", lambda c, s: _FakeConn(cur))
    results = [
        {"short": "bakery", "run_id": "R1", "action": "loaded", "is_publishable": True,
         "rows_loaded": 2, "rows_expected": 2, "observed_date": "2026-07-03",
         "load_date": "2026-07-03", "dag_run_id": "D", "engine": "trino"},
        {"short": "clinic", "run_id": "R2", "action": "loaded", "is_publishable": False,
         "rows_loaded": 1, "rows_expected": 3, "observed_date": "2026-07-03",
         "load_date": "2026-07-03", "dag_run_id": "D", "engine": "pyiceberg"},
    ]
    out = wh.write_manifest(results)
    assert out == {"published": 1, "datasets": 2}
    # 커밋 배칭 — 종별 delete+insert(=2N) 대신 단일 DELETE + 단일 INSERT(2행<100).
    deletes, inserts = cur.of("DELETE"), cur.of("INSERT")
    assert len(deletes) == 1 and len(inserts) == 1
    assert len(deletes[0][1]) == 4                               # (source_id, run_id) 2쌍 = 4 파라미터
    ins_params = inserts[0][1]
    assert len(ins_params) == 24                                 # 2행 × 12칼럼 flatten
    assert {ins_params[4], ins_params[4 + 12]} == {"SUCCESS", "FAILED"}  # status 는 각 행 offset 4
