"""#203 — pyiceberg 직접 적재 경로 검증.

호스트 pytest 에 pyarrow 는 있고 pyiceberg 는 없다 → Arrow 변환은 진짜로 검증하고,
pyiceberg(카탈로그·표현식)만 가짜로 갈아끼운다. 고정할 계약:

1. `_bronze_columns` — records → 컬럼 dict 가 Trino 경로(load)의 VALUES 튜플과 동일 의미.
2. `_arrow_bronze_table` — Iceberg 스키마 타입 일치(record_seq int32, collected_at ts, 나머지 string).
3. `PyicebergBronzeWarehouse.load` — 멱등(ingest_ts delete-then-append), 빈 입력은 카탈로그 접근 0.
4. `build_warehouse` 엔진 스위치 — pyiceberg/trino 분기.

sys.path 삽입은 conftest.py 가 담당한다.
"""
import sys
import types
from datetime import datetime

import pytest

from culture_ingest.common.config import RunContext
from culture_ingest.common.warehouse import (
    BronzeWarehouse,
    PyicebergBronzeWarehouse,
    WarehouseSettings,
    _arrow_bronze_table,
    _bronze_columns,
)
from culture_ingest.source.datasets import BY_NAME
from culture_ingest.source.ingest import build_warehouse

CTX = RunContext(load_date="2026-07-08", ingest_ts="20260708T000000Z", run_id="run-1")
DS = BY_NAME["kopis_boxoffice"]
SETTINGS = WarehouseSettings(host="trino", port=8080, user="airflow",
                             http_scheme="http", catalog="iceberg_dev", schema="culture")


def _records(n=3):
    return [(f"raw/k/page-{i}.xml", f"page-{i}.xml", {"mt20id": f"PF{i}"}) for i in range(n)]


# ── _bronze_columns: Trino 경로와 동일 계약 ──────────────────────────────────

def test_bronze_columns_match_load_contract():
    cols = _bronze_columns(DS, CTX, _records(2), collected_at=datetime(2026, 7, 8, 0, 0, 0))
    assert cols["dataset"] == ["kopis_boxoffice", "kopis_boxoffice"]
    assert cols["record_seq"] == [0, 1]                       # enumerate 순서
    assert cols["raw_object_key"] == ["raw/k/page-0.xml", "raw/k/page-1.xml"]
    assert cols["page_no"] == ["page-0.xml", "page-1.xml"]
    assert cols["ingest_ts"] == ["20260708T000000Z"] * 2
    assert cols["run_id"] == ["run-1"] * 2
    assert '"mt20id": "PF0"' in cols["record_json"][0]         # ensure_ascii=False JSON


# ── _arrow_bronze_table: Iceberg 스키마 타입 일치 (진짜 pyarrow) ──────────────

def test_arrow_table_schema_types():
    import pyarrow as pa

    cols = _bronze_columns(DS, CTX, _records(3), collected_at=datetime(2026, 7, 8, 0, 0, 0))
    tbl = _arrow_bronze_table(cols)
    assert tbl.num_rows == 3
    schema = tbl.schema
    assert schema.field("record_seq").type == pa.int32()      # Iceberg integer = 32bit
    assert schema.field("collected_at").type == pa.timestamp("us")  # timestamp(6), tz 없음
    assert schema.field("record_json").type == pa.string()
    assert [f.name for f in schema] == list(  # 컬럼 순서 = _COLUMNS
        __import__("culture_ingest.common.warehouse", fromlist=["_COLUMNS"])._COLUMNS)


# ── PyicebergBronzeWarehouse.load: 멱등 delete-then-append ────────────────────

class _FakeTxn:
    def __init__(self, log): self.log = log
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def delete(self, flt): self.log.append(("delete", flt))
    def append(self, arrow): self.log.append(("append", arrow.num_rows))


class _FakeTable:
    def __init__(self, log): self.log = log
    def transaction(self): return _FakeTxn(self.log)


class _FakeCatalog:
    def __init__(self, log): self.log = log
    def load_table(self, ident):
        self.log.append(("load_table", ident))
        return _FakeTable(self.log)


@pytest.fixture()
def fake_pyiceberg(monkeypatch):
    """pyiceberg.expressions.EqualTo 를 가짜로(호스트에 pyiceberg 미설치) — (col,val) 캡처."""
    captured = {}

    def EqualTo(col, val):
        captured["filter"] = (col, val)
        return ("EqualTo", col, val)

    mod = types.ModuleType("pyiceberg.expressions")
    mod.EqualTo = EqualTo
    monkeypatch.setitem(sys.modules, "pyiceberg", types.ModuleType("pyiceberg"))
    monkeypatch.setitem(sys.modules, "pyiceberg.expressions", mod)
    return captured


def test_pyiceberg_load_idempotent_delete_then_append(fake_pyiceberg):
    log: list = []
    wh = PyicebergBronzeWarehouse(SETTINGS, catalog=_FakeCatalog(log))
    n = wh.load(DS, CTX, _records(3))
    assert n == 3
    # 순서: 테이블 로드 → delete(ingest_ts) → append(3행)
    assert log[0] == ("load_table", "culture.bronze_kopis_boxoffice")
    assert log[1][0] == "delete"
    assert fake_pyiceberg["filter"] == ("ingest_ts", "20260708T000000Z")  # 이번 파티션만
    assert log[2] == ("append", 3)


def test_pyiceberg_load_empty_skips_catalog(fake_pyiceberg):
    log: list = []
    wh = PyicebergBronzeWarehouse(SETTINGS, catalog=_FakeCatalog(log))
    assert wh.load(DS, CTX, []) == 0
    assert log == []   # 빈 입력은 카탈로그 접근 0


# ── build_warehouse 엔진 스위치 ──────────────────────────────────────────────

def test_build_warehouse_engine_switch(monkeypatch):
    monkeypatch.setenv("R2_DEV_ENDPOINT", "x"); monkeypatch.setenv("R2_DEV_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("R2_DEV_SECRET_ACCESS_KEY", "x"); monkeypatch.setenv("R2_DEV_BUCKET_NAME", "x")
    assert isinstance(build_warehouse("dev", engine="pyiceberg"), PyicebergBronzeWarehouse)
    assert isinstance(build_warehouse("dev", engine="trino"), BronzeWarehouse)
    monkeypatch.setenv("CULTURE_BRONZE_ENGINE", "pyiceberg")
    assert isinstance(build_warehouse("dev"), PyicebergBronzeWarehouse)  # env 경유
    monkeypatch.delenv("CULTURE_BRONZE_ENGINE", raising=False)
    assert isinstance(build_warehouse("dev"), BronzeWarehouse)           # 기본 trino
