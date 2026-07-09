"""#203 — pyiceberg 직접 write 경로: arrow 변환·delete+append 커밋 1회·재시도.

pyiceberg/pyarrow 는 이미지 전용 의존성 — 미설치 로컬에선 모듈 전체 skip,
전체 검증은 Airflow 컨테이너에서 돈다(계획 Task 7).
"""
from __future__ import annotations

import pytest

pytest.importorskip("pyiceberg")
pytest.importorskip("pyarrow")

from datetime import datetime

from culture_ingest.common.config import RunContext
from culture_ingest.common.warehouse import _bronze_rows, _arrow_table, _COLUMNS
from culture_ingest.source.datasets import BY_NAME

CTX = RunContext(load_date="2026-07-09", ingest_ts="20260709T030000Z", run_id="t")
DS = BY_NAME["kopis_festival"]


def _records(n=3):
    return [
        (f"raw/culture/kopis/kopis_festival/p{i}.xml", f"p{i}.xml", {"제목": f"축제{i}", "값": None})
        for i in range(n)
    ]


def test_bronze_rows_shape_and_values():
    rows = _bronze_rows(DS, CTX, _records())
    assert len(rows) == 3
    r = rows[1]
    assert set(r) == set(_COLUMNS)
    assert r["dataset"] == "kopis_festival"
    assert r["record_seq"] == 1
    assert r["record_json"] == '{"제목": "축제1", "값": null}'  # ensure_ascii=False 한글 보존
    assert r["raw_object_key"].endswith("p1.xml")
    assert r["ingest_ts"] == "20260709T030000Z"
    assert isinstance(r["collected_at"], datetime)
    assert r["collected_at"].tzinfo is None  # 테이블 timestamp(6) = tz 없는 UTC


def test_arrow_table_schema_matches_bronze():
    import pyarrow as pa

    tbl = _arrow_table(_bronze_rows(DS, CTX, _records()))
    assert tbl.num_rows == 3
    assert tbl.schema.names == list(_COLUMNS)
    assert tbl.schema.field("record_seq").type == pa.int32()
    assert tbl.schema.field("collected_at").type == pa.timestamp("us")
    assert tbl.schema.field("record_json").type == pa.string()
    assert tbl.column("record_json")[1].as_py() == '{"제목": "축제1", "값": null}'


# ── PyicebergBronzeWarehouse.load — 커밋 1회·청크·멱등 delete·재시도 ────────────

from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo

from culture_ingest.common.config import CatalogSettings
from culture_ingest.common.warehouse import (
    PyicebergBronzeWarehouse,
    WarehouseSettings,
)

WS = WarehouseSettings(host="trino", port=8080, user="t", http_scheme="http",
                       catalog="iceberg_dev", schema="culture")
CAT = CatalogSettings(target="dev", uri="https://cat", warehouse="wh", token="FAKETOKEN123456",
                      s3_endpoint="https://r2", s3_access_key_id="a",
                      s3_secret_access_key="FAKESECRET123456", s3_region="auto")


class FakeTxn:
    def __init__(self, table):
        self.table = table

    def __enter__(self):
        self.table.log.append("begin")
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.table.commits += 1
            self.table.log.append("commit")
        return False

    def delete(self, expr):
        self.table.log.append(("delete", expr))

    def append(self, arrow_tbl):
        self.table.log.append(("append", arrow_tbl.num_rows))


class FakeTable:
    def __init__(self, fail_commits=0):
        self.log = []
        self.commits = 0
        self.refreshes = 0
        self._fail = fail_commits

    def transaction(self):
        table = self

        class Txn(FakeTxn):
            def __exit__(self, exc_type, exc, tb):
                if exc_type is None and table._fail > 0:
                    table._fail -= 1
                    raise CommitFailedException("optimistic lock conflict")
                return super().__exit__(exc_type, exc, tb)

        return Txn(self)

    def refresh(self):
        self.refreshes += 1


def _warehouse(table):
    wh = PyicebergBronzeWarehouse(WS, CAT, table_loader=lambda name: table, sleep=lambda s: None)
    wh.ensure_table = lambda name: f"iceberg_dev.culture.bronze_{name}"  # Trino DDL 차단
    return wh


def test_load_single_commit_delete_then_append():
    table = FakeTable()
    rows = _warehouse(table).load(DS, CTX, _records(5))
    assert rows == 5
    assert table.commits == 1  # 트랜잭션 전체 = 커밋 1회 (#203 핵심)
    kinds = [e[0] if isinstance(e, tuple) else e for e in table.log]
    assert kinds == ["begin", "delete", "append", "commit"]
    assert table.log[1][1] == EqualTo("ingest_ts", CTX.ingest_ts)  # 멱등 필터


def test_load_chunks_within_one_commit():
    table = FakeTable()
    rows = _warehouse(table).load(DS, CTX, _records(5), chunk_rows=2)
    assert rows == 5
    appends = [e for e in table.log if isinstance(e, tuple) and e[0] == "append"]
    assert [a[1] for a in appends] == [2, 2, 1]  # 메모리용 청크 분할
    assert table.commits == 1                     # 커밋은 여전히 1회


def test_load_empty_records_is_noop():
    table = FakeTable()
    assert _warehouse(table).load(DS, CTX, []) == 0
    assert table.log == []  # 테이블 로드·트랜잭션 자체가 없어야 함


def test_load_retries_commit_conflict_then_succeeds():
    table = FakeTable(fail_commits=2)
    rows = _warehouse(table).load(DS, CTX, _records(3))
    assert rows == 3
    assert table.refreshes == 2   # 실패마다 refresh 후 재시도
    assert table.commits == 1     # 최종 성공 커밋


def test_load_raises_after_max_attempts():
    table = FakeTable(fail_commits=4)  # MAX_COMMIT_ATTEMPTS=4 전부 소진
    with pytest.raises(CommitFailedException):
        _warehouse(table).load(DS, CTX, _records(3))
