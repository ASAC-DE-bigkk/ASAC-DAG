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
