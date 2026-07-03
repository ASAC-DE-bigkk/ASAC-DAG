"""load_bronze 경로 테스트: summary의 raw 키 노출 + 리포트 정리 + raw→bronze 적재.

sys.path 삽입은 conftest.py 가 담당한다 (형제 테스트와 동일 관례).
"""
import pytest

from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import DatasetResult
from culture_ingest.source.datasets import BY_NAME
from culture_ingest.source.ingest import build_run_report, load_bronze_from_raw

CTX = RunContext(load_date="2026-07-03", ingest_ts="20260703T000000Z", run_id="test")


def _summary(name="kopis_boxoffice", error="", keys=("raw/culture/k/page-0001.xml",)):
    r = DatasetResult(name=name, source="kopis", endpoint="boxoffice", prefix="raw/culture/k")
    r.error = error
    r.object_keys = list(keys)
    r.pages, r.rows = len(keys), 3
    return r.summary()


def test_summary_includes_object_keys():
    s = _summary()
    assert s["object_keys"] == ["raw/culture/k/page-0001.xml"]


def test_run_report_strips_object_keys():
    report = build_run_report([_summary()], CTX, expected_total=1)
    assert all("object_keys" not in row for row in report["datasets"])
    assert report["coverage"]["landed"] == 1  # 기존 집계는 그대로


class FakeSink:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects

    def get(self, key: str) -> bytes:
        return self.objects[key]


class FakeWarehouse:
    def __init__(self, fail_on: set[str] | None = None):
        self.calls: list[tuple] = []
        self.fail_on = fail_on or set()

    def load(self, ds, ctx, records):
        if ds.name in self.fail_on:
            raise RuntimeError("trino down")
        self.calls.append((ds.name, ctx.ingest_ts, records))
        return len(records)


def _kopis_body(ds, ids):  # 실제 레지스트리의 row_tag로 XML 생성
    rows = "".join(f"<{ds.row_tag}><mt20id>{i}</mt20id></{ds.row_tag}>" for i in ids)
    return f"<dbs>{rows}</dbs>".encode()


def _seoul_body(ds, titles):
    import json
    return json.dumps({ds.endpoint: {"row": [{"TITLE": t} for t in titles]}}).encode()


def test_load_bronze_parses_raw_and_loads_per_dataset():
    kp = BY_NAME["kopis_performance"]
    se = BY_NAME["seoul_cultural_event"]
    sink = FakeSink({
        "raw/culture/kopis/kopis_performance/x/page-0001.xml": _kopis_body(kp, ["A", "B"]),
        "raw/culture/seoul/seoul_cultural_event/x/page-000001.json": _seoul_body(se, ["t1", "t2", "t3"]),
    })
    wh = FakeWarehouse()
    summaries = [
        _summary(name="kopis_performance", keys=("raw/culture/kopis/kopis_performance/x/page-0001.xml",)),
        _summary(name="seoul_cultural_event", keys=("raw/culture/seoul/seoul_cultural_event/x/page-000001.json",)),
    ]
    loaded = load_bronze_from_raw(CTX, summaries, sink=sink, warehouse=wh)
    assert loaded == {"kopis_performance": 2, "seoul_cultural_event": 3}
    # records 튜플 형태 = (raw_object_key, page_no=파일명, record dict) — warehouse.load 계약
    name, ts, records = wh.calls[0]
    assert ts == CTX.ingest_ts
    assert records[0][0].endswith("page-0001.xml") and records[0][1] == "page-0001.xml"
    assert records[0][2]["mt20id"] == "A"


def test_load_bronze_isolates_failure_and_fails_loud_at_end():
    kp = BY_NAME["kopis_performance"]
    se = BY_NAME["seoul_cultural_event"]
    sink = FakeSink({
        "k1": _kopis_body(kp, ["A"]),
        "s1": _seoul_body(se, ["t"]),
    })
    wh = FakeWarehouse(fail_on={"kopis_performance"})
    summaries = [
        _summary(name="kopis_performance", keys=("k1",)),
        _summary(name="seoul_cultural_event", keys=("s1",)),
    ]
    with pytest.raises(RuntimeError, match="kopis_performance"):
        load_bronze_from_raw(CTX, summaries, sink=sink, warehouse=wh)
    # 실패한 데이터셋과 무관하게 나머지는 적재됨
    assert [c[0] for c in wh.calls] == ["seoul_cultural_event"]


def test_load_bronze_skips_errored_summaries_and_empty_input():
    wh = FakeWarehouse()
    skipped = _summary(name="kopis_performance", error="skipped (include_detail=False)", keys=())
    assert load_bronze_from_raw(CTX, [skipped], sink=FakeSink({}), warehouse=wh) == {}
    assert load_bronze_from_raw(CTX, [], sink=FakeSink({}), warehouse=wh) == {}
    assert wh.calls == []
