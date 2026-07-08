"""#202 — load_bronze 데이터셋 병렬화 검증.

26분 병목(커밋 고정비 × 순차 216쿼리)의 1단계: 데이터셋 단위 ThreadPool 병렬화.
고정할 계약:

1. ``max_workers > 1`` 이면 실제로 **동시에** 돈다 (겹침 실측 — 회귀 시 조용히
   순차로 돌아가 26분이 되돌아오는 걸 막는다).
2. 기존 의미 불변 — 실패 격리·말미 fail loud·파싱 유실 대조·반환 dict 는
   순차와 동일. ``max_workers=1`` 은 현행 순차 경로 그대로.
3. 진행 로그 — 데이터셋별 완료 로그(행수·소요)가 남는다 (26분 블랙박스 해소).

sys.path 삽입은 conftest.py 가 담당한다 (형제 테스트와 동일 관례).
"""
import json
import threading
import time

import pytest

from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import DatasetResult
from culture_ingest.source.datasets import BY_NAME
from culture_ingest.source.ingest import load_bronze_from_raw

CTX = RunContext(load_date="2026-07-08", ingest_ts="20260708T000000Z", run_id="test")


def _summary(name, keys, rows):
    ds = BY_NAME[name]
    r = DatasetResult(name=name, source=ds.source, endpoint=ds.endpoint, prefix=f"raw/culture/{name}")
    r.object_keys = list(keys)
    r.pages, r.rows = len(keys), rows
    return r.summary()


def _seoul_body(name, titles):
    ds = BY_NAME[name]
    return json.dumps({ds.endpoint: {"row": [{"TITLE": t} for t in titles]}}).encode()


class FakeSink:
    def __init__(self, objects):
        self.objects = objects

    def get(self, key):
        return self.objects[key]


class ConcurrencyProbeWarehouse:
    """load() 진입/이탈을 세어 최대 동시 실행 수를 기록하는 가짜 웨어하우스."""

    def __init__(self, dwell_sec=0.05, fail_on=None):
        self.calls = []
        self.fail_on = fail_on or set()
        self._dwell = dwell_sec
        self._lock = threading.Lock()
        self._active = 0
        self.max_active = 0

    def load(self, ds, ctx, records):
        with self._lock:
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        try:
            time.sleep(self._dwell)  # 겹침이 생길 시간 창
            if ds.name in self.fail_on:
                raise RuntimeError("trino down")
            with self._lock:
                self.calls.append(ds.name)
            return len(records)
        finally:
            with self._lock:
                self._active -= 1


def _two_dataset_fixture():
    ev, sj = "seoul_cultural_event", "seoul_sejong"
    sink = FakeSink({
        "e1": _seoul_body(ev, ["a", "b"]),
        "s1": _seoul_body(sj, ["c"]),
    })
    summaries = [_summary(ev, ("e1",), 2), _summary(sj, ("s1",), 1)]
    return ev, sj, sink, summaries


def test_parallel_actually_overlaps():
    ev, sj, sink, summaries = _two_dataset_fixture()
    wh = ConcurrencyProbeWarehouse()
    loaded = load_bronze_from_raw(CTX, summaries, sink=sink, warehouse=wh, max_workers=4)
    assert loaded == {ev: 2, sj: 1}
    assert wh.max_active >= 2  # 실제 동시 실행 — 조용한 순차 회귀 방지


def test_sequential_default_never_overlaps():
    _, _, sink, summaries = _two_dataset_fixture()
    wh = ConcurrencyProbeWarehouse()
    load_bronze_from_raw(CTX, summaries, sink=sink, warehouse=wh)  # 기본 = 순차
    assert wh.max_active == 1


def test_parallel_isolates_failure_and_fails_loud_at_end():
    ev, sj, sink, summaries = _two_dataset_fixture()
    wh = ConcurrencyProbeWarehouse(fail_on={"seoul_cultural_event"})
    with pytest.raises(RuntimeError, match="seoul_cultural_event"):
        load_bronze_from_raw(CTX, summaries, sink=sink, warehouse=wh, max_workers=4)
    assert wh.calls == [sj]  # 실패와 무관하게 나머지는 적재됨 (순차와 동일 의미)


def test_parallel_parse_loss_check_kept():
    # 파싱 유실 대조(적재 전 fail loud)가 병렬 경로에서도 살아있는지.
    ev, sj, sink, summaries = _two_dataset_fixture()
    summaries[0]["rows"] = 99  # fetch 행수와 불일치 조작
    wh = ConcurrencyProbeWarehouse()
    with pytest.raises(RuntimeError, match="파싱"):
        load_bronze_from_raw(CTX, summaries, sink=sink, warehouse=wh, max_workers=4)
    assert wh.calls == [sj]  # 의심 데이터셋은 테이블에 안 씀


def test_per_dataset_timing_logged(capsys):
    ev, sj, sink, summaries = _two_dataset_fixture()
    wh = ConcurrencyProbeWarehouse()
    load_bronze_from_raw(CTX, summaries, sink=sink, warehouse=wh, max_workers=4)
    out = capsys.readouterr().out
    assert f"[load] {ev}" in out and f"[load] {sj}" in out  # 블랙박스 해소(#202)
