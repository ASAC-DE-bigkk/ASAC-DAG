"""load_bronze 경로 테스트: summary의 raw 키 노출 + 리포트 정리 + raw→bronze 적재.

sys.path 삽입은 conftest.py 가 담당한다 (형제 테스트와 동일 관례).
"""
from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import DatasetResult
from culture_ingest.source.ingest import build_run_report

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
