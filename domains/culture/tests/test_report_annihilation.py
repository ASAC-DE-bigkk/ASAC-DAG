"""#185 — 상류 전멸 run 이 success 로 위장하는 구멍 검증.

7/7 사고(#182)의 재현: plan 이 전멸하면 expected=0 이 되고, failed 목록도 비어
``slo_passed=True`` 인 "정직하지 않은" 리포트가 나왔다. 여기서는 두 층을 고정한다:

1. ``build_run_report`` — expected=0 이면 SLO 는 통과일 수 없다.
2. ``annihilation_reason`` — report 태스크가 "리포트/알림 발송 후 스스로 실패"할지
   판정하는 순수 함수. 전멸(plan 죽음·수집 0건)만 잡고, 부분 실패·전부 의도적
   skip 은 기존 동작(초록 run + SLO surface)을 유지한다.

sys.path 삽입은 conftest.py 가 담당한다 (형제 테스트와 동일 관례).
"""
from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import DatasetResult
from culture_ingest.source.ingest import annihilation_reason, build_run_report

CTX = RunContext(load_date="2026-07-07", ingest_ts="20260707T150000Z", run_id="test")


def _summary(name="kopis_boxoffice", error="", rows=3):
    r = DatasetResult(name=name, source="kopis", endpoint="boxoffice", prefix="raw/culture/k")
    r.error = error
    r.rows = rows
    return r.summary()


# ── build_run_report: expected=0 은 SLO 통과가 될 수 없다 ────────────────────

def test_plan_annihilation_fails_slo():
    # 7/7 사고 모드 그대로: plan 전멸 → summaries 없음, expected=0.
    report = build_run_report([], CTX, expected_total=0)
    assert report["slo_passed"] is False
    assert report["coverage"]["expected"] == 0


def test_normal_run_slo_unchanged():
    report = build_run_report([_summary()], CTX, expected_total=1)
    assert report["slo_passed"] is True  # 기존 PASS 판정은 무변화


# ── annihilation_reason: 전멸만 잡는다 ───────────────────────────────────────

def test_reason_when_plan_dead():
    report = build_run_report([], CTX, expected_total=0)
    assert annihilation_reason(report["coverage"])  # plan 죽음 → run 실패 사유 있음


def test_reason_when_all_fetch_failed():
    # plan 은 됐지만(expected=2) 수집이 하나도 착지하지 못한 run.
    summaries = [
        _summary(name="a", error="boom", rows=0),
        _summary(name="b", error="task failed (no result reported)", rows=0),
    ]
    report = build_run_report(summaries, CTX, expected_total=2)
    assert report["slo_passed"] is False
    assert annihilation_reason(report["coverage"])


def test_no_reason_on_partial_failure():
    # 일부만 실패 → 기존 동작 유지(run 초록 + SLO surface). 전멸 아님.
    summaries = [_summary(name="a"), _summary(name="b", error="boom", rows=0)]
    report = build_run_report(summaries, CTX, expected_total=2)
    assert report["slo_passed"] is False        # SLO 는 실패로 surface
    assert annihilation_reason(report["coverage"]) is None


def test_no_reason_when_all_skipped():
    # 전부 의도적 skip(수집할 게 없던 run) — 실패가 아니므로 run 을 빨갛게 하지 않는다.
    summaries = [_summary(name="a", error="skipped (include_detail=False)", rows=0)]
    report = build_run_report(summaries, CTX, expected_total=1)
    assert report["slo_passed"] is True
    assert annihilation_reason(report["coverage"]) is None
