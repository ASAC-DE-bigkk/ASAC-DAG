"""#619 확정안이 culture 기록에 요구한 것 — 규격 부품과 리포트 반영을 함께 잠근다.

여기서 지키는 건 셋이다: 기록 고유키(정정 ③) · 행 수 출처(단계별 혼합안) · NULL≠0.
"""
import pytest

from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import DatasetResult
from culture_ingest.ops.record_spec import (
    NOT_OBSERVED,
    ROW_COUNT_PRIORITY,
    ROW_COUNT_SOURCES,
    SOURCE_BRONZE_RUN_MANIFEST,
    SOURCE_RAW_MANIFEST,
    build_event_id,
    measured_total,
    ratio_pct,
    rows_source_for,
)
from culture_ingest.source.ingest import build_run_report

CTX = RunContext(load_date="2026-07-31", ingest_ts="20260731T030000Z", run_id="scheduled__x")


def _summary(name="kopis_boxoffice", error="", rows=3, iceberg=None):
    r = DatasetResult(name=name, source="kopis", endpoint="boxoffice", prefix="raw/culture/k")
    r.error = error
    r.rows = rows
    r.iceberg_rows = iceberg
    return r.summary()


# ── 기록 고유키 (정정 ③: 점검 기준이 파일 수 → event_id) ──────────────────────

def test_event_id_is_stable_for_same_identity():
    a = build_event_id(domain="culture", run_id="r1")
    b = build_event_id(run_id="r1", domain="culture")  # 인자 순서 무관
    assert a == b


def test_event_id_changes_with_run():
    assert build_event_id(domain="culture", run_id="r1") != build_event_id(
        domain="culture", run_id="r2"
    )


def test_event_id_ignores_none_fields():
    # 필드가 하나 늘 때(값 None) 과거 기록의 고유키가 통째로 바뀌면 대조가 매번 깨진다.
    assert build_event_id(domain="culture") == build_event_id(domain="culture", upstream=None)


def test_report_event_id_is_idempotent_across_retries():
    """리포트 태스크가 재시도돼도 같은 고유키 — 재시도가 기록을 늘리면 이중 집계가 된다.

    공통 모듈(``common/ops/product_observability.py``)은 identity 에 ``try_number`` 를
    넣어 재시도마다 별개 이벤트를 남긴다. culture 리포트는 적재 후 1회 실측한 행 수를
    싣기 때문에 그 방식을 따르면 3회 재시도한 run 의 적재량이 3배로 부푼다.
    """
    first = build_run_report([_summary()], CTX, expected_total=1)
    again = build_run_report([_summary()], CTX, expected_total=1)
    assert first["event_id"] == again["event_id"]
    assert first["record_count"] == 1


# ── 행 수 출처 (단계별 혼합안) ────────────────────────────────────────────────

def test_vocabulary_matches_the_common_contract():
    """어휘는 culture 가 정한 게 아니라 공통 계약(product-observability/v2)을 따른 것.

    같은 뜻에 도메인마다 다른 이름을 쓰면 조회 DB 합류에 매핑 표가 하나 더 생긴다 —
    #619 가 없애려는 "제각각"이 이름 층위로 옮겨 앉는 것이다. 이 목록이 어긋나면
    공통 계약이 바뀐 것이므로 culture 도 같이 고쳐야 한다.
    """
    assert set(ROW_COUNT_SOURCES) == {
        "raw_manifest", "bronze_run_manifest", "iceberg_snapshot",
        "count_query", "publication_ledger", "not_observed",
    }


def test_row_count_source_names_both_numbers():
    report = build_run_report([_summary(iceberg=3)], CTX, expected_total=1)
    src = report["row_count_source"]
    assert src["total_rows"] == SOURCE_RAW_MANIFEST                  # 수집이 센 값
    assert src["total_iceberg_rows"] == SOURCE_BRONZE_RUN_MANIFEST   # 적재가 실측한 값
    assert set(src.values()) <= set(ROW_COUNT_SOURCES)


def test_report_carries_the_common_row_count_pair():
    # 공통 계약의 (row_count, rows_source) 짝을 그대로 실어, 도메인을 모르는 조회
    # 모델도 이 기록의 정본 행 수를 한 필드에서 읽을 수 있게 한다.
    report = build_run_report([_summary(iceberg=3)], CTX, expected_total=1)
    assert report["row_count"] == report["total_iceberg_rows"] == 3
    assert report["rows_source"] == SOURCE_BRONZE_RUN_MANIFEST


def test_unmeasured_rows_are_marked_not_observed():
    # 재지 않은 수치에 정본 출처를 적어 두면 "쟀는데 0"으로 읽힌다. 공통 계약도
    # 같은 조합(NULL + 정본 출처)을 거부한다.
    report = build_run_report([_summary(iceberg=None)], CTX, expected_total=1, load_failed=True)
    assert report["row_count"] is None
    assert report["rows_source"] == NOT_OBSERVED
    assert report["row_count_source"]["total_iceberg_rows"] == NOT_OBSERVED


def test_rows_source_is_derived_not_validated():
    """유도라 어긋난 조합 자체가 만들어지지 않는다 — 검증이면 여기서 예외가 났다.

    공통 모듈은 같은 규칙을 ``raise ValueError`` 로 지키는데, 그 호출이 fail-open
    경계 밖에 있으면 관측 코드가 본 작업을 죽인다(#619 착수 전 1).
    """
    assert rows_source_for(None, SOURCE_BRONZE_RUN_MANIFEST) == NOT_OBSERVED
    assert rows_source_for(0, SOURCE_BRONZE_RUN_MANIFEST) == SOURCE_BRONZE_RUN_MANIFEST


def test_row_count_priority_prefers_measurement_over_self_report():
    # culture 의 선언: 실측이 태스크 자기보고보다 앞선다(#619 보완 3).
    assert ROW_COUNT_PRIORITY[0] == SOURCE_BRONZE_RUN_MANIFEST
    assert ROW_COUNT_PRIORITY.index(SOURCE_BRONZE_RUN_MANIFEST) < ROW_COUNT_PRIORITY.index(
        SOURCE_RAW_MANIFEST
    )
    assert set(ROW_COUNT_PRIORITY) <= set(ROW_COUNT_SOURCES)
    assert NOT_OBSERVED not in ROW_COUNT_PRIORITY  # 미측정은 셀 수 없다


# ── NULL≠0 ───────────────────────────────────────────────────────────────────

def test_measured_total_returns_none_when_nothing_measured():
    assert measured_total([None, None]) == (None, 0)


def test_measured_total_counts_a_real_zero():
    # 0 은 "쟀는데 0행"이라 측정 건수에 들어간다 — None 과 섞이면 안 된다.
    assert measured_total([0, None]) == (0, 1)


def test_measured_total_partial():
    assert measured_total([5, None, 7]) == (12, 2)


def test_ratio_pct_is_none_without_denominator():
    assert ratio_pct(0, 0) is None
    assert ratio_pct(1, 2) == 50.0


def test_load_failure_does_not_claim_zero_rows_loaded():
    """적재 태스크가 죽은 run 이 '전 데이터셋 0행 적재'를 주장하지 않는다.

    확정안이 금지한 모양 그대로였던 자리다 — 측정한 적 없는 사실이 0 으로 기록되면
    대시보드가 그날의 적재 추이를 진짜 0 으로 그린다.
    """
    report = build_run_report(
        [_summary(name="a", iceberg=None), _summary(name="b", iceberg=None)],
        CTX, expected_total=2, load_failed=True,
    )
    assert report["total_iceberg_rows"] is None
    assert report["iceberg_rows_measured"] == 0
    assert all(d["iceberg_rows"] is None for d in report["datasets"])


def test_partial_measurement_reports_its_denominator():
    report = build_run_report(
        [_summary(name="a", iceberg=5), _summary(name="b", iceberg=None)],
        CTX, expected_total=2,
    )
    assert report["total_iceberg_rows"] == 5
    assert report["iceberg_rows_measured"] == 1     # 분자
    assert report["coverage"]["landed"] == 2        # 분모 — 관측 커버리지 50%


def test_annihilated_plan_has_null_coverage_not_zero_pct():
    # expected=0 은 분모가 없어 달성률이 정의되지 않는다. 0.0 은 "0% 달성"이라는 주장이다.
    report = build_run_report([], CTX, expected_total=0)
    assert report["coverage"]["coverage_pct"] is None
    assert report["slo_passed"] is False  # 기존 #185 판정은 무변화


@pytest.mark.parametrize("landed,expected,pct", [(1, 1, 100.0), (1, 2, 50.0), (0, 3, 0.0)])
def test_coverage_pct_unchanged_when_denominator_exists(landed, expected, pct):
    summaries = [_summary(name=f"ok{i}", iceberg=1) for i in range(landed)]
    summaries += [_summary(name=f"bad{i}", error="boom") for i in range(expected - landed)]
    report = build_run_report(summaries, CTX, expected_total=expected)
    assert report["coverage"]["coverage_pct"] == pct


# ── ASK-Seoul#78 F-* 식별 묶음 ────────────────────────────────────────────────

def test_report_carries_environment_and_dag_identity():
    """적재기 폴백을 안 타게 리포트가 자기 환경·DAG 를 밝힌다(Z-7 양방향 오염 통로)."""
    report = build_run_report(
        [_summary()], CTX, expected_total=1,
        environment="prod", dag_id="culture_bronze", task_id="report",
    )
    assert report["environment"] == "prod"
    assert report["dag_id"] == "culture_bronze"
    assert report["task_id"] == "report"


def test_report_identity_is_null_when_unknown_not_guessed():
    # 모르는 값을 "dev" 같은 기본값으로 채우면 그게 곧 거짓 기록이다(F-3 NULL≠0 과 같은 원칙).
    report = build_run_report([_summary()], CTX, expected_total=1)
    assert report["environment"] is None
    assert report["dag_id"] is None
    assert report["task_id"] is None


def test_dag_identity_is_metadata_not_part_of_the_key():
    """`dag_id`·`task_id` 는 기록의 설명이지 정체성이 아니다 — 붙어도 고유키는 그대로."""
    plain = build_run_report([_summary()], CTX, expected_total=1)
    tagged = build_run_report(
        [_summary()], CTX, expected_total=1, dag_id="culture_bronze", task_id="report",
    )
    assert plain["event_id"] == tagged["event_id"]


def test_environment_is_part_of_the_key():
    """🔴 dev·prod 가 같은 고유키를 가지면 한 조회 DB 에서 서로를 덮어쓴다.

    스케줄 run 의 `run_id`(``scheduled__…``)와 `ingest_ts`(data interval 유도)는 두
    인스턴스가 같은 값이라, 환경이 빠지면 같은 스케줄의 dev 기록과 prod 기록이 구분되지
    않는다. `_ops_run_event` 의 자연키가 event_id 라 upsert 가 조용히 하나를 지운다.
    """
    dev = build_run_report([_summary()], CTX, expected_total=1, environment="dev")
    prod = build_run_report([_summary()], CTX, expected_total=1, environment="prod")
    assert dev["event_id"] != prod["event_id"]


def test_same_environment_still_idempotent():
    """환경이 같으면 몇 번을 다시 써도 같은 키 — 재시도가 기록을 늘리지 않는다."""
    a = build_run_report([_summary()], CTX, expected_total=1, environment="prod")
    b = build_run_report([_summary()], CTX, expected_total=1, environment="prod")
    assert a["event_id"] == b["event_id"]
