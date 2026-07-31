import datetime as dt

from culture_ingest.slo import loader


class FakeSink:
    def __init__(self, objects):
        self._o = objects  # {key: bytes}

    def list(self, prefix):
        return [k for k in self._o if k.startswith(prefix)]

    def get(self, key):
        return self._o[key]


def _key(ts):
    return f"raw/culture/_reports/load_date=2026-07-07/ingest_ts={ts}/run_report.json"


# --- A1: 리포트 스캔 순수 함수 ---

def test_report_ingest_ts_extracts():
    assert loader.report_ingest_ts(_key("20260707T000000Z")) == "20260707T000000Z"
    assert loader.report_ingest_ts("raw/culture/foo.json") is None


def test_scan_skips_loaded_and_sorts():
    objs = {
        _key("20260707T000000Z"): b'{"ingest_ts":"20260707T000000Z","run_id":"a"}',
        _key("20260706T000000Z"): b'{"ingest_ts":"20260706T000000Z","run_id":"b"}',
        "raw/culture/_reports/load_date=2026-07-07/ingest_ts=20260707T000000Z/other.json": b"{}",
    }
    got = loader.scan_new_reports(FakeSink(objs), already_loaded={"20260706T000000Z"})
    assert [r["run_id"] for _, r in got] == ["a"]  # 로드된 것 스킵 + run_report.json만


def test_report_records_shape():
    recs = loader.report_records([("k1", {"run_id": "a"})])
    assert recs == [("k1", 0, {"run_id": "a"})]


# --- A2: dag_run 타입드 행 빌더 ---

class FakeDagRun:
    def __init__(self, dag_id, run_id, state, run_type, start, end):
        self.dag_id, self.run_id, self.state, self.run_type = dag_id, run_id, state, run_type
        self.start_date, self.end_date = start, end


def test_dag_run_row_typed_kst():
    r = FakeDagRun(
        "culture_bronze", "scheduled__2026-07-07T18:00:00+00:00", "success", "scheduled",
        dt.datetime(2026, 7, 7, 18, 0, tzinfo=dt.timezone.utc),
        dt.datetime(2026, 7, 7, 18, 5, tzinfo=dt.timezone.utc),
    )
    row = loader.dag_run_row(r, domain="culture")
    assert row["dag_id"] == "culture_bronze"
    assert row["duration_sec"] == 300.0
    assert row["load_date"] == "2026-07-08"  # 18:00Z +9h = 07-08 03:00 KST
    assert row["start_at"].startswith("2026-07-08T03:00:00")
    assert row["domain"] == "culture"


def test_dag_run_row_running_has_no_end():
    r = FakeDagRun(
        "culture_transform", "manual__x", "running", "manual",
        dt.datetime(2026, 7, 7, 0, 0, tzinfo=dt.timezone.utc), None,
    )
    row = loader.dag_run_row(r, domain="culture")
    assert row["end_at"] is None and row["duration_sec"] is None


def test_dag_run_row_carries_retry_counts():
    """#201 — 재시도로 살아난 런이 지표에 남아야 한다(state 만으로는 깨끗한 런과 동일)."""
    r = FakeDagRun(
        "culture_bronze", "scheduled__2026-07-29T18:00:00+00:00", "success", "scheduled",
        dt.datetime(2026, 7, 29, 18, 0, tzinfo=dt.timezone.utc),
        dt.datetime(2026, 7, 29, 18, 11, tzinfo=dt.timezone.utc),
    )
    r.retried_tasks, r.max_try = 3, 2          # 7/30 실측 형태
    row = loader.dag_run_row(r, domain="culture")
    assert row["state"] == "success"           # 겉으로는 성공한 런인데
    assert row["retried_tasks"] == 3           # 세 태스크가 재시도로 살아났다
    assert row["max_try"] == 2


def test_missing_retry_counts_stay_null_not_zero():
    """🔑 관측 공백은 NULL 이다 — 0 으로 접으면 '무재시도'로 위장한다.

    구 표에서 읽은 행, task_instance 조인이 비는 run(막 시작) 이 여기 해당한다.
    ``COUNT`` 기반 마트가 0 과 NULL 을 다르게 세야 '초록 위장'(#147)이 안 생긴다.
    """
    r = FakeDagRun(
        "culture_bronze", "manual__x", "running", "manual",
        dt.datetime(2026, 7, 29, 18, 0, tzinfo=dt.timezone.utc), None,
    )
    row = loader.dag_run_row(r, domain="culture")
    assert row["retried_tasks"] is None and row["max_try"] is None


def test_zero_retries_is_recorded_as_zero():
    """반대쪽 — 실제로 0 회면 0 으로 남아야 한다(NULL 로 뭉개면 관측한 사실이 사라진다)."""
    r = FakeDagRun(
        "culture_bronze", "scheduled__2026-07-30T18:00:00+00:00", "success", "scheduled",
        dt.datetime(2026, 7, 30, 18, 0, tzinfo=dt.timezone.utc),
        dt.datetime(2026, 7, 30, 18, 9, tzinfo=dt.timezone.utc),
    )
    r.retried_tasks, r.max_try = 0, 1          # 7/31 실측 — 완화 ② 첫 실전
    row = loader.dag_run_row(r, domain="culture")
    assert row["retried_tasks"] == 0 and row["max_try"] == 1


def test_slo_dag_ids_are_four_culture_dags():
    assert loader.CULTURE_SLO_DAG_IDS == (
        "culture_bronze", "culture_transform", "culture_maintenance", "culture_facility_refresh",
    )
