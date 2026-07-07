"""실행 메트릭 로그 단위 테스트 (#188) — track 기록·매핑·실패안전·dbt 파싱·폴백."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common import runmetrics  # noqa: E402
from common.runmetrics import (  # noqa: E402
    MetricsFileSink,
    MetricsR2Sink,
    _RECORD_FIELDS,
    parse_dbt_run_results,
    resolve_sink,
    track,
)


# ── 테스트용 Airflow context 더블 ────────────────────────────────────────────────
class _TI:
    def __init__(self, dag_id, task_id, try_number):
        self.dag_id = dag_id
        self.task_id = task_id
        self.try_number = try_number


def _context(run_id="manual__2026-07-07T00:00:00+00:00", try_number=1,
             data_interval_end=None, **extra):
    ctx = {
        "ti": _TI("transit_master_bronze", "land_subway_station_master", try_number),
        "run_id": run_id,
        "data_interval_end": data_interval_end,
    }
    ctx.update(extra)
    return ctx


def _read_only_record(tmp_path):
    """tmp sink 에 쓰인 유일 레코드를 읽어 돌려준다."""
    files = list(tmp_path.rglob("*.json"))
    assert len(files) == 1, files
    return files[0], json.loads(files[0].read_text(encoding="utf-8"))


# ── 정상 기록: 스키마 키 전부 존재 ───────────────────────────────────────────────
def test_track_writes_all_schema_keys(tmp_path):
    sink = MetricsFileSink(root=tmp_path)

    @track(layer="bronze", domain="transit", sink=sink)
    def land(spec_key, **context):
        return {"rows": 784, "bytes": 2048}

    result = land(**_context(), spec_key="subway_station_master")
    assert result == {"rows": 784, "bytes": 2048}

    path, record = _read_only_record(tmp_path)
    assert set(record.keys()) == set(_RECORD_FIELDS)
    assert record["layer"] == "bronze"
    assert record["domain"] == "transit"
    assert record["dag_id"] == "transit_master_bronze"
    assert record["task_id"] == "land_subway_station_master"
    assert record["status"] == "success"
    assert record["skipped"] is False
    assert record["rows"] == 784
    assert record["bytes"] == 2048
    assert record["error_id"] is None
    assert record["started_at"].endswith("+00:00")  # UTC ISO8601
    assert record["duration_s"] is not None and record["duration_s"] >= 0
    assert record["api_calls"] == 0 and record["retries"] == 0


# ── 반환 dict 매핑: inserted→rows, skipped ───────────────────────────────────────
def test_inserted_maps_to_rows_and_skipped_flag(tmp_path):
    sink = MetricsFileSink(root=tmp_path)

    @track(layer="bronze", domain="transit", sink=sink)
    def load(**context):
        # 멱등 skip 태스크 반환 형태(master load_master 와 동일).
        return {"inserted": 0, "skipped": True, "existing": 784}

    load(**_context())
    _, record = _read_only_record(tmp_path)
    assert record["rows"] == 0           # inserted → rows
    assert record["skipped"] is True
    assert record["status"] == "skipped"  # skipped=True → status=skipped


def test_non_dict_return_leaves_rows_null(tmp_path):
    sink = MetricsFileSink(root=tmp_path)

    @track(layer="bronze", domain="transit", sink=sink)
    def f(**context):
        return None

    f(**_context())
    _, record = _read_only_record(tmp_path)
    assert record["rows"] is None and record["bytes"] is None
    assert record["skipped"] is False and record["status"] == "success"


# ── 예외 시: failed 기록 + 재던짐 ────────────────────────────────────────────────
def test_exception_records_failed_and_reraises(tmp_path):
    sink = MetricsFileSink(root=tmp_path)

    @track(layer="bronze", domain="transit", sink=sink)
    def boom(**context):
        raise RuntimeError("적재 실패")

    with pytest.raises(RuntimeError, match="적재 실패"):
        boom(**_context())

    _, record = _read_only_record(tmp_path)
    assert record["status"] == "failed"
    assert record["skipped"] is False
    assert record["finished_at"] is not None


# ── 기록 실패 무해성: sink 예외가 태스크를 실패시키지 않음 ────────────────────────
def test_sink_failure_is_harmless(tmp_path):
    class BrokenSink(MetricsFileSink):
        def write(self, record):
            raise OSError("disk full")

    @track(layer="bronze", domain="transit", sink=BrokenSink())
    def f(**context):
        return {"rows": 5}

    # sink 가 터져도 콜러블 반환은 그대로, 예외 전파 없음.
    assert f(**_context()) == {"rows": 5}


def test_sink_failure_does_not_swallow_callable_exception(tmp_path):
    class BrokenSink(MetricsFileSink):
        def write(self, record):
            raise OSError("disk full")

    @track(layer="bronze", domain="transit", sink=BrokenSink())
    def boom(**context):
        raise ValueError("원 예외")

    # 기록이 실패해도 원 콜러블 예외가 보존되어 재던져진다.
    with pytest.raises(ValueError, match="원 예외"):
        boom(**_context())


# ── run_id 파일명 안전화 ─────────────────────────────────────────────────────────
def test_run_id_filename_is_sanitized(tmp_path):
    sink = MetricsFileSink(root=tmp_path)

    @track(layer="bronze", domain="transit", sink=sink)
    def f(**context):
        return {"rows": 1}

    f(**_context(run_id="scheduled__2026-07-07T00:00:00+09:00", try_number=2))
    path, record = _read_only_record(tmp_path)
    name = path.name
    # ':' '+' 등 파일명 불가 문자가 남지 않는다. try<N> 포함.
    assert ":" not in name and "+" not in name
    assert name.endswith("__try2.json")
    assert record["run_id"] == "scheduled__2026-07-07T00:00:00+09:00"  # 본문엔 원본 보존
    # date= 파티션은 started_at UTC 날짜.
    assert path.parent.name.startswith("date=")


# ── schedule_delay_s: data_interval_end 기준, 음수/부재면 null ────────────────────
def test_schedule_delay_null_when_no_interval(tmp_path):
    sink = MetricsFileSink(root=tmp_path)

    @track(layer="bronze", domain="transit", sink=sink)
    def f(**context):
        return {"rows": 1}

    f(**_context(data_interval_end=None))
    _, record = _read_only_record(tmp_path)
    assert record["schedule_delay_s"] is None


def test_schedule_delay_positive_when_interval_in_past(tmp_path):
    from datetime import datetime, timedelta, timezone
    sink = MetricsFileSink(root=tmp_path)
    past = datetime.now(timezone.utc) - timedelta(seconds=60)

    @track(layer="bronze", domain="transit", sink=sink)
    def f(**context):
        return {"rows": 1}

    f(**_context(data_interval_end=past))
    _, record = _read_only_record(tmp_path)
    assert record["schedule_delay_s"] is not None
    assert record["schedule_delay_s"] >= 60


# ── HttpCore 카운터 집계 ─────────────────────────────────────────────────────────
def test_http_counters_aggregated(tmp_path):
    sink = MetricsFileSink(root=tmp_path)

    @track(layer="bronze", domain="transit", sink=sink)
    def f(**context):
        # 컬렉터 활성 구간에서 요청/재시도를 직접 note(HttpCore 대체).
        runmetrics.note_http_request()
        runmetrics.note_http_request()
        runmetrics.note_http_retry()
        return {"rows": 3}

    f(**_context())
    _, record = _read_only_record(tmp_path)
    assert record["api_calls"] == 2
    assert record["retries"] == 1


def test_http_counters_noop_when_inactive():
    # 컬렉터 비활성(기본)에서는 no-op — 예외 없이 조용히 무시.
    runmetrics.note_http_request()
    runmetrics.note_http_retry()


# ── resource 부재 폴백 ───────────────────────────────────────────────────────────
def test_resource_absent_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr(runmetrics, "_resource", None)
    sink = MetricsFileSink(root=tmp_path)

    @track(layer="bronze", domain="transit", sink=sink)
    def f(**context):
        return {"rows": 1}

    f(**_context())
    _, record = _read_only_record(tmp_path)
    assert record["peak_rss_mb"] is None
    assert record["cpu_time_s"] is None
    # 시간 측정(wall clock)은 resource 와 무관 — 여전히 존재.
    assert record["duration_s"] is not None


# ── 콜러블 인자 필터링(context 미수용 콜러블) ────────────────────────────────────
def test_callable_without_kwargs_still_runs(tmp_path):
    sink = MetricsFileSink(root=tmp_path)
    calls = []

    @track(layer="bronze", domain="transit", sink=sink)
    def ingest():  # context 를 받지 않는 실시간 콜러블(ingest_parking 형태)
        calls.append(1)
        return {"parking": 5}

    ingest(**_context())
    assert calls == [1]
    _, record = _read_only_record(tmp_path)
    assert record["status"] == "success"
    assert record["rows"] is None  # 'parking' 은 관례 키 아님 → null


# ── dbt run_results 파싱 ─────────────────────────────────────────────────────────
_FIXTURE = {
    "metadata": {"invocation_id": "inv-123"},
    "results": [
        {
            "unique_id": "model.transit.silver_subway_arrival",
            "status": "success",
            "execution_time": 3.5,
            "adapter_response": {"code": "INSERT", "rows_affected": 120},
            "timing": [
                {"name": "compile", "started_at": "2026-07-07T06:42:53.000000Z",
                 "completed_at": "2026-07-07T06:42:53.100000Z"},
                {"name": "execute", "started_at": "2026-07-07T06:42:54.000000Z",
                 "completed_at": "2026-07-07T06:42:57.500000Z"},
            ],
        },
        {
            "unique_id": "test.transit.not_null_x",
            "status": "warn",
            "execution_time": 0.4,
            "adapter_response": {"code": "SUCCESS", "rows_affected": -1},
            "timing": [
                {"name": "execute", "started_at": "2026-07-07T06:43:00.000000Z",
                 "completed_at": "2026-07-07T06:43:00.400000Z"},
            ],
        },
        {
            "unique_id": "model.transit.silver_skipped",
            "status": "skipped",
            "execution_time": 0.0,
            "adapter_response": {},
            "timing": [],
        },
    ],
}


def test_parse_dbt_run_results(tmp_path):
    path = tmp_path / "run_results.json"
    path.write_text(json.dumps(_FIXTURE), encoding="utf-8")

    records = parse_dbt_run_results(path, domain="transit", target="dev")
    assert len(records) == 3
    for r in records:
        assert set(r.keys()) == set(_RECORD_FIELDS)
        assert r["layer"] == "silver"
        assert r["domain"] == "transit"
        assert r["target"] == "dev"
        assert r["run_id"] == "inv-123"
        assert r["dag_id"] is None        # silver 는 dag 없음
        assert r["peak_rss_mb"] is None   # 메모리·CPU·api 필드는 null
        assert r["cpu_time_s"] is None
        assert r["api_calls"] is None

    first = records[0]
    assert first["task_id"] == "silver_subway_arrival"
    assert first["status"] == "success"
    assert first["rows"] == 120
    assert first["duration_s"] == 3.5
    assert first["started_at"] == "2026-07-07T06:42:54.000000Z"   # execute 타이밍
    assert first["finished_at"] == "2026-07-07T06:42:57.500000Z"

    warn = records[1]
    assert warn["status"] == "success"   # warn → success(실패 아님)
    assert warn["rows"] is None          # rows_affected=-1 → null

    skipped = records[2]
    assert skipped["status"] == "skipped"
    assert skipped["skipped"] is True


def test_dump_dbt_run_results_writes_files(tmp_path):
    path = tmp_path / "run_results.json"
    path.write_text(json.dumps(_FIXTURE), encoding="utf-8")
    out = tmp_path / "metrics"
    sink = MetricsFileSink(root=out)

    records = runmetrics.dump_dbt_run_results(path, domain="transit",
                                              target="dev", sink=sink)
    files = list(out.rglob("*.json"))
    assert len(files) == len(records) == 3


# ── R2 sink: 키 규약·직렬화 (fake put_object — 네트워크 없음) ─────────────────────
class _FakePut:
    def __init__(self):
        self.calls = []  # (key, payload) 튜플

    def __call__(self, key, payload):
        self.calls.append((key, payload))


def test_r2_sink_bronze_key_convention_and_payload():
    put = _FakePut()
    sink = MetricsR2Sink(put_object=put)

    @track(layer="bronze", domain="transit", sink=sink)
    def f(**context):
        return {"rows": 7}

    f(**_context(run_id="scheduled__2026-07-07T00:00:00+09:00", try_number=2))
    assert len(put.calls) == 1
    key, payload = put.calls[0]
    # metrics/date=YYYY-MM-DD/domain=<domain>/<dag>__<task>__<run 안전화>__tryN.json
    assert key.startswith("metrics/date=")
    assert "/domain=transit/" in key
    assert key.endswith("__try2.json")
    assert ":" not in key.split("/")[-1] and "+" not in key.split("/")[-1]
    record = json.loads(payload.decode("utf-8"))
    assert set(record.keys()) == set(_RECORD_FIELDS)
    assert record["run_id"] == "scheduled__2026-07-07T00:00:00+09:00"  # 본문 원본 보존
    assert record["rows"] == 7


def test_r2_sink_silver_key_uses_model_and_invocation(tmp_path):
    put = _FakePut()
    sink = MetricsR2Sink(put_object=put)
    path = tmp_path / "run_results.json"
    path.write_text(json.dumps(_FIXTURE), encoding="utf-8")

    runmetrics.dump_dbt_run_results(path, domain="transit", target="dev", sink=sink)
    assert len(put.calls) == 3
    key = put.calls[0][0]
    # silver 는 dag 없음 — <domain> 아래 <모델명>__<invocation>.json
    assert "/domain=transit/" in key
    assert key.endswith("silver_subway_arrival__inv-123.json")
    assert "unknown" not in key  # dag_id 부재가 이름에 새지 않음


def test_r2_put_failure_is_harmless():
    def broken_put(key, payload):
        raise ConnectionError("R2 unreachable")

    @track(layer="bronze", domain="transit", sink=MetricsR2Sink(put_object=broken_put))
    def f(**context):
        return {"rows": 1}

    # R2 PUT 실패(자격 없음·네트워크 등)도 본 태스크를 실패시키지 않는다.
    assert f(**_context()) == {"rows": 1}


# ── sink 선택 로직: 기본 R2, ASAC_METRICS_DIR 설정 시 파일 ────────────────────────
def test_resolve_sink_default_is_r2(monkeypatch):
    monkeypatch.delenv("ASAC_METRICS_DIR", raising=False)
    assert isinstance(resolve_sink(), MetricsR2Sink)


def test_resolve_sink_env_override_is_file(monkeypatch, tmp_path):
    monkeypatch.setenv("ASAC_METRICS_DIR", str(tmp_path))
    sink = resolve_sink()
    assert isinstance(sink, MetricsFileSink)
    # 지정 디렉토리 아래에 실제로 쓰인다.
    record = runmetrics._blank_record()
    record.update(dag_id="d", task_id="t", run_id="r", try_number=1,
                  started_at="2026-07-07T00:00:00+00:00")
    written = sink.write(record)
    assert written.is_file() and str(written).startswith(str(tmp_path))
