"""bronze: 완전성 점검 · run_id 저장 경로/마커 · NDJSON 1파일 적재 단위 테스트."""
import json

from bronze import bronze_tasks
from bronze.validators import assess_completeness
from common import paths
from common.schemas import Dataset
from common.storage import Storage


class FakeStorage(Storage):
    def __init__(self):
        self.data = {}

    def write_bytes(self, key, data):
        self.data[key] = data

    def read_bytes(self, key):
        return self.data[key]

    def exists(self, key):
        return key in self.data

    def list_keys(self, prefix):
        return sorted(k for k in self.data if k.startswith(prefix))


# ── 완전성 점검 ──
def test_completeness_full_sweep_is_ok():
    complete, verified, status = assess_completeness(
        rows_total=981, list_total_count=981, stopped_by_cap=False)
    assert (complete, verified, status) == (True, True, "ok")


def test_completeness_capped_is_partial():
    complete, _v, status = assess_completeness(
        rows_total=400, list_total_count=981, stopped_by_cap=True)
    assert complete is False and status == "partial"


def test_completeness_count_mismatch_is_partial():
    complete, _v, status = assess_completeness(
        rows_total=900, list_total_count=981, stopped_by_cap=False)
    assert complete is False and status == "partial"


def test_completeness_empty_dataset_ok():
    complete, _v, status = assess_completeness(
        rows_total=0, list_total_count=0, stopped_by_cap=False)
    assert complete is True and status == "ok"


# ── 저장 경로(run_id 폴더) ──
def test_bronze_object_key_one_file_per_api():
    # run_id 날짜(YYYY-MM-DD)가 YYYY/MM/DD 파티션으로 펼쳐진다.
    assert paths.bronze_object_key(run_id="2026-06-29_103045_123", short="general_restaurant") == (
        "bronze/commerce/2026/06/29/run_id=2026-06-29_103045_123/general_restaurant.jsonl")


def test_keys_honor_storage_prefix():
    assert paths.bronze_object_key(prefix="dev/exi", run_id="r", short="clinic") == (
        "dev/exi/bronze/commerce/run_id=r/clinic.jsonl")
    assert paths.silver_key(prefix="dev/exi", short="lodging", observed_date="2026-06-29") == (
        "dev/exi/silver/commerce/lodging/observed_date=2026-06-29/part-000.parquet")


def test_marker_keys_inside_run_id():
    assert paths.bronze_marker_key(run_id="r", short="clinic", status=paths.MARKER_COMPLETED) == (
        "bronze/commerce/run_id=r/_markers/clinic.completed")
    assert paths.bronze_run_marker_key(run_id="r", status=paths.MARKER_INCOMPLETE) == (
        "bronze/commerce/run_id=r/_markers/_RUN.incomplete")


def test_silver_key_partitions_by_observed_date():
    assert paths.silver_key(short="lodging", observed_date="2026-06-29") == (
        "silver/commerce/lodging/observed_date=2026-06-29/part-000.parquet")


# ── bronze 적재: API당 1파일(NDJSON) + 마커(completed|incomplete) ──
_DS = Dataset(oa_id="OA-1", name_ko="x", short="clinic", category="health_medical",
              schedule="daily", service_name="LOCALDATA_010102")
_BASE = {"short": "clinic", "oa_id": "OA-1", "name_ko": "x", "service_name": "LOCALDATA_010102",
         "observed_date": "2026-06-29", "run_id": "airflow_r", "bronze_run_id": "R"}


def _write(st, raw_pages, status, complete):
    return bronze_tasks._write_bronze(
        st, prefix="", bronze_run_id="R", dataset=_DS, raw_pages=raw_pages,
        page_metas=[{"page": i + 1} for i in range(len(raw_pages))], base=_BASE,
        status=status, rows_total=sum(1 for _ in raw_pages), list_total_count=2,
        complete=complete, schema_version="v1", base_url="http://x", started_at="t")


def test_write_bronze_ndjson_and_completed_marker():
    st = FakeStorage()
    summary = _write(st, [b'{"a":1}', b'{"a":2}'], status="ok", complete=True)
    bkey = "bronze/commerce/run_id=R/clinic.jsonl"
    assert summary["bronze_key"] == bkey
    assert st.data[bkey] == b'{"a":1}\n{"a":2}\n'          # 줄당 원본 페이지
    mkey = "bronze/commerce/run_id=R/_markers/clinic.completed"
    assert mkey in st.data
    marker = json.loads(st.data[mkey].decode("utf-8"))
    assert marker["marker"] == "completed" and marker["bronze_key"] == bkey
    assert marker["complete"] is True and "***" in marker["source_uri"]   # 키 마스킹


def test_write_bronze_incomplete_marker_when_partial():
    st = FakeStorage()
    summary = _write(st, [b'{"a":1}'], status="partial", complete=False)
    assert summary["status"] == "partial"
    assert "bronze/commerce/run_id=R/_markers/clinic.incomplete" in st.data
    assert "bronze/commerce/run_id=R/_markers/clinic.completed" not in st.data


def test_write_bronze_empty_dataset_no_file_completed_marker():
    st = FakeStorage()
    summary = _write(st, [], status="ok", complete=True)
    assert summary["bronze_key"] is None                  # 빈 데이터셋 → 파일 없음
    assert "bronze/commerce/run_id=R/clinic.jsonl" not in st.data
    assert "bronze/commerce/run_id=R/_markers/clinic.completed" in st.data
