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

    def delete(self, key):
        self.data.pop(key, None)


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
        "raw/commerce/2026/06/29/run_id=2026-06-29_103045_123/general_restaurant.jsonl")


def test_keys_honor_storage_prefix():
    assert paths.bronze_object_key(prefix="dev/exi", run_id="r", short="clinic") == (
        "dev/exi/raw/commerce/run_id=r/clinic.jsonl")
    assert paths.silver_key(prefix="dev/exi", short="lodging", observed_date="2026-06-29") == (
        "dev/exi/silver/commerce/lodging/observed_date=2026-06-29/part-000.parquet")


def test_marker_keys_inside_run_id():
    assert paths.bronze_marker_key(run_id="r", short="clinic", status=paths.MARKER_COMPLETED) == (
        "raw/commerce/run_id=r/_markers/clinic.completed")
    assert paths.bronze_run_marker_key(run_id="r", status=paths.MARKER_INCOMPLETE) == (
        "raw/commerce/run_id=r/_markers/_RUN.incomplete")


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


def _page(rows, code="INFO-000"):
    return json.dumps({"LOCALDATA_010102": {
        "list_total_count": len(rows), "RESULT": {"CODE": code, "MESSAGE": "정상"},
        "row": rows}}, ensure_ascii=False).encode("utf-8")


def test_write_bronze_first_run_sorts_rows_and_seeds_diff_target():
    # feat/58 이후: _write_bronze 는 페이지를 파싱→row 로 증분 저장(UPDATEDT desc), diff-target 시드.
    st = FakeStorage()
    r_old = {"MGTNO": "A", "UPDATEDT": "2026-06-29 09:00:00", "BPLCNM": "가게A"}
    r_new = {"MGTNO": "B", "UPDATEDT": "2026-06-30 10:00:00", "BPLCNM": "가게B"}
    summary = _write(st, [_page([r_old, r_new])], status="ok", complete=True)

    bkey = "raw/commerce/run_id=R/clinic.jsonl"
    assert summary["bronze_key"] == bkey and summary["increment_mode"] == "first"
    # row-NDJSON(줄당 레코드 1개) + UPDATEDT 내림차순(최신 B 먼저)
    lines = [json.loads(l) for l in st.data[bkey].decode("utf-8").splitlines() if l.strip()]
    assert [r["MGTNO"] for r in lines] == ["B", "A"]
    # diff-target(정렬 전체본, 수집일 태깅) + 검증키 사이드카 — run_id="R" 은 날짜 형식이
    # 아니라 observed_date(2026-06-29) 로 폴백 태깅된다.
    assert "raw/commerce/_diff_target/clinic.2026-06-29.jsonl" in st.data
    assert "raw/commerce/_diff_target/clinic.2026-06-29.key" in st.data
    assert "raw/commerce/run_id=R/_full/clinic.jsonl" not in st.data   # landing 이동 후 삭제(완료)
    # 마커: completed + 증분 리니지(검증키/모드/diff 위치)
    marker = json.loads(st.data["raw/commerce/run_id=R/_markers/clinic.completed"].decode("utf-8"))
    assert marker["marker"] == "completed" and marker["increment_mode"] == "first"
    assert marker["diff_target_key"].endswith("clinic.2026-06-29.jsonl")
    assert marker["verification_key"] and "***" in marker["source_uri"]   # 키 마스킹


def test_write_bronze_identical_second_run_stores_no_increment():
    # 같은 데이터 재수집: diff-target 과 검증키 동일 → 증분 파일 미생성(중복 저장 0), 마커만.
    st = FakeStorage()
    rows = [{"MGTNO": "A", "UPDATEDT": "2026-06-29 09:00:00", "BPLCNM": "가게A"}]
    _write(st, [_page(rows)], status="ok", complete=True)              # 첫 수집(mode=first)
    st.data = {k: v for k, v in st.data.items() if "/run_id=" not in k}  # run 폴더만 비움(diff-target 유지)
    summary = _write(st, [_page(rows)], status="ok", complete=True)    # 동일 데이터 재수집
    assert summary["increment_mode"] == "identical"
    assert summary["bronze_key"] is None                               # 증분 없음 → 파일 미생성
    assert "raw/commerce/run_id=R/clinic.jsonl" not in st.data


def test_write_bronze_incomplete_marker_when_partial():
    st = FakeStorage()
    summary = _write(st, [b'{"a":1}'], status="partial", complete=False)
    assert summary["status"] == "partial"
    assert "raw/commerce/run_id=R/_markers/clinic.incomplete" in st.data
    assert "raw/commerce/run_id=R/_markers/clinic.completed" not in st.data


def test_write_bronze_empty_dataset_no_file_completed_marker():
    st = FakeStorage()
    summary = _write(st, [], status="ok", complete=True)
    assert summary["bronze_key"] is None                  # 빈 데이터셋 → 파일 없음
    assert "raw/commerce/run_id=R/clinic.jsonl" not in st.data
    assert "raw/commerce/run_id=R/_markers/clinic.completed" in st.data
