"""silver: 정규화 · 스키마 검증 · bronze→silver 적재(로컬 스토리지) 테스트."""
import json

from common import registry
from common.schemas import COMMON_COLUMNS
from silver import silver_tasks
from silver.validators import validate_normalized


def _row(extra=True):
    r = {c: f"  {c}_val  " for c in COMMON_COLUMNS}  # 값 양쪽 공백(서울 API 패딩 모사)
    if extra:
        r["UPTAENM"] = "한식"           # 공통 외 컬럼 → 정규화에서 버려져야 함
    return r


def test_normalize_extracts_common_columns_and_strips():
    out = silver_tasks.normalize_rows([_row()])
    assert set(out[0].keys()) == set(COMMON_COLUMNS)     # 공통 컬럼만
    assert out[0]["BPLCNM"] == "BPLCNM_val"               # 공백 제거
    assert "UPTAENM" not in out[0]                        # 비공통 제거


def test_normalize_fills_missing_with_empty():
    out = silver_tasks.normalize_rows([{"BPLCNM": "x"}])
    assert out[0]["MGTNO"] == "" and out[0]["BPLCNM"] == "x"


def test_validate_normalized_reports_ok_and_missing():
    good = silver_tasks.normalize_rows([_row()])
    assert validate_normalized(good)["ok"] is True
    bad = [{c: "v" for c in COMMON_COLUMNS if c != "UPDATEDT"}]
    rep = validate_normalized(bad)
    assert rep["ok"] is False and "UPDATEDT" in rep["missing_columns"]


def test_build_silver_reads_ndjson_and_writes_parquet(tmp_path, monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("COMMERCE_STORAGE_PREFIX", raising=False)
    from common.storage import get_storage

    ds = registry.by_short("food_cold_storage")          # service_name = LOCALDATA_072207
    # bronze = API당 1파일, 줄당 원본 페이지(여기선 2페이지 = 2줄)
    def page():
        return json.dumps({ds.service_name: {
            "list_total_count": 4, "RESULT": {"CODE": "INFO-000", "MESSAGE": "ok"},
            "row": [_row(), _row()]}})
    bronze_key = "raw/commerce/run_id=R/food_cold_storage.jsonl"
    get_storage().write_bytes(bronze_key, (page() + "\n" + page() + "\n").encode("utf-8"))

    res = silver_tasks.build_silver("food_cold_storage", "2026-06-29", bronze_key)
    assert res["rows"] == 4 and res["validation"]["ok"] is True   # 2줄 × 2행
    assert res["silver_key"] == (
        "silver/commerce/food_cold_storage/observed_date=2026-06-29/part-000.parquet")
    assert get_storage().exists(res["silver_key"])
