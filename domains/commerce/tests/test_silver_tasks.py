"""silver: 정규화 · 스키마 검증 · bronze→silver 적재(로컬 스토리지) 테스트."""
import json

from commerce_core import registry
from commerce_core.schemas import COMMON_COLUMNS
from silver import enrich_tasks
from silver import quality_tasks
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


def test_sgg_prefix_mismatch_detects_admin_legal_code_disagreement():
    rows = [
        {"sgg_code": "11110", "legal_dong_code": "1111010100",
         "admin_dong_code": "1111051500", "sgg_name": "종로구"},
        {"sgg_code": "11110", "legal_dong_code": "1111010100",
         "admin_dong_code": "1168051500", "sgg_name": "종로구"},
    ]
    mismatches = enrich_tasks._sgg_prefix_mismatches(rows)
    assert len(mismatches) == 1
    assert mismatches[0]["legal_prefix"] == "11110"
    assert mismatches[0]["admin_prefix"] == "11680"


def _patch_masked_address_query(monkeypatch, *, fetch_row, watermark):
    """공통 셋업 — Trino/알림/워터마크를 목킹하고 실행된 (sql, params) 를 캡처해 반환한다."""
    sent = []
    executed = []

    class Cursor:
        def execute(self, sql, params=None):
            executed.append((sql, params))

        def fetchall(self):
            return [fetch_row]

    class Conn:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    monkeypatch.setattr(
        quality_tasks, "_qualified",
        lambda: ("iceberg_dev", "commerce", "iceberg_dev.commerce"),
    )
    monkeypatch.setattr(quality_tasks, "_connect", lambda _catalog, _schema: Conn())
    monkeypatch.setattr(quality_tasks, "_prev_silver_watermark", lambda: watermark)
    monkeypatch.setattr(
        quality_tasks, "log_event",
        lambda event, **kwargs: {"event": event, **kwargs},
    )
    monkeypatch.setattr(quality_tasks, "notify_quality_event", lambda **kwargs: sent.append(kwargs))
    return sent, executed


def test_masked_address_quality_summary_scopes_to_new_rows_and_notifies(monkeypatch):
    from datetime import datetime

    wm = datetime(2026, 7, 13, 5, 0, 0)
    sent, executed = _patch_masked_address_query(
        monkeypatch, fetch_row=(100, 5, 5.0, 4, 0), watermark=wm)

    summary = quality_tasks.notify_masked_address_dong_skip_summary()

    assert summary["event"] == "masked_address_dong_mapping_skipped"
    assert summary["level"] == "warning"
    assert summary["affected_rows"] == 5
    assert summary["settled_rows"] == 100
    # #3: 신규 유입분으로 스코프됐음을 표기
    assert summary["scope"] == "new_since_last_run"
    assert summary["since_collected_at"] == wm.isoformat()
    sql, params = executed[0]
    assert "silver_license_current" in sql
    assert "collected_at >" in sql              # 워터마크 이후로만 집계
    assert params == (wm,)                        # 워터마크가 바인딩됨
    assert sent[0]["task"] == quality_tasks.MASKED_ADDRESS_TASK
    assert sent[0]["level"] == "warning"
    assert sent[0]["metrics"]["affected_ratio_pct"] == 5


def test_masked_address_quality_summary_no_new_rows_is_info_no_alert(monkeypatch):
    """핵심(#3): 이번 실행에 신규 유입이 없으면(settled=0) info 로만 남기고 외부 알림 안 함.

    기존엔 기적재(현재 존재) 마스킹 주소를 매일 반복 경고했다 — 이제 신규분이 없으면 조용하다."""
    from datetime import datetime

    sent, executed = _patch_masked_address_query(
        monkeypatch, fetch_row=(0, 0, None, 0, 0), watermark=datetime(2026, 7, 13, 5, 0, 0))

    summary = quality_tasks.notify_masked_address_dong_skip_summary()

    assert summary["settled_rows"] == 0
    assert summary["affected_rows"] == 0
    assert summary["level"] == "info"
    assert sent == []                             # 신규 마스킹 주소 없음 → 알림 미발송


def test_build_silver_reads_ndjson_and_writes_parquet(tmp_path, monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("COMMERCE_STORAGE_PREFIX", raising=False)
    from commerce_core.storage import get_storage

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
