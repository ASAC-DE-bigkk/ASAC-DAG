"""#196 — KCISA 소스 키·데이터셋·ingest 배선 테스트."""
from __future__ import annotations

from culture_ingest.source import config as culture_config


def test_source_keys_reads_cult(tmp_path):
    envf = tmp_path / ".env"
    envf.write_text(
        "KOPIS_SERVICE_KEY=k\nSEOUL_API_KEY_CULT=s\nPUBLIC_DATA_API_KEY_CULT=c\n",
        encoding="utf-8",
    )
    keys = culture_config.source_keys(str(envf))
    assert keys.cult == "c"
    assert "PUBLIC_DATA_API_KEY_CULT" not in culture_config.missing_keys(keys)


def test_missing_cult_reported(tmp_path):
    envf = tmp_path / ".env"
    envf.write_text("KOPIS_SERVICE_KEY=k\nSEOUL_API_KEY_CULT=s\n", encoding="utf-8")
    keys = culture_config.source_keys(str(envf))
    assert any("CULT" in m for m in culture_config.missing_keys(keys))


# ── 데이터셋 등록 ──────────────────────────────────────────────────────────────
from culture_ingest.source.datasets import BY_NAME, plan_dataset_names


def test_kcisa_dataset_registered():
    ds = BY_NAME["kcisa_seoul_event"]
    assert ds.source == "kcisa" and ds.kind == "kcisa_list"
    assert ds.endpoint == "area2" and ds.row_tag == "item"
    assert ds.base_params == {"sido": "서울"}
    assert ds.min_rows == 300 and ds.volume_drop_threshold == 0.7
    assert ds.refresh == "daily"


def test_kcisa_in_daily_plan():
    names = plan_dataset_names([], include_detail=True)
    assert "kcisa_seoul_event" in names   # 자정 일배치에 포함
