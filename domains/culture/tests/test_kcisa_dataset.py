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
