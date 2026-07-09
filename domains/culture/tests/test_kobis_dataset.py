"""#197 — KOBIS 소스 키 배선 + 데이터셋 등록 + ingest 디스패치.

config(키 필수화) → datasets(2벌 등록) → ingest(kobis_boxoffice 분기) 순으로
"소스 추가" seam 이 전부 이어졌는지 검증한다. seoul→kcisa 에 이은 3번째 소스.
"""
from __future__ import annotations


# ── config: KOBIS 소스 키 필수화 ──────────────────────────────────────────────

def test_source_keys_reads_kobis(monkeypatch):
    monkeypatch.setenv("KOPIS_SERVICE_KEY", "k")
    monkeypatch.setenv("SEOUL_API_KEY_CULT", "s")
    monkeypatch.setenv("KOBIS_SERVICE_KEY", "kobis-key-value")
    from culture_ingest.source.config import source_keys
    keys = source_keys()
    assert keys.kobis == "kobis-key-value"


def test_missing_keys_flags_absent_kobis(monkeypatch):
    monkeypatch.setenv("KOPIS_SERVICE_KEY", "k")
    monkeypatch.setenv("SEOUL_API_KEY_CULT", "s")
    monkeypatch.delenv("KOBIS_SERVICE_KEY", raising=False)
    from culture_ingest.source.config import missing_keys, source_keys
    assert "KOBIS_SERVICE_KEY" in missing_keys(source_keys())
