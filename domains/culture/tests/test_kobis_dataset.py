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


# ── datasets: KOBIS 박스오피스 2벌 등록 ───────────────────────────────────────

def test_kobis_datasets_registered():
    from culture_ingest.source.datasets import BY_NAME
    for name in ("kobis_boxoffice_nation", "kobis_boxoffice_seoul"):
        ds = BY_NAME[name]
        assert ds.source == "kobis"
        assert ds.kind == "kobis_boxoffice"
        assert ds.endpoint == "searchDailyBoxOfficeList"
        assert ds.load_pattern == "snapshot_append"
        assert ds.min_rows == 5
        assert ds.key_fields == ("movieCd", "movieNm")


def test_kobis_seoul_carries_wideareacd_nation_does_not():
    from culture_ingest.source.datasets import BY_NAME
    assert BY_NAME["kobis_boxoffice_seoul"].base_params.get("wideAreaCd") == "0105001"
    assert "wideAreaCd" not in BY_NAME["kobis_boxoffice_nation"].base_params
