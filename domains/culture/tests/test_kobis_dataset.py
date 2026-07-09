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


# ── ingest: kobis_boxoffice 분기 디스패치 ─────────────────────────────────────

import json  # noqa: E402

from culture_ingest.common.config import RunContext  # noqa: E402
from culture_ingest.common.http import Page  # noqa: E402
from culture_ingest.common.landing import Landing, LocalSink  # noqa: E402
from culture_ingest.source.datasets import BY_NAME  # noqa: E402
from culture_ingest.source.ingest import Clients, IngestOptions, ingest_dataset  # noqa: E402


class _FakeKobis:
    """daily_boxoffice 호출 인자를 기록하고 top10 JSON Page 를 돌려준다."""

    def __init__(self):
        self.calls = []

    def daily_boxoffice(self, target_dt, wide_area_cd=None):
        self.calls.append((target_dt, wide_area_cd))
        body = json.dumps({
            "boxOfficeResult": {
                "dailyBoxOfficeList": [
                    {"rank": str(i + 1), "movieCd": f"m{i}", "movieNm": f"영화{i}"}
                    for i in range(10)
                ]
            }
        }).encode("utf-8")
        return Page(index=1, body=body, row_count=10, ext="json")


def _run_kobis(tmp_path, name):
    ds = BY_NAME[name]
    ctx = RunContext(load_date="2026-07-09", ingest_ts="20260709T030000Z", run_id="t")
    landing = Landing(LocalSink(str(tmp_path)), "raw/culture", ctx)
    fake = _FakeKobis()
    clients = Clients(kopis=None, seoul=None, kobis=fake)
    res = ingest_dataset(ds, clients, landing, IngestOptions())
    return res, fake


def test_ingest_dispatches_kobis_nation(tmp_path):
    res, fake = _run_kobis(tmp_path, "kobis_boxoffice_nation")
    assert res.error == "", res.error  # #196 교훈: params 미설정 UnboundLocalError 그물
    assert res.rows == 10
    assert res.pages == 1
    # targetDt = load_date(2026-07-09) - 1일 = 20260708, 전국이라 wideAreaCd=None
    assert fake.calls == [("20260708", None)]


def test_ingest_dispatches_kobis_seoul(tmp_path):
    res, fake = _run_kobis(tmp_path, "kobis_boxoffice_seoul")
    assert res.error == "", res.error
    assert res.rows == 10
    assert fake.calls == [("20260708", "0105001")]
