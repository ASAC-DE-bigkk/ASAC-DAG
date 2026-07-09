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


# ── ingest 디스패치 (kcisa_list → clients.kcisa) ──────────────────────────────
from culture_ingest.common.config import RunContext
from culture_ingest.common.http import Page
from culture_ingest.common.landing import Landing, LocalSink
from culture_ingest.source.ingest import IngestOptions, ingest_dataset


class _FakeKcisa:
    def list_pages(self, path, base_params, rows, max_pages):
        assert path == "area2" and base_params == {"sido": "서울"}
        body = (b"<response><body><items>"
                b"<item><seq>1</seq><title>A</title></item></items></body></response>")
        yield Page(index=1, body=body, row_count=1, ext="xml")


class _Clients:
    kopis = None
    seoul = None
    kcisa = _FakeKcisa()


def test_ingest_dispatches_kcisa_list(tmp_path):
    ds = BY_NAME["kcisa_seoul_event"]
    ctx = RunContext(load_date="2026-07-09", ingest_ts="20260709T000000Z", run_id="t")
    landing = Landing(LocalSink(str(tmp_path)), "raw/culture", ctx)
    res = ingest_dataset(ds, _Clients(), landing, IngestOptions())
    assert res.error == "", f"적재 후 에러 없어야 함: {res.error!r}"  # write_manifest 의 params 배선 포함
    assert res.rows == 1 and res.pages == 1
    assert res.object_keys and res.object_keys[0].endswith("page-0001.xml")
