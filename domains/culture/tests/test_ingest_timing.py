from culture_ingest.common.landing import DatasetResult


def test_dataset_result_summary_has_timing_fields():
    r = DatasetResult(name="kopis_performance", source="kopis", endpoint="pblprfr", prefix="p")
    r.duration_sec = 4.1
    r.finished_ts = "20260701T000012Z"
    s = r.summary()
    assert s["duration_sec"] == 4.1
    assert s["finished_ts"] == "20260701T000012Z"


from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import Landing, LocalSink
from culture_ingest.source.ingest import ingest_dataset, IngestOptions, Clients
from culture_ingest.source.datasets import BY_NAME
import re


def _run(tmp_path, name, include_detail=False):
    ds = BY_NAME[name]
    ctx = RunContext(load_date="2026-07-01", ingest_ts="20260701T000000Z", run_id="t")
    landing = Landing(LocalSink(str(tmp_path)), "bronze/culture", ctx)
    opts = IngestOptions(include_detail=include_detail, max_detail=5)

    class DeadKopis:
        def list_pages(self, *a, **k): return []
        def list_ids(self, *a, **k): return []
        def fetch_once(self, *a, **k): raise RuntimeError("no net")
        def detail(self, *a, **k): raise RuntimeError("no net")

    clients = Clients(kopis=DeadKopis(), seoul=None)
    return ingest_dataset(ds, clients, landing, opts, warehouse=None)


def test_timing_set_on_skipped_path(tmp_path):
    # kopis_detail + include_detail=False -> 이른 return(skipped) 경로에서도 타이밍 남음
    r = _run(tmp_path, "kopis_performance_detail", include_detail=False)
    assert "skipped" in r.error
    assert r.finished_ts and re.match(r"^\d{8}T\d{6}Z$", r.finished_ts)
    assert r.duration_sec >= 0.0
