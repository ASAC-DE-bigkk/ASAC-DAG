from culture_ingest.common.landing import DatasetResult


def test_dataset_result_summary_has_timing_fields():
    r = DatasetResult(name="kopis_performance", source="kopis", endpoint="pblprfr", prefix="p")
    r.duration_sec = 4.1
    r.finished_ts = "20260701T000012Z"
    s = r.summary()
    assert s["duration_sec"] == 4.1
    assert s["finished_ts"] == "20260701T000012Z"
