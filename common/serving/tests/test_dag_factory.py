from common.serving import dag_factory
from common.serving.dag_factory import publication_record_payload
from common.serving.publisher import ProductRecord


def _capture_wrapper_build_calls(monkeypatch, path, module_name):
    import importlib.util
    import sys
    import types

    captured = []
    sentinel_dag = object()
    factory_module = types.ModuleType("common.serving.dag_factory")

    def build_serving_export_dag(**kwargs):
        captured.append(dict(kwargs))
        return sentinel_dag

    factory_module.build_serving_export_dag = build_serving_export_dag
    monkeypatch.setitem(sys.modules, "common.serving.dag_factory", factory_module)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return captured


def test_publication_xcom_payload_exposes_stage_and_snapshot_rollback_state():
    record = ProductRecord(
        product_id="weather_place_risk_window",
        model_name="gold_weather_place_risk_window",
        publication_id="publication-1",
        source_run_id="run-1",
        published_at="2026-07-29T00:00:00+00:00",
        serving_status="failed",
        reason="API smoke test 실패",
        stage="api_smoke",
        rollback_status="restored",
        projection_schema_hash="projection-hash-1",
        source_content_hash="source-hash-1",
        d1_content_hash="d1-hash-1",
    )

    payload = publication_record_payload(record)

    assert payload["publication_id"] == "publication-1"
    assert payload["stage"] == "api_smoke"
    assert payload["rollback_status"] == "restored"
    assert payload["projection_schema_hash"] == "projection-hash-1"
    assert payload["source_content_hash"] == "source-hash-1"
    assert payload["d1_content_hash"] == "d1-hash-1"


def test_serving_export_factory_keeps_content_parity_opt_in_default_false():
    import inspect

    assert inspect.signature(dag_factory.build_serving_export_dag).parameters[
        "verify_content_parity"
    ].default is False


def test_non_target_serving_wrappers_do_not_opt_into_content_parity(monkeypatch):
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]

    calls = []
    for module_name, relative_path in (
        ("citydata_serving_export_under_test", "domains/citydata/citydata_serving_export.py"),
        ("culture_serving_export_under_test", "domains/culture/culture_serving_export.py"),
        ("transit_serving_export_under_test", "domains/transit/transit_serving_export.py"),
    ):
        calls.extend(_capture_wrapper_build_calls(monkeypatch, root / relative_path, module_name))

    assert calls
    assert all("verify_content_parity" not in call for call in calls)


def test_d1_product_event_carries_runtime_publication_id_and_delay(monkeypatch):
    captured = []
    monkeypatch.setattr(
        dag_factory,
        "record_product_event",
        lambda context, **kwargs: captured.append((context, kwargs)) or kwargs,
    )
    record = ProductRecord(
        product_id="weather_place_risk_window",
        model_name="gold_weather_place_risk_window",
        publication_id="publication-1",
        source_run_id="run-1",
        published_at="2026-07-30T00:10:00+00:00",
        serving_status="published",
        reason="ok",
        published_row_count=427,
        freshness="2026-07-30T00:03:00+00:00",
    )

    dag_factory.record_publication_events({"run_id": "run-1"}, "weather", [record])

    assert captured == [
        (
            {"run_id": "run-1"},
            {
                "domain": "weather",
                "layer": "d1",
                "product_ids": ("weather_place_risk_window",),
                "status": "success",
                "row_count": 427,
                "rows_source": "publication_ledger",
                "publication_id": "publication-1",
                "quality": {
                    "publication_delay": {
                        "value": 7,
                        "unit": "minute",
                        "quality_state": "observed",
                        "null_meaning": None,
                    }
                },
            },
        )
    ]


def test_failed_d1_product_event_does_not_treat_default_zero_as_observed(
    monkeypatch,
):
    captured = []
    monkeypatch.setattr(
        dag_factory,
        "record_product_event",
        lambda context, **kwargs: captured.append((context, kwargs)) or kwargs,
    )
    record = ProductRecord(
        product_id="weather_place_risk_window",
        model_name="gold_weather_place_risk_window",
        publication_id="publication-failed",
        source_run_id="run-1",
        published_at="2026-07-30T00:10:00+00:00",
        serving_status="failed",
        reason="D1 write failed",
    )

    dag_factory.record_publication_events({"run_id": "run-1"}, "weather", [record])

    event = captured[0][1]
    assert event["status"] == "failed"
    assert event["row_count"] is None
    assert event["rows_source"] == "not_observed"
