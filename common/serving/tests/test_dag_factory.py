from common.serving.dag_factory import publication_record_payload
from common.serving.publisher import ProductRecord


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
    )

    payload = publication_record_payload(record)

    assert payload["publication_id"] == "publication-1"
    assert payload["stage"] == "api_smoke"
    assert payload["rollback_status"] == "restored"
