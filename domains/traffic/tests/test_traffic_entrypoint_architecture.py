from pathlib import Path


DOMAIN_ROOT = Path(__file__).resolve().parents[1]


def test_traffic_production_entrypoints_stay_below_400_lines():
    for relative_path in (
        "traffic_incident_bronze.py",
        "traffic_incident_landing.py",
        "traffic_incident_manual.py",
        "traffic_flow_bronze.py",
        "traffic_incident_transform.py",
    ):
        path = DOMAIN_ROOT / relative_path
        assert len(path.read_text(encoding="utf-8").splitlines()) <= 400, relative_path


def test_traffic_entrypoints_delegate_recording_and_notification_support():
    bronze = (DOMAIN_ROOT / "traffic_incident_bronze.py").read_text(encoding="utf-8")
    transform = (DOMAIN_ROOT / "traffic_incident_transform.py").read_text(
        encoding="utf-8"
    )

    assert "traffic_ingest.bronze_dag_support" in bronze
    assert "def send_traffic_discord" not in bronze
    assert "traffic_ingest.transform_dag_support" in transform
    assert "R2RecoveryRecordSink().write" not in transform
