import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.collection_slots import traffic_incident_slot


KST = ZoneInfo("Asia/Seoul")


def test_incident_slot_floors_logical_date_to_five_minutes_in_utc():
    slot = traffic_incident_slot(datetime(2026, 8, 8, 9, 7, tzinfo=KST))

    assert slot.collection_slot_at == "2026-08-08T00:05:00+00:00"
    assert slot.scheduled_at == "2026-08-08T00:05:00+00:00"
    assert slot.deadline_at == "2026-08-08T00:20:00+00:00"
    assert slot.grain == {"source_id": "seoul_traffic_incident"}
    assert slot.collection_contract_id == "traffic.incident.v1"
    assert slot.is_scheduled is True


def test_incident_slot_accepts_iso_timestamp_and_rejects_naive_time():
    assert traffic_incident_slot("2026-08-08T00:09:00+00:00").collection_slot_at == (
        "2026-08-08T00:05:00+00:00"
    )

    with pytest.raises(ValueError, match="timezone"):
        traffic_incident_slot(datetime(2026, 8, 8, 0, 9))
