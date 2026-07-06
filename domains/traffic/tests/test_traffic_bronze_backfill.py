import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_incident_bronze as dag_module  # noqa: E402


class Dag:
    dag_id = "traffic_incident_bronze_backfill"


def acc_info_payload() -> bytes:
    return b"""
<AccInfo>
  <list_total_count>1</list_total_count>
  <RESULT><CODE>INFO-000</CODE><MESSAGE>OK</MESSAGE></RESULT>
  <row>
    <acc_id>A1</acc_id>
    <occr_date>20260705</occr_date>
    <occr_time>0820</occr_time>
    <acc_type>test</acc_type>
  </row>
</AccInfo>
"""


def test_land_seoul_traffic_raw_object_keys_rebuilds_loader_input(monkeypatch):
    raw_key = (
        "raw/traffic_incident/seoul_traffic_incident/load_date=2026-07-05/"
        "20260705T082000KST_AccInfo-1-1000_request-1.xml"
    )

    class BackfillDagRun:
        conf = {"raw_object_keys": raw_key}

    monkeypatch.setattr(dag_module, "download_raw_object", lambda _object_key, _log_label: acc_info_payload())

    result = dag_module.land_seoul_traffic_raw_object_keys(
        dag=Dag(),
        dag_run=BackfillDagRun(),
        run_id="manual__backfill",
    )

    assert result["raw_object_keys"] == [raw_key]
    assert result["result_code"] == "INFO-000"
    assert result["list_total_count"] == 1
    assert result["page_count"] == 1
    assert result["raw_objects"][0]["request_id"] == "request-1"
    assert result["raw_objects"][0]["start_index"] == 1
    assert result["raw_objects"][0]["end_index"] == 1000
