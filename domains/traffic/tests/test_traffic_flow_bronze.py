import json

from traffic_ingest.flow_bronze import load_traffic_flow_batch


class Cursor:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)


def _payload():
    return json.dumps(
        {
            "TrafficInfo": {
                "list_total_count": 1,
                "RESULT": {"CODE": "INFO-000", "MESSAGE": "ok"},
                "row": [
                    {
                        "LINK_ID": "1220003800",
                        "PRCS_SPD": "31.2",
                        "PRCS_TRV_TIME": "42",
                    }
                ],
            }
        }
    ).encode("utf-8")


def test_flow_bronze_load_deletes_same_run_link_before_insert():
    cursor = Cursor()
    payload = _payload()
    import hashlib

    descriptor = {
        "request_id": "request-1",
        "link_id": "1220003800",
        "raw_object_key": "raw/traffic_flow/one.json",
        "raw_hash": hashlib.sha256(payload).hexdigest(),
        "http_status": 200,
        "collected_at": "2026-07-15T01:02:03+00:00",
        "row_count": 1,
    }

    result = load_traffic_flow_batch(
        raw_result={"raw_objects": [descriptor], "expected_rows": 1},
        dag_run_id="manual__flow",
        cursor_factory=lambda: (cursor, "iceberg_dev", "ask_seoul"),
        create_table=lambda _cursor, catalog, schema: (
            f"{catalog}.{schema}.bronze_seoul_traffic_flow"
        ),
        download_raw_object=lambda _key, _label: payload,
    )

    assert result["inserted"] == 1
    assert result["page_count"] == 1
    assert any("DELETE FROM iceberg_dev.ask_seoul.bronze_seoul_traffic_flow" in stmt for stmt in cursor.statements)
    assert any("INSERT INTO iceberg_dev.ask_seoul.bronze_seoul_traffic_flow" in stmt for stmt in cursor.statements)
