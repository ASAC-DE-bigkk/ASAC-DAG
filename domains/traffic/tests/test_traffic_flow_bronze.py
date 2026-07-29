import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.raw_manifest import build_raw_manifest

from traffic_ingest.flow_bronze import (
    load_traffic_flow_batch,
    verify_seoul_traffic_flow_bronze_runtime,
)
from traffic_ingest.errors import TrafficCompletenessError


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


def _raw_result(*descriptors):
    manifest_key = "raw/traffic_flow/_manifest.json"
    return {
        "raw_objects": list(descriptors),
        "expected_rows": sum(int(item["row_count"]) for item in descriptors),
        "manifest_key": manifest_key,
    }, manifest_key


def _manifest_bytes(manifest_key, descriptors):
    return json.dumps(
        build_raw_manifest(
            run_id="manual__flow",
            dataset="seoul_traffic_flow",
            load_date="2026-07-15",
            object_keys=[item["raw_object_key"] for item in descriptors],
            expected_count=len(descriptors),
            actual_count=len(descriptors),
            completed_at="2026-07-15T01:02:04+00:00",
        )
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

    raw_result, manifest_key = _raw_result(descriptor)
    result = load_traffic_flow_batch(
        raw_result=raw_result,
        dag_run_id="manual__flow",
        cursor_factory=lambda: (cursor, "iceberg_dev", "ask_seoul"),
        create_table=lambda _cursor, catalog, schema: (
            f"{catalog}.{schema}.bronze_seoul_traffic_flow"
        ),
        download_raw_object=lambda key, _label: (
            _manifest_bytes(manifest_key, [descriptor])
            if key == manifest_key
            else payload
        ),
    )

    assert result["inserted"] == 1
    assert result["page_count"] == 1
    assert any("DELETE FROM iceberg_dev.ask_seoul.bronze_seoul_traffic_flow" in stmt for stmt in cursor.statements)
    assert any("INSERT INTO iceberg_dev.ask_seoul.bronze_seoul_traffic_flow" in stmt for stmt in cursor.statements)


def test_flow_bronze_load_batches_dml_for_multiple_links_and_zero_rows():
    cursor = Cursor()
    payload = _payload()
    import hashlib

    first = {
        "request_id": "request-1",
        "link_id": "1220003800",
        "raw_object_key": "raw/traffic_flow/one.json",
        "raw_hash": hashlib.sha256(payload).hexdigest(),
        "http_status": 200,
        "collected_at": "2026-07-15T01:02:03+00:00",
        "row_count": 1,
    }
    zero_payload = json.dumps(
        {
            "TrafficInfo": {
                "list_total_count": 0,
                "RESULT": {"CODE": "INFO-200", "MESSAGE": "no data"},
            }
        }
    ).encode("utf-8")
    second = {
        "request_id": "request-2",
        "link_id": "1220003900",
        "raw_object_key": "raw/traffic_flow/two.json",
        "raw_hash": hashlib.sha256(zero_payload).hexdigest(),
        "http_status": 200,
        "collected_at": "2026-07-15T01:02:03+00:00",
        "row_count": 0,
    }

    raw_result, manifest_key = _raw_result(first, second)
    load_traffic_flow_batch(
        raw_result=raw_result,
        dag_run_id="manual__flow",
        cursor_factory=lambda: (cursor, "iceberg_dev", "ask_seoul"),
        create_table=lambda _cursor, catalog, schema: (
            f"{catalog}.{schema}.bronze_seoul_traffic_flow"
        ),
        download_raw_object=lambda key, _label: (
            _manifest_bytes(manifest_key, [first, second])
            if key == manifest_key
            else (payload if key == first["raw_object_key"] else zero_payload)
        ),
    )

    assert len(cursor.statements) == 4
    assert sum("DELETE FROM" in stmt for stmt in cursor.statements) == 2
    assert sum("INSERT INTO" in stmt for stmt in cursor.statements) == 2
    assert all(link_id in cursor.statements[0] for link_id in (first["link_id"], second["link_id"]))
    assert all(link_id in cursor.statements[1] for link_id in (first["link_id"], second["link_id"]))
    assert "request-1" in cursor.statements[2]
    assert "request-2" in cursor.statements[2]
    assert "request-1" in cursor.statements[3]
    assert "request-2" not in cursor.statements[3]


def test_flow_bronze_missing_manifest_blocks_database_mutation():
    cursor = Cursor()

    with pytest.raises(TrafficCompletenessError, match="manifest is missing"):
        load_traffic_flow_batch(
            raw_result={
                "raw_objects": [{"raw_object_key": "raw/traffic_flow/one.json"}]
            },
            dag_run_id="manual__missing-manifest",
            cursor_factory=lambda: pytest.fail("must fail before Trino"),
            create_table=lambda *_args: pytest.fail("must fail before table DDL"),
            download_raw_object=lambda *_args: pytest.fail("must fail before R2"),
        )

    assert cursor.statements == []


class VerifyCursor:
    def __init__(self):
        self._row = None

    def execute(self, statement):
        if "bronze_seoul_traffic_flow_request_audit" in statement:
            self._row = (9, 9)
        elif "count(*) AS table_rows" in statement:
            self._row = (6,)
        else:
            raise AssertionError(f"unexpected SQL: {statement}")

    def fetchone(self):
        return self._row


def test_flow_bronze_verify_counts_zero_row_responses_from_audit_raw_keys():
    cursor = VerifyCursor()

    assert verify_seoul_traffic_flow_bronze_runtime(
        dag_run_id="scheduled__flow",
        expected_rows=6,
        expected_raw_objects=9,
        cursor_factory=lambda: (cursor, "iceberg_dev", "ask_seoul"),
    ) == 6
