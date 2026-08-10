import hashlib
import json
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.raw_manifest import build_raw_manifest  # noqa: E402
from traffic_ingest.errors import TrafficCompletenessError  # noqa: E402
from traffic_ingest.link_reference_bronze import (  # noqa: E402
    LinkReferenceTables,
    create_seoul_traffic_link_reference_tables,
    load_traffic_link_reference_batch,
    unresolved_link_reference_ids,
    verify_seoul_traffic_link_reference_runtime,
)
from traffic_ingest.link_reference_info import (  # noqa: E402
    LINK_INFO_SERVICE,
    LINK_VERTEX_SERVICE,
)


INFO_PAYLOAD = """<LinkInfo>
  <list_total_count>1</list_total_count>
  <RESULT><CODE>INFO-000</CODE><MESSAGE>ok</MESSAGE></RESULT>
  <row>
    <LINK_ID>1220003800</LINK_ID><ROAD_NAME>테스트로</ROAD_NAME>
    <ST_NODE_NM>시점</ST_NODE_NM><ED_NODE_NM>종점</ED_NODE_NM>
    <MAP_DIST>182.3</MAP_DIST><REG_CD>11000</REG_CD>
  </row>
</LinkInfo>""".encode("utf-8")

VERTEX_PAYLOAD = b"""<LinkVerInfo>
  <list_total_count>2</list_total_count>
  <RESULT><CODE>INFO-000</CODE><MESSAGE>ok</MESSAGE></RESULT>
  <row><LINK_ID>1220003800</LINK_ID><VER_SEQ>1</VER_SEQ><GRS80TM_X>194000</GRS80TM_X><GRS80TM_Y>451000</GRS80TM_Y></row>
  <row><LINK_ID>1220003800</LINK_ID><VER_SEQ>2</VER_SEQ><GRS80TM_X>194001</GRS80TM_X><GRS80TM_Y>451001</GRS80TM_Y></row>
</LinkVerInfo>"""


class Cursor:
    def __init__(self, rows=None):
        self.statements: list[str] = []
        self.rows = list(rows or [])

    def execute(self, statement):
        self.statements.append(" ".join(statement.split()))

    def fetchall(self):
        return self.rows


def _descriptor(service_name: str, payload: bytes) -> dict[str, object]:
    row_count = 1 if service_name == LINK_INFO_SERVICE else 2
    return {
        "request_id": f"request-{service_name}",
        "source_id": "seoul_traffic_link_reference",
        "service_name": service_name,
        "request_params_json": json.dumps(
            {"api": service_name, "link_id": "1220003800"}
        ),
        "link_id": "1220003800",
        "raw_object_key": f"raw/link-reference/{service_name}.xml",
        "raw_hash": hashlib.sha256(payload).hexdigest(),
        "http_status": 200,
        "collected_at": "2026-08-09T15:01:02+00:00",
        "load_date": "2026-08-10",
        "result_code": "INFO-000",
        "result_msg": "ok",
        "list_total_count": row_count,
        "row_count": row_count,
    }


def _raw_result(*descriptors: dict[str, object]) -> tuple[dict, bytes]:
    manifest_key = "raw/link-reference/_manifest.json"
    result = {
        "source_id": "seoul_traffic_link_reference",
        "requested_link_ids": ["1220003800"],
        "raw_objects": list(descriptors),
        "raw_object_keys": [item["raw_object_key"] for item in descriptors],
        "manifest_key": manifest_key,
        "expected_raw_objects": len(descriptors),
        "is_publishable": True,
    }
    manifest = json.dumps(
        build_raw_manifest(
            run_id="manual__link-ref",
            dataset="seoul_traffic_link_reference",
            load_date="2026-08-10",
            object_keys=result["raw_object_keys"],
            expected_count=len(descriptors),
            actual_count=len(descriptors),
            completed_at="2026-08-09T15:01:03+00:00",
            status="complete",
        )
    ).encode("utf-8")
    return result, manifest


def _download_map(manifest: bytes, *descriptors: dict[str, object]):
    payloads = {
        "raw/link-reference/_manifest.json": manifest,
        descriptors[0]["raw_object_key"]: INFO_PAYLOAD,
        descriptors[1]["raw_object_key"]: VERTEX_PAYLOAD,
    }
    return lambda key, _label: payloads[key]


def _tables() -> LinkReferenceTables:
    return LinkReferenceTables(
        info="iceberg_dev.ask_seoul.bronze_seoul_traffic_link_info",
        vertex="iceberg_dev.ask_seoul.bronze_seoul_traffic_link_vertex",
        audit="iceberg_dev.ask_seoul.bronze_seoul_traffic_link_request_audit",
    )


def test_create_tables_declares_native_info_vertex_and_service_audit_grains():
    cursor = Cursor()

    tables = create_seoul_traffic_link_reference_tables(
        cursor, "iceberg_dev", "ask_seoul"
    )

    ddl = "\n".join(cursor.statements)
    assert tables == _tables()
    assert "bronze_seoul_traffic_link_info" in ddl
    assert "road_name varchar" in ddl
    assert "bronze_seoul_traffic_link_vertex" in ddl
    assert "vertex_sequence varchar" in ddl
    assert "grs80tm_x varchar" in ddl
    assert "bronze_seoul_traffic_link_request_audit" in ddl
    assert "service_name varchar" in ddl


def test_loader_preflights_both_hashes_before_any_database_statement():
    cursor = Cursor()
    info = _descriptor(LINK_INFO_SERVICE, INFO_PAYLOAD)
    vertex = _descriptor(LINK_VERTEX_SERVICE, VERTEX_PAYLOAD)
    raw_result, manifest = _raw_result(info, vertex)
    payloads = {
        raw_result["manifest_key"]: manifest,
        info["raw_object_key"]: INFO_PAYLOAD,
        vertex["raw_object_key"]: b"corrupted",
    }

    with pytest.raises(TrafficCompletenessError, match="hash mismatch"):
        load_traffic_link_reference_batch(
            raw_result=raw_result,
            dag_run_id="manual__link-ref",
            cursor_factory=lambda: (cursor, "iceberg_dev", "ask_seoul"),
            create_tables=lambda *_args: pytest.fail(
                "payload preflight must finish before DDL"
            ),
            download_raw_object=lambda key, _label: payloads[key],
        )

    assert cursor.statements == []


def test_loader_replaces_same_run_link_and_writes_one_info_two_vertices():
    cursor = Cursor()
    info = _descriptor(LINK_INFO_SERVICE, INFO_PAYLOAD)
    vertex = _descriptor(LINK_VERTEX_SERVICE, VERTEX_PAYLOAD)
    raw_result, manifest = _raw_result(info, vertex)

    result = load_traffic_link_reference_batch(
        raw_result=raw_result,
        dag_run_id="manual__link-ref",
        cursor_factory=lambda: (cursor, "iceberg_dev", "ask_seoul"),
        create_tables=lambda *_args: _tables(),
        download_raw_object=_download_map(manifest, info, vertex),
    )

    assert result == {
        "source_id": "seoul_traffic_link_reference",
        "raw_object_keys": raw_result["raw_object_keys"],
        "inserted_info": 1,
        "inserted_vertices": 2,
        "audit_rows": 2,
        "requested_link_ids": ["1220003800"],
        "is_publishable": True,
    }
    assert sum(statement.startswith("DELETE FROM") for statement in cursor.statements) == 3
    assert sum(statement.startswith("INSERT INTO") for statement in cursor.statements) == 3
    assert "LinkInfo" in cursor.statements[3]
    assert "LinkVerInfo" in cursor.statements[3]
    assert "테스트로" in cursor.statements[4]
    assert "194000" in cursor.statements[5]
    assert "194001" in cursor.statements[5]


def test_loader_rejects_a_missing_service_pair_before_database_statement():
    cursor = Cursor()
    info = _descriptor(LINK_INFO_SERVICE, INFO_PAYLOAD)
    raw_result, manifest = _raw_result(info)
    payloads = {
        raw_result["manifest_key"]: manifest,
        info["raw_object_key"]: INFO_PAYLOAD,
    }

    with pytest.raises(TrafficCompletenessError, match="complete service pair"):
        load_traffic_link_reference_batch(
            raw_result=raw_result,
            dag_run_id="manual__link-ref",
            cursor_factory=lambda: (cursor, "iceberg_dev", "ask_seoul"),
            create_tables=lambda *_args: pytest.fail(
                "pair preflight must finish before DDL"
            ),
            download_raw_object=lambda key, _label: payloads[key],
        )

    assert cursor.statements == []


def test_cache_requires_one_same_run_successful_info_vertex_pair():
    cursor = Cursor(rows=[("complete",)])

    result = unresolved_link_reference_ids(
        ["complete", "split-across-runs", "missing-vertex", "complete"],
        cursor_factory=lambda: (cursor, "iceberg_dev", "ask_seoul"),
        create_tables=lambda *_args: _tables(),
    )

    assert result == ["split-across-runs", "missing-vertex"]
    sql = cursor.statements[-1].lower()
    assert "group by link_id, dag_run_id" in sql
    assert "info_success_count = 1" in sql
    assert "vertex_success_count = 1" in sql
    assert "info_actual_count = 1" in sql
    assert "vertex_actual_count = vertex_audit_row_count" in sql
    assert "vertex_sequence_distinct_count = vertex_actual_count" in sql


def test_cache_empty_input_does_not_open_trino():
    assert unresolved_link_reference_ids(
        [], cursor_factory=lambda: pytest.fail("empty input must not open Trino")
    ) == []


class VerifyCursor:
    def __init__(self):
        self.statements: list[str] = []
        self.row = None

    def execute(self, statement):
        compact = " ".join(statement.split())
        self.statements.append(compact)
        if "bronze_seoul_traffic_link_request_audit" in compact:
            self.row = (2, 2)
        elif "bronze_seoul_traffic_link_vertex" in compact:
            self.row = (2,)
        elif "bronze_seoul_traffic_link_info" in compact:
            self.row = (1,)
        else:
            raise AssertionError(f"unexpected SQL: {compact}")

    def fetchone(self):
        return self.row


def test_runtime_verifier_checks_all_three_relation_counts():
    cursor = VerifyCursor()

    result = verify_seoul_traffic_link_reference_runtime(
        dag_run_id="manual__link-ref",
        expected_info_rows=1,
        expected_vertex_rows=2,
        expected_raw_objects=2,
        cursor_factory=lambda: (cursor, "iceberg_dev", "ask_seoul"),
    )

    assert result == {
        "info_rows": 1,
        "vertex_rows": 2,
        "raw_objects": 2,
        "audit_rows": 2,
    }
