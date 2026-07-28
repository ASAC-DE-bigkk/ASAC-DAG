"""Iceberg Bronze writer for Seoul TOPIS TrafficInfo snapshots."""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

from traffic_ingest.common.runtime import (
    create_schema_if_needed,
    sql_int,
    sql_string,
    sql_timestamp,
    trino_cursor,
)
from traffic_ingest.errors import TrafficCompletenessError, TrafficSourceSchemaError
from traffic_ingest.bronze_batch import validate_traffic_raw_manifest
from traffic_ingest.flow_info import (
    SOURCE_ID,
    parse_traffic_info_response,
    request_params_json,
)


BRONZE_TABLE = "bronze_seoul_traffic_flow"
REQUEST_AUDIT_TABLE = "bronze_seoul_traffic_flow_request_audit"
KST = ZoneInfo("Asia/Seoul")


def _qualified_table(catalog: str, schema: str) -> str:
    return f"{catalog}.{schema}.{BRONZE_TABLE}"


def request_audit_table_for(qualified_bronze_table: str) -> str:
    return f"{qualified_bronze_table.rsplit('.', 1)[0]}.{REQUEST_AUDIT_TABLE}"


def create_seoul_traffic_flow_bronze_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = _qualified_table(catalog, schema)
    create_schema_if_needed(cursor, qualified_schema)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            request_id varchar,
            source_id varchar,
            request_params_json varchar,
            link_id varchar,
            prcs_spd varchar,
            prcs_trv_time varchar,
            raw_object_key varchar,
            payload_hash varchar,
            http_status integer,
            result_code varchar,
            result_msg varchar,
            list_total_count integer,
            row_count integer,
            collected_at timestamp(6),
            load_date varchar,
            dag_run_id varchar
        )
        WITH (format = 'PARQUET')
        """
    )
    audit_table = f"{qualified_schema}.{REQUEST_AUDIT_TABLE}"
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {audit_table} (
            request_id varchar,
            source_id varchar,
            request_params_json varchar,
            link_id varchar,
            raw_object_key varchar,
            payload_hash varchar,
            http_status integer,
            result_code varchar,
            result_msg varchar,
            list_total_count integer,
            row_count integer,
            collected_at timestamp(6),
            load_date varchar,
            dag_run_id varchar
        )
        WITH (format = 'PARQUET')
        """
    )
    return qualified_table


def _validate_descriptor(descriptor: dict[str, Any]) -> None:
    required = (
        "request_id",
        "link_id",
        "raw_object_key",
        "raw_hash",
        "http_status",
        "collected_at",
    )
    missing = [name for name in required if descriptor.get(name) in (None, "")]
    if missing:
        raise TrafficSourceSchemaError(
            "Traffic flow raw descriptor is missing: " + ", ".join(missing)
        )


def _audit_values(
    *,
    descriptor: dict[str, Any],
    metadata: dict[str, Any],
    dag_run_id: str,
) -> str:
    link_id = descriptor["link_id"]
    collected_at = datetime.fromisoformat(str(descriptor["collected_at"]))
    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    return "(" + ", ".join(
        (
            sql_string(descriptor["request_id"]),
            sql_string(SOURCE_ID),
            sql_string(request_params_json(link_id)),
            sql_string(link_id),
            sql_string(descriptor["raw_object_key"]),
            sql_string(descriptor["raw_hash"]),
            sql_int(descriptor["http_status"]),
            sql_string(metadata.get("result_code")),
            sql_string(metadata.get("result_msg")),
            sql_int(metadata.get("list_total_count")),
            sql_int(metadata.get("row_count")),
            sql_timestamp(collected_at),
            sql_string(load_date),
            sql_string(dag_run_id),
        )
    ) + ")"


def _flow_row_values(
    *,
    descriptor: dict[str, Any],
    metadata: dict[str, Any],
    rows: list[dict[str, Any]],
    dag_run_id: str,
) -> list[str]:
    link_id = descriptor["link_id"]
    collected_at = datetime.fromisoformat(str(descriptor["collected_at"]))
    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    values: list[str] = []
    for row in rows:
        values.append(
            "(" + ", ".join(
                (
                    sql_string(descriptor["request_id"]),
                    sql_string(SOURCE_ID),
                    sql_string(request_params_json(link_id)),
                    sql_string(row.get("link_id") or link_id),
                    sql_string(row.get("prcs_spd")),
                    sql_string(row.get("prcs_trv_time")),
                    sql_string(descriptor["raw_object_key"]),
                    sql_string(descriptor["raw_hash"]),
                    sql_int(descriptor["http_status"]),
                    sql_string(metadata.get("result_code")),
                    sql_string(metadata.get("result_msg")),
                    sql_int(metadata.get("list_total_count")),
                    sql_int(metadata.get("row_count")),
                    sql_timestamp(collected_at),
                    sql_string(load_date),
                    sql_string(dag_run_id),
                )
            ) + ")"
        )
    return values


def load_traffic_flow_batch(
    *,
    raw_result: dict[str, Any],
    dag_run_id: str,
    cursor_factory=trino_cursor,
    create_table=create_seoul_traffic_flow_bronze_table,
    download_raw_object: Callable[[str, str], bytes],
) -> dict[str, Any]:
    raw_objects = raw_result.get("raw_objects") or []
    if not isinstance(raw_objects, list) or not raw_objects:
        raise TrafficCompletenessError(
            "Traffic flow raw landing result is empty; cannot load Bronze rows."
        )
    validate_traffic_raw_manifest(
        raw_result,
        dag_run_id=dag_run_id,
        dataset=SOURCE_ID,
        download_raw_object=download_raw_object,
    )
    cursor, catalog, schema = cursor_factory()
    qualified_table = create_table(cursor, catalog, schema)
    prepared: list[tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]] = []
    for descriptor in raw_objects:
        _validate_descriptor(descriptor)
        payload = download_raw_object(
            str(descriptor["raw_object_key"]),
            "Seoul TrafficInfo raw payload",
        )
        actual_hash = hashlib.sha256(payload).hexdigest()
        if actual_hash != str(descriptor["raw_hash"]):
            raise TrafficCompletenessError(
                "Traffic flow raw payload hash mismatch: "
                f"raw_object_key={descriptor['raw_object_key']}"
            )
        metadata, rows = parse_traffic_info_response(payload)
        if int(metadata["row_count"]) != int(descriptor.get("row_count", -1)):
            raise TrafficCompletenessError(
                "Traffic flow raw row_count mismatch: "
                f"link_id={descriptor['link_id']}"
            )
        prepared.append((descriptor, metadata, rows))

    if prepared:
        link_ids = ", ".join(sql_string(item[0]["link_id"]) for item in prepared)
        audit_table = request_audit_table_for(qualified_table)
        where = (
            f"source_id = {sql_string(SOURCE_ID)} "
            f"AND dag_run_id = {sql_string(dag_run_id)} "
            f"AND link_id IN ({link_ids})"
        )
        cursor.execute(f"DELETE FROM {audit_table} WHERE {where}")
        cursor.execute(f"DELETE FROM {qualified_table} WHERE {where}")

        audit_values = [
            _audit_values(
                descriptor=descriptor,
                metadata=metadata,
                dag_run_id=dag_run_id,
            )
            for descriptor, metadata, _rows in prepared
        ]
        cursor.execute(
            f"""
            INSERT INTO {audit_table} (
                request_id, source_id, request_params_json, link_id, raw_object_key,
                payload_hash, http_status, result_code, result_msg, list_total_count,
                row_count, collected_at, load_date, dag_run_id
            ) VALUES {', '.join(audit_values)}
            """
        )

        flow_values = [
            value
            for descriptor, metadata, rows in prepared
            for value in _flow_row_values(
                descriptor=descriptor,
                metadata=metadata,
                rows=rows,
                dag_run_id=dag_run_id,
            )
        ]
        if flow_values:
            cursor.execute(
                f"""
                INSERT INTO {qualified_table} (
                    request_id, source_id, request_params_json, link_id, prcs_spd,
                    prcs_trv_time, raw_object_key, payload_hash, http_status, result_code,
                    result_msg, list_total_count, row_count, collected_at, load_date, dag_run_id
                ) VALUES {', '.join(flow_values)}
                """
            )

    inserted = sum(len(rows) for _descriptor, _metadata, rows in prepared)
    return {
        "source_id": SOURCE_ID,
        "raw_object_keys": [item["raw_object_key"] for item in raw_objects],
        "inserted": inserted,
        "expected_rows": int(raw_result.get("expected_rows", inserted)),
        "page_count": len(raw_objects),
        "is_publishable": bool(raw_result.get("is_publishable", True)),
    }


def verify_seoul_traffic_flow_bronze_runtime(
    *,
    dag_run_id: str,
    expected_rows: int,
    expected_raw_objects: int,
    cursor_factory=trino_cursor,
) -> int:
    cursor, catalog, schema = cursor_factory()
    qualified_table = _qualified_table(catalog, schema)
    audit_table = request_audit_table_for(qualified_table)
    where = (
        f"source_id = {sql_string(SOURCE_ID)} "
        f"AND dag_run_id = {sql_string(dag_run_id)}"
    )
    cursor.execute(
        f"""
        SELECT count(*) AS table_rows
        FROM {qualified_table}
        WHERE {where}
        """
    )
    table_rows = cursor.fetchone()[0]
    cursor.execute(
        f"""
        SELECT count(DISTINCT raw_object_key) AS raw_objects, count(*) AS audit_rows
        FROM {audit_table}
        WHERE {where}
        """
    )
    raw_objects, audit_rows = cursor.fetchone()
    if int(table_rows) != int(expected_rows):
        raise TrafficCompletenessError(
            f"Traffic flow bronze row count mismatch: expected={expected_rows}, actual={table_rows}"
        )
    if int(raw_objects) != int(expected_raw_objects) or int(audit_rows) != int(expected_raw_objects):
        raise TrafficCompletenessError(
            "Traffic flow bronze raw/audit count mismatch: "
            f"expected={expected_raw_objects}, raw={raw_objects}, audit={audit_rows}"
        )
    print(
        "traffic_flow_bronze "
        f"table_rows={table_rows} raw_object_count={raw_objects} audit_rows={audit_rows}"
    )
    return int(table_rows)


__all__ = [
    "BRONZE_TABLE",
    "REQUEST_AUDIT_TABLE",
    "create_seoul_traffic_flow_bronze_table",
    "load_traffic_flow_batch",
    "verify_seoul_traffic_flow_bronze_runtime",
]
