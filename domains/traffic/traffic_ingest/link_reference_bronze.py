"""Iceberg Bronze writer and cache for TOPIS road link references."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from traffic_ingest.bronze_batch import validate_traffic_raw_manifest
from traffic_ingest.common.runtime import (
    create_schema_if_needed,
    sql_int,
    sql_string,
    sql_timestamp,
    trino_cursor,
)
from traffic_ingest.errors import (
    TrafficCompletenessError,
    TrafficSourceSchemaError,
)
from traffic_ingest.flow_info import normalize_link_ids
from traffic_ingest.link_reference_info import (
    LINK_INFO_SERVICE,
    LINK_VERTEX_SERVICE,
    SOURCE_ID,
    parse_link_reference_response,
    request_params_json,
)


LINK_INFO_TABLE = "bronze_seoul_traffic_link_info"
LINK_VERTEX_TABLE = "bronze_seoul_traffic_link_vertex"
REQUEST_AUDIT_TABLE = "bronze_seoul_traffic_link_request_audit"


@dataclass(frozen=True)
class LinkReferenceTables:
    info: str
    vertex: str
    audit: str


@dataclass(frozen=True)
class _PreparedReference:
    descriptor: dict[str, Any]
    metadata: dict[str, Any]
    rows: list[dict[str, Any]]
    collected_at: datetime
    load_date: str


def _tables(catalog: str, schema: str) -> LinkReferenceTables:
    qualified_schema = f"{catalog}.{schema}"
    return LinkReferenceTables(
        info=f"{qualified_schema}.{LINK_INFO_TABLE}",
        vertex=f"{qualified_schema}.{LINK_VERTEX_TABLE}",
        audit=f"{qualified_schema}.{REQUEST_AUDIT_TABLE}",
    )


def create_seoul_traffic_link_reference_tables(
    cursor, catalog: str, schema: str
) -> LinkReferenceTables:
    qualified_schema = f"{catalog}.{schema}"
    tables = _tables(catalog, schema)
    create_schema_if_needed(cursor, qualified_schema)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tables.info} (
            request_id varchar,
            source_id varchar,
            service_name varchar,
            request_params_json varchar,
            link_id varchar,
            road_name varchar,
            start_node_name varchar,
            end_node_name varchar,
            map_distance varchar,
            region_code varchar,
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
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tables.vertex} (
            request_id varchar,
            source_id varchar,
            service_name varchar,
            request_params_json varchar,
            link_id varchar,
            vertex_sequence varchar,
            grs80tm_x varchar,
            grs80tm_y varchar,
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
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tables.audit} (
            request_id varchar,
            source_id varchar,
            service_name varchar,
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
    return tables


def _required(descriptor: dict[str, Any], name: str) -> Any:
    value = descriptor.get(name)
    if value in (None, ""):
        raise TrafficSourceSchemaError(
            "Traffic link reference descriptor is missing: " + name
        )
    return value


def _integer(value: object, *, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise TrafficSourceSchemaError(
            f"Traffic link reference descriptor {field} must be an integer"
        ) from exc


def _prepare_reference(
    descriptor: object,
    *,
    download_raw_object: Callable[[str, str], bytes],
) -> _PreparedReference:
    if not isinstance(descriptor, dict):
        raise TrafficSourceSchemaError(
            "Traffic link reference descriptor must be a mapping"
        )
    service_name = str(_required(descriptor, "service_name"))
    if service_name not in {LINK_INFO_SERVICE, LINK_VERTEX_SERVICE}:
        raise TrafficSourceSchemaError(
            "Traffic link reference descriptor has an unknown service_name"
        )
    if str(_required(descriptor, "source_id")) != SOURCE_ID:
        raise TrafficSourceSchemaError(
            "Traffic link reference descriptor source_id mismatch"
        )
    link_id = normalize_link_ids([_required(descriptor, "link_id")])[0]
    raw_object_key = str(_required(descriptor, "raw_object_key"))
    expected_hash = str(_required(descriptor, "raw_hash"))
    payload = download_raw_object(
        raw_object_key,
        f"Seoul {service_name} raw payload",
    )
    actual_hash = hashlib.sha256(payload).hexdigest()
    if actual_hash != expected_hash:
        raise TrafficCompletenessError(
            "Traffic link reference raw payload hash mismatch: "
            f"raw_object_key={raw_object_key}"
        )
    metadata, rows = parse_link_reference_response(
        service_name,
        payload,
        requested_link_id=link_id,
    )
    recorded_row_count = _integer(
        _required(descriptor, "row_count"), field="row_count"
    )
    recorded_total_count = _integer(
        _required(descriptor, "list_total_count"), field="list_total_count"
    )
    if recorded_row_count != len(rows) or recorded_row_count != int(
        metadata["row_count"]
    ):
        raise TrafficCompletenessError(
            f"{service_name} descriptor row_count mismatch: link_id={link_id}"
        )
    if recorded_total_count != int(metadata["list_total_count"]):
        raise TrafficCompletenessError(
            f"{service_name} descriptor list_total_count mismatch: link_id={link_id}"
        )
    if str(_required(descriptor, "result_code")) != str(
        metadata["result_code"]
    ):
        raise TrafficCompletenessError(
            f"{service_name} descriptor result_code mismatch: link_id={link_id}"
        )
    try:
        collected_at = datetime.fromisoformat(
            str(_required(descriptor, "collected_at"))
        )
        load_date = datetime.strptime(
            str(_required(descriptor, "load_date")), "%Y-%m-%d"
        ).date().isoformat()
    except ValueError as exc:
        raise TrafficSourceSchemaError(
            "Traffic link reference descriptor time fields are invalid"
        ) from exc
    _integer(_required(descriptor, "http_status"), field="http_status")
    _required(descriptor, "request_id")
    return _PreparedReference(
        descriptor=descriptor,
        metadata=metadata,
        rows=rows,
        collected_at=collected_at,
        load_date=load_date,
    )


def _validate_pair_set(
    prepared: list[_PreparedReference], raw_result: dict[str, Any]
) -> list[str]:
    grouped: dict[str, list[str]] = {}
    for item in prepared:
        link_id = str(item.descriptor["link_id"])
        grouped.setdefault(link_id, []).append(
            str(item.descriptor["service_name"])
        )
    requested = normalize_link_ids(
        raw_result.get("requested_link_ids") or list(grouped)
    )
    if set(requested) != set(grouped):
        raise TrafficCompletenessError(
            "Traffic link reference requested link set does not match raw descriptors"
        )
    expected_services = [LINK_INFO_SERVICE, LINK_VERTEX_SERVICE]
    incomplete = [
        link_id
        for link_id in requested
        if sorted(grouped.get(link_id, [])) != sorted(expected_services)
    ]
    if incomplete:
        raise TrafficCompletenessError(
            "Traffic link reference requires one complete service pair per link: "
            + ",".join(incomplete)
        )
    return requested


def _common_values(item: _PreparedReference, dag_run_id: str) -> tuple[str, ...]:
    descriptor = item.descriptor
    service_name = str(descriptor["service_name"])
    link_id = str(descriptor["link_id"])
    return (
        sql_string(descriptor["request_id"]),
        sql_string(SOURCE_ID),
        sql_string(service_name),
        sql_string(request_params_json(service_name, link_id)),
        sql_string(link_id),
        sql_string(descriptor["raw_object_key"]),
        sql_string(descriptor["raw_hash"]),
        sql_int(descriptor["http_status"]),
        sql_string(item.metadata.get("result_code")),
        sql_string(item.metadata.get("result_msg")),
        sql_int(item.metadata.get("list_total_count")),
        sql_int(item.metadata.get("row_count")),
        sql_timestamp(item.collected_at),
        sql_string(item.load_date),
        sql_string(dag_run_id),
    )


def _audit_row(item: _PreparedReference, dag_run_id: str) -> str:
    return "(" + ", ".join(_common_values(item, dag_run_id)) + ")"


def _info_rows(item: _PreparedReference, dag_run_id: str) -> list[str]:
    common = _common_values(item, dag_run_id)
    prefix = common[:5]
    suffix = common[5:]
    return [
        "("
        + ", ".join(
            (
                *prefix,
                sql_string(row.get("road_name")),
                sql_string(row.get("start_node_name")),
                sql_string(row.get("end_node_name")),
                sql_string(row.get("map_distance")),
                sql_string(row.get("region_code")),
                *suffix,
            )
        )
        + ")"
        for row in item.rows
    ]


def _vertex_rows(item: _PreparedReference, dag_run_id: str) -> list[str]:
    common = _common_values(item, dag_run_id)
    prefix = common[:5]
    suffix = common[5:]
    return [
        "("
        + ", ".join(
            (
                *prefix,
                sql_string(row.get("vertex_sequence")),
                sql_string(row.get("grs80tm_x")),
                sql_string(row.get("grs80tm_y")),
                *suffix,
            )
        )
        + ")"
        for row in item.rows
    ]


def load_traffic_link_reference_batch(
    *,
    raw_result: dict[str, Any],
    dag_run_id: str,
    cursor_factory=trino_cursor,
    create_tables=create_seoul_traffic_link_reference_tables,
    download_raw_object: Callable[[str, str], bytes],
) -> dict[str, Any]:
    raw_objects = raw_result.get("raw_objects") or []
    if not isinstance(raw_objects, list) or not raw_objects:
        raise TrafficCompletenessError(
            "Traffic link reference raw landing result is empty"
        )
    validate_traffic_raw_manifest(
        raw_result,
        dag_run_id=dag_run_id,
        dataset=SOURCE_ID,
        download_raw_object=download_raw_object,
    )
    prepared = [
        _prepare_reference(
            descriptor,
            download_raw_object=download_raw_object,
        )
        for descriptor in raw_objects
    ]
    requested_link_ids = _validate_pair_set(prepared, raw_result)

    cursor, catalog, schema = cursor_factory()
    tables = create_tables(cursor, catalog, schema)
    link_list = ", ".join(sql_string(link_id) for link_id in requested_link_ids)
    where = (
        f"source_id = {sql_string(SOURCE_ID)} "
        f"AND dag_run_id = {sql_string(dag_run_id)} "
        f"AND link_id IN ({link_list})"
    )
    for table in (tables.audit, tables.info, tables.vertex):
        cursor.execute(f"DELETE FROM {table} WHERE {where}")

    audit_values = [_audit_row(item, dag_run_id) for item in prepared]
    cursor.execute(
        f"""
        INSERT INTO {tables.audit} (
            request_id, source_id, service_name, request_params_json, link_id,
            raw_object_key, payload_hash, http_status, result_code, result_msg,
            list_total_count, row_count, collected_at, load_date, dag_run_id
        ) VALUES {', '.join(audit_values)}
        """
    )
    info_values = [
        value
        for item in prepared
        if item.descriptor["service_name"] == LINK_INFO_SERVICE
        for value in _info_rows(item, dag_run_id)
    ]
    vertex_values = [
        value
        for item in prepared
        if item.descriptor["service_name"] == LINK_VERTEX_SERVICE
        for value in _vertex_rows(item, dag_run_id)
    ]
    if info_values:
        cursor.execute(
            f"""
            INSERT INTO {tables.info} (
                request_id, source_id, service_name, request_params_json, link_id,
                road_name, start_node_name, end_node_name, map_distance, region_code,
                raw_object_key, payload_hash, http_status, result_code, result_msg,
                list_total_count, row_count, collected_at, load_date, dag_run_id
            ) VALUES {', '.join(info_values)}
            """
        )
    if vertex_values:
        cursor.execute(
            f"""
            INSERT INTO {tables.vertex} (
                request_id, source_id, service_name, request_params_json, link_id,
                vertex_sequence, grs80tm_x, grs80tm_y, raw_object_key, payload_hash,
                http_status, result_code, result_msg, list_total_count, row_count,
                collected_at, load_date, dag_run_id
            ) VALUES {', '.join(vertex_values)}
            """
        )

    incomplete = [
        link_id
        for link_id in requested_link_ids
        if not any(
            item.descriptor["link_id"] == link_id
            and item.descriptor["service_name"] == LINK_INFO_SERVICE
            and item.metadata["result_code"] == "INFO-000"
            and len(item.rows) == 1
            for item in prepared
        )
        or not any(
            item.descriptor["link_id"] == link_id
            and item.descriptor["service_name"] == LINK_VERTEX_SERVICE
            and item.metadata["result_code"] == "INFO-000"
            and len(item.rows) >= 1
            for item in prepared
        )
    ]
    if incomplete:
        raise TrafficCompletenessError(
            "Traffic link reference materialization is incomplete: "
            + ",".join(incomplete)
        )
    return {
        "source_id": SOURCE_ID,
        "raw_object_keys": [item["raw_object_key"] for item in raw_objects],
        "inserted_info": len(info_values),
        "inserted_vertices": len(vertex_values),
        "audit_rows": len(audit_values),
        "requested_link_ids": requested_link_ids,
        "is_publishable": bool(raw_result.get("is_publishable", True)),
    }


def unresolved_link_reference_ids(
    link_ids: list[str],
    *,
    cursor_factory=trino_cursor,
    create_tables=create_seoul_traffic_link_reference_tables,
) -> list[str]:
    normalized = normalize_link_ids(link_ids)
    if not normalized:
        return []
    cursor, catalog, schema = cursor_factory()
    tables = create_tables(cursor, catalog, schema)
    link_list = ", ".join(sql_string(link_id) for link_id in normalized)
    cursor.execute(
        f"""
        WITH audit AS (
            SELECT
                link_id,
                dag_run_id,
                count_if(service_name = {sql_string(LINK_INFO_SERVICE)}
                         AND result_code = 'INFO-000') AS info_success_count,
                count_if(service_name = {sql_string(LINK_VERTEX_SERVICE)}
                         AND result_code = 'INFO-000') AS vertex_success_count,
                max(CASE WHEN service_name = {sql_string(LINK_INFO_SERVICE)}
                         THEN row_count END) AS info_audit_row_count,
                max(CASE WHEN service_name = {sql_string(LINK_INFO_SERVICE)}
                         THEN list_total_count END) AS info_audit_total_count,
                max(CASE WHEN service_name = {sql_string(LINK_VERTEX_SERVICE)}
                         THEN row_count END) AS vertex_audit_row_count,
                max(CASE WHEN service_name = {sql_string(LINK_VERTEX_SERVICE)}
                         THEN list_total_count END) AS vertex_audit_total_count
            FROM {tables.audit}
            WHERE source_id = {sql_string(SOURCE_ID)}
              AND link_id IN ({link_list})
            GROUP BY link_id, dag_run_id
        ),
        info_actual AS (
            SELECT link_id, dag_run_id, count(*) AS info_actual_count
            FROM {tables.info}
            WHERE source_id = {sql_string(SOURCE_ID)}
              AND link_id IN ({link_list})
            GROUP BY link_id, dag_run_id
        ),
        vertex_actual AS (
            SELECT
                link_id,
                dag_run_id,
                count(*) AS vertex_actual_count,
                count(DISTINCT try_cast(vertex_sequence AS integer))
                    AS vertex_sequence_distinct_count
            FROM {tables.vertex}
            WHERE source_id = {sql_string(SOURCE_ID)}
              AND link_id IN ({link_list})
            GROUP BY link_id, dag_run_id
        )
        SELECT DISTINCT audit.link_id
        FROM audit
        LEFT JOIN info_actual
          ON audit.link_id = info_actual.link_id
         AND audit.dag_run_id = info_actual.dag_run_id
        LEFT JOIN vertex_actual
          ON audit.link_id = vertex_actual.link_id
         AND audit.dag_run_id = vertex_actual.dag_run_id
        WHERE info_success_count = 1
          AND vertex_success_count = 1
          AND info_audit_row_count = 1
          AND info_audit_total_count = 1
          AND info_actual_count = 1
          AND vertex_audit_row_count >= 1
          AND vertex_audit_row_count = vertex_audit_total_count
          AND vertex_actual_count = vertex_audit_row_count
          AND vertex_sequence_distinct_count = vertex_actual_count
        """
    )
    complete = {str(row[0]) for row in cursor.fetchall()}
    return [link_id for link_id in normalized if link_id not in complete]


def verify_seoul_traffic_link_reference_runtime(
    *,
    dag_run_id: str,
    expected_info_rows: int,
    expected_vertex_rows: int,
    expected_raw_objects: int,
    cursor_factory=trino_cursor,
) -> dict[str, int]:
    cursor, catalog, schema = cursor_factory()
    tables = _tables(catalog, schema)
    where = (
        f"source_id = {sql_string(SOURCE_ID)} "
        f"AND dag_run_id = {sql_string(dag_run_id)}"
    )
    cursor.execute(f"SELECT count(*) FROM {tables.info} WHERE {where}")
    info_rows = int(cursor.fetchone()[0])
    cursor.execute(f"SELECT count(*) FROM {tables.vertex} WHERE {where}")
    vertex_rows = int(cursor.fetchone()[0])
    cursor.execute(
        f"""
        SELECT count(DISTINCT raw_object_key), count(*)
        FROM {tables.audit}
        WHERE {where}
        """
    )
    raw_objects, audit_rows = (int(value) for value in cursor.fetchone())
    expected = (
        expected_info_rows,
        expected_vertex_rows,
        expected_raw_objects,
        expected_raw_objects,
    )
    actual = (info_rows, vertex_rows, raw_objects, audit_rows)
    if actual != expected:
        raise TrafficCompletenessError(
            "Traffic link reference Bronze count mismatch: "
            f"expected={expected}, actual={actual}"
        )
    return {
        "info_rows": info_rows,
        "vertex_rows": vertex_rows,
        "raw_objects": raw_objects,
        "audit_rows": audit_rows,
    }


__all__ = [
    "LINK_INFO_TABLE",
    "LINK_VERTEX_TABLE",
    "REQUEST_AUDIT_TABLE",
    "LinkReferenceTables",
    "create_seoul_traffic_link_reference_tables",
    "load_traffic_link_reference_batch",
    "unresolved_link_reference_ids",
    "verify_seoul_traffic_link_reference_runtime",
]
