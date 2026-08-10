"""Seoul TOPIS LinkInfo and LinkVerInfo source contracts."""

from __future__ import annotations

import json
import math
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any
from urllib.parse import quote

from common.raw_path import build_raw_run_prefix
from traffic_ingest.common.runtime import raw_prefix
from traffic_ingest.errors import (
    TrafficBronzeConfigurationError,
    TrafficSourceBusinessError,
    TrafficSourceEmptyResponseError,
    TrafficSourceSchemaError,
)
from traffic_ingest.flow_info import KST, normalize_link_ids


LINK_INFO_SERVICE = "LinkInfo"
LINK_VERTEX_SERVICE = "LinkVerInfo"
SOURCE_ID = "seoul_traffic_link_reference"
SOURCE_DOMAIN = "traffic"
DEFAULT_API_BASE_URL = "http://openapi.seoul.go.kr:8088"
SERVICE_END_INDEX = {
    LINK_INFO_SERVICE: 1,
    LINK_VERTEX_SERVICE: 1000,
}


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _mapping_value(row: dict[str, Any], name: str) -> object:
    if name in row:
        return row[name]
    lower_name = name.lower()
    for key, value in row.items():
        if str(key).lower() == lower_name:
            return value
    return None


def _xml_value(row: ET.Element, name: str) -> str | None:
    lower_name = name.lower()
    child = next(
        (candidate for candidate in row if candidate.tag.lower() == lower_name),
        None,
    )
    return _text(child.text if child is not None else None)


def _service_name(value: object) -> str:
    service_name = _text(value)
    if service_name not in SERVICE_END_INDEX:
        raise TrafficBronzeConfigurationError(
            "Traffic link reference service_name must be LinkInfo or LinkVerInfo"
        )
    return service_name


def _link_id(value: object) -> str:
    return normalize_link_ids([value])[0]


def request_params_json(service_name: str, link_id: str) -> str:
    service_name = _service_name(service_name)
    return json.dumps(
        {
            "api": service_name,
            "format": "xml",
            "start_index": 1,
            "end_index": SERVICE_END_INDEX[service_name],
            "link_id": _link_id(link_id),
        },
        ensure_ascii=True,
        sort_keys=True,
    )


def build_link_reference_api_url(
    service_name: str,
    link_id: str,
    *,
    api_key: str,
    base_url: str | None = None,
    fmt: str = "xml",
) -> str:
    service_name = _service_name(service_name)
    if fmt not in {"xml", "json"}:
        raise TrafficBronzeConfigurationError(
            "Traffic link reference format must be xml or json"
        )
    base = (
        base_url
        or os.environ.get("SEOUL_OPEN_API_BASE_URL")
        or DEFAULT_API_BASE_URL
    ).rstrip("/")
    safe_link_id = quote(_link_id(link_id), safe="")
    return (
        f"{base}/{api_key}/{fmt}/{service_name}/1/"
        f"{SERVICE_END_INDEX[service_name]}/{safe_link_id}/"
    )


def build_link_reference_raw_object_key(
    collected_at: datetime,
    dag_run_id: str,
    service_name: str,
    link_id: str,
    *,
    landing_load_date: str | None = None,
) -> str:
    service_name = _service_name(service_name)
    collected_kst = collected_at.astimezone(KST)
    if landing_load_date is None:
        load_date = collected_kst.date().isoformat()
    else:
        try:
            load_date = datetime.strptime(
                landing_load_date, "%Y-%m-%d"
            ).date().isoformat()
        except (TypeError, ValueError) as exc:
            raise TrafficBronzeConfigurationError(
                "Traffic link reference landing_load_date must be YYYY-MM-DD"
            ) from exc
    safe_link_id = _link_id(link_id)
    run_prefix = build_raw_run_prefix(
        raw_prefix=raw_prefix(),
        domain=SOURCE_DOMAIN,
        source_id=SOURCE_ID,
        load_date=load_date,
        run_id=dag_run_id,
    )
    return (
        f"{run_prefix}/"
        f"{collected_kst:%Y%m%dT%H%M%SKST}_{service_name}-"
        f"link_id={safe_link_id}.xml"
    )


def _json_envelope(
    service_name: str, raw_bytes: bytes
) -> tuple[str, str | None, int, list[dict[str, Any]]]:
    try:
        document = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TrafficSourceSchemaError(
            f"{service_name} response is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(document, dict):
        raise TrafficSourceSchemaError(f"{service_name} response root must be an object")
    envelope = _mapping_value(document, service_name)
    if not isinstance(envelope, dict):
        raise TrafficSourceSchemaError(
            f"{service_name} response is missing {service_name} envelope"
        )
    result = _mapping_value(envelope, "RESULT")
    if not isinstance(result, dict):
        raise TrafficSourceSchemaError(f"{service_name} response is missing RESULT")
    result_code = _text(_mapping_value(result, "CODE"))
    result_msg = _text(_mapping_value(result, "MESSAGE"))
    if not result_code:
        raise TrafficSourceSchemaError(f"{service_name} RESULT is missing CODE")
    total_count = _total_count(
        service_name, _mapping_value(envelope, "list_total_count")
    )
    raw_rows = _mapping_value(envelope, "row") or []
    if isinstance(raw_rows, dict):
        raw_rows = [raw_rows]
    if not isinstance(raw_rows, list) or any(
        not isinstance(row, dict) for row in raw_rows
    ):
        raise TrafficSourceSchemaError(
            f"{service_name} row must be an object or list of objects"
        )
    return result_code, result_msg, total_count, raw_rows


def _xml_envelope(
    service_name: str, raw_bytes: bytes
) -> tuple[str, str | None, int, list[ET.Element]]:
    try:
        root = ET.fromstring(raw_bytes)
    except (UnicodeDecodeError, ET.ParseError) as exc:
        raise TrafficSourceSchemaError(
            f"{service_name} response is not valid XML"
        ) from exc
    if root.tag.lower() != service_name.lower():
        raise TrafficSourceSchemaError(
            f"{service_name} XML response root does not match service"
        )
    result = next(
        (child for child in root if child.tag.lower() == "result"),
        None,
    )
    if result is None:
        raise TrafficSourceSchemaError(
            f"{service_name} XML response is missing RESULT"
        )
    result_code = _xml_value(result, "CODE")
    result_msg = _xml_value(result, "MESSAGE")
    if not result_code:
        raise TrafficSourceSchemaError(
            f"{service_name} XML RESULT is missing CODE"
        )
    total_count = _total_count(
        service_name,
        next(
            (
                child.text
                for child in root
                if child.tag.lower() == "list_total_count"
            ),
            None,
        ),
    )
    raw_rows = [child for child in root if child.tag.lower() == "row"]
    return result_code, result_msg, total_count, raw_rows


def _total_count(service_name: str, value: object) -> int:
    try:
        total_count = int(_text(value) or 0)
    except ValueError as exc:
        raise TrafficSourceSchemaError(
            f"{service_name} list_total_count must be an integer"
        ) from exc
    if total_count < 0:
        raise TrafficSourceSchemaError(
            f"{service_name} list_total_count must not be negative"
        )
    return total_count


def _row_value(row: dict[str, Any] | ET.Element, name: str) -> str | None:
    if isinstance(row, dict):
        return _text(_mapping_value(row, name))
    return _xml_value(row, name)


def _normalize_rows(
    service_name: str,
    raw_rows: list[dict[str, Any]] | list[ET.Element],
    *,
    requested_link_id: str,
) -> list[dict[str, str | None]]:
    rows: list[dict[str, str | None]] = []
    sequences: set[int] = set()
    for raw_row in raw_rows:
        link_id = _row_value(raw_row, "LINK_ID")
        if link_id != requested_link_id:
            raise TrafficSourceSchemaError(
                f"{service_name} row does not match requested link_id"
            )
        if service_name == LINK_INFO_SERVICE:
            road_name = _row_value(raw_row, "ROAD_NAME")
            if not road_name:
                raise TrafficSourceSchemaError("LinkInfo ROAD_NAME is required")
            rows.append(
                {
                    "link_id": link_id,
                    "road_name": road_name,
                    "start_node_name": _row_value(raw_row, "ST_NODE_NM"),
                    "end_node_name": _row_value(raw_row, "ED_NODE_NM"),
                    "map_distance": _row_value(raw_row, "MAP_DIST"),
                    "region_code": _row_value(raw_row, "REG_CD"),
                }
            )
            continue

        sequence_text = _row_value(raw_row, "VER_SEQ")
        x_text = _row_value(raw_row, "GRS80TM_X")
        y_text = _row_value(raw_row, "GRS80TM_Y")
        try:
            sequence = int(sequence_text or "")
        except ValueError as exc:
            raise TrafficSourceSchemaError(
                "LinkVerInfo vertex_sequence must be an integer"
            ) from exc
        if sequence in sequences:
            raise TrafficSourceSchemaError(
                "LinkVerInfo vertex_sequence must be unique per link"
            )
        try:
            x_value = float(x_text or "")
            y_value = float(y_text or "")
        except ValueError as exc:
            raise TrafficSourceSchemaError(
                "LinkVerInfo GRS80TM coordinates must be numeric"
            ) from exc
        if not math.isfinite(x_value) or not math.isfinite(y_value):
            raise TrafficSourceSchemaError(
                "LinkVerInfo GRS80TM coordinates must be finite"
            )
        sequences.add(sequence)
        rows.append(
            {
                "link_id": link_id,
                "vertex_sequence": sequence_text,
                "grs80tm_x": x_text,
                "grs80tm_y": y_text,
            }
        )
    return rows


def parse_link_reference_response(
    service_name: str,
    raw_bytes: bytes,
    *,
    requested_link_id: str,
) -> tuple[dict[str, object], list[dict[str, str | None]]]:
    service_name = _service_name(service_name)
    requested_link_id = _link_id(requested_link_id)
    if not raw_bytes.strip():
        raise TrafficSourceEmptyResponseError(f"{service_name} response body is empty")
    if raw_bytes.lstrip().startswith(b"<"):
        result_code, result_msg, total_count, raw_rows = _xml_envelope(
            service_name, raw_bytes
        )
    else:
        result_code, result_msg, total_count, raw_rows = _json_envelope(
            service_name, raw_bytes
        )

    if result_code == "INFO-200":
        raw_rows = []
        total_count = 0
    elif result_code != "INFO-000":
        raise TrafficSourceBusinessError(
            f"Seoul {service_name} API returned resultCode={result_code}, "
            f"resultMsg={result_msg}"
        )

    if total_count != len(raw_rows):
        raise TrafficSourceSchemaError(
            f"{service_name} list_total_count does not match row count"
        )
    if result_code == "INFO-000" and service_name == LINK_INFO_SERVICE:
        if len(raw_rows) != 1:
            raise TrafficSourceSchemaError(
                "LinkInfo success response must contain exactly one row"
            )
    if result_code == "INFO-000" and service_name == LINK_VERTEX_SERVICE:
        if not raw_rows:
            raise TrafficSourceSchemaError(
                "LinkVerInfo success response must contain at least one row"
            )
        if len(raw_rows) > SERVICE_END_INDEX[LINK_VERTEX_SERVICE]:
            raise TrafficSourceSchemaError(
                "LinkVerInfo row count exceeds the requested page limit"
            )

    rows = _normalize_rows(
        service_name,
        raw_rows,
        requested_link_id=requested_link_id,
    )
    return {
        "service_name": service_name,
        "result_code": result_code,
        "result_msg": result_msg,
        "list_total_count": total_count,
        "row_count": len(rows),
    }, rows


__all__ = [
    "LINK_INFO_SERVICE",
    "LINK_VERTEX_SERVICE",
    "SERVICE_END_INDEX",
    "SOURCE_ID",
    "build_link_reference_api_url",
    "build_link_reference_raw_object_key",
    "parse_link_reference_response",
    "request_params_json",
]
