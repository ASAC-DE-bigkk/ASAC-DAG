"""Seoul TOPIS TrafficInfo source contract and link-id resolution."""

from __future__ import annotations

import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Callable
from urllib.parse import quote
from zoneinfo import ZoneInfo

from traffic_ingest.common.runtime import (
    raw_prefix,
    sql_string,
    trino_cursor,
)
from traffic_ingest.errors import (
    TrafficBronzeConfigurationError,
    TrafficSourceBusinessError,
    TrafficSourceSchemaError,
)


KST = ZoneInfo("Asia/Seoul")
SOURCE_ID = "seoul_traffic_flow"
SOURCE_DOMAIN = "traffic_flow"
SERVICE_NAME = "TrafficInfo"
DEFAULT_API_BASE_URL = "http://openapi.seoul.go.kr:8088"
LINK_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _mapping_value(row: dict, name: str) -> object:
    if name in row:
        return row[name]
    lower_name = name.lower()
    for key, value in row.items():
        if str(key).lower() == lower_name:
            return value
    return None


def _xml_value(row: ET.Element, name: str) -> str | None:
    child = row.find(name)
    if child is None:
        lower_name = name.lower()
        child = next(
            (candidate for candidate in row if candidate.tag.lower() == lower_name),
            None,
        )
    return _text(child.text if child is not None else None)


def _safe_link_id(value: object) -> str:
    link_id = _text(value)
    if not link_id or not LINK_ID_PATTERN.fullmatch(link_id):
        raise TrafficBronzeConfigurationError(
            "TrafficInfo link_id must contain only letters, digits, '_' or '-'."
        )
    return link_id


def normalize_link_ids(values: object) -> list[str]:
    """Normalize explicit link ids while preserving first-seen order."""
    if isinstance(values, str):
        values = values.split(",")
    if not isinstance(values, (list, tuple, set)):
        raise TrafficBronzeConfigurationError(
            "Traffic flow link_ids must be a string or a list."
        )
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        link_id = _safe_link_id(value)
        if link_id not in seen:
            normalized.append(link_id)
            seen.add(link_id)
    return normalized


def _max_link_count(environ: dict[str, str] | None = None) -> int:
    environ = os.environ if environ is None else environ
    value = environ.get("SEOUL_TRAFFIC_FLOW_MAX_LINKS", "1000")
    try:
        limit = int(value)
    except (TypeError, ValueError) as exc:
        raise TrafficBronzeConfigurationError(
            "SEOUL_TRAFFIC_FLOW_MAX_LINKS must be an integer."
        ) from exc
    if limit < 1:
        raise TrafficBronzeConfigurationError(
            "SEOUL_TRAFFIC_FLOW_MAX_LINKS must be positive."
        )
    return limit


def resolve_flow_link_ids(
    *,
    conf: dict[str, Any] | None = None,
    incident_run_id: str | None = None,
    cursor_factory: Callable = trino_cursor,
    environ: dict[str, str] | None = None,
) -> list[str]:
    """Resolve links from manual input or one exact Incident Bronze snapshot."""
    conf = conf or {}
    environ = os.environ if environ is None else environ
    explicit = conf.get("link_ids")
    if explicit in (None, ""):
        explicit = environ.get("SEOUL_TRAFFIC_FLOW_LINK_IDS")
    if explicit not in (None, ""):
        return normalize_link_ids(explicit)[: _max_link_count(environ)]

    if not str(incident_run_id or "").strip():
        raise TrafficBronzeConfigurationError(
            "Traffic flow incident_run_id is required when link_ids are not explicit."
        )

    cursor, catalog, schema = cursor_factory()
    qualified = f"{catalog}.{schema}"
    cursor.execute(
        f"""
        SELECT DISTINCT cast(incident.link_id AS varchar) AS link_id
        FROM {qualified}.bronze_seoul_traffic_incident AS incident
        WHERE incident.dag_run_id = {sql_string(str(incident_run_id))}
          AND incident.link_id IS NOT NULL
          AND trim(cast(incident.link_id AS varchar)) <> ''
        ORDER BY link_id
        LIMIT {_max_link_count(environ)}
        """
    )
    rows = cursor.fetchall()
    return normalize_link_ids([row[0] for row in rows])


def request_params_json(link_id: str) -> str:
    return json.dumps(
        {
            "api": SERVICE_NAME,
            "format": "xml",
            "start_index": 1,
            "end_index": 1,
            "link_id": _safe_link_id(link_id),
        },
        ensure_ascii=True,
        sort_keys=True,
    )


def build_api_url(
    link_id: str,
    *,
    api_key: str,
    base_url: str | None = None,
    fmt: str = "xml",
) -> str:
    """Build the per-link URL without ever storing the key in source metadata."""
    safe_link_id = quote(_safe_link_id(link_id), safe="")
    base = (base_url or os.environ.get("SEOUL_OPEN_API_BASE_URL") or DEFAULT_API_BASE_URL).rstrip("/")
    if fmt not in {"xml", "json"}:
        raise TrafficBronzeConfigurationError("TrafficInfo format must be xml or json")
    return f"{base}/{api_key}/{fmt}/{SERVICE_NAME}/1/1/{safe_link_id}/"


def build_raw_object_key(
    collected_at: datetime,
    dag_run_id: str,
    link_id: str,
) -> str:
    collected_kst = collected_at.astimezone(KST)
    safe_run_id = re.sub(r"[^A-Za-z0-9_.=-]", "_", dag_run_id)
    safe_link_id = _safe_link_id(link_id)
    return (
        f"{raw_prefix().rstrip('/')}/{SOURCE_DOMAIN}/{SOURCE_ID}/"
        f"load_date={collected_kst:%Y-%m-%d}/dag_run_id={safe_run_id}/"
        f"{collected_kst:%Y%m%dT%H%M%SKST}_TrafficInfo-link_id={safe_link_id}.xml"
    )


def parse_traffic_info_response(raw_bytes: bytes) -> tuple[dict[str, Any], list[dict[str, str | None]]]:
    if raw_bytes.lstrip().startswith(b"<"):
        return _parse_traffic_info_xml(raw_bytes)
    try:
        document = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TrafficSourceSchemaError("TrafficInfo response is not valid UTF-8 JSON") from exc
    if not isinstance(document, dict):
        raise TrafficSourceSchemaError("TrafficInfo response root must be an object")

    envelope = document.get(SERVICE_NAME)
    if not isinstance(envelope, dict):
        raise TrafficSourceSchemaError("TrafficInfo response is missing TrafficInfo envelope")
    result = envelope.get("RESULT")
    if not isinstance(result, dict):
        raise TrafficSourceSchemaError("TrafficInfo response is missing RESULT")
    result_code = _text(result.get("CODE"))
    result_msg = _text(result.get("MESSAGE"))
    if not result_code:
        raise TrafficSourceSchemaError("TrafficInfo RESULT is missing CODE")

    try:
        total_count = int(_text(envelope.get("list_total_count")) or 0)
    except ValueError as exc:
        raise TrafficSourceSchemaError("TrafficInfo list_total_count must be an integer") from exc

    raw_rows = envelope.get("row") or []
    if isinstance(raw_rows, dict):
        raw_rows = [raw_rows]
    if not isinstance(raw_rows, list) or any(not isinstance(row, dict) for row in raw_rows):
        raise TrafficSourceSchemaError("TrafficInfo row must be an object or list of objects")

    if result_code == "INFO-200":
        raw_rows = []
    elif result_code != "INFO-000":
        raise TrafficSourceBusinessError(
            f"Seoul TrafficInfo API returned resultCode={result_code}, resultMsg={result_msg}"
        )

    rows = [
        {
            "link_id": _text(_mapping_value(row, "LINK_ID")),
            "prcs_spd": _text(_mapping_value(row, "PRCS_SPD")),
            "prcs_trv_time": _text(_mapping_value(row, "PRCS_TRV_TIME")),
        }
        for row in raw_rows
    ]
    return {
        "result_code": result_code,
        "result_msg": result_msg,
        "list_total_count": total_count,
        "row_count": len(rows),
    }, rows


def _parse_traffic_info_xml(
    raw_bytes: bytes,
) -> tuple[dict[str, Any], list[dict[str, str | None]]]:
    try:
        root = ET.fromstring(raw_bytes)
    except (UnicodeDecodeError, ET.ParseError) as exc:
        raise TrafficSourceSchemaError("TrafficInfo response is not valid XML") from exc
    result = root.find("RESULT")
    if result is None:
        raise TrafficSourceSchemaError("TrafficInfo XML response is missing RESULT")
    result_code = _text(result.findtext("CODE"))
    result_msg = _text(result.findtext("MESSAGE"))
    if not result_code:
        raise TrafficSourceSchemaError("TrafficInfo XML RESULT is missing CODE")
    try:
        total_count = int(_text(root.findtext("list_total_count")) or 0)
    except ValueError as exc:
        raise TrafficSourceSchemaError(
            "TrafficInfo XML list_total_count must be an integer"
        ) from exc
    if result_code == "INFO-200":
        source_rows = []
    elif result_code != "INFO-000":
        raise TrafficSourceBusinessError(
            f"Seoul TrafficInfo API returned resultCode={result_code}, resultMsg={result_msg}"
        )
    else:
        source_rows = root.findall("row")
    rows = [
        {
            "link_id": _xml_value(row, "LINK_ID"),
            "prcs_spd": _xml_value(row, "PRCS_SPD"),
            "prcs_trv_time": _xml_value(row, "PRCS_TRV_TIME"),
        }
        for row in source_rows
    ]
    return {
        "result_code": result_code,
        "result_msg": result_msg,
        "list_total_count": total_count,
        "row_count": len(rows),
    }, rows


def traffic_api_key(environ: dict[str, str] | None = None) -> str:
    environ = os.environ if environ is None else environ
    value = environ.get("SEOUL_OPEN_API_KEY") or environ.get("SEOUL_API_KEY_TRIC")
    if not value:
        raise TrafficBronzeConfigurationError(
            "Missing required environment variable: SEOUL_OPEN_API_KEY"
        )
    return value


__all__ = [
    "KST",
    "SERVICE_NAME",
    "SOURCE_ID",
    "build_api_url",
    "build_raw_object_key",
    "normalize_link_ids",
    "parse_traffic_info_response",
    "request_params_json",
    "resolve_flow_link_ids",
    "traffic_api_key",
]
