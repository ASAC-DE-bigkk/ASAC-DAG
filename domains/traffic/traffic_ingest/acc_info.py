import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from zoneinfo import ZoneInfo

from traffic_ingest.common.runtime import raw_prefix
from traffic_ingest.errors import (
    TrafficInvalidWindowError,
    TrafficSourceBusinessError,
    TrafficSourceSchemaError,
)


KST = ZoneInfo("Asia/Seoul")

SOURCE_ID = "seoul_traffic_incident"
SOURCE_DOMAIN = "traffic_incident"


def build_raw_object_key(
    collected_at: datetime,
    request_id: str,
    start_index: int,
    end_index: int,
) -> str:
    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    return (
        f"{raw_prefix().rstrip('/')}/{SOURCE_DOMAIN}/{SOURCE_ID}/load_date={load_date}/"
        f"{collected_at.astimezone(KST).strftime('%Y%m%dT%H%M%SKST')}"
        f"_AccInfo-{start_index}-{end_index}_{request_id}.xml"
    )


def request_params_json(start_index: int, end_index: int) -> str:
    return json.dumps(
        {
            "api": "AccInfo",
            "format": "xml",
            "start_index": start_index,
            "end_index": end_index,
        },
        ensure_ascii=True,
        sort_keys=True,
    )


def metadata_total_count(metadata: dict) -> int:
    value = metadata.get("list_total_count")
    if value is None or value == "":
        return 0
    return int(value)


def _int_setting(value: object, name: str) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise TrafficInvalidWindowError(f"{name} must be an integer: {value}") from exc


def resolve_acc_info_page_window(
    conf: dict | None = None,
    environ: dict | None = None,
) -> tuple[int, int, int]:
    conf = conf or {}
    environ = os.environ if environ is None else environ
    start_index = _int_setting(
        conf.get("start_index", environ.get("SEOUL_ACC_INFO_START_INDEX", "1")),
        "start_index",
    )
    end_index = _int_setting(
        conf.get("end_index", environ.get("SEOUL_ACC_INFO_END_INDEX", "1000")),
        "end_index",
    )
    page_size_value = conf.get("page_size", environ.get("SEOUL_ACC_INFO_PAGE_SIZE"))
    page_size = (
        _int_setting(page_size_value, "page_size")
        if page_size_value not in (None, "")
        else end_index - start_index + 1
    )
    if start_index < 1:
        raise TrafficInvalidWindowError(f"start_index must be positive: {start_index}")
    if end_index < start_index:
        raise TrafficInvalidWindowError(
            f"end_index must be >= start_index: {start_index}, {end_index}"
        )
    if page_size < 1:
        raise TrafficInvalidWindowError(f"page_size must be positive: {page_size}")
    return start_index, end_index, page_size


def next_acc_info_page_ranges(
    start_index: int,
    end_index: int,
    list_total_count: int,
    page_size: int | None = None,
) -> list[tuple[int, int]]:
    if start_index < 1:
        raise TrafficInvalidWindowError(f"start_index must be positive: {start_index}")
    if end_index < start_index:
        raise TrafficInvalidWindowError(
            f"end_index must be >= start_index: {start_index}, {end_index}"
        )

    resolved_page_size = (
        page_size if page_size is not None else (end_index - start_index + 1)
    )
    if resolved_page_size < 1:
        raise TrafficInvalidWindowError(
            f"page_size must be positive: {resolved_page_size}"
        )
    if list_total_count <= end_index:
        return []

    ranges = []
    page_start = end_index + 1
    while page_start <= list_total_count:
        page_end = min(page_start + resolved_page_size - 1, list_total_count)
        ranges.append((page_start, page_end))
        page_start = page_end + 1
    return ranges


def xml_text(element: ET.Element | None, name: str) -> str | None:
    if element is None:
        return None
    child = element.find(name)
    if child is None or child.text is None:
        return None
    return child.text.strip()


def parse_seoul_acc_info_response(raw_bytes: bytes) -> tuple[dict, list[dict]]:
    try:
        root = ET.fromstring(raw_bytes)
    except ET.ParseError as exc:
        raise TrafficSourceSchemaError(
            "Seoul AccInfo response is not valid XML"
        ) from exc
    if root.tag == "RESULT":
        code = xml_text(root, "CODE")
        message = xml_text(root, "MESSAGE")
        raise TrafficSourceBusinessError(
            f"Seoul AccInfo API returned resultCode={code}, resultMsg={message}"
        )
    if root.tag != "AccInfo":
        raise TrafficSourceSchemaError(
            f"Unexpected Seoul AccInfo root element: {root.tag}"
        )

    result = root.find("RESULT")
    if result is None:
        raise TrafficSourceSchemaError("Seoul AccInfo response is missing RESULT")
    code = xml_text(result, "CODE")
    message = xml_text(result, "MESSAGE")
    if not code:
        raise TrafficSourceSchemaError("Seoul AccInfo RESULT is missing CODE")
    if code != "INFO-000":
        raise TrafficSourceBusinessError(
            f"Seoul AccInfo API returned resultCode={code}, resultMsg={message}"
        )

    rows = []
    for row in root.findall("row"):
        rows.append(
            {
                "acc_id": xml_text(row, "acc_id"),
                "occr_date": xml_text(row, "occr_date"),
                "occr_time": xml_text(row, "occr_time"),
                "exp_clr_date": xml_text(row, "exp_clr_date"),
                "exp_clr_time": xml_text(row, "exp_clr_time"),
                "acc_type": xml_text(row, "acc_type"),
                "acc_dtype": xml_text(row, "acc_dtype"),
                "link_id": xml_text(row, "link_id"),
                "grs80tm_x": xml_text(row, "grs80tm_x"),
                "grs80tm_y": xml_text(row, "grs80tm_y"),
                "acc_info": xml_text(row, "acc_info"),
                "acc_road_code": xml_text(row, "acc_road_code"),
            }
        )

    metadata = {
        "result_code": code,
        "result_msg": message,
        "list_total_count": xml_text(root, "list_total_count"),
        "row_count": len(rows),
    }
    return metadata, rows
