import json
import os
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime
from zoneinfo import ZoneInfo

from traffic_ingest.common.runtime import raw_prefix, required_env


SEOUL_OPEN_API_BASE_URL = os.environ.get(
    "SEOUL_OPEN_API_BASE_URL",
    "http://openapi.seoul.go.kr:8088",
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


def build_seoul_acc_info_url(start_index: int, end_index: int) -> str:
    api_key = urllib.parse.quote(required_env("SEOUL_OPEN_API_KEY"), safe="")
    return f"{SEOUL_OPEN_API_BASE_URL.rstrip('/')}/{api_key}/xml/AccInfo/{start_index}/{end_index}/"


def xml_text(element: ET.Element | None, name: str) -> str | None:
    if element is None:
        return None
    child = element.find(name)
    if child is None or child.text is None:
        return None
    return child.text.strip()


def parse_seoul_acc_info_response(raw_bytes: bytes) -> tuple[dict, list[dict]]:
    root = ET.fromstring(raw_bytes)
    if root.tag == "RESULT":
        code = xml_text(root, "CODE")
        message = xml_text(root, "MESSAGE")
        raise RuntimeError(f"Seoul AccInfo API returned resultCode={code}, resultMsg={message}")
    if root.tag != "AccInfo":
        raise RuntimeError(f"Unexpected Seoul AccInfo root element: {root.tag}")

    result = root.find("RESULT")
    code = xml_text(result, "CODE")
    message = xml_text(result, "MESSAGE")
    if code != "INFO-000":
        raise RuntimeError(f"Seoul AccInfo API returned resultCode={code}, resultMsg={message}")

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
