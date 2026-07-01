import json
import os
import urllib.parse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from weather_ingest.common.runtime import raw_prefix, required_env


KMA_BASE_URL = os.environ.get(
    "KMA_BASE_URL",
    "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0",
)
KMA_BASE_TIMES = ["0200", "0500", "0800", "1100", "1400", "1700", "2000", "2300"]
KST = ZoneInfo("Asia/Seoul")

SOURCE_ID = "kma_vilage_fcst"
SOURCE_DOMAIN = "weather_forecast"


def build_raw_object_key(
    collected_at: datetime,
    request_id: str,
    base_date: str,
    base_time: str,
) -> str:
    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    return (
        f"{raw_prefix().rstrip('/')}/{SOURCE_DOMAIN}/{SOURCE_ID}/load_date={load_date}/"
        f"{collected_at.astimezone(KST).strftime('%Y%m%dT%H%M%SKST')}"
        f"_base-{base_date}{base_time}_{request_id}.json"
    )


def request_params_json(base_date: str, base_time: str, nx: int, ny: int) -> str:
    return json.dumps(
        {
            "api": "getVilageFcst",
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": nx,
            "ny": ny,
            "numOfRows": os.environ.get("KMA_NUM_OF_ROWS", "1000"),
            "pageNo": os.environ.get("KMA_PAGE_NO", "1"),
        },
        ensure_ascii=True,
        sort_keys=True,
    )


def resolve_kma_base_datetime() -> tuple[str, str]:
    override_date = os.environ.get("KMA_BASE_DATE")
    override_time = os.environ.get("KMA_BASE_TIME")
    if override_date or override_time:
        if not override_date or not override_time:
            raise RuntimeError("KMA_BASE_DATE and KMA_BASE_TIME must be set together.")
        return override_date, override_time

    delay_minutes = int(os.environ.get("KMA_PUBLISH_DELAY_MINUTES", "20"))
    available_at = datetime.now(KST) - timedelta(minutes=delay_minutes)
    hhmm = available_at.strftime("%H%M")
    candidates = [base_time for base_time in KMA_BASE_TIMES if base_time <= hhmm]
    if candidates:
        return available_at.strftime("%Y%m%d"), candidates[-1]

    previous_day = available_at - timedelta(days=1)
    return previous_day.strftime("%Y%m%d"), KMA_BASE_TIMES[-1]


def build_kma_url(base_date: str, base_time: str, nx: int, ny: int) -> str:
    params = {
        "serviceKey": required_env("KMA_SERVICE_KEY"),
        "numOfRows": os.environ.get("KMA_NUM_OF_ROWS", "1000"),
        "pageNo": os.environ.get("KMA_PAGE_NO", "1"),
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": str(nx),
        "ny": str(ny),
    }
    query = urllib.parse.urlencode(params, safe="%")
    return f"{KMA_BASE_URL.rstrip('/')}/getVilageFcst?{query}"


def parse_kma_response(raw_bytes: bytes) -> tuple[dict, list[dict]]:
    payload = json.loads(raw_bytes.decode("utf-8"))
    response = payload.get("response") or {}
    header = response.get("header") or {}
    body = response.get("body") or {}
    result_code = str(header.get("resultCode", ""))
    result_msg = str(header.get("resultMsg", ""))

    if result_code != "00":
        raise RuntimeError(f"KMA API returned resultCode={result_code}, resultMsg={result_msg}")

    items_node = ((body.get("items") or {}).get("item")) or []
    if isinstance(items_node, dict):
        rows = [items_node]
    elif isinstance(items_node, list):
        rows = items_node
    else:
        raise RuntimeError(f"Unexpected KMA item payload type: {type(items_node).__name__}")

    metadata = {
        "result_code": result_code,
        "result_msg": result_msg,
        "total_count": body.get("totalCount"),
        "row_count": len(rows),
    }
    return metadata, rows
