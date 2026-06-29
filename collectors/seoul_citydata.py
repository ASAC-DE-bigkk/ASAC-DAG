import json
import os
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests
import trino.dbapi

from collectors.r2_storage import upload_json

BASE_URL = "http://openapi.seoul.go.kr:8088"
SERVICE_NAME = "citydata_ppltn"


def build_url(api_key: str, area_name: str, fmt: str = "json", start: int = 1, end: int = 5) -> str:
    encoded_area = quote(area_name, safe="")
    return f"{BASE_URL}/{api_key}/{fmt}/{SERVICE_NAME}/{start}/{end}/{encoded_area}"


def load_areas(path: Path) -> list[str]:
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    return [line.strip() for line in lines if line.strip() and not line.startswith("#")]


def save_raw(base_dir: Path, area_name: str, fetched_at: datetime, payload: dict) -> Path:
    """각 항목을 JSONL 포맷 (JSON Lines)으로 append - 매우 빠름"""
    day_dir = Path(base_dir) / fetched_at.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    file_path = day_dir / f"{area_name}.jsonl"

    entry = {"fetched_at": fetched_at.isoformat(), "payload": payload}

    # 한 줄씩 append (매우 빠름)
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    return file_path


def fetch(area_name: str, api_key: str, http_get=requests.get) -> dict:
    url = build_url(api_key=api_key, area_name=area_name)
    response = http_get(url, timeout=10)
    response.raise_for_status()
    try:
        return response.json()
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON response: {response.text[:200]}") from e


def _sql_string(value: str) -> str:
    """SQL 문자열로 변환 (이스케이프 처리)"""
    return "'" + value.replace("'", "''") + "'"


def _batch_insert_to_bronze(batch_data: list[dict]) -> None:
    """배치로 Trino Bronze에 INSERT (한 번에!)"""
    trino_conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog="iceberg",
        schema="seoul_ppltn",
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    cursor = trino_conn.cursor()

    # 테이블 생성 (없으면)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.seoul_ppltn.bronze_seoul_ppltn (
            area_nm varchar,
            area_cd varchar,
            area_congest_lvl varchar,
            area_congest_msg varchar,
            area_ppltn_min varchar,
            area_ppltn_max varchar,
            male_ppltn_rate varchar,
            female_ppltn_rate varchar,
            ppltn_rate_0 varchar,
            ppltn_rate_10 varchar,
            ppltn_rate_20 varchar,
            ppltn_rate_30 varchar,
            ppltn_rate_40 varchar,
            ppltn_rate_50 varchar,
            ppltn_rate_60 varchar,
            ppltn_rate_70 varchar,
            resnt_ppltn_rate varchar,
            non_resnt_ppltn_rate varchar,
            replace_yn varchar,
            ppltn_time varchar,
            fcst_yn varchar,
            ingested_at varchar
        )
        WITH (format = 'PARQUET')
        """
    )

    # 배치 INSERT SQL 구성 (모두 한 번에)
    values_list = []
    for item in batch_data:
        area_data = item["area_data"]
        values = {
            "area_nm": _sql_string(area_data.get("AREA_NM", "")),
            "area_cd": _sql_string(area_data.get("AREA_CD", "")),
            "area_congest_lvl": _sql_string(area_data.get("AREA_CONGEST_LVL", "")),
            "area_congest_msg": _sql_string(area_data.get("AREA_CONGEST_MSG", "")),
            "area_ppltn_min": _sql_string(str(area_data.get("AREA_PPLTN_MIN", ""))),
            "area_ppltn_max": _sql_string(str(area_data.get("AREA_PPLTN_MAX", ""))),
            "male_ppltn_rate": _sql_string(str(area_data.get("MALE_PPLTN_RATE", ""))),
            "female_ppltn_rate": _sql_string(str(area_data.get("FEMALE_PPLTN_RATE", ""))),
            "ppltn_rate_0": _sql_string(str(area_data.get("PPLTN_RATE_0", ""))),
            "ppltn_rate_10": _sql_string(str(area_data.get("PPLTN_RATE_10", ""))),
            "ppltn_rate_20": _sql_string(str(area_data.get("PPLTN_RATE_20", ""))),
            "ppltn_rate_30": _sql_string(str(area_data.get("PPLTN_RATE_30", ""))),
            "ppltn_rate_40": _sql_string(str(area_data.get("PPLTN_RATE_40", ""))),
            "ppltn_rate_50": _sql_string(str(area_data.get("PPLTN_RATE_50", ""))),
            "ppltn_rate_60": _sql_string(str(area_data.get("PPLTN_RATE_60", ""))),
            "ppltn_rate_70": _sql_string(str(area_data.get("PPLTN_RATE_70", ""))),
            "resnt_ppltn_rate": _sql_string(str(area_data.get("RESNT_PPLTN_RATE", ""))),
            "non_resnt_ppltn_rate": _sql_string(str(area_data.get("NON_RESNT_PPLTN_RATE", ""))),
            "replace_yn": _sql_string(area_data.get("REPLACE_YN", "")),
            "ppltn_time": _sql_string(area_data.get("PPLTN_TIME", "")),
            "fcst_yn": _sql_string(area_data.get("FCST_YN", "")),
            "ingested_at": _sql_string(item["ingested_at"]),
        }
        values_list.append(
            f"({values['area_nm']}, {values['area_cd']}, {values['area_congest_lvl']}, "
            f"{values['area_congest_msg']}, {values['area_ppltn_min']}, {values['area_ppltn_max']}, "
            f"{values['male_ppltn_rate']}, {values['female_ppltn_rate']}, "
            f"{values['ppltn_rate_0']}, {values['ppltn_rate_10']}, {values['ppltn_rate_20']}, "
            f"{values['ppltn_rate_30']}, {values['ppltn_rate_40']}, {values['ppltn_rate_50']}, "
            f"{values['ppltn_rate_60']}, {values['ppltn_rate_70']}, "
            f"{values['resnt_ppltn_rate']}, {values['non_resnt_ppltn_rate']}, "
            f"{values['replace_yn']}, {values['ppltn_time']}, {values['fcst_yn']}, "
            f"{values['ingested_at']})"
        )

    # 한 번에 INSERT!
    if values_list:
        insert_sql = f"INSERT INTO iceberg.seoul_ppltn.bronze_seoul_ppltn VALUES {', '.join(values_list)}"
        cursor.execute(insert_sql)

    trino_conn.commit()
    trino_conn.close()


def collect_and_store(
    area_names: list[str],
    api_key: str,
    base_dir: Path,
    r2_client,
    r2_bucket: str,
    http_get=requests.get,
    fetched_at: datetime | None = None,
) -> list[dict]:
    timestamp = fetched_at or datetime.now()
    results = []
    batch_data = []  # Trino 배치 INSERT용

    for area_name in area_names:
        try:
            payload = fetch(area_name=area_name, api_key=api_key, http_get=http_get)
        except Exception as exc:
            results.append(
                {"area_name": area_name, "local_path": None, "r2_key": None, "error": str(exc)}
            )
            continue

        local_path = save_raw(base_dir=base_dir, area_name=area_name, fetched_at=timestamp, payload=payload)
        r2_key = (
            f"bronze/population/{timestamp.strftime('%Y-%m-%d')}/{timestamp.strftime('%H')}"
            f"/{timestamp.strftime('%M')}/{area_name}.json"
        )

        # R2 업로드
        try:
            upload_json(r2_client, bucket=r2_bucket, key=r2_key, payload=payload)
        except Exception as exc:
            results.append(
                {"area_name": area_name, "local_path": local_path, "r2_key": None, "error": str(exc)}
            )
            continue

        # Trino 배치 INSERT용 데이터 수집
        try:
            data_list = payload.get("SeoulRtd.citydata_ppltn", [])
            if isinstance(data_list, list) and len(data_list) > 0:
                area_data = data_list[0]
                batch_data.append({
                    "area_data": area_data,
                    "ingested_at": timestamp.isoformat(),
                })
        except Exception:
            pass

        results.append(
            {"area_name": area_name, "local_path": local_path, "r2_key": r2_key, "error": None}
        )

    # 배치 INSERT (한 번에!)
    if batch_data:
        try:
            _batch_insert_to_bronze(batch_data)
        except Exception:
            pass

    return results
