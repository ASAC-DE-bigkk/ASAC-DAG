import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import trino.dbapi


def load_seoul_ppltn_to_bronze(
    data_dir: Path,
    catalog: str = "iceberg",
    schema: str = "seoul_ppltn",
    table: str = "bronze_seoul_ppltn",
) -> dict:
    """R2에서 수집한 raw JSON을 파싱해서 Trino Bronze 테이블에 로드"""

    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog,
        schema=schema,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )

    cursor = connection.cursor()

    # 스키마 생성
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # 테이블 생성
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
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
            raw_file_path varchar,
            ingested_at varchar
        )
        WITH (
            format = 'PARQUET'
        )
        """
    )

    # 로컬 raw 데이터 파일 읽기
    raw_dir = Path(data_dir) / "raw"
    if not raw_dir.exists():
        return {"status": "no_data", "loaded_count": 0, "error": "raw directory not found"}

    loaded_count = 0
    errors = []

    for day_dir in sorted(raw_dir.iterdir()):
        if not day_dir.is_dir():
            continue

        for json_file in day_dir.glob("*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    content = json.load(f)

                # 다양한 JSON 구조에 대응
                if isinstance(content, list):
                    entries = content
                elif isinstance(content, dict) and "entries" in content:
                    entries = content["entries"]
                else:
                    entries = [content]

                # 각 엔트리 파싱
                for entry in entries:
                    if isinstance(entry, dict) and "payload" in entry:
                        payload_wrapper = entry["payload"]
                        fetched_at = entry.get("fetched_at", datetime.now().isoformat())
                    else:
                        payload_wrapper = entry
                        fetched_at = datetime.now().isoformat()

                    # payload_wrapper가 dict 형태인지 확인
                    if not isinstance(payload_wrapper, dict):
                        continue

                    # "SeoulRtd.citydata_ppltn" 추출
                    data_list = payload_wrapper.get("SeoulRtd.citydata_ppltn", [])
                    if not isinstance(data_list, list) or len(data_list) == 0:
                        continue

                    # 첫 번째 요소가 실제 데이터
                    payload = data_list[0]

                    # 데이터 추출
                    values = {
                        "area_nm": _sql_string(payload.get("AREA_NM", "")),
                        "area_cd": _sql_string(payload.get("AREA_CD", "")),
                        "area_congest_lvl": _sql_string(payload.get("AREA_CONGEST_LVL", "")),
                        "area_congest_msg": _sql_string(payload.get("AREA_CONGEST_MSG", "")),
                        "area_ppltn_min": _sql_string(str(payload.get("AREA_PPLTN_MIN", ""))),
                        "area_ppltn_max": _sql_string(str(payload.get("AREA_PPLTN_MAX", ""))),
                        "male_ppltn_rate": _sql_string(str(payload.get("MALE_PPLTN_RATE", ""))),
                        "female_ppltn_rate": _sql_string(str(payload.get("FEMALE_PPLTN_RATE", ""))),
                        "ppltn_rate_0": _sql_string(str(payload.get("PPLTN_RATE_0", ""))),
                        "ppltn_rate_10": _sql_string(str(payload.get("PPLTN_RATE_10", ""))),
                        "ppltn_rate_20": _sql_string(str(payload.get("PPLTN_RATE_20", ""))),
                        "ppltn_rate_30": _sql_string(str(payload.get("PPLTN_RATE_30", ""))),
                        "ppltn_rate_40": _sql_string(str(payload.get("PPLTN_RATE_40", ""))),
                        "ppltn_rate_50": _sql_string(str(payload.get("PPLTN_RATE_50", ""))),
                        "ppltn_rate_60": _sql_string(str(payload.get("PPLTN_RATE_60", ""))),
                        "ppltn_rate_70": _sql_string(str(payload.get("PPLTN_RATE_70", ""))),
                        "resnt_ppltn_rate": _sql_string(str(payload.get("RESNT_PPLTN_RATE", ""))),
                        "non_resnt_ppltn_rate": _sql_string(str(payload.get("NON_RESNT_PPLTN_RATE", ""))),
                        "replace_yn": _sql_string(payload.get("REPLACE_YN", "")),
                        "ppltn_time": _sql_string(payload.get("PPLTN_TIME", "")),
                        "fcst_yn": _sql_string(payload.get("FCST_YN", "")),
                        "raw_file_path": _sql_string(str(json_file)),
                        "ingested_at": _sql_string(fetched_at),
                    }

                    insert_sql = f"""
                    INSERT INTO {catalog}.{schema}.{table} VALUES (
                        {values['area_nm']},
                        {values['area_cd']},
                        {values['area_congest_lvl']},
                        {values['area_congest_msg']},
                        {values['area_ppltn_min']},
                        {values['area_ppltn_max']},
                        {values['male_ppltn_rate']},
                        {values['female_ppltn_rate']},
                        {values['ppltn_rate_0']},
                        {values['ppltn_rate_10']},
                        {values['ppltn_rate_20']},
                        {values['ppltn_rate_30']},
                        {values['ppltn_rate_40']},
                        {values['ppltn_rate_50']},
                        {values['ppltn_rate_60']},
                        {values['ppltn_rate_70']},
                        {values['resnt_ppltn_rate']},
                        {values['non_resnt_ppltn_rate']},
                        {values['replace_yn']},
                        {values['ppltn_time']},
                        {values['fcst_yn']},
                        {values['raw_file_path']},
                        {values['ingested_at']}
                    )
                    """
                    cursor.execute(insert_sql)
                    loaded_count += 1

            except Exception as exc:
                errors.append({"file": str(json_file), "error": str(exc)})

    connection.commit()
    connection.close()

    return {
        "status": "success" if not errors else "partial",
        "loaded_count": loaded_count,
        "errors": errors,
    }


def _sql_string(value: str) -> str:
    """SQL 문자열로 변환 (이스케이프 처리)"""
    return "'" + value.replace("'", "''") + "'"
