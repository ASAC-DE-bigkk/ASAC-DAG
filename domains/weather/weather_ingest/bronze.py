from datetime import datetime
from zoneinfo import ZoneInfo

from weather_ingest.common.runtime import (
    create_schema_if_needed,
    sql_int,
    sql_string,
    sql_timestamp,
    trino_cursor,
)
from weather_ingest.kma import SOURCE_ID, request_params_json


BRONZE_TABLE = "bronze_kma_vilage_fcst"
KST = ZoneInfo("Asia/Seoul")


def ensure_kma_bronze_schema(cursor, qualified_table: str) -> None:
    for column_name, column_type in (
        ("request_params_json", "varchar"),
        ("load_date", "varchar"),
    ):
        cursor.execute(
            f"ALTER TABLE {qualified_table} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )


def create_kma_bronze_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.{BRONZE_TABLE}"
    create_schema_if_needed(cursor, qualified_schema)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            request_id varchar,
            source_id varchar,
            request_params_json varchar,
            place_id varchar,
            base_date varchar,
            base_time varchar,
            nx integer,
            ny integer,
            category varchar,
            fcst_date varchar,
            fcst_time varchar,
            fcst_value varchar,
            raw_object_key varchar,
            payload_hash varchar,
            http_status integer,
            result_code varchar,
            result_msg varchar,
            total_count integer,
            item_count integer,
            collected_at timestamp(6),
            load_date varchar,
            dag_run_id varchar
        )
        WITH (
            format = 'PARQUET'
        )
        """
    )
    ensure_kma_bronze_schema(cursor, qualified_table)
    return qualified_table


def insert_kma_bronze_rows(
    cursor,
    qualified_table: str,
    rows: list[dict],
    metadata: dict,
    request_id: str,
    place_id: str,
    base_date: str,
    base_time: str,
    nx: int,
    ny: int,
    raw_object_key: str,
    raw_hash: str,
    http_status: int,
    collected_at: datetime,
    dag_run_id: str,
) -> int:
    if not rows:
        raise RuntimeError("KMA API returned no forecast rows.")

    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    request_params = request_params_json(base_date, base_time, nx, ny)
    values = []
    for row in rows:
        values.append(
            "("
            f"{sql_string(request_id)}, "
            f"{sql_string(SOURCE_ID)}, "
            f"{sql_string(request_params)}, "
            f"{sql_string(place_id)}, "
            f"{sql_string(row.get('baseDate'))}, "
            f"{sql_string(row.get('baseTime'))}, "
            f"{sql_int(row.get('nx'))}, "
            f"{sql_int(row.get('ny'))}, "
            f"{sql_string(row.get('category'))}, "
            f"{sql_string(row.get('fcstDate'))}, "
            f"{sql_string(row.get('fcstTime'))}, "
            f"{sql_string(row.get('fcstValue'))}, "
            f"{sql_string(raw_object_key)}, "
            f"{sql_string(raw_hash)}, "
            f"{sql_int(http_status)}, "
            f"{sql_string(metadata.get('result_code'))}, "
            f"{sql_string(metadata.get('result_msg'))}, "
            f"{sql_int(metadata.get('total_count'))}, "
            f"{sql_int(metadata.get('row_count'))}, "
            f"{sql_timestamp(collected_at)}, "
            f"{sql_string(load_date)}, "
            f"{sql_string(dag_run_id)}"
            ")"
        )

    cursor.execute(
        f"""
        INSERT INTO {qualified_table} (
            request_id,
            source_id,
            request_params_json,
            place_id,
            base_date,
            base_time,
            nx,
            ny,
            category,
            fcst_date,
            fcst_time,
            fcst_value,
            raw_object_key,
            payload_hash,
            http_status,
            result_code,
            result_msg,
            total_count,
            item_count,
            collected_at,
            load_date,
            dag_run_id
        )
        VALUES {", ".join(values)}
        """
    )
    return len(rows)


def verify_kma_bronze_runtime() -> int:
    cursor, catalog, schema = trino_cursor()
    qualified_table = f"{catalog}.{schema}.{BRONZE_TABLE}"
    cursor.execute(
        f"""
        SELECT
            count(*) AS row_count,
            count(DISTINCT raw_object_key) AS raw_object_count,
            max(collected_at) AS last_collected_at
        FROM {qualified_table}
        WHERE source_id = {sql_string(SOURCE_ID)}
        """
    )
    row = cursor.fetchone()
    print(
        "kma_vilage_fcst_bronze "
        f"row_count={row[0]} raw_object_count={row[1]} last_collected_at={row[2]}"
    )
    return int(row[0])
