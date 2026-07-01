from datetime import datetime
from zoneinfo import ZoneInfo

from traffic_ingest.acc_info import SOURCE_ID, request_params_json
from traffic_ingest.common.runtime import (
    create_schema_if_needed,
    sql_int,
    sql_string,
    sql_timestamp,
    trino_cursor,
)


BRONZE_TABLE = "bronze_seoul_traffic_incident"
KST = ZoneInfo("Asia/Seoul")


def ensure_seoul_traffic_bronze_schema(cursor, qualified_table: str) -> None:
    for column_name, column_type in (
        ("request_params_json", "varchar"),
        ("load_date", "varchar"),
    ):
        cursor.execute(
            f"ALTER TABLE {qualified_table} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )


def create_seoul_traffic_bronze_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.{BRONZE_TABLE}"
    create_schema_if_needed(cursor, qualified_schema)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            request_id varchar,
            source_id varchar,
            request_params_json varchar,
            start_index integer,
            end_index integer,
            acc_id varchar,
            occr_date varchar,
            occr_time varchar,
            exp_clr_date varchar,
            exp_clr_time varchar,
            acc_type varchar,
            acc_dtype varchar,
            link_id varchar,
            grs80tm_x varchar,
            grs80tm_y varchar,
            acc_info varchar,
            acc_road_code varchar,
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
        WITH (
            format = 'PARQUET'
        )
        """
    )
    ensure_seoul_traffic_bronze_schema(cursor, qualified_table)
    return qualified_table


def insert_seoul_traffic_bronze_rows(
    cursor,
    qualified_table: str,
    rows: list[dict],
    metadata: dict,
    request_id: str,
    start_index: int,
    end_index: int,
    raw_object_key: str,
    raw_hash: str,
    http_status: int,
    collected_at: datetime,
    dag_run_id: str,
) -> int:
    rows_to_insert = rows or [{}]
    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    request_params = request_params_json(start_index, end_index)
    values = []
    for row in rows_to_insert:
        values.append(
            "("
            f"{sql_string(request_id)}, "
            f"{sql_string(SOURCE_ID)}, "
            f"{sql_string(request_params)}, "
            f"{sql_int(start_index)}, "
            f"{sql_int(end_index)}, "
            f"{sql_string(row.get('acc_id'))}, "
            f"{sql_string(row.get('occr_date'))}, "
            f"{sql_string(row.get('occr_time'))}, "
            f"{sql_string(row.get('exp_clr_date'))}, "
            f"{sql_string(row.get('exp_clr_time'))}, "
            f"{sql_string(row.get('acc_type'))}, "
            f"{sql_string(row.get('acc_dtype'))}, "
            f"{sql_string(row.get('link_id'))}, "
            f"{sql_string(row.get('grs80tm_x'))}, "
            f"{sql_string(row.get('grs80tm_y'))}, "
            f"{sql_string(row.get('acc_info'))}, "
            f"{sql_string(row.get('acc_road_code'))}, "
            f"{sql_string(raw_object_key)}, "
            f"{sql_string(raw_hash)}, "
            f"{sql_int(http_status)}, "
            f"{sql_string(metadata.get('result_code'))}, "
            f"{sql_string(metadata.get('result_msg'))}, "
            f"{sql_int(metadata.get('list_total_count'))}, "
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
            start_index,
            end_index,
            acc_id,
            occr_date,
            occr_time,
            exp_clr_date,
            exp_clr_time,
            acc_type,
            acc_dtype,
            link_id,
            grs80tm_x,
            grs80tm_y,
            acc_info,
            acc_road_code,
            raw_object_key,
            payload_hash,
            http_status,
            result_code,
            result_msg,
            list_total_count,
            row_count,
            collected_at,
            load_date,
            dag_run_id
        )
        VALUES {", ".join(values)}
        """
    )
    return len(rows)


def verify_seoul_traffic_bronze_runtime() -> int:
    cursor, catalog, schema = trino_cursor()
    qualified_table = f"{catalog}.{schema}.{BRONZE_TABLE}"
    cursor.execute(
        f"""
        SELECT
            count(*) AS table_rows,
            count(DISTINCT raw_object_key) AS raw_object_count,
            max(collected_at) AS last_collected_at
        FROM {qualified_table}
        WHERE source_id = {sql_string(SOURCE_ID)}
        """
    )
    row = cursor.fetchone()
    print(
        "seoul_traffic_incident_bronze "
        f"table_rows={row[0]} raw_object_count={row[1]} last_collected_at={row[2]}"
    )
    return int(row[0])
