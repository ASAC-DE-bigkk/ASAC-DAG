import hashlib
import json
import os
import re
import uuid
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


SEOUL_OPEN_API_BASE_URL = os.environ.get(
    "SEOUL_OPEN_API_BASE_URL",
    "http://openapi.seoul.go.kr:8088",
)
KST = ZoneInfo("Asia/Seoul")

SOURCE_ID = "seoul_traffic_incident"
SOURCE_DOMAIN = "traffic_incident"
BRONZE_TABLE = "bronze_seoul_traffic_incident"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def is_dev_target() -> bool:
    return os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def r2_env_name(name: str) -> str:
    if is_dev_target():
        dev_name = "R2_DEV_" + name.removeprefix("R2_")
        if os.environ.get(dev_name):
            return dev_name
    return name


def r2_env(name: str) -> str:
    return required_env(r2_env_name(name))


def trino_catalog() -> str:
    if is_dev_target():
        return os.environ.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    return os.environ.get("TRINO_ICEBERG_CATALOG", "iceberg")


def ask_seoul_schema() -> str:
    return os.environ.get("ASK_SEOUL_SCHEMA", "ask_seoul")


def sql_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def sql_string(value: object) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_int(value: object) -> str:
    if value is None or value == "":
        return "NULL"
    return str(int(value))


def sql_timestamp(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return "TIMESTAMP " + sql_string(utc_value.strftime("%Y-%m-%d %H:%M:%S.%f"))


def build_raw_object_key(
    collected_at: datetime,
    request_id: str,
    start_index: int,
    end_index: int,
) -> str:
    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    if is_dev_target():
        raw_prefix = os.environ.get("ASK_SEOUL_DEV_RAW_PREFIX", "bronze")
    else:
        raw_prefix = os.environ.get("ASK_SEOUL_RAW_PREFIX", "bronze")
    return (
        f"{raw_prefix.rstrip('/')}/{SOURCE_DOMAIN}/{SOURCE_ID}/load_date={load_date}/"
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


def upload_raw_object(raw_bytes: bytes, object_key: str) -> str:
    import boto3

    boto3.client(
        "s3",
        endpoint_url=r2_env("R2_ENDPOINT"),
        aws_access_key_id=r2_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=r2_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    ).put_object(
        Bucket=r2_env("R2_BUCKET_NAME"),
        Key=object_key,
        Body=raw_bytes,
        ContentType="application/xml; charset=utf-8",
    )
    print(f"Uploaded Seoul traffic raw payload to R2: {object_key}")
    return object_key


def fetch_url(url: str) -> tuple[int, bytes]:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "ask-seoul-traffic-bronze/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.status, response.read()


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


def trino_cursor():
    import trino.dbapi

    catalog = sql_identifier(trino_catalog())
    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return connection.cursor(), catalog, sql_identifier(ask_seoul_schema())


def create_schema_if_needed(cursor, qualified_schema: str) -> None:
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
    except Exception as exc:
        if "Namespace already exists" not in str(exc):
            raise


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


def ingest_seoul_traffic_incident(**context) -> dict:
    collected_at = datetime.now(timezone.utc)
    request_id = str(uuid.uuid4())
    start_index = int(os.environ.get("SEOUL_ACC_INFO_START_INDEX", "1"))
    end_index = int(os.environ.get("SEOUL_ACC_INFO_END_INDEX", "1000"))

    url = build_seoul_acc_info_url(start_index=start_index, end_index=end_index)
    http_status, raw_bytes = fetch_url(url)
    metadata, rows = parse_seoul_acc_info_response(raw_bytes)
    raw_hash = hashlib.sha256(raw_bytes).hexdigest()
    raw_object_key = build_raw_object_key(
        collected_at=collected_at,
        request_id=request_id,
        start_index=start_index,
        end_index=end_index,
    )
    upload_raw_object(raw_bytes, raw_object_key)

    cursor, catalog, schema = trino_cursor()
    qualified_table = create_seoul_traffic_bronze_table(cursor, catalog, schema)
    inserted = insert_seoul_traffic_bronze_rows(
        cursor=cursor,
        qualified_table=qualified_table,
        rows=rows,
        metadata=metadata,
        request_id=request_id,
        start_index=start_index,
        end_index=end_index,
        raw_object_key=raw_object_key,
        raw_hash=raw_hash,
        http_status=http_status,
        collected_at=collected_at,
        dag_run_id=context["run_id"],
    )
    print(f"Inserted {inserted} Seoul traffic rows into {qualified_table}")
    return {
        "source_id": SOURCE_ID,
        "raw_object_key": raw_object_key,
        "inserted": inserted,
    }


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


with DAG(
    dag_id="seoul_traffic_incident_bronze",
    description="Loads Seoul TOPIS AccInfo XML into R2 and validates the Iceberg bronze runtime.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=os.environ.get("ASK_SEOUL_TRAFFIC_DAG_SCHEDULE") or None,
    catchup=False,
    max_active_runs=1,
    tags=["ask_seoul", "traffic", "bronze", "r2", "iceberg"],
) as dag:
    ingest_traffic = PythonOperator(
        task_id="ingest_seoul_traffic_incident",
        python_callable=ingest_seoul_traffic_incident,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
    )

    verify_bronze = PythonOperator(
        task_id="verify_seoul_traffic_bronze_runtime",
        python_callable=verify_seoul_traffic_bronze_runtime,
    )

    ingest_traffic >> verify_bronze
