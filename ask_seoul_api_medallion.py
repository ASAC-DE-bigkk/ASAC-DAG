import hashlib
import json
import os
import re
import shlex
import uuid
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/elt_smoke")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
DBT_BIN = os.environ.get("DBT_BIN", "dbt")
DBT_TARGET = os.environ.get("DBT_TARGET", "prod")
ASK_SEOUL_SCHEMA = os.environ.get("ASK_SEOUL_SCHEMA", "ask_seoul")

KMA_BASE_URL = os.environ.get(
    "KMA_BASE_URL",
    "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0",
)
SEOUL_OPEN_API_BASE_URL = os.environ.get(
    "SEOUL_OPEN_API_BASE_URL",
    "http://openapi.seoul.go.kr:8088",
)

KMA_BASE_TIMES = ["0200", "0500", "0800", "1100", "1400", "1700", "2000", "2300"]
KST = ZoneInfo("Asia/Seoul")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SOURCE_REGISTRY = {
    "kma_vilage_fcst": {
        "domain": "weather_forecast",
    },
    "seoul_traffic_incident": {
        "domain": "traffic_incident",
    },
}


def dbt_command(args: str) -> str:
    dbt_bin = shlex.quote(DBT_BIN)
    dbt_target = shlex.quote(DBT_TARGET)
    project_dir = shlex.quote(DBT_PROJECT_DIR)
    profiles_dir = shlex.quote(DBT_PROFILES_DIR)
    ask_seoul_schema = shlex.quote(sql_identifier(ASK_SEOUL_SCHEMA))
    return (
        "set -euo pipefail\n"
        f"cd {project_dir}\n"
        f"ASK_SEOUL_SCHEMA={ask_seoul_schema} "
        f"DBT_PROFILES_DIR={profiles_dir} "
        f"{dbt_bin} --no-use-colors --target {dbt_target} {args}"
    )


def is_dev_target() -> bool:
    return DBT_TARGET == "dev"


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
    return "TIMESTAMP " + sql_string(value.strftime("%Y-%m-%d %H:%M:%S.%f"))


def current_utc() -> datetime:
    return datetime.now(timezone.utc)


def payload_hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def build_load_prefix(source_id: str, collected_at: datetime) -> str:
    source_config = SOURCE_REGISTRY[source_id]
    load_date = collected_at.strftime("%Y-%m-%d")
    ts_nodash = collected_at.strftime("%Y%m%dT%H%M%S%f")
    if is_dev_target():
        default_prefix = f"dev/{ASK_SEOUL_SCHEMA}/raw"
        raw_prefix = os.environ.get("ASK_SEOUL_DEV_RAW_PREFIX", default_prefix)
    else:
        raw_prefix = os.environ.get("ASK_SEOUL_RAW_PREFIX", "raw")
    return (
        f"{raw_prefix.rstrip('/')}/domain={source_config['domain']}"
        f"/source_id={source_id}/load_date={load_date}/run_ts={ts_nodash}"
    )


def r2_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=r2_env("R2_ENDPOINT"),
        aws_access_key_id=r2_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=r2_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )


def upload_raw_object(raw_bytes: bytes, object_key: str, content_type: str) -> str:
    r2_client().put_object(
        Bucket=r2_env("R2_BUCKET_NAME"),
        Key=object_key,
        Body=raw_bytes,
        ContentType=content_type,
    )
    print(f"Uploaded raw object to R2: {object_key}")
    return object_key


def fetch_url(url: str) -> tuple[int, bytes]:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "ask-seoul-airflow-medallion-smoke/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.status, response.read()


def encode_service_key(service_key: str) -> str:
    if "%" in service_key:
        return service_key
    return urllib.parse.quote_plus(service_key)


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
        "numOfRows": os.environ.get("KMA_NUM_OF_ROWS", "1000"),
        "pageNo": os.environ.get("KMA_PAGE_NO", "1"),
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": str(nx),
        "ny": str(ny),
    }
    query = "serviceKey=" + encode_service_key(required_env("KMA_SERVICE_KEY"))
    query += "&" + urllib.parse.urlencode(params)
    return f"{KMA_BASE_URL.rstrip('/')}/getVilageFcst?{query}"


def parse_kma_response(raw_bytes: bytes) -> tuple[dict, list[dict]]:
    payload = json.loads(raw_bytes.decode("utf-8"))
    response = payload.get("response", {})
    header = response.get("header", {})
    body = response.get("body", {})

    result_code = str(header.get("resultCode", ""))
    result_msg = str(header.get("resultMsg", ""))
    if result_code != "00":
        raise RuntimeError(f"KMA API returned resultCode={result_code}, resultMsg={result_msg}")

    items_node = body.get("items", {})
    items = items_node.get("item", []) if isinstance(items_node, dict) else []
    if isinstance(items, dict):
        items = [items]
    if not isinstance(items, list):
        raise RuntimeError("KMA response body.items.item is not a list.")
    if not items:
        raise RuntimeError("KMA API returned no forecast items.")

    metadata = {
        "result_code": result_code,
        "result_msg": result_msg,
        "total_count": body.get("totalCount"),
        "item_count": len(items),
    }
    return metadata, items


def seoul_api_key() -> str:
    value = os.environ.get("SEOUL_OPEN_API_KEY")
    if value:
        return value
    if os.environ.get("SEOUL_OPEN_API_USE_SAMPLE", "").lower() == "true":
        return "sample"
    raise RuntimeError("Missing required environment variable: SEOUL_OPEN_API_KEY")


def build_seoul_acc_info_url(start_index: int, end_index: int) -> str:
    key = urllib.parse.quote(seoul_api_key(), safe="")
    return f"{SEOUL_OPEN_API_BASE_URL.rstrip('/')}/{key}/xml/AccInfo/{start_index}/{end_index}/"


def xml_text(element: ET.Element, name: str) -> str | None:
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
    code = xml_text(result, "CODE") if result is not None else None
    message = xml_text(result, "MESSAGE") if result is not None else None
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


def trino_cursor(schema: str):
    import trino.dbapi

    catalog = sql_identifier(trino_catalog())
    schema = sql_identifier(schema)
    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog,
        schema=schema,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return connection.cursor(), catalog, schema


def create_schema_if_needed(cursor, qualified_schema: str) -> None:
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
    except Exception as exc:
        if "Namespace already exists" in str(exc):
            return
        raise


def create_kma_bronze_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.bronze_kma_vilage_fcst"
    create_schema_if_needed(cursor, qualified_schema)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            request_id varchar,
            source_id varchar,
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
            dag_run_id varchar
        )
        WITH (
            format = 'PARQUET'
        )
        """
    )
    return qualified_table


def create_seoul_traffic_bronze_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.bronze_seoul_traffic_incident"
    create_schema_if_needed(cursor, qualified_schema)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            request_id varchar,
            source_id varchar,
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
            dag_run_id varchar
        )
        WITH (
            format = 'PARQUET'
        )
        """
    )
    return qualified_table


def insert_kma_bronze_rows(
    cursor,
    qualified_table: str,
    rows: list[dict],
    metadata: dict,
    request_id: str,
    place_id: str,
    raw_object_key: str,
    raw_hash: str,
    http_status: int,
    collected_at: datetime,
    dag_run_id: str,
) -> int:
    values = []
    for row in rows:
        values.append(
            "("
            f"{sql_string(request_id)}, "
            f"{sql_string('kma_vilage_fcst')}, "
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
            f"{sql_string(metadata['result_code'])}, "
            f"{sql_string(metadata['result_msg'])}, "
            f"{sql_int(metadata['total_count'])}, "
            f"{sql_int(metadata['item_count'])}, "
            f"{sql_timestamp(collected_at)}, "
            f"{sql_string(dag_run_id)}"
            ")"
        )

    cursor.execute(
        f"""
        INSERT INTO {qualified_table} (
            request_id,
            source_id,
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
            dag_run_id
        )
        VALUES {", ".join(values)}
        """
    )
    return len(rows)


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
    if not rows:
        return 0

    values = []
    for row in rows:
        values.append(
            "("
            f"{sql_string(request_id)}, "
            f"{sql_string('seoul_traffic_incident')}, "
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
            f"{sql_string(metadata['result_code'])}, "
            f"{sql_string(metadata['result_msg'])}, "
            f"{sql_int(metadata['list_total_count'])}, "
            f"{sql_int(metadata['row_count'])}, "
            f"{sql_timestamp(collected_at)}, "
            f"{sql_string(dag_run_id)}"
            ")"
        )

    cursor.execute(
        f"""
        INSERT INTO {qualified_table} (
            request_id,
            source_id,
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
            dag_run_id
        )
        VALUES {", ".join(values)}
        """
    )
    return len(rows)


def ingest_kma_vilage_fcst(**context) -> dict:
    collected_at = current_utc()
    request_id = str(uuid.uuid4())
    place_id = os.environ.get("ASK_SEOUL_KMA_PLACE_ID", "seoul_station")
    nx = int(os.environ.get("ASK_SEOUL_KMA_NX", "60"))
    ny = int(os.environ.get("ASK_SEOUL_KMA_NY", "127"))
    base_date, base_time = resolve_kma_base_datetime()

    url = build_kma_url(base_date=base_date, base_time=base_time, nx=nx, ny=ny)
    http_status, raw_bytes = fetch_url(url)
    metadata, rows = parse_kma_response(raw_bytes)
    raw_hash = payload_hash(raw_bytes)
    raw_object_key = (
        f"{build_load_prefix('kma_vilage_fcst', collected_at)}"
        f"/place_id={place_id}/base_date={base_date}/base_time={base_time}/{request_id}.json"
    )
    upload_raw_object(raw_bytes, raw_object_key, "application/json; charset=utf-8")

    cursor, catalog, schema = trino_cursor(ASK_SEOUL_SCHEMA)
    qualified_table = create_kma_bronze_table(cursor, catalog, schema)
    inserted = insert_kma_bronze_rows(
        cursor=cursor,
        qualified_table=qualified_table,
        rows=rows,
        metadata=metadata,
        request_id=request_id,
        place_id=place_id,
        raw_object_key=raw_object_key,
        raw_hash=raw_hash,
        http_status=http_status,
        collected_at=collected_at,
        dag_run_id=context["run_id"],
    )
    print(f"Inserted {inserted} KMA rows into {qualified_table}")
    return {"source_id": "kma_vilage_fcst", "inserted": inserted}


def ingest_seoul_traffic_incident(**context) -> dict:
    collected_at = current_utc()
    request_id = str(uuid.uuid4())
    start_index = int(os.environ.get("SEOUL_ACC_INFO_START_INDEX", "1"))
    end_index = int(os.environ.get("SEOUL_ACC_INFO_END_INDEX", "1000"))

    url = build_seoul_acc_info_url(start_index=start_index, end_index=end_index)
    http_status, raw_bytes = fetch_url(url)
    metadata, rows = parse_seoul_acc_info_response(raw_bytes)
    raw_hash = payload_hash(raw_bytes)
    raw_object_key = (
        f"{build_load_prefix('seoul_traffic_incident', collected_at)}"
        f"/service=AccInfo/start_index={start_index}/end_index={end_index}/{request_id}.xml"
    )
    upload_raw_object(raw_bytes, raw_object_key, "application/xml; charset=utf-8")

    cursor, catalog, schema = trino_cursor(ASK_SEOUL_SCHEMA)
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
    return {"source_id": "seoul_traffic_incident", "inserted": inserted}


def query_gold_ask_seoul_summary() -> int:
    cursor, catalog, schema = trino_cursor(ASK_SEOUL_SCHEMA)
    qualified_table = f"{catalog}.{schema}.gold_ask_seoul_api_ingestion_summary"
    cursor.execute(
        f"""
        SELECT
            source_id,
            row_count,
            raw_object_count,
            first_service_time,
            last_service_time,
            last_collected_at
        FROM {qualified_table}
        ORDER BY source_id
        """
    )
    rows = cursor.fetchall()
    if not rows:
        raise RuntimeError(f"No rows returned from {qualified_table}")

    for row in rows:
        print(
            "gold_ask_seoul_api_ingestion_summary "
            f"source_id={row[0]} row_count={row[1]} raw_object_count={row[2]} "
            f"first_service_time={row[3]} last_service_time={row[4]} "
            f"last_collected_at={row[5]}"
        )
    return len(rows)


with DAG(
    dag_id="ask_seoul_api_medallion",
    description="Loads KMA and Seoul traffic API data into R2/Iceberg bronze, then validates dbt silver/gold models.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ask_seoul", "dbt", "trino", "iceberg"],
) as dag:
    ingest_kma = PythonOperator(
        task_id="ingest_kma_vilage_fcst",
        python_callable=ingest_kma_vilage_fcst,
    )

    ingest_traffic = PythonOperator(
        task_id="ingest_seoul_traffic_incident",
        python_callable=ingest_seoul_traffic_incident,
    )

    run_ask_seoul_models = BashOperator(
        task_id="run_ask_seoul_models",
        bash_command=dbt_command(
            "run --select silver_kma_vilage_fcst "
            "silver_seoul_traffic_incident "
            "gold_ask_seoul_api_ingestion_summary"
        ),
    )

    test_ask_seoul_models = BashOperator(
        task_id="test_ask_seoul_models",
        bash_command=dbt_command(
            "test --select silver_kma_vilage_fcst "
            "silver_seoul_traffic_incident "
            "gold_ask_seoul_api_ingestion_summary "
            "assert_gold_ask_seoul_row_counts_positive"
        ),
    )

    query_gold_summary = PythonOperator(
        task_id="query_gold_ask_seoul_summary",
        python_callable=query_gold_ask_seoul_summary,
    )

    [ingest_kma, ingest_traffic] >> run_ask_seoul_models >> test_ask_seoul_models >> query_gold_summary
