import hashlib
import json
import os
import re
import uuid
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


KMA_BASE_URL = os.environ.get(
    "KMA_BASE_URL",
    "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0",
)
KMA_BASE_TIMES = ["0200", "0500", "0800", "1100", "1400", "1700", "2000", "2300"]
KST = ZoneInfo("Asia/Seoul")

SOURCE_ID = "kma_vilage_fcst"
SOURCE_DOMAIN = "weather_forecast"
BRONZE_TABLE = "bronze_kma_vilage_fcst"
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
    base_date: str,
    base_time: str,
) -> str:
    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    if is_dev_target():
        raw_prefix = os.environ.get("ASK_SEOUL_DEV_RAW_PREFIX", "bronze")
    else:
        raw_prefix = os.environ.get("ASK_SEOUL_RAW_PREFIX", "bronze")
    return (
        f"{raw_prefix.rstrip('/')}/{SOURCE_DOMAIN}/{SOURCE_ID}/load_date={load_date}/"
        f"{collected_at.astimezone(KST).strftime('%Y%m%dT%H%M%SKST')}"
        f"_base-{base_date}{base_time}_{request_id}.json"
    )


def upload_raw_object(raw_bytes: bytes, object_key: str) -> str:
    import boto3

    bucket_name = r2_env("R2_BUCKET_NAME")
    boto3.client(
        "s3",
        endpoint_url=r2_env("R2_ENDPOINT"),
        aws_access_key_id=r2_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=r2_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    ).put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=raw_bytes,
        ContentType="application/json; charset=utf-8",
    )
    print(f"Uploaded KMA raw payload to R2: {object_key}")
    return object_key


def fetch_url(url: str) -> tuple[int, bytes]:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "ask-seoul-kma-bronze/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.status, response.read()


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


def create_kma_bronze_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.{BRONZE_TABLE}"
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
    if not rows:
        raise RuntimeError("KMA API returned no forecast rows.")

    values = []
    for row in rows:
        values.append(
            "("
            f"{sql_string(request_id)}, "
            f"{sql_string(SOURCE_ID)}, "
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


def ingest_kma_vilage_fcst(**context) -> dict:
    collected_at = datetime.now(timezone.utc)
    request_id = str(uuid.uuid4())
    place_id = os.environ.get("ASK_SEOUL_KMA_PLACE_ID", "seoul_station")
    nx = int(os.environ.get("ASK_SEOUL_KMA_NX", "60"))
    ny = int(os.environ.get("ASK_SEOUL_KMA_NY", "127"))
    base_date, base_time = resolve_kma_base_datetime()

    url = build_kma_url(base_date=base_date, base_time=base_time, nx=nx, ny=ny)
    http_status, raw_bytes = fetch_url(url)
    metadata, rows = parse_kma_response(raw_bytes)
    raw_hash = hashlib.sha256(raw_bytes).hexdigest()
    raw_object_key = build_raw_object_key(
        collected_at=collected_at,
        request_id=request_id,
        base_date=base_date,
        base_time=base_time,
    )
    upload_raw_object(raw_bytes, raw_object_key)

    cursor, catalog, schema = trino_cursor()
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
    return {
        "source_id": SOURCE_ID,
        "raw_object_key": raw_object_key,
        "inserted": inserted,
    }


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


with DAG(
    dag_id="kma_vilage_fcst_bronze",
    description="Loads KMA getVilageFcst raw JSON into R2 and validates the Iceberg bronze runtime.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=os.environ.get("ASK_SEOUL_KMA_DAG_SCHEDULE") or None,
    catchup=False,
    max_active_runs=1,
    tags=["ask_seoul", "kma", "bronze", "r2", "iceberg"],
) as dag:
    ingest_kma = PythonOperator(
        task_id="ingest_kma_vilage_fcst",
        python_callable=ingest_kma_vilage_fcst,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
    )

    verify_bronze = PythonOperator(
        task_id="verify_kma_bronze_runtime",
        python_callable=verify_kma_bronze_runtime,
    )

    ingest_kma >> verify_bronze
