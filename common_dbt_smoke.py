import os
import shlex
import csv
import re
from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/elt_smoke")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
DBT_BIN = os.environ.get("DBT_BIN", "dbt")
DBT_TARGET = os.environ.get("DBT_TARGET", "prod")
SAMPLE_EVENTS_PATH = os.environ.get(
    "SAMPLE_EVENTS_PATH",
    os.path.join(DBT_PROJECT_DIR, "seeds", "sample_events.csv"),
)
BRONZE_TABLE = "bronze_sample_events"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def dbt_command(args: str) -> str:
    dbt_bin = shlex.quote(DBT_BIN)
    dbt_target = shlex.quote(DBT_TARGET)
    project_dir = shlex.quote(DBT_PROJECT_DIR)
    profiles_dir = shlex.quote(DBT_PROFILES_DIR)
    return (
        "set -euo pipefail\n"
        f"cd {project_dir}\n"
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


def r2_env(name: str) -> str:
    """R2 자격증명 — canonical ``R2_*`` 한 세트. 키 이름이 배포 환경을 담지 않는다.

    과거엔 dev 타깃일 때 ``R2_DEV_*`` 를 먼저 봤으나, 호스트 ENV2 개편에서 그 키가 사라졌다
    (Trino 카탈로그도 canonical 한 세트만 읽는다). 분기를 남겨 두면 누가 ``R2_DEV_*`` 를
    채우는 순간 같은 기록이 두 버킷으로 갈린다.
    """
    return required_env(name)


def trino_catalog() -> str:
    """canonical 키 하나 — 값이 배포 환경을 따라간다(미설정 시 기본만 타깃별)."""
    return os.environ.get("TRINO_ICEBERG_CATALOG") or (
        "iceberg_dev" if is_dev_target() else "iceberg")


def smoke_schema() -> str:
    """canonical 키 하나 — 값이 배포 환경을 따라간다(미설정 시 기본만 타깃별)."""
    return os.environ.get("SMOKE_SCHEMA") or (
        "dev_local" if is_dev_target() else "ops_smoke")


def sql_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_timestamp(value: str) -> str:
    # The sample fixture is deliberately controlled by this repository.
    datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return f"TIMESTAMP {sql_string(value)}"


def build_raw_object_key() -> str:
    """스모크 랜딩 키 — *_RAW_PREFIX env 는 존 루트만(raw / dev/<id>/raw), 도메인 이하는
    코드가 조립한다(ASK-Seoul#60: raw 첫 세그먼트=도메인 계약. 스모크 도메인 = ops_smoke)."""
    now = datetime.now(timezone.utc)
    load_date = now.strftime("%Y-%m-%d")
    ts_nodash = now.strftime("%Y%m%dT%H%M%S%f")
    # 존 루트도 canonical 키 하나(``R2_RAW_PREFIX``)로 받는다 — 환경은 값이 정한다.
    # 미설정 시 기본만 타깃에 따라 다르다(dev 스모크는 개인 샌드박스 아래로).
    raw_root = os.environ.get("R2_RAW_PREFIX") or (
        f"dev/{smoke_schema()}/raw" if is_dev_target() else "raw")
    return (
        f"{raw_root.rstrip('/')}/ops_smoke/sample_events"
        f"/load_date={load_date}/sample_events_{ts_nodash}.csv"
    )


def current_dag_run_id() -> str:
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "unknown")


def upload_raw_sample_events(sample_path: str, raw_object_key: str) -> str:
    import boto3

    bucket_name = r2_env("R2_BUCKET_NAME")
    endpoint_url = r2_env("R2_ENDPOINT")
    access_key_id = r2_env("R2_ACCESS_KEY_ID")
    secret_access_key = r2_env("R2_SECRET_ACCESS_KEY")

    if not os.path.exists(sample_path):
        raise FileNotFoundError(f"Sample source file not found: {sample_path}")

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
    )
    s3_client.upload_file(
        sample_path,
        bucket_name,
        raw_object_key,
        ExtraArgs={"ContentType": "text/csv"},
    )

    print(f"Uploaded sample events to R2 object: {raw_object_key}")
    return raw_object_key


def load_bronze_sample_events(sample_path: str, raw_object_key: str, dag_run_id: str) -> int:
    import trino.dbapi

    catalog = sql_identifier(trino_catalog())
    schema = sql_identifier(smoke_schema())
    table = sql_identifier(BRONZE_TABLE)
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.{table}"

    with open(sample_path, newline="", encoding="utf-8") as sample_file:
        rows = list(csv.DictReader(sample_file))

    if not rows:
        raise RuntimeError(f"No rows found in sample source file: {sample_path}")

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    values = []
    for row in rows:
        values.append(
            "("
            f"{sql_string(row['event_id'])}, "
            f"{sql_string(row['event_type'])}, "
            f"{sql_timestamp(row['event_ts'])}, "
            f"{sql_string(row['payload'])}, "
            f"{sql_string(raw_object_key)}, "
            f"TIMESTAMP {sql_string(ingested_at)}, "
            f"{sql_string(dag_run_id)}"
            ")"
        )

    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog,
        schema=schema,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    cursor = connection.cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            event_id varchar,
            event_type varchar,
            event_ts timestamp(6),
            payload varchar,
            raw_object_key varchar,
            ingested_at timestamp(6),
            dag_run_id varchar
        )
        WITH (
            format = 'PARQUET'
        )
        """
    )
    cursor.execute(
        f"""
        INSERT INTO {qualified_table} (
            event_id,
            event_type,
            event_ts,
            payload,
            raw_object_key,
            ingested_at,
            dag_run_id
        )
        VALUES {", ".join(values)}
        """
    )

    print(f"Inserted {len(rows)} rows into {qualified_table}")
    return len(rows)


def ingest_sample_events(sample_path: str) -> int:
    raw_object_key = build_raw_object_key()
    upload_raw_sample_events(sample_path, raw_object_key)
    return load_bronze_sample_events(sample_path, raw_object_key, current_dag_run_id())


with DAG(
    dag_id="common_dbt_smoke",
    description="Loads sample events into R2/Iceberg bronze, then validates dbt silver and gold models.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=2,
    tags=["dbt", "trino", "iceberg", "smoke"],
) as dag:
    ingest_events = PythonOperator(
        task_id="ingest_sample_events",
        python_callable=ingest_sample_events,
        op_kwargs={
            "sample_path": SAMPLE_EVENTS_PATH,
        },
    )

    run_medallion_models = BashOperator(
        task_id="run_medallion_models",
        bash_command=dbt_command("run --select silver_sample_events gold_event_type_metrics"),
    )

    test_medallion_models = BashOperator(
        task_id="test_medallion_models",
        bash_command=dbt_command("test"),
    )

    ingest_events >> run_medallion_models >> test_medallion_models
