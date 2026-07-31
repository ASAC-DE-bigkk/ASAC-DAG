import hashlib
import os
import re
from datetime import datetime, timezone

from common.http import HttpCore, NoAuth
from traffic_ingest.errors import TrafficBronzeConfigurationError


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_TRINO_PORT = 8080
MIN_TCP_PORT = 1
MAX_TCP_PORT = 65535


def is_dev_target() -> bool:
    return (
        os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod"))
        == "dev"
    )


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise TrafficBronzeConfigurationError(
            f"Missing required environment variable: {name}"
        )
    return value


def r2_env_name(name: str) -> str:
    if is_dev_target():
        return "R2_DEV_" + name.removeprefix("R2_")
    return name


def r2_env(name: str) -> str:
    return required_env(r2_env_name(name))


def raw_prefix() -> str:
    if is_dev_target():
        return os.environ.get("ASK_SEOUL_DEV_RAW_PREFIX", "raw")
    return os.environ.get("ASK_SEOUL_RAW_PREFIX", "raw")


def checkpoint_prefix() -> str:
    """landing checkpoint 루트 — 기본 ops 존, `TRAFFIC_CHECKPOINT_PREFIX` 는 롤백용(#60 약속②).

    checkpoint 는 다음 실행의 재개 지점을 바꾸는 가변 상태라, 불변 박제 구역인
    raw 밖(ops/control)이 목적지다. 기본값이 곧 목적지이므로 배포만 하면 맞고,
    env 는 구 위치(`raw/_checkpoints`)로 되돌릴 때만 쓴다 — common(#573)·
    culture(#579)·recovery(#585)와 같은 방식.
    """
    configured = os.environ.get("TRAFFIC_CHECKPOINT_PREFIX", "").strip()
    if configured:
        return configured.rstrip("/")
    return "ops/control/checkpoints/traffic"


def trino_catalog() -> str:
    if is_dev_target():
        return os.environ.get("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    return os.environ.get("TRINO_ICEBERG_CATALOG", "iceberg")


def ask_seoul_schema() -> str:
    return os.environ.get("ASK_SEOUL_SCHEMA", "ask_seoul")


def trino_port() -> int:
    configured = os.environ.get("TRINO_PORT", str(DEFAULT_TRINO_PORT))
    try:
        port = int(configured)
    except (TypeError, ValueError) as exc:
        raise TrafficBronzeConfigurationError(
            f"TRINO_PORT must be an integer: {configured}"
        ) from exc
    if not MIN_TCP_PORT <= port <= MAX_TCP_PORT:
        raise TrafficBronzeConfigurationError(
            f"TRINO_PORT must be between {MIN_TCP_PORT} and {MAX_TCP_PORT}: {port}"
        )
    return port


def sql_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise TrafficBronzeConfigurationError(f"Unsafe SQL identifier: {value}")
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


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


# HTTP 호출 경계만 공통 클라이언트(#78)로 위임. max_attempts=1 — 기존 fetch_url 은
# 단발 호출이었고 재시도는 Airflow task retries 소관(HttpCore 재시도를 켜면 이중 재시도).
# rate_limit=None — 기존 코드에 호출 간 지연 없음(동작 보존 명시).
_HTTP = HttpCore(source="seoul_topis", timeout=30.0, max_attempts=1, rate_limit=None)


def fetch_url(url: str, user_agent: str) -> tuple[int, bytes]:
    # URL 은 호출측(acc_info)이 키까지 조립해 완성된 형태로 들어온다 → NoAuth.
    # 성공 기준은 core 기본(OK_2XX) — urlopen 이 2xx 만 반환하던 동작과 동일.
    response = _HTTP.get(url, headers={"User-Agent": user_agent}, auth=NoAuth())
    return response.status, response.content


def upload_raw_object(
    raw_bytes: bytes,
    object_key: str,
    content_type: str,
    log_label: str,
) -> str:
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
        ContentType=content_type,
    )
    print(f"Uploaded {log_label} to R2: {object_key}")
    return object_key


def download_raw_object(object_key: str, log_label: str) -> bytes:
    import boto3

    bucket_name = r2_env("R2_BUCKET_NAME")
    response = boto3.client(
        "s3",
        endpoint_url=r2_env("R2_ENDPOINT"),
        aws_access_key_id=r2_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=r2_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    ).get_object(Bucket=bucket_name, Key=object_key)
    raw_bytes = response["Body"].read()
    print(f"Downloaded {log_label} from R2: {object_key}")
    return raw_bytes


def trino_cursor():
    port = trino_port()
    import trino.dbapi

    catalog = sql_identifier(trino_catalog())
    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=port,
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
