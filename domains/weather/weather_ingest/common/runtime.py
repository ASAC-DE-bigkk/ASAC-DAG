import hashlib
import os
import re
import urllib.parse
import time
from datetime import datetime, timezone

from common.errors import types as error_types
from common.http import HttpCore, NoAuth, OK_2XX, QueryKey
from common.http.errors import HttpProblemError


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


def raw_prefix() -> str:
    if is_dev_target():
        return os.environ.get("ASK_SEOUL_DEV_RAW_PREFIX", "raw")
    return os.environ.get("ASK_SEOUL_RAW_PREFIX", "raw")


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


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


_HTTP = HttpCore(source="weather_kma", timeout=30.0, max_attempts=1, rate_limit=None)


def http_retry_delay(response_headers: dict[str, str] | None, attempt: int, base_delay_seconds: float) -> float:
    for key, value in (response_headers or {}).items():
        if key.lower() == "retry-after":
            try:
                return max(0.0, float(value))
            except ValueError:
                break
    return min(base_delay_seconds * (2 ** (attempt - 1)), 300.0)


def _redacted_request_url(url: str, params: dict[str, str] | None) -> str:
    if not params:
        return url
    return f"{url}?{urllib.parse.urlencode(params, safe='%')}"


def fetch_url(
    url: str,
    user_agent: str,
    *,
    max_attempts: int = 1,
    retry_statuses: tuple[int, ...] = (),
    retry_base_delay_seconds: float = 1.0,
) -> tuple[int, bytes]:
    retry_codes = set(retry_statuses)
    auth = QueryKey("serviceKey", required_env("KMA_SERVICE_KEY"))
    for attempt in range(1, max_attempts + 1):
        prepared = auth.apply(
            url,
            None,
            {"User-Agent": user_agent},
        )
        try:
            response = _HTTP.get(
                prepared.url,
                params=prepared.params,
                headers=prepared.headers,
                auth=NoAuth(),
                expected_status=tuple(range(200, 600)),
            )
        except HttpProblemError:
            raise

        if response.status in OK_2XX:
            return response.status, response.content
        if response.status not in retry_codes:
            break
        if attempt >= max_attempts:
            raise HttpProblemError(
                error_types.HTTP_ERROR,
                method="GET",
                url=_redacted_request_url(prepared.url, prepared.params),
                status=response.status,
                detail=f"status={response.status}",
                attempts=attempt,
                source_system="weather_kma",
            )

        delay = http_retry_delay(response.headers, attempt, retry_base_delay_seconds)
        print(
            f"Source API HTTP {response.status}; retrying in {delay:.1f}s "
            f"(attempt {attempt + 1}/{max_attempts})"
        )
        time.sleep(delay)
    raise HttpProblemError(
        error_types.HTTP_ERROR,
        method="GET",
        url=_redacted_request_url(prepared.url, prepared.params),
        status=response.status,
        detail=f"status={response.status}",
        attempts=max_attempts,
        source_system="weather_kma",
    )


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
