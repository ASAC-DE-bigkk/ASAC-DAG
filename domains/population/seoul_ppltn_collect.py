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


SEOUL_OPEN_API_BASE_URL = os.environ.get(
    "SEOUL_OPEN_API_BASE_URL",
    "http://openapi.seoul.go.kr:8088",
)
KST = ZoneInfo("Asia/Seoul")

SOURCE_ID = "seoul_ppltn"
SOURCE_DOMAIN = "population"
BRONZE_TABLE = "bronze_seoul_ppltn"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# citydata_ppltn(서울시 실시간 도시데이터) 인구혼잡도 대상 장소(AREA_NM) 121곳.
# 장소명이 요청 URL에 그대로 박히므로 장소당 1회 호출(fan-out)한다.
AREAS = [
    "강남 MICE 관광특구",
    "동대문 관광특구",
    "명동 관광특구",
    "이태원 관광특구",
    "잠실 관광특구",
    "종로·청계 관광특구",
    "홍대 관광특구",
    "경복궁",
    "광화문·덕수궁",
    "보신각",
    "서울 암사동 유적",
    "창덕궁·종묘",
    "가산디지털단지역",
    "강남역",
    "건대입구역",
    "고덕역",
    "고속터미널역",
    "교대역",
    "구로디지털단지역",
    "구로역",
    "군자역",
    "대림역",
    "동대문역",
    "미아사거리역",
    "발산역",
    "사당역",
    "삼각지역",
    "서울대입구역",
    "서울역",
    "서울식물원·마곡나루역",
    "성신여대입구역",
    "선릉역",
    "신논현역·논현역",
    "수유역",
    "신도림역",
    "신림역",
    "신촌·이대역",
    "양재역",
    "역삼역",
    "연신내역",
    "오목교역·목동운동장",
    "왕십리역",
    "용산역",
    "이태원역",
    "잠실역",
    "장한평역",
    "천호역",
    "총신대입구(이수)역",
    "충정로역",
    "합정역",
    "혜화역",
    "홍대입구역(2호선)",
    "회기역",
    "가락시장",
    "가로수길",
    "광장(전통)시장",
    "김포공항",
    "노량진",
    "덕수궁길·정동길",
    "북촌한옥마을",
    "서촌",
    "성수카페거리",
    "쌍문동",
    "압구정로데오거리",
    "여의도",
    "연남동",
    "영등포 타임스퀘어",
    "용리단길",
    "이태원 앤틱가구거리",
    "인사동",
    "장지역",
    "창동 신경제 중심지",
    "청담동 명품거리",
    "청량리 제기동 일대 전통시장",
    "해방촌·경리단길",
    "DDP(동대문디자인플라자)",
    "DMC(디지털미디어시티)",
    "강서한강공원",
    "고척돔",
    "광나루한강공원",
    "광화문광장",
    "국립중앙박물관·용산가족공원",
    "난지한강공원",
    "남산공원",
    "노들섬",
    "뚝섬역",
    "뚝섬한강공원",
    "망원한강공원",
    "반포한강공원",
    "북서울꿈의숲",
    "서리풀공원·몽마르뜨공원",
    "서울대공원",
    "서울숲공원",
    "아차산",
    "양화한강공원",
    "어린이대공원",
    "여의도한강공원",
    "월드컵공원",
    "응봉산",
    "이촌한강공원",
    "잠실종합운동장",
    "잠실한강공원",
    "잠원한강공원",
    "청계산",
    "북창동 먹자골목",
    "남대문시장",
    "익선동",
    "신정네거리역",
    "잠실새내역",
    "잠실(송파나루역)",
    "송리단길·호수단길",
    "신촌 스타광장",
    "보라매공원",
    "서대문독립공원",
    "안양천",
    "여의서로",
    "올림픽공원",
    "홍제폭포",
    "용현녹지공원",
    "시의회",
    "숭례문",
]

# bronze 테이블에 적재할 citydata_ppltn 필드(원천 그대로 varchar로 보존).
PPLTN_FIELDS = [
    "AREA_NM",
    "AREA_CD",
    "AREA_CONGEST_LVL",
    "AREA_CONGEST_MSG",
    "AREA_PPLTN_MIN",
    "AREA_PPLTN_MAX",
    "MALE_PPLTN_RATE",
    "FEMALE_PPLTN_RATE",
    "PPLTN_RATE_0",
    "PPLTN_RATE_10",
    "PPLTN_RATE_20",
    "PPLTN_RATE_30",
    "PPLTN_RATE_40",
    "PPLTN_RATE_50",
    "PPLTN_RATE_60",
    "PPLTN_RATE_70",
    "RESNT_PPLTN_RATE",
    "NON_RESNT_PPLTN_RATE",
    "REPLACE_YN",
    "PPLTN_TIME",
    "FCST_YN",
]


def is_dev_target() -> bool:
    return os.environ.get("DBT_TARGET", "prod") == "dev"


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


def seoul_ppltn_schema() -> str:
    return os.environ.get("SEOUL_PPLTN_SCHEMA", "seoul_ppltn")


def sql_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def sql_string(value: object) -> str:
    if value is None:
        return "NULL"
    text = str(value).strip()
    if text == "":
        return "NULL"
    return "'" + text.replace("'", "''") + "'"


def sql_int(value: object) -> str:
    if value is None or value == "":
        return "NULL"
    return str(int(value))


def sql_timestamp(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return "TIMESTAMP " + sql_string(utc_value.strftime("%Y-%m-%d %H:%M:%S.%f"))


def raw_prefix() -> str:
    if is_dev_target():
        return os.environ.get("SEOUL_PPLTN_DEV_RAW_PREFIX", "bronze")
    return os.environ.get("SEOUL_PPLTN_RAW_PREFIX", "bronze")


def build_raw_object_key(area_name: str, collected_at: datetime) -> str:
    # seoul-dev 버킷 기준: bronze/population/<날짜>/<시>/<분>/<장소>.json (KST)
    kst = collected_at.astimezone(KST)
    return (
        f"{raw_prefix().rstrip('/')}/{SOURCE_DOMAIN}/"
        f"{kst.strftime('%Y-%m-%d')}/{kst.strftime('%H')}/{kst.strftime('%M')}/"
        f"{area_name}.json"
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
        ContentType="application/json; charset=utf-8",
    )
    return object_key


def fetch_url(url: str) -> tuple[int, bytes]:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "ask-seoul-ppltn-bronze/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.status, response.read()


def build_seoul_ppltn_url(area_name: str) -> str:
    api_key = urllib.parse.quote(required_env("SEOUL_API_KEY"), safe="")
    area = urllib.parse.quote(area_name, safe="")
    return f"{SEOUL_OPEN_API_BASE_URL.rstrip('/')}/{api_key}/json/citydata_ppltn/1/5/{area}"


def parse_seoul_ppltn_response(raw_bytes: bytes) -> tuple[dict, dict]:
    data = json.loads(raw_bytes)

    # 정상 응답: {"SeoulRtd.citydata_ppltn": [ { AREA_NM, ... } ]}
    rows = data.get("SeoulRtd.citydata_ppltn")
    result = data.get("RESULT") or {}
    code = result.get("RESULT.CODE") or result.get("CODE")
    message = result.get("RESULT.MESSAGE") or result.get("MESSAGE")

    if not isinstance(rows, list) or len(rows) == 0:
        raise RuntimeError(
            f"Seoul citydata_ppltn returned no rows (resultCode={code}, resultMsg={message})"
        )

    row = {field: rows[0].get(field) for field in PPLTN_FIELDS}
    metadata = {"result_code": code, "result_msg": message}
    return metadata, row


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
    return connection.cursor(), catalog, sql_identifier(seoul_ppltn_schema())


def create_schema_if_needed(cursor, qualified_schema: str) -> None:
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
    except Exception as exc:
        if "Namespace already exists" not in str(exc):
            raise


def create_seoul_ppltn_bronze_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.{BRONZE_TABLE}"
    create_schema_if_needed(cursor, qualified_schema)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            request_id varchar,
            source_id varchar,
            requested_area_nm varchar,
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
            raw_object_key varchar,
            payload_hash varchar,
            http_status integer,
            result_code varchar,
            result_msg varchar,
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


def build_bronze_row_values(
    area_name: str,
    row: dict,
    metadata: dict,
    request_id: str,
    raw_object_key: str,
    raw_hash: str,
    http_status: int,
    collected_at: datetime,
    dag_run_id: str,
) -> str:
    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    return (
        "("
        f"{sql_string(request_id)}, "
        f"{sql_string(SOURCE_ID)}, "
        f"{sql_string(area_name)}, "
        f"{sql_string(row.get('AREA_NM'))}, "
        f"{sql_string(row.get('AREA_CD'))}, "
        f"{sql_string(row.get('AREA_CONGEST_LVL'))}, "
        f"{sql_string(row.get('AREA_CONGEST_MSG'))}, "
        f"{sql_string(row.get('AREA_PPLTN_MIN'))}, "
        f"{sql_string(row.get('AREA_PPLTN_MAX'))}, "
        f"{sql_string(row.get('MALE_PPLTN_RATE'))}, "
        f"{sql_string(row.get('FEMALE_PPLTN_RATE'))}, "
        f"{sql_string(row.get('PPLTN_RATE_0'))}, "
        f"{sql_string(row.get('PPLTN_RATE_10'))}, "
        f"{sql_string(row.get('PPLTN_RATE_20'))}, "
        f"{sql_string(row.get('PPLTN_RATE_30'))}, "
        f"{sql_string(row.get('PPLTN_RATE_40'))}, "
        f"{sql_string(row.get('PPLTN_RATE_50'))}, "
        f"{sql_string(row.get('PPLTN_RATE_60'))}, "
        f"{sql_string(row.get('PPLTN_RATE_70'))}, "
        f"{sql_string(row.get('RESNT_PPLTN_RATE'))}, "
        f"{sql_string(row.get('NON_RESNT_PPLTN_RATE'))}, "
        f"{sql_string(row.get('REPLACE_YN'))}, "
        f"{sql_string(row.get('PPLTN_TIME'))}, "
        f"{sql_string(row.get('FCST_YN'))}, "
        f"{sql_string(raw_object_key)}, "
        f"{sql_string(raw_hash)}, "
        f"{sql_int(http_status)}, "
        f"{sql_string(metadata.get('result_code'))}, "
        f"{sql_string(metadata.get('result_msg'))}, "
        f"{sql_timestamp(collected_at)}, "
        f"{sql_string(load_date)}, "
        f"{sql_string(dag_run_id)}"
        ")"
    )


def insert_seoul_ppltn_bronze_rows(cursor, qualified_table: str, values: list[str]) -> None:
    if not values:
        return
    cursor.execute(
        f"""
        INSERT INTO {qualified_table} (
            request_id,
            source_id,
            requested_area_nm,
            area_nm,
            area_cd,
            area_congest_lvl,
            area_congest_msg,
            area_ppltn_min,
            area_ppltn_max,
            male_ppltn_rate,
            female_ppltn_rate,
            ppltn_rate_0,
            ppltn_rate_10,
            ppltn_rate_20,
            ppltn_rate_30,
            ppltn_rate_40,
            ppltn_rate_50,
            ppltn_rate_60,
            ppltn_rate_70,
            resnt_ppltn_rate,
            non_resnt_ppltn_rate,
            replace_yn,
            ppltn_time,
            fcst_yn,
            raw_object_key,
            payload_hash,
            http_status,
            result_code,
            result_msg,
            collected_at,
            load_date,
            dag_run_id
        )
        VALUES {", ".join(values)}
        """
    )


def ingest_seoul_ppltn(**context) -> dict:
    collected_at = datetime.now(timezone.utc)
    dag_run_id = context["run_id"]

    cursor, catalog, schema = trino_cursor()
    qualified_table = create_seoul_ppltn_bronze_table(cursor, catalog, schema)

    values: list[str] = []
    failures: list[dict] = []

    for area_name in AREAS:
        request_id = str(uuid.uuid4())
        try:
            url = build_seoul_ppltn_url(area_name)
            http_status, raw_bytes = fetch_url(url)
            metadata, row = parse_seoul_ppltn_response(raw_bytes)
            raw_hash = hashlib.sha256(raw_bytes).hexdigest()
            raw_object_key = build_raw_object_key(area_name, collected_at)
            upload_raw_object(raw_bytes, raw_object_key)
            values.append(
                build_bronze_row_values(
                    area_name=area_name,
                    row=row,
                    metadata=metadata,
                    request_id=request_id,
                    raw_object_key=raw_object_key,
                    raw_hash=raw_hash,
                    http_status=http_status,
                    collected_at=collected_at,
                    dag_run_id=dag_run_id,
                )
            )
        except Exception as exc:
            failures.append({"area_nm": area_name, "error": str(exc)})

    insert_seoul_ppltn_bronze_rows(cursor, qualified_table, values)

    for failure in failures:
        print(f"collect failed for {failure['area_nm']}: {failure['error']}")
    print(
        f"seoul_ppltn collected {len(values)}/{len(AREAS)} areas into {qualified_table} "
        f"(failed={len(failures)})"
    )

    if not values:
        raise RuntimeError(f"all {len(AREAS)} areas failed to collect")

    return {
        "source_id": SOURCE_ID,
        "collected": len(values),
        "failed": len(failures),
    }


def verify_seoul_ppltn_bronze_runtime() -> int:
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
        "seoul_ppltn_bronze "
        f"table_rows={row[0]} raw_object_count={row[1]} last_collected_at={row[2]}"
    )
    return int(row[0])


with DAG(
    dag_id="seoul_ppltn_collect",
    description="Collects Seoul citydata_ppltn for 118 areas into R2 raw + Iceberg bronze.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ask_seoul", "population", "bronze", "r2", "iceberg"],
) as dag:
    ingest_ppltn = PythonOperator(
        task_id="ingest_seoul_ppltn",
        python_callable=ingest_seoul_ppltn,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
    )

    verify_bronze = PythonOperator(
        task_id="verify_seoul_ppltn_bronze_runtime",
        python_callable=verify_seoul_ppltn_bronze_runtime,
    )

    ingest_ppltn >> verify_bronze
