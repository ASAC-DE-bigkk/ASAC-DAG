"""서울 공영주차 ELT (transit 도메인) — GetParkingInfo 실시간 점유 → R2 객체 + Iceberg bronze.

지하철과 동일 envelope 패턴(JSON):
  - 단일 호출(123개 전체) → R2 raw 객체(원본) + Iceberg bronze.
  - 이 DAG = Bronze 한정. silver/gold(변환·정제)는 ASAC-DBT 별도.
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# 동봉 패키지 import (Airflow 3.x 는 dags 하위폴더를 sys.path 에 자동 추가 안 함)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from seoul_transit import config
from seoul_transit.parking import collect_parking
from seoul_transit.r2_landing import land

CATALOG = os.environ.get("TRINO_ICEBERG_CATALOG", "iceberg")
SCHEMA = os.environ.get("SMOKE_SCHEMA", "ops_smoke")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

DOMAIN = os.environ.get("TRANSIT_DOMAIN", "transit")
SOURCE = os.environ.get("PARKING_SOURCE", "seoul_parking")
DATASET = "parking"
TABLE = "bronze_parking"


def sql_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def sql_str(value) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def current_dag_run_id() -> str:
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "unknown")


def _land_objects(res: dict, run_id: str) -> None:
    """bronze 객체 적재(원본 응답 JSON). 변환/정제는 ASAC-DBT silver — DAG 은 bronze 까지만."""
    res_b = land(
        stage="raw", domain=DOMAIN, source=SOURCE, dataset=DATASET,
        pages=[json.dumps(res["raw"], ensure_ascii=False)],
        endpoint=res["endpoint"], kind=DATASET, rows=res["rows"],
        run_id=run_id, request_params=res["request_params"], ext="json",
    )
    print(f"object landed [{DATASET}] rows={res['rows']}: bronze={res_b['manifest_key']}")


def _load_bronze(records: list, dag_run_id: str) -> int:
    """envelope 레코드를 Iceberg bronze 로 적재 (raw=주차장 행 JSON 문자열)."""
    import trino.dbapi

    cat, sch, tbl = sql_identifier(CATALOG), sql_identifier(SCHEMA), sql_identifier(TABLE)
    qualified = f"{cat}.{sch}.{tbl}"
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=CATALOG, schema=SCHEMA,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cat}.{sch}")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified} (
            source varchar,
            ts_source varchar,
            ts_collected varchar,
            lat varchar,
            lon varchar,
            raw varchar,
            ingested_at timestamp(6),
            dag_run_id varchar
        ) WITH (format = 'PARQUET')
        """
    )
    if not records:
        print(f"{qualified}: 0 rows (skip insert)")
        return 0

    values = []
    for e in records:
        values.append(
            "("
            f"{sql_str(e.get('source'))}, "
            f"{sql_str(e.get('ts_source'))}, "
            f"{sql_str(e.get('ts_collected'))}, "
            f"{sql_str(e.get('lat'))}, "
            f"{sql_str(e.get('lon'))}, "
            f"{sql_str(json.dumps(e.get('raw'), ensure_ascii=False))}, "
            f"TIMESTAMP {sql_str(ingested_at)}, "
            f"{sql_str(dag_run_id)}"
            ")"
        )
    cur.execute(
        f"INSERT INTO {qualified} (source, ts_source, ts_collected, lat, lon, raw, ingested_at, dag_run_id) "
        f"VALUES {', '.join(values)}"
    )
    print(f"{qualified}: inserted {len(records)} rows")
    return len(records)


def ingest_parking() -> dict:
    key = config.load_key()
    dag_run_id = current_dag_run_id()
    res = collect_parking(key)
    _land_objects(res, dag_run_id)
    n = _load_bronze(res["records"], dag_run_id)
    print(f"ingest counts: {{'parking': {n}}}")
    return {"parking": n}


with DAG(
    dag_id="transit_parking_bronze",
    description="서울 공영주차 실시간 점유(GetParkingInfo) → R2 객체 + Iceberg bronze. silver 는 ASAC-DBT.",
    start_date=datetime(2026, 1, 1),
    schedule=config.schedule_for("parking", "*/20 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["seoul", "transit", "parking", "ingest", "bronze", "trino", "iceberg"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_parking",
        python_callable=ingest_parking,
    )
