"""서울 지하철 ELT (transit 도메인) — 수집 → R2 객체 적재(bronze 원본 + silver 변환) → Iceberg bronze(Trino).

sample(dbt_trino_iceberg_smoke) 패턴을 따른다:
  - 클래식 DAG + PythonOperator(ingest)
  - bronze 는 Trino INSERT 로 적재
차이: 소스가 고정 CSV 가 아니라 실시간 API(seoul_transit collector).

이 DAG = Bronze 한정(도메인 부트스트랩 + 지하철 적재). silver/gold/dbt 는 ASAC-DBT 별도 이슈.
동봉 패키지 seoul_transit 는 같은 폴더에 있고, Airflow 3.x 는 dags 하위 디렉터리를
sys.path 에 자동 추가하지 않으므로(plugins 마운트도 없음) 아래에서 직접 path 에 올려 import 한다.
"""

import json
import os
import re
import sys
from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# 동봉 패키지(seoul_transit)는 이 DAG 파일과 같은 폴더(dags/transit/)에 있다.
# Airflow 3.x 는 dags 하위 디렉터리를 sys.path 에 자동 추가하지 않고(plugins 마운트도 없음),
# 그래서 자기 폴더를 직접 path 에 올려 패키지를 import 한다.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from seoul_transit import config
from seoul_transit.r2_landing import land
from seoul_transit.subway import collect_subway_arrival, collect_subway_position, fetch_subway_raw

CATALOG = os.environ.get("TRINO_ICEBERG_CATALOG", "iceberg")
SCHEMA = os.environ.get("SMOKE_SCHEMA", "ops_smoke")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# R2 객체 적재 (raw) — 팀 <stage>/<domain>/<source> 규약
DOMAIN = os.environ.get("TRANSIT_DOMAIN", "transit")
SOURCE = os.environ.get("TRANSIT_SOURCE", "seoul_subway")

# (bronze 테이블, collector) 매핑
SOURCES = {
    "bronze_subway_arrival": ("subway_arrival", collect_subway_arrival),
    "bronze_subway_position": ("subway_position", collect_subway_position),
}


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


def _land_objects(dataset: str, key: str, records: list, run_id: str) -> None:
    """객체 메달리온 적재: bronze(원본 그대로) + silver(변환 envelope). 팀 <stage>/<domain> 규약."""
    # bronze: 원본 API 응답 그대로
    f = fetch_subway_raw(key, dataset)
    res_b = land(
        stage="bronze", domain=DOMAIN, source=SOURCE, dataset=dataset,
        pages=[json.dumps(f["raw"], ensure_ascii=False)],
        endpoint=f["endpoint"], kind=dataset, rows=f["rows"],
        run_id=run_id, request_params=f["request_params"], ext="json",
    )
    # silver: 변환값 = collect_* envelope (현재 우리가 보여주는 그대로), 한 줄=한 레코드
    res_s = land(
        stage="silver", domain=DOMAIN, source=SOURCE, dataset=dataset,
        pages=["\n".join(json.dumps(r, ensure_ascii=False) for r in records)],
        kind=dataset, rows=len(records), run_id=run_id, ext="jsonl",
    )
    print(f"object landed: bronze={res_b['manifest_key']} / silver={res_s['manifest_key']}")


def _load_bronze(table: str, records: list, dag_run_id: str) -> int:
    """envelope 레코드를 Iceberg bronze 로 적재 (원본 raw 는 JSON 문자열로 보존)."""
    import trino.dbapi

    cat, sch, tbl = sql_identifier(CATALOG), sql_identifier(SCHEMA), sql_identifier(table)
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


def ingest_subway() -> dict:
    key = config.load_key()
    dag_run_id = current_dag_run_id()
    counts = {}
    for table, (dataset, collector) in SOURCES.items():
        records = collector(key)
        # 1) 객체 메달리온 적재 (bronze 원본 + silver 변환) — records 재사용
        _land_objects(dataset, key, records, dag_run_id)
        # 2) Iceberg bronze 적재
        counts[dataset] = _load_bronze(table, records, dag_run_id)
    print(f"ingest counts: {counts}")
    return counts


with DAG(
    dag_id="transit_subway_elt",
    description="지하철 실시간 → R2 raw 랜딩 → Iceberg bronze(Trino). silver/gold 는 ASAC-DBT.",
    start_date=datetime(2026, 1, 1),
    schedule=config.schedule_for("subway", "*/5 * * * *"),
    catchup=False,
    max_active_runs=1,
    tags=["seoul", "transit", "subway", "ingest", "bronze", "trino", "iceberg"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_subway",
        python_callable=ingest_subway,
    )
