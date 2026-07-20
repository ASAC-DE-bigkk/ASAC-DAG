"""transit 보존 정책 집행 DAG (#369 4단계) — 실시간 데이터 **주 단위(월~일 KST)** 보존.

이번 주(월요일 00:00 KST 이후) 데이터만 유지 — @daily 로 돌지만 실제 삭제는 주가
바뀐 뒤(월요일 런)에 지난주 분이 한꺼번에 나간다 (그 외 요일은 위생 점검 성격).
  1. purge_r2_raw    : 실시간 dataset raw(ingest_ts < 주 경계) + 만료 pending 마커
  2. purge_bronze    : bronze DELETE(주 경계) → optimize → expire_snapshots/remove_orphan(7d)

전제(충족 확인됨): dbt silver·gold 는 incremental — bronze 를 잘라도 이력이 안 잘린다.
⚠️ 삭제된 실시간 원본은 복구 불가 — 주 내 미적재분(방치된 pending)은 영구 소실이므로
   만료 pending 마커 발견 시 Discord WARN 으로 가시화한다.
⚠️ 월요일 00시 직후에는 직전 주 원본이 사라지므로 R2 재적재식 복구 윈도우도 함께 리셋된다.

순수 로직(판정·SQL)은 seoul_transit.maintenance — 이 파일은 오케스트레이션만.
"""

import logging
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# 동봉 패키지 import (Airflow 3.x 는 dags 하위폴더를 sys.path 에 자동 추가 안 함)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.discord import COLOR_WARN, send_embed
from common.errors.airflow import problem_failure_callback
from common.runmetrics import track

from seoul_transit import config, loader, maintenance
from seoul_transit.r2_landing import delete_keys, list_keys

LOGGER = logging.getLogger(__name__)

# dev 게이트(#369 리뷰) — transit_master_bronze 와 동일 규약. 파괴적 DELETE 가
# TRINO_ICEBERG_CATALOG 하나에 의존하지 않게 한다.
CATALOG = loader.trino_catalog()
SCHEMA = loader.transit_schema()
DOMAIN = config.TRANSIT_DOMAIN

record_transit_problem = problem_failure_callback(domain=DOMAIN, source_system="maintenance")


ARCHIVE_TABLE = os.environ.get("TRANSIT_ARCHIVE_TABLE", "gold_transit_dong_15min")


def assert_archive_caught_up() -> dict:
    """purge 선행 게이트 — gold 아카이브가 삭제 대상 구간을 이미 소비했는지 확인한다.

    원본(R2 raw·bronze)은 주 경계로 지워지고 gold 아카이브가 유일한 장기 저장소라(#286),
    변환이 밀린 상태로 purge 가 돌면 그 주 데이터는 어디에도 남지 않는다. 그래서
    "아카이브 최신 버킷 >= 이번 주 월요일 00:00 KST" 를 만족할 때만 downstream 을 진행한다.

    실패가 아니라 **skip** 으로 막는다: 변환이 늦은 것 자체는 이 DAG 의 잘못이 아니고,
    다음 @daily 런에서 자동으로 재평가된다. 삭제는 되돌릴 수 없으므로 "확신 없으면 안 지운다".
    아카이브 테이블이 아직 없거나 비어 있어도(개시 직후) 같은 이유로 막는다.
    """
    from airflow.exceptions import AirflowSkipException
    import trino.dbapi

    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=CATALOG, schema=SCHEMA,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    cur = conn.cursor()
    cat, sch = maintenance.sql_identifier(CATALOG), maintenance.sql_identifier(SCHEMA)
    cur.execute(f"SELECT table_name FROM {cat}.information_schema.tables "
                f"WHERE table_schema = '{SCHEMA}' AND table_name = '{ARCHIVE_TABLE}'")
    if not cur.fetchall():
        raise AirflowSkipException(
            f"아카이브 테이블 {ARCHIVE_TABLE} 없음 — 변환 미수행 상태로 판단해 purge 보류"
        )

    qualified = f"{cat}.{sch}.{maintenance.sql_identifier(ARCHIVE_TABLE)}"
    cur.execute(f"SELECT cast(max(bucket_at) as varchar) FROM {qualified}")
    row = cur.fetchone()
    max_bucket_at = row[0] if row else None
    required = maintenance.archive_watermark_required()

    if not maintenance.is_archive_caught_up(max_bucket_at):
        raise AirflowSkipException(
            f"아카이브 미도달 — {ARCHIVE_TABLE}.max(bucket_at)={max_bucket_at or '없음'} "
            f"< 요구 {required} (KST). 변환이 이 구간을 집계한 뒤 purge 한다."
        )

    print(f"archive caught up: max(bucket_at)={max_bucket_at} >= {required} (KST) — purge 진행")
    return {"max_bucket_at": max_bucket_at, "required": required}


def purge_r2_raw() -> dict:
    """실시간 dataset raw(ingest_ts < 이번 주 월요일 00:00 KST) + 만료 pending 마커 삭제.

    나열은 r2_landing.list_keys(페이지네이션 내장) 재사용 — 단발 list_objects_v2
    (1,000키 캡)로 loader 장기 장애 시 초과분을 놓치던 결함 해소(#369 리뷰).
    """
    cutoff = maintenance.cutoff_ingest_ts()
    summary: dict[str, int] = {}

    for dataset in sorted(maintenance.SOURCE_BY_DATASET):
        prefix = maintenance.raw_prefix(dataset, DOMAIN)
        expired = [k for k in list_keys(prefix) if maintenance.is_expired_key(k, cutoff)]
        summary[dataset] = delete_keys(expired)

    # 만료 pending 마커 — raw 가 지워져 영원히 적재 불가 = loader 실패 방치 신호.
    stale = [
        k for k in list_keys(config.LOADER_PENDING_PREFIX)
        if maintenance.is_stale_pending(k, cutoff)
    ]
    if stale:
        send_embed(
            title=f"⚠️ transit 보존창(주 단위) 초과 pending 마커 {len(stale)}건 제거",
            description=(
                "loader 가 해당 주 안에 적재하지 못한 마커를 제거했습니다 — "
                f"해당 구간 실시간 데이터는 **영구 소실**입니다.\n"
                f"예: `{stale[0]}`"
            ),
            color=COLOR_WARN, domain=DOMAIN,
        )
        summary["stale_pending"] = delete_keys(stale)

    print(f"purge_r2_raw cutoff<{cutoff}: {summary}")
    return summary


def purge_bronze() -> dict:
    """bronze 실시간 테이블 보존 집행 — 주 경계 DELETE + optimize + 스냅샷 회수(7d)."""
    import trino.dbapi

    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=CATALOG, schema=SCHEMA,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    cur = conn.cursor()
    cat, sch = maintenance.sql_identifier(CATALOG), maintenance.sql_identifier(SCHEMA)
    cur.execute(f"SELECT table_name FROM {cat}.information_schema.tables "
                f"WHERE table_schema = '{SCHEMA}'")
    existing = {r[0] for r in cur.fetchall()}

    cutoff = maintenance.cutoff_bronze_ts()
    summary: dict[str, int] = {}
    for table in maintenance.bronze_tables():
        if table not in existing:
            continue  # 수집 제외 dataset(#212)의 테이블이 아직 없으면 건너뜀
        qualified = f"{cat}.{sch}.{maintenance.sql_identifier(table)}"
        for stmt in maintenance.purge_sql(qualified, cutoff):
            cur.execute(stmt)
        cur.execute(f"SELECT count(*) FROM {qualified}")
        summary[table] = cur.fetchone()[0]

    print(f"purge_bronze cutoff<{cutoff}Z 잔여행: {summary}")
    return summary


with DAG(
    dag_id="transit_maintenance",
    description="transit 실시간 데이터 주 단위(월~일 KST) 보존 집행 — 다음 주 시작 시 "
                "지난주 R2 raw + Iceberg bronze 삭제(DELETE·optimize·expire_snapshots). 마스터 제외.",
    start_date=datetime(2026, 1, 1),
    schedule=config.schedule_for("transit_maintenance", "@daily"),
    catchup=False,
    max_active_runs=1,
    tags=["seoul", "transit", "maintenance", "retention", "trino", "iceberg"],
) as dag:
    # purge 선행 게이트: 아카이브가 삭제 구간을 소비했는지 확인(미도달이면 skip).
    archive_gate = PythonOperator(
        task_id="assert_archive_caught_up",
        python_callable=assert_archive_caught_up,
        on_failure_callback=record_transit_problem,
    )
    purge_r2 = PythonOperator(
        task_id="purge_r2_raw",
        python_callable=track(layer="bronze", domain="transit")(purge_r2_raw),
        on_failure_callback=record_transit_problem,
    )
    purge_tables = PythonOperator(
        task_id="purge_bronze",
        python_callable=track(layer="bronze", domain="transit")(purge_bronze),
        on_failure_callback=record_transit_problem,
    )
    archive_gate >> purge_r2 >> purge_tables
