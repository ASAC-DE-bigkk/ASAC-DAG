"""transit bronze loader (#369) — pending 마커 소비 → R2 원본 재파싱 → Iceberg 청크 적재.

수집·적재 분리의 적재 쪽: collector(고빈도)가 남긴 pending 마커
(state/transit/loader_pending/<dataset>/<ingest_ts>__<run>.json)를 시간순으로 처리한다.

멱등성: 마커 단위로 `DELETE WHERE dag_run_id=<collector run>` 후 재적재 —
loader 가 중간에 죽어도 마커가 남아 다음 런이 통째로 재처리(중복 없음).
마커 하나의 실패는 다른 마커를 막지 않는다(개별 격리, 실패 마커는 남겨 재시도;
태스크는 마지막에 실패로 마감해 #77 콜백·Discord 경보를 태운다).

순수 로직(파싱·청크·DDL)은 seoul_transit.loader — 이 파일은 오케스트레이션만.
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# 동봉 패키지 import (Airflow 3.x 는 dags 하위폴더를 sys.path 에 자동 추가 안 함)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback
from common.runmetrics import track

from seoul_transit import config, loader
from seoul_transit.r2_landing import delete_key, get_bytes, get_json, list_keys

LOGGER = logging.getLogger(__name__)

# dev 게이트(#369 리뷰) — transit_master_bronze 와 동일 규약(loader.trino_catalog)
CATALOG = loader.trino_catalog()
SCHEMA = loader.transit_schema()
DOMAIN = config.TRANSIT_DOMAIN

# 공통 에러 모듈(#77)
record_transit_problem = problem_failure_callback(domain=DOMAIN, source_system="bronze_loader")


def _trino_cursor():
    import trino.dbapi

    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=CATALOG, schema=SCHEMA,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return conn.cursor()


def _load_marker(cursor, marker_key: str, marker: dict, ensured: set) -> int:
    """마커 1개 적재 — manifest·페이지 재다운로드 → 파싱 → 멱등 DELETE → 청크 INSERT."""
    # table/shape 는 마커의 스냅샷을 믿지 않고 dataset 에서 재파생(#369 리뷰) —
    # TABLE_SPECS 변경 후 남아 있던 구마커가 옛 테이블로 적재되는 것 방지.
    table, shape = loader.TABLE_SPECS[marker["dataset"]]
    marker = {**marker, "table": table, "shape": shape}

    manifest = get_json(marker["manifest_key"])
    pages = [get_bytes(k) for k in manifest["object_keys"]]
    rows = loader.build_rows(marker, manifest, pages)

    # 파싱 0행 vs manifest 카운트 대조(#369 리뷰) — 파서 회귀가 "INSERT 없이 마커만
    # 삭제·성공"으로 무경보 공백이 되는 것을 차단. manifest 도 0행(심야 등)이면 정상.
    manifest_rows = int(manifest.get("rows") or 0)
    if not rows and manifest_rows > 0:
        raise RuntimeError(
            f"파싱 0행 but manifest rows={manifest_rows} [{marker['dataset']}] — "
            f"파서 회귀 의심, 마커 보존 ({marker['manifest_key']})"
        )

    cat = loader.sql_identifier(CATALOG)
    sch = loader.sql_identifier(SCHEMA)
    tbl = loader.sql_identifier(table)
    qualified = f"{cat}.{sch}.{tbl}"
    if qualified not in ensured:  # DDL 은 런당 테이블별 1회 — no-op 왕복 제거(#369 리뷰)
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {cat}.{sch}")
        cursor.execute(loader.create_table_ddl(qualified, shape))
        ensured.add(qualified)

    # 멱등 재적재 — 이 collector run 의 기존 행 제거(중간 실패 재시도 대비).
    cursor.execute(
        f"DELETE FROM {qualified} WHERE dag_run_id = {loader.sql_str(marker['run_id'])}"
    )
    if rows:
        ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        values = [
            loader.value_literal(
                r, ingested_at=ingested_at, dag_run_id=marker["run_id"],
                shape=marker["shape"],
            )
            for r in rows
        ]
        columns = loader.insert_columns(marker["shape"])
        for chunk in loader.chunk_values(values):
            cursor.execute(
                f"INSERT INTO {qualified} ({columns}) VALUES {', '.join(chunk)}"
            )
    delete_key(marker_key)
    LOGGER.info("loaded [%s] %s rows=%d ← %s",
                marker["dataset"], qualified, len(rows), marker["manifest_key"])
    return len(rows)


def load_pending() -> dict:
    """pending 마커 전량을 시간순 처리. 개별 실패는 격리하고 마지막에 실패로 마감."""
    marker_keys = list_keys(config.LOADER_PENDING_PREFIX)
    if not marker_keys:
        print("pending 없음 — skip")
        return {"markers": 0, "rows": 0}

    cursor = _trino_cursor()
    loaded_rows = 0
    done = 0
    ensured: set = set()
    failures: list[tuple[str, str]] = []
    for marker_key in marker_keys:  # list_keys 는 사전순 = ingest_ts 시간순
        try:
            marker = get_json(marker_key)
            loaded_rows += _load_marker(cursor, marker_key, marker, ensured)
            done += 1
        except Exception as exc:  # noqa: BLE001 — 마커 격리, 실패분은 다음 런 재시도
            failures.append((marker_key, f"{type(exc).__name__}: {exc}"))
            LOGGER.exception("마커 적재 실패(다음 런 재시도): %s", marker_key)

    print(f"loaded markers={done}/{len(marker_keys)} rows={loaded_rows} failures={len(failures)}")
    if failures:
        raise RuntimeError(
            f"pending 마커 {len(failures)}/{len(marker_keys)}건 적재 실패 "
            f"(마커 보존됨 — 다음 런 재시도): {failures[0][0]} · {failures[0][1]}"
        )
    return {"markers": done, "rows": loaded_rows}


with DAG(
    dag_id="transit_bronze_loader",
    description="pending 마커 기반 R2→Iceberg bronze 일괄 적재(#369). collector 와 분리된 저빈도 적재.",
    start_date=datetime(2026, 1, 1),
    schedule=config.schedule_for("transit_loader", "*/10 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["seoul", "transit", "bronze", "loader", "trino", "iceberg"],
) as dag:
    load = PythonOperator(
        task_id="load_pending",
        # 실행 메트릭(#188)
        python_callable=track(layer="bronze", domain="transit")(load_pending),
        on_failure_callback=record_transit_problem,
    )
