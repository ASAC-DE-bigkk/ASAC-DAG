"""transit 보존 정책 집행 DAG (#369 4단계) — 실시간 데이터 **주 단위(월~일 KST)** 보존.

이번 주(월요일 00:00 KST 이후) 데이터만 유지 — @daily 로 돌지만 실제 삭제는 주가
바뀐 뒤(월요일 런)에 지난주 분이 한꺼번에 나간다 (그 외 요일은 위생 점검 성격).
  1. purge_r2_raw    : 실시간 dataset raw(ingest_ts < 주 경계) + 만료 pending 마커
  2. purge_bronze    : bronze DELETE(주 경계) → optimize → expire_snapshots/remove_orphan(7d)

전제(충족 확인됨): dbt silver·gold 는 incremental — bronze 를 잘라도 이력이 안 잘린다.
⚠️ 삭제된 실시간 원본은 복구 불가 — 주 내 미적재분(방치된 pending)은 영구 소실이므로
   만료 pending 마커 발견 시 Discord WARN 으로 가시화한다.
⚠️ 월요일 00시 직후에는 직전 주 원본이 사라지므로 R2 재적재식 복구 윈도우도 함께 리셋된다.

silver·gold 유지보수 체인 (#748, purge 체인과 **독립** — archive 게이트에 안 걸림):
  pause_transform → maintain_silver_gold → storage_cleanup → resume_transform(all_done)
  15분 merge 가 쌓는 소파일을 매일 optimize 로 병합하고 스냅샷·metadata 잔재를 회수한다.
  optimize ↔ merge 커밋 충돌 방지는 citydata 관례(Variable 플래그 + transform 쪽
  check_transform_gate skip + drain 대기). 압축은 언제 돌아도 안전하므로 아카이브
  게이트(#443) 뒤에 두지 않는다 — 게이트는 '삭제' 보호 장치다.

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
    # 구경로(#547 이사 전)도 함께 스윕 — 이사 후 고립 마커의 무알림 잔존 방지.
    stale = [
        k
        for prefix in (*config.LOADER_PENDING_LEGACY_PREFIXES, config.LOADER_PENDING_PREFIX)
        for k in list_keys(prefix)
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


def _trino_cursor():
    """maintenance 태스크 공용 Trino 커서 (#748 에서 purge_bronze 와 공유하도록 추출)."""
    import trino.dbapi

    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=CATALOG, schema=SCHEMA,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return conn.cursor()


def purge_bronze() -> dict:
    """bronze 실시간 테이블 보존 집행 — 주 경계 DELETE + optimize + 스냅샷 회수(7d)."""
    cur = _trino_cursor()
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


# ── silver·gold 유지보수 체인 (#748) ─────────────────────────────────────────────


def pause_transform(**context) -> None:
    """maintenance 플래그 ON + 진행 중 transform run 배수 대기 (citydata 관례).

    플래그가 서면 transit_transform·transform_heavy 의 check_transform_gate 가 새 run 을
    skip 한다. 이미 running 인 run 은 이어지므로 drain_seconds 로 in-flight 를 배수한다
    — 기본 420s 는 dbt build 실측 상한(#443 344초) + 여유.
    """
    import time

    from airflow.models import Variable

    drain = int(context["params"].get("drain_seconds", 420))
    Variable.set(maintenance.MAINT_FLAG, "1")
    print(f"[maintenance] {maintenance.MAINT_FLAG}=1 — transform 차단, 배수 대기 {drain}s")
    time.sleep(drain)
    print("[maintenance] 배수 완료 — silver·gold 유지보수 진행")


def resume_transform(**_) -> None:
    from airflow.models import Variable

    Variable.set(maintenance.MAINT_FLAG, "0")
    print(f"[maintenance] {maintenance.MAINT_FLAG}=0 — transform 재개")


def maintain_silver_gold(**context) -> dict:
    """silver·gold 전 테이블 optimize + expire_snapshots/remove_orphan (DELETE 없음).

    대상은 information_schema 런타임 발견 — bronze_*(보존 체인 담당)·dbt 임시 테이블만
    제외하고 자동 편입(culture 관례). 테이블별 격리: 하나가 실패해도 나머지는 계속,
    끝에 실패 목록으로 태스크를 실패시켜 재시도·경보를 남긴다(citydata 관례).
    """
    retention = str(context["params"].get("retention", maintenance.SNAPSHOT_RETENTION))
    cur = _trino_cursor()
    cat, sch = maintenance.sql_identifier(CATALOG), maintenance.sql_identifier(SCHEMA)
    cur.execute(
        f"SELECT table_name FROM {cat}.information_schema.tables "
        f"WHERE table_schema = '{SCHEMA}' AND table_type = 'BASE TABLE'"
    )
    targets = sorted(t for (t,) in cur.fetchall() if maintenance.is_maintain_target(t))

    results: dict[str, str] = {}
    for table in targets:
        qualified = f"{cat}.{sch}.{maintenance.sql_identifier(table)}"
        try:
            for stmt in maintenance.maintain_sql(qualified, retention):
                cur.execute(stmt)
                cur.fetchall()  # 일부 procedure 는 통계 행 반환 — 소비해야 다음 execute 가능
            results[table] = "ok"
        except Exception as exc:  # noqa: BLE001 — 테이블별 격리, 배치는 계속
            results[table] = f"error: {type(exc).__name__}: {exc}"
    for table, status in sorted(results.items()):
        print(f"[maintain] {table}: {status}")
    failed = [t for t, s in results.items() if s != "ok"]
    if failed:
        raise RuntimeError(f"maintain 실패 테이블: {', '.join(failed)}")
    return results


def storage_cleanup(**context) -> dict:
    """R2 Data Catalog 가 못 잡는 잔재를 boto3 로 정리 (citydata run_storage_cleanup 이식).

    (1) 살아있는 테이블의 옛 metadata.json — write.metadata.delete-after-commit 이
        R2 관리형 카탈로그에선 무효라 커밋마다 무한 증식(citydata 실측 71%),
    (2) drop/full-refresh 로 버려진 테이블 디렉터리 — 카탈로그에서 사라져
        remove_orphan_files(테이블 내부만 봄)가 못 보는 영역.

    안전 규칙: $metadata_log_entries 의 현재 참조 metadata·살아있는 디렉터리의
    비-metadata 파일·cleanup_hours 이내 최근 파일은 보존. 정리 범위는 transit 스키마의
    __r2_data_catalog/<uuid> 프리픽스로만 제한(타 도메인 불가침).
    """
    from datetime import datetime, timedelta, timezone

    from seoul_transit.r2_landing import _client_and_bucket

    hours = int(context["params"].get("cleanup_hours", 6))
    cur = _trino_cursor()
    cat, sch = maintenance.sql_identifier(CATALOG), maintenance.sql_identifier(SCHEMA)

    # 살아있는 상태 수집 — bronze 포함 스키마 전체(살아있는 디렉터리를 orphan 으로
    # 오판하지 않도록 대상 필터 없이 전수).
    keep_meta: set[str] = set()
    live_dirs: set[str] = set()
    prefixes: set[str] = set()
    cur.execute(f"SHOW TABLES FROM {cat}.{sch}")
    for (tbl,) in cur.fetchall():
        ident = f'{cat}.{sch}."{maintenance.sql_identifier(tbl)}$metadata_log_entries"'
        try:
            cur.execute(f"SELECT file FROM {ident}")
            rows = cur.fetchall()
        except Exception:  # noqa: BLE001 — metadata 테이블 없는 객체(뷰 등)는 무시
            continue
        for (path,) in rows:
            m = maintenance.WAREHOUSE_META_RE.match(path or "")
            if not m:
                continue
            prefixes.add(m.group("prefix"))
            live_dirs.add(m.group("dir"))
            keep_meta.add(m.group("name"))
    if not prefixes:
        print("[storage_cleanup] 살아있는 테이블 없음 — 아무것도 지우지 않음")
        return {"orphan_dir_objects": 0, "old_metadata_objects": 0}

    client, bucket = _client_and_bucket()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    tally = {"orphan_dir_objects": 0, "orphan_dir_bytes": 0,
             "old_metadata_objects": 0, "old_metadata_bytes": 0}
    batch: list[str] = []

    def flush() -> None:
        if batch:
            client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})
            batch.clear()

    paginator = client.get_paginator("list_objects_v2")
    for prefix in sorted(prefixes):
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                parts = key.split("/")  # __r2_data_catalog / <uuid> / <table-dir> / …
                if len(parts) < 3 or obj["LastModified"] > cutoff:
                    continue
                table_dir = parts[2]
                if table_dir not in live_dirs:
                    batch.append(key)
                    tally["orphan_dir_objects"] += 1
                    tally["orphan_dir_bytes"] += obj["Size"]
                elif key.endswith(".metadata.json") and parts[-1] not in keep_meta:
                    batch.append(key)
                    tally["old_metadata_objects"] += 1
                    tally["old_metadata_bytes"] += obj["Size"]
                if len(batch) >= 1000:
                    flush()
    flush()
    print(f"[storage_cleanup] 버려진 디렉터리 {tally['orphan_dir_objects']}개"
          f"/{tally['orphan_dir_bytes'] / 1e6:.0f}MB, "
          f"옛 metadata {tally['old_metadata_objects']}개"
          f"/{tally['old_metadata_bytes'] / 1e6:.0f}MB 정리")
    return tally


with DAG(
    dag_id="transit_maintenance",
    description="transit 실시간 데이터 주 단위(월~일 KST) 보존 집행 — 다음 주 시작 시 "
                "지난주 R2 raw + Iceberg bronze 삭제(DELETE·optimize·expire_snapshots). 마스터 제외.",
    start_date=datetime(2026, 1, 1),
    schedule=config.schedule_for("transit_maintenance", "@daily"),
    catchup=False,
    max_active_runs=1,
    # silver·gold 체인(#748) 파라미터 — 트리거 시 덮어쓰기 가능.
    params={"retention": maintenance.SNAPSHOT_RETENTION, "drain_seconds": 420, "cleanup_hours": 6},
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

    # silver·gold 유지보수 체인(#748) — purge 체인과 독립(아카이브 게이트 비적용:
    # 게이트는 '삭제' 보호 장치고 압축·회수는 언제 돌아도 안전하다).
    pause_task = PythonOperator(
        task_id="pause_transform",
        python_callable=pause_transform,
        on_failure_callback=record_transit_problem,
    )
    maintain_task = PythonOperator(
        task_id="maintain_silver_gold",
        python_callable=track(layer="silver", domain="transit")(maintain_silver_gold),
        on_failure_callback=record_transit_problem,
    )
    cleanup_task = PythonOperator(
        task_id="storage_cleanup",
        python_callable=track(layer="silver", domain="transit")(storage_cleanup),
        on_failure_callback=record_transit_problem,
    )
    # maintain/cleanup 이 실패해도 transform 은 반드시 재개(pause 채 방치 방지).
    resume_task = PythonOperator(
        task_id="resume_transform",
        python_callable=resume_transform,
        trigger_rule="all_done",
        on_failure_callback=record_transit_problem,
    )
    pause_task >> maintain_task >> cleanup_task >> resume_task
