"""Airflow DAG: population Iceberg 테이블 주간 유지보수.

주 1회 두 단계로 R2 저장/메타데이터 누적을 억제한다. 5분 주기 수집·변환이
스냅샷·파일을 빠르게 쌓으므로(용량 대부분이 죽은 스냅샷/파일), 정기 정리가 필요하다.

  1. ``maintain``        Trino ``optimize`` + ``expire_snapshots`` + ``remove_orphan_files``
                         (작은 파일 병합, 옛 스냅샷/커밋 찌꺼기 정리 — 테이블 내부)
  2. ``storage_cleanup`` boto3로 R2가 못 잡는 잔재 정리 —
                         (a) 살아있는 테이블의 옛 metadata.json (delete-after-commit이
                             R2 관리형 카탈로그에선 무효라 무한 증식),
                         (b) drop/full-refresh로 버려진 테이블 디렉터리(카탈로그에서
                             사라져 remove_orphan_files가 못 봄).

수집(population_bronze)·변환(population_transform)과 **독립** — 현재 데이터는
건드리지 않고 죽은 파일/옛 버전/버려진 디렉터리만 정리한다.

파라미터 (트리거 시 덮어쓰기):
  target           "dev" | "prod"  (기본 dev)
  retention        스냅샷/고아 보존 기간 (기본 3d; 카탈로그 min-retention 이상이어야 함)
  cleanup_hours    metadata/버려진 디렉터리 보존 시간 (기본 6h; 진행 중 커밋 보호)
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ppltn_ingest.source.maintenance import run_maintenance, run_storage_cleanup  # noqa: E402

KST = "Asia/Seoul"

DEFAULT_PARAMS = {"target": "dev", "retention": "3d", "cleanup_hours": 6}


def _maintain(**context) -> None:
    params = context["params"]
    results = run_maintenance(params["target"], retention=params.get("retention", "3d"))
    for tbl, status in results.items():
        print(f"[population maintenance] {tbl}: {status}")
    failed = [t for t, s in results.items() if not s == "ok"]
    if failed:
        # 일부 테이블 유지보수 실패는 드러내되(재시도), 데이터엔 영향 없음.
        raise RuntimeError(f"maintenance failed for: {', '.join(failed)}")


def _storage_cleanup(**context) -> None:
    params = context["params"]
    tally = run_storage_cleanup(params["target"], retention_hours=int(params.get("cleanup_hours", 6)))
    print(
        "[population storage_cleanup] "
        f"버려진 디렉터리 {tally['orphan_dir_objects']}개/{tally['orphan_dir_bytes'] / 1024 / 1024:.0f}MB, "
        f"옛 metadata {tally['old_metadata_objects']}개/{tally['old_metadata_bytes'] / 1024 / 1024:.0f}MB 정리"
    )


with DAG(
    dag_id="population_maintenance",
    description="Weekly Iceberg maintenance (Trino optimize/expire/orphan + boto3 metadata/dropped-dir cleanup).",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="0 4 * * 0",  # 매주 일요일 04:00 KST (오프피크)
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    params=DEFAULT_PARAMS,
    tags=["maintenance", "population", "iceberg", "r2"],
) as dag:
    maintain = PythonOperator(task_id="maintain", python_callable=_maintain)
    storage_cleanup = PythonOperator(task_id="storage_cleanup", python_callable=_storage_cleanup)

    maintain >> storage_cleanup
